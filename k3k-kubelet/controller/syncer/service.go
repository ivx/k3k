package syncer

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const (
	serviceControllerName = "service-syncer-controller"
	serviceFinalizerName  = "service.k3k.io/finalizer"
)

type ServiceReconciler struct {
	*SyncerContext

	// conflicts records, per virtual Service UID, when the host first
	// rejected its ClusterIP or NodePorts as already allocated.
	conflicts sync.Map
	// now is the clock (tests replace it).
	now func() time.Time
}

// allocationConflictGrace is how long an "already allocated" rejection must
// last before the virtual Service gets a new value. The host releases the IP
// and ports of a deleted Service asynchronously: right after the host copy
// was deleted (by hand, or by the drift repair), the new copy is rejected
// with its own old values for a moment. That is not a collision.
const allocationConflictGrace = 30 * time.Second

// AddServiceSyncer adds service syncer controller to the manager of the virtual cluster
func AddServiceSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string, recorder record.EventRecorder) error {
	translator := translate.ToHostTranslator{
		ClusterName:      clusterName,
		ClusterNamespace: clusterNamespace,
	}

	reconciler := ServiceReconciler{
		SyncerContext: &SyncerContext{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			Translator:       translator,
			Recorder:         recorder,
		},
	}

	name := reconciler.Translator.TranslateName(clusterNamespace, serviceControllerName)

	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(&corev1.Service{}, builder.WithPredicates(predicate.NewPredicateFuncs(reconciler.filterResources))).
		// Host-side changes (deleted copy, changed ClusterIP, LoadBalancer
		// status) map back to the virtual Service.
		WatchesRawSource(source.Kind(hostMgr.GetCache(), ctrlruntimeclient.Object(&corev1.Service{}),
			handler.EnqueueRequestsFromMapFunc(reconciler.mapHostToVirtual))).
		Complete(&reconciler)
}

func (r *ServiceReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace)
	ctx = ctrl.LoggerInto(ctx, log)

	if req.Name == "kubernetes" || req.Name == "kube-dns" {
		return reconcile.Result{}, nil
	}

	var (
		virtService corev1.Service
		cluster     v1beta1.Cluster
	)

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.VirtualClient.Get(ctx, req.NamespacedName, &virtService); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	syncedService := r.service(&virtService)

	if err := controllerutil.SetOwnerReference(&cluster, syncedService, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	// handle deletion
	if !virtService.DeletionTimestamp.IsZero() {
		// deleting the synced service if exists
		if err := r.HostClient.Delete(ctx, syncedService); err != nil {
			return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
		}

		// remove the finalizer after cleaning up the synced service
		if controllerutil.RemoveFinalizer(&virtService, serviceFinalizerName) {
			if err := r.VirtualClient.Update(ctx, &virtService); err != nil {
				return reconcile.Result{}, err
			}
		}

		return reconcile.Result{}, nil
	}

	// host events reach the reconciler without the virtual-side filter
	if !r.filterResources(&virtService) {
		return reconcile.Result{}, nil
	}

	// Add finalizer if it does not exist
	if controllerutil.AddFinalizer(&virtService, serviceFinalizerName) {
		if err := r.VirtualClient.Update(ctx, &virtService); err != nil {
			return reconcile.Result{}, err
		}
	}

	if err := stampContentHash(syncedService); err != nil {
		return reconcile.Result{}, err
	}

	// create or update the service on host
	var hostService corev1.Service
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: syncedService.Name, Namespace: r.ClusterNamespace}, &hostService); err != nil {
		if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}

		log.Info("creating the service on the host cluster")

		if err := r.HostClient.Create(ctx, syncedService); err != nil {
			return r.hostRejected(ctx, &virtService, "create", err)
		}

		r.conflicts.Delete(virtService.UID)

		return reconcile.Result{}, nil
	}

	// In shared mode the host copy must carry the virtual ClusterIP: the
	// virtual DNS answers with it, and the host only forwards to IPs it
	// knows. The ClusterIP is immutable, so a different one (for example
	// after a reset of the virtual datastore) needs a new host copy.
	if hostService.Spec.ClusterIP != virtService.Spec.ClusterIP {
		log.Info("host copy has a different ClusterIP, recreating it", "host", hostService.Spec.ClusterIP, "virtual", virtService.Spec.ClusterIP)

		if r.Recorder != nil {
			r.Recorder.Eventf(&virtService, corev1.EventTypeWarning, ReasonHostCopyRecreated,
				"host copy had ClusterIP %s instead of %s, recreating it", hostService.Spec.ClusterIP, virtService.Spec.ClusterIP)
		}

		uid := hostService.UID
		if err := r.HostClient.Delete(ctx, &hostService, ctrlruntimeclient.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}

		// the delete event of the host copy requeues the virtual Service
		return reconcile.Result{}, nil
	}

	if contentChanged(syncedService, &hostService) {
		log.Info("updating service on the host cluster")

		// The host apiserver owns IP-family allocation: the host service may have been
		// expanded to dual-stack while the virtual service is single-stack. Re-submitting
		// the virtual family fields is rejected ("must be 'SingleStack' to release the
		// secondary cluster IP"), so preserve the host-allocated values on update.
		syncedService.Spec.ClusterIP = hostService.Spec.ClusterIP
		syncedService.Spec.ClusterIPs = hostService.Spec.ClusterIPs
		syncedService.Spec.IPFamilies = hostService.Spec.IPFamilies
		syncedService.Spec.IPFamilyPolicy = hostService.Spec.IPFamilyPolicy
		syncedService.Spec.HealthCheckNodePort = hostService.Spec.HealthCheckNodePort

		if err := r.HostClient.Update(ctx, syncedService); err != nil {
			return r.hostRejected(ctx, &virtService, "update", err)
		}

		r.conflicts.Delete(virtService.UID)
	}

	return reconcile.Result{}, r.syncStatus(ctx, &virtService, &hostService)
}

// hostRejected handles a failed write of the host copy. The virtual and the
// host API server allocate ClusterIPs and NodePorts from the same ranges
// without knowing each other, so the host can reject a value as "already
// allocated". Retrying the same value never succeeds: the virtual side must
// allocate a new one. Every other error is reported on the virtual Service
// and retried.
func (r *ServiceReconciler) hostRejected(ctx context.Context, virtService *corev1.Service, action string, err error) (reconcile.Result, error) {
	ipConflict, nodePorts := allocationConflicts(err)

	if ipConflict || len(nodePorts) > 0 {
		now := r.clock()

		first, _ := r.conflicts.LoadOrStore(virtService.UID, now)
		if since := now.Sub(first.(time.Time)); since < allocationConflictGrace {
			ctrl.LoggerFrom(ctx).Info("host rejected the service as already allocated, waiting before reallocation",
				"service", virtService.Name, "namespace", virtService.Namespace, "since", since.String(), "error", err.Error())

			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}

		r.conflicts.Delete(virtService.UID)
	}

	switch {
	case ipConflict:
		return reconcile.Result{}, r.reallocateClusterIP(ctx, virtService)
	case len(nodePorts) > 0:
		return reconcile.Result{}, r.reallocateNodePorts(ctx, virtService, nodePorts)
	default:
		return reconcile.Result{}, r.syncFailed(virtService, action, err)
	}
}

func (r *ServiceReconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}

	return time.Now()
}

// nodePortField matches the field path of a rejected NodePort.
var nodePortField = regexp.MustCompile(`^spec\.ports\[(\d+)\]\.nodePort$`)

// allocationConflicts reads an "Invalid" error of the host API server and
// reports whether the ClusterIP and which ports (by index) are already
// allocated on the host.
func allocationConflicts(err error) (bool, []int) {
	if !apierrors.IsInvalid(err) {
		return false, nil
	}

	var status apierrors.APIStatus
	if !errors.As(err, &status) || status.Status().Details == nil {
		return false, nil
	}

	var (
		ipConflict bool
		nodePorts  []int
	)

	for _, cause := range status.Status().Details.Causes {
		if !strings.Contains(cause.Message, "already allocated") {
			continue
		}

		if strings.HasPrefix(cause.Field, "spec.clusterIP") {
			ipConflict = true
			continue
		}

		if m := nodePortField.FindStringSubmatch(cause.Field); m != nil {
			if i, convErr := strconv.Atoi(m[1]); convErr == nil {
				nodePorts = append(nodePorts, i)
			}
		}
	}

	return ipConflict, nodePorts
}

// reallocateClusterIP creates the virtual Service again without its
// ClusterIP, so that the virtual API server assigns a new one. The ClusterIP
// is immutable; there is no other way to change it.
func (r *ServiceReconciler) reallocateClusterIP(ctx context.Context, virtService *corev1.Service) error {
	log := ctrl.LoggerFrom(ctx)
	oldIP := virtService.Spec.ClusterIP

	log.Info("host already uses the ClusterIP, recreating the virtual service for a new IP", "service", virtService.Name, "namespace", virtService.Namespace, "clusterIP", oldIP)

	fresh := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            virtService.Name,
			Namespace:       virtService.Namespace,
			Labels:          virtService.Labels,
			Annotations:     virtService.Annotations,
			OwnerReferences: virtService.OwnerReferences,
		},
		Spec: *virtService.Spec.DeepCopy(),
	}
	fresh.Spec.ClusterIP = ""
	fresh.Spec.ClusterIPs = nil

	// without the finalizer the delete completes at once
	if controllerutil.RemoveFinalizer(virtService, serviceFinalizerName) {
		if err := r.VirtualClient.Update(ctx, virtService); err != nil {
			return err
		}
	}

	uid := virtService.UID
	if err := r.VirtualClient.Delete(ctx, virtService, ctrlruntimeclient.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err := r.VirtualClient.Create(ctx, fresh); err != nil {
		// the owner of the Service (an operator) was faster
		if apierrors.IsAlreadyExists(err) {
			return nil
		}

		return err
	}

	if r.Recorder != nil {
		r.Recorder.Eventf(fresh, corev1.EventTypeWarning, "ClusterIPConflict",
			"the host already used ClusterIP %s, the service was created again with ClusterIP %s", oldIP, fresh.Spec.ClusterIP)
	}

	return nil
}

// reallocateNodePorts clears the rejected NodePorts on the virtual Service,
// so that the virtual API server assigns new ones.
func (r *ServiceReconciler) reallocateNodePorts(ctx context.Context, virtService *corev1.Service, indexes []int) error {
	var cleared []string

	for _, i := range indexes {
		if i < len(virtService.Spec.Ports) && virtService.Spec.Ports[i].NodePort != 0 {
			cleared = append(cleared, strconv.Itoa(int(virtService.Spec.Ports[i].NodePort)))
			virtService.Spec.Ports[i].NodePort = 0
		}
	}

	if len(cleared) == 0 {
		return nil
	}

	ctrl.LoggerFrom(ctx).Info("host already uses the NodePorts, reallocating them in the virtual cluster", "service", virtService.Name, "namespace", virtService.Namespace, "nodePorts", cleared)

	if err := r.VirtualClient.Update(ctx, virtService); err != nil {
		return err
	}

	if r.Recorder != nil {
		r.Recorder.Eventf(virtService, corev1.EventTypeWarning, "NodePortConflict",
			"the host already used NodePort %s, the virtual cluster assigned a new one", strings.Join(cleared, ", "))
	}

	return nil
}

// syncStatus copies the host service's LoadBalancer status back to the virtual
// service so in-cluster consumers (e.g. external-dns) see the assigned ingress.
// Host status changes requeue the virtual service through the host watch.
func (r *ServiceReconciler) syncStatus(ctx context.Context, virtService, hostService *corev1.Service) error {
	if virtService.Spec.Type != corev1.ServiceTypeLoadBalancer {
		return nil
	}

	if equality.Semantic.DeepEqual(virtService.Status.LoadBalancer, hostService.Status.LoadBalancer) {
		return nil
	}

	orig := virtService.DeepCopy()
	virtService.Status.LoadBalancer = hostService.Status.LoadBalancer

	return r.VirtualClient.Status().Patch(ctx, virtService, ctrlruntimeclient.MergeFrom(orig))
}

func (r *ServiceReconciler) filterResources(object ctrlruntimeclient.Object) bool {
	var cluster v1beta1.Cluster

	ctx := context.Background()

	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}

	// check for serviceSyncConfig
	syncConfig := cluster.Spec.Sync.Services

	// If syncing is disabled, only process deletions to allow for cleanup.
	if !syncConfig.Enabled {
		return object.GetDeletionTimestamp() != nil
	}

	labelSelector := labels.SelectorFromSet(syncConfig.Selector)
	if labelSelector.Empty() {
		return true
	}

	return labelSelector.Matches(labels.Set(object.GetLabels()))
}

func (s *ServiceReconciler) service(obj *corev1.Service) *corev1.Service {
	hostService := obj.DeepCopy()
	s.Translator.TranslateTo(hostService)
	// don't sync finalizers to the host
	return hostService
}
