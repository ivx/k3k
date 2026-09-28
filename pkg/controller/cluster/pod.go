package cluster

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/k3k-kubelet/translate"
)

const (
	podController = "k3k-pod-controller"
)

type PodReconciler struct {
	Client ctrlruntimeclient.Client
	Scheme *runtime.Scheme

	// VirtualClientFor returns a client for the virtual cluster (tests
	// replace it). Defaults to newVirtualClient.
	VirtualClientFor func(ctx context.Context, hostClient ctrlruntimeclient.Client, clusterName, clusterNamespace string) (ctrlruntimeclient.Client, error)

	// terminating remembers, per host Pod, the virtual Pod that is deleted
	// together with it, so that the virtual Pod can be removed at once when
	// the host Pod is gone (the deleted host Pod no longer carries the
	// translation annotations).
	terminating sync.Map
}

// terminatingPod is the virtual counterpart of a terminating host Pod.
type terminatingPod struct {
	cluster types.NamespacedName
	virtual types.NamespacedName
	uid     types.UID
}

// AddPodController adds a new controller for Pods to the manager.
// It will reconcile the Pods of the Host Cluster with the one of the Virtual Cluster.
func AddPodController(ctx context.Context, mgr manager.Manager, maxConcurrentReconciles int) error {
	reconciler := PodReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Named(podController).
		WithEventFilter(newClusterPredicate()).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles}).
		Complete(&reconciler)
}

func (r *PodReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(1).Info("Reconciling Pod")

	var pod corev1.Pod
	if err := r.Client.Get(ctx, req.NamespacedName, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, r.removeVirtualPod(ctx, req.NamespacedName)
		}

		return reconcile.Result{}, err
	}

	if pod.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	// get cluster from the object
	cluster := clusterNamespacedName(&pod)

	virtualClient, err := r.virtualClient(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, err
	}

	virtName := pod.GetAnnotations()[translate.ResourceNameAnnotation]
	virtNamespace := pod.GetAnnotations()[translate.ResourceNamespaceAnnotation]
	virtKey := types.NamespacedName{Name: virtName, Namespace: virtNamespace}

	// Remember the virtual Pod incarnation of this host Pod: the UID that the
	// kubelet stamped at creation, or the current one of the virtual Pod.
	uid := types.UID(pod.GetAnnotations()[translate.VirtualUIDAnnotation])
	if uid == "" {
		var virtPod corev1.Pod
		if err := virtualClient.Get(ctx, virtKey, &virtPod); err == nil {
			uid = virtPod.UID
		} else if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
	}

	if uid != "" {
		r.terminating.Store(req.NamespacedName, terminatingPod{cluster: cluster, virtual: virtKey, uid: uid})
	}

	virtPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      virtName,
			Namespace: virtNamespace,
		},
	}

	log.V(1).Info("Deleting Virtual Pod", "name", virtName, "namespace", virtNamespace)

	return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(virtualClient.Delete(ctx, &virtPod))
}

// removeVirtualPod runs when a host Pod is gone. Its containers are stopped,
// but the virtual Pod may still wait for the rest of its grace period: the
// pod controller of the virtual kubelet removes it only after
// deletionGracePeriodSeconds. With a long grace period (for example 900 s)
// a StatefulSet cannot create the Pod again for that time, and a node drain
// stalls. The virtual Pod of that incarnation is removed at once; a newer
// Pod with the same name (UID precondition) is not touched.
func (r *PodReconciler) removeVirtualPod(ctx context.Context, hostKey types.NamespacedName) error {
	value, ok := r.terminating.LoadAndDelete(hostKey)
	if !ok {
		return nil
	}

	ref := value.(terminatingPod)

	virtualClient, err := r.virtualClient(ctx, ref.cluster)
	if err != nil {
		r.terminating.Store(hostKey, ref)

		return err
	}

	var virtPod corev1.Pod
	if err := virtualClient.Get(ctx, ref.virtual, &virtPod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		// keep the entry for the requeue: the host Pod stays NotFound, so this
		// is the only record of the virtual Pod (a drain can make the virtual
		// API server briefly unreachable exactly now)
		r.terminating.Store(hostKey, ref)

		return err
	}

	// only a Pod that is already being deleted, and only the same incarnation
	if virtPod.UID != ref.uid || virtPod.DeletionTimestamp.IsZero() {
		return nil
	}

	ctrl.LoggerFrom(ctx).Info("Host Pod is gone, removing the terminating virtual Pod", "name", ref.virtual.Name, "namespace", ref.virtual.Namespace)

	uid := ref.uid

	err = virtualClient.Delete(ctx, &virtPod,
		ctrlruntimeclient.GracePeriodSeconds(0),
		ctrlruntimeclient.Preconditions{UID: &uid})

	// NotFound: already gone; Conflict: the UID precondition failed, a newer
	// Pod has the name now
	if err == nil || apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}

	r.terminating.Store(hostKey, ref)

	return err
}

func (r *PodReconciler) virtualClient(ctx context.Context, cluster types.NamespacedName) (ctrlruntimeclient.Client, error) {
	newClient := r.VirtualClientFor
	if newClient == nil {
		newClient = newVirtualClient
	}

	return newClient(ctx, r.Client, cluster.Name, cluster.Namespace)
}
