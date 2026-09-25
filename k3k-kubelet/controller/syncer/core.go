package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

// Shared building blocks of the syncers: the host-side watch that maps a
// host copy back to its virtual object, the feedback on the virtual object
// when the host rejects a write, the content hash that keeps host writes
// idempotent, and the sweep that removes host copies whose virtual object
// is gone (for example after a reset of the virtual cluster datastore).

const (
	// ReasonSyncFailed is the event reason on a virtual object whose host
	// copy could not be created or updated.
	ReasonSyncFailed = "SyncFailed"
	// ReasonHostCopyRecreated is the event reason on a virtual object whose
	// host copy was deleted and created again to repair a difference.
	ReasonHostCopyRecreated = "HostCopyRecreated"

	orphanSweepInterval = 10 * time.Minute
)

// mapHostToVirtual enqueues the virtual object a synced host object belongs
// to, using the annotations the translator stamps on the way down.
func (s *SyncerContext) mapHostToVirtual(_ context.Context, obj ctrlruntimeclient.Object) []reconcile.Request {
	if obj.GetLabels()[translate.ClusterNameLabel] != s.ClusterName {
		return nil
	}

	annotations := obj.GetAnnotations()

	name := annotations[translate.ResourceNameAnnotation]
	if name == "" {
		return nil
	}

	return []reconcile.Request{{NamespacedName: types.NamespacedName{
		Name:      name,
		Namespace: annotations[translate.ResourceNamespaceAnnotation],
	}}}
}

// syncFailed reports a rejected host write on the virtual object and returns
// the error, so that the reconciler still retries with backoff.
func (s *SyncerContext) syncFailed(virtObj runtime.Object, action string, err error) error {
	if s.Recorder != nil && err != nil {
		s.Recorder.Eventf(virtObj, corev1.EventTypeWarning, ReasonSyncFailed, "%s of the host copy failed: %v", action, err)
	}

	return err
}

// isSyncedCopy reports whether a host object is a copy that a syncer of this
// virtual cluster created: cluster label, translation annotation and an owner
// reference to the k3k Cluster. Objects of the k3k controller (server
// Services, server PVCs) and objects of host operators never match.
func (s *SyncerContext) isSyncedCopy(obj metav1.Object) bool {
	if obj.GetLabels()[translate.ClusterNameLabel] != s.ClusterName {
		return false
	}

	if obj.GetAnnotations()[translate.ResourceNameAnnotation] == "" {
		return false
	}

	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "Cluster" && ref.Name == s.ClusterName && ref.APIVersion == v1beta1.SchemeGroupVersion.String() {
			return true
		}
	}

	return false
}

// ContentHashAnnotation records, on a host copy of a built-in kind, a hash of
// what the virtual side asked for. The syncers write only when it changes, so
// that the host-side watch cannot start a write loop with host controllers
// that add their own labels, annotations or defaults.
const ContentHashAnnotation = SpecHashAnnotation

// stampContentHash sets ContentHashAnnotation on obj from its labels, its
// other annotations and every top-level field except metadata and status.
func stampContentHash(obj ctrlruntimeclient.Object) error {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	delete(annotations, ContentHashAnnotation)
	obj.SetAnnotations(annotations)

	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}

	delete(content, "metadata")
	delete(content, "status")
	delete(content, "apiVersion")
	delete(content, "kind")

	content["labels"] = obj.GetLabels()
	content["annotations"] = annotations

	// json.Marshal sorts map keys: deterministic across runs
	raw, err := json.Marshal(content)
	if err != nil {
		return err
	}

	sum := sha256.Sum256(raw)
	annotations[ContentHashAnnotation] = hex.EncodeToString(sum[:])
	obj.SetAnnotations(annotations)

	return nil
}

// contentChanged reports whether the host copy carries a different content
// hash than the desired object (stamped by stampContentHash).
func contentChanged(desired, host ctrlruntimeclient.Object) bool {
	return desired.GetAnnotations()[ContentHashAnnotation] != host.GetAnnotations()[ContentHashAnnotation]
}

// OrphanSweeper removes host copies whose virtual object does not exist. It
// runs on the leader, once at start and then periodically. The virtual side
// is read directly from the API server, not from a cache that may still be
// warming up.
type OrphanSweeper struct {
	*SyncerContext

	HostReader    ctrlruntimeclient.Reader
	VirtualReader ctrlruntimeclient.Reader
	Interval      time.Duration

	// Kinds lists the swept kinds: a constructor for the host list and one
	// for an empty virtual object of the same kind.
	Kinds []SweptKind
}

// SweptKind describes one kind the sweeper handles.
type SweptKind struct {
	Name    string
	NewList func() ctrlruntimeclient.ObjectList
	NewObj  func() ctrlruntimeclient.Object
	// ReportOnly logs orphans without deleting them (kinds that hold data).
	ReportOnly bool
}

// AddOrphanSweeper registers the sweeper for the built-in kinds with the
// virtual manager (leader election applies).
func AddOrphanSweeper(virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	sweeper := &OrphanSweeper{
		SyncerContext: &SyncerContext{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			Translator:       translate.ToHostTranslator{ClusterName: clusterName, ClusterNamespace: clusterNamespace},
		},
		HostReader:    hostMgr.GetAPIReader(),
		VirtualReader: virtMgr.GetAPIReader(),
		Interval:      orphanSweepInterval,
		Kinds:         BuiltinSweptKinds(),
	}

	return virtMgr.Add(sweeper)
}

// BuiltinSweptKinds are the kinds of the built-in syncers. PVCs hold data: an
// orphaned host PVC is only reported, never deleted.
func BuiltinSweptKinds() []SweptKind {
	return []SweptKind{
		{Name: "Service", NewList: func() ctrlruntimeclient.ObjectList { return &corev1.ServiceList{} }, NewObj: func() ctrlruntimeclient.Object { return &corev1.Service{} }},
		{Name: "ConfigMap", NewList: func() ctrlruntimeclient.ObjectList { return &corev1.ConfigMapList{} }, NewObj: func() ctrlruntimeclient.Object { return &corev1.ConfigMap{} }},
		{Name: "Secret", NewList: func() ctrlruntimeclient.ObjectList { return &corev1.SecretList{} }, NewObj: func() ctrlruntimeclient.Object { return &corev1.Secret{} }},
		{Name: "Ingress", NewList: func() ctrlruntimeclient.ObjectList { return &networkingv1.IngressList{} }, NewObj: func() ctrlruntimeclient.Object { return &networkingv1.Ingress{} }},
		{Name: "PersistentVolumeClaim", NewList: func() ctrlruntimeclient.ObjectList { return &corev1.PersistentVolumeClaimList{} }, NewObj: func() ctrlruntimeclient.Object { return &corev1.PersistentVolumeClaim{} }, ReportOnly: true},
	}
}

// Start implements manager.Runnable.
func (o *OrphanSweeper) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", o.ClusterName, "clusterNamespace", o.ClusterNamespace, "controller", "orphan-sweeper")
	ctx = ctrl.LoggerInto(ctx, log)

	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()

	for {
		if err := o.Sweep(ctx); err != nil {
			log.Error(err, "orphan sweep failed")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Sweep runs one pass over all kinds.
func (o *OrphanSweeper) Sweep(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx)

	for _, kind := range o.Kinds {
		list := kind.NewList()
		if err := o.HostReader.List(ctx, list,
			ctrlruntimeclient.InNamespace(o.ClusterNamespace),
			ctrlruntimeclient.MatchingLabels{translate.ClusterNameLabel: o.ClusterName}); err != nil {
			return err
		}

		items, err := metaItems(list)
		if err != nil {
			return err
		}

		for _, hostObj := range items {
			if !o.isSyncedCopy(hostObj) {
				continue
			}

			key := types.NamespacedName{
				Name:      hostObj.GetAnnotations()[translate.ResourceNameAnnotation],
				Namespace: hostObj.GetAnnotations()[translate.ResourceNamespaceAnnotation],
			}

			err := o.VirtualReader.Get(ctx, key, kind.NewObj())
			if err == nil {
				continue
			}

			if !apierrors.IsNotFound(err) {
				return err
			}

			if kind.ReportOnly {
				log.Info("host copy has no virtual object, kept (holds data)", "kind", kind.Name, "host", hostObj.GetName(), "virtual", key.String())
				continue
			}

			log.Info("deleting host copy without a virtual object", "kind", kind.Name, "host", hostObj.GetName(), "virtual", key.String())

			uid := hostObj.GetUID()
			if err := o.HostClient.Delete(ctx, hostObj, ctrlruntimeclient.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				return err
			}
		}
	}

	return nil
}

// metaItems returns the items of a typed list as client objects.
func metaItems(list ctrlruntimeclient.ObjectList) ([]ctrlruntimeclient.Object, error) {
	objs, err := meta.ExtractList(list)
	if err != nil {
		return nil, err
	}

	result := make([]ctrlruntimeclient.Object, 0, len(objs))

	for _, o := range objs {
		if obj, ok := o.(ctrlruntimeclient.Object); ok {
			result = append(result, obj)
		}
	}

	return result, nil
}

// writeHostCopy creates the host copy, or updates it when the content the
// virtual side asks for changed, and reports a rejected write on the virtual
// object. current is an empty object of the same kind.
func (s *SyncerContext) writeHostCopy(ctx context.Context, virtObj, desired, current ctrlruntimeclient.Object) error {
	log := ctrl.LoggerFrom(ctx)

	if err := stampContentHash(desired); err != nil {
		return err
	}

	if err := s.HostClient.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(desired), current); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		log.Info("creating the host copy", "name", desired.GetName())

		if err := s.HostClient.Create(ctx, desired); err != nil {
			return s.syncFailed(virtObj, "create", err)
		}

		return nil
	}

	if !contentChanged(desired, current) {
		return nil
	}

	log.Info("updating the host copy", "name", desired.GetName())

	desired.SetResourceVersion(current.GetResourceVersion())

	if err := s.HostClient.Update(ctx, desired); err != nil {
		if apierrors.IsConflict(err) {
			return err
		}

		return s.syncFailed(virtObj, "update", err)
	}

	return nil
}
