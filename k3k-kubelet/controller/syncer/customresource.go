package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	corev1 "k8s.io/api/core/v1"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const (
	crControllerName = "customresource-syncer-controller"
	crFinalizerName  = "customresource.k3k.io/finalizer"

	// Substitution variables usable in patch values.
	varVCDNS  = "$(VC_DNS)"
	varVCName = "$(VC_NAME)"
	varHostNS = "$(HOST_NS)"
)

// CustomResourceReconciler generically syncs one custom resource type from
// the virtual cluster down to the host cluster: new resource types are
// configuration (spec.sync.customResources), not code. The host cluster runs
// the operator; the virtual cluster only holds the API surface. Optionally
// the host object's status flows back to the virtual object.
type CustomResourceReconciler struct {
	*SyncerContext
	GVK schema.GroupVersionKind
}

// AddCustomResourceSyncers registers one syncer controller per enabled
// sync.customResources entry. The entry LIST is read once at kubelet start
// (adding a new type needs a kubelet restart); the enabled flag of a
// registered entry is honored live through the reconciler, with deletions
// still processed for cleanup. Because controller-runtime informers start
// with a full LIST, pre-existing virtual objects are replayed at startup —
// enabling a type and restarting the kubelet backfills everything.
func AddCustomResourceSyncers(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	var cluster v1beta1.Cluster

	// The manager caches have not started yet — use the direct reader.
	if err := hostMgr.GetAPIReader().Get(ctx, types.NamespacedName{Name: clusterName, Namespace: clusterNamespace}, &cluster); err != nil {
		return fmt.Errorf("customresource syncer: reading cluster: %w", err)
	}

	if cluster.Spec.Sync == nil {
		return nil
	}

	for _, cfg := range cluster.Spec.Sync.CustomResources {
		if !cfg.Enabled {
			continue
		}

		if err := addCustomResourceSyncer(virtMgr, hostMgr, clusterName, clusterNamespace, cfg); err != nil {
			return fmt.Errorf("customresource syncer for %s/%s: %w", cfg.APIVersion, cfg.Kind, err)
		}
	}

	return nil
}

func addCustomResourceSyncer(virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string, cfg v1beta1.CustomResourceSyncConfig) error {
	gv, err := schema.ParseGroupVersion(cfg.APIVersion)
	if err != nil {
		return err
	}

	gvk := gv.WithKind(cfg.Kind)

	reconciler := CustomResourceReconciler{
		SyncerContext: &SyncerContext{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
			VirtualClient:    virtMgr.GetClient(),
			HostClient:       hostMgr.GetClient(),
			Translator: translate.ToHostTranslator{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
			},
		},
		GVK: gvk,
	}

	virtObj := &unstructured.Unstructured{}
	virtObj.SetGroupVersionKind(gvk)

	hostObj := &unstructured.Unstructured{}
	hostObj.SetGroupVersionKind(gvk)

	name := reconciler.Translator.TranslateName(clusterNamespace, strings.ToLower(cfg.Kind)+"-"+crControllerName)

	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(virtObj).
		// Host-side changes (status updates by the operator) map back to the
		// originating virtual object via the translation annotations.
		WatchesRawSource(source.Kind(hostMgr.GetCache(), ctrlruntimeclient.Object(hostObj),
			handler.EnqueueRequestsFromMapFunc(reconciler.mapHostToVirtual))).
		Complete(&reconciler)
}

// mapHostToVirtual enqueues the virtual object a synced host object belongs
// to, using the annotations the translator stamps on the way down.
func (r *CustomResourceReconciler) mapHostToVirtual(ctx context.Context, obj ctrlruntimeclient.Object) []reconcile.Request {
	labels := obj.GetLabels()
	if labels[translate.ClusterNameLabel] != r.ClusterName {
		return nil
	}

	annotations := obj.GetAnnotations()

	name := annotations[translate.ResourceNameAnnotation]
	namespace := annotations[translate.ResourceNamespaceAnnotation]

	if name == "" {
		return nil
	}

	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}}}
}

func (r *CustomResourceReconciler) config(cluster *v1beta1.Cluster) *v1beta1.CustomResourceSyncConfig {
	if cluster.Spec.Sync == nil {
		return nil
	}

	for i := range cluster.Spec.Sync.CustomResources {
		cfg := &cluster.Spec.Sync.CustomResources[i]

		gv, err := schema.ParseGroupVersion(cfg.APIVersion)
		if err != nil {
			continue
		}

		if gv.WithKind(cfg.Kind) == r.GVK {
			return cfg
		}
	}

	return nil
}

func (r *CustomResourceReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace, "gvk", r.GVK.String())
	ctx = ctrl.LoggerInto(ctx, log)

	var cluster v1beta1.Cluster
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	cfg := r.config(&cluster)
	if cfg == nil {
		return reconcile.Result{}, nil
	}

	virtObj := &unstructured.Unstructured{}
	virtObj.SetGroupVersionKind(r.GVK)

	if err := r.VirtualClient.Get(ctx, req.NamespacedName, virtObj); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	// Disabled (but still registered): only clean up deletions.
	if !cfg.Enabled && virtObj.GetDeletionTimestamp().IsZero() {
		return reconcile.Result{}, nil
	}

	hostObj, err := r.translated(ctx, virtObj, cfg)
	if err != nil {
		return reconcile.Result{}, err
	}

	if err := controllerutil.SetOwnerReference(&cluster, hostObj, r.HostClient.Scheme()); err != nil {
		return reconcile.Result{}, err
	}

	// handle deletion: host object first, then release the finalizer
	if !virtObj.GetDeletionTimestamp().IsZero() {
		if err := r.HostClient.Delete(ctx, hostObj); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}

		if controllerutil.RemoveFinalizer(virtObj, crFinalizerName) {
			if err := r.VirtualClient.Update(ctx, virtObj); err != nil {
				return reconcile.Result{}, err
			}
		}

		return reconcile.Result{}, nil
	}

	if controllerutil.AddFinalizer(virtObj, crFinalizerName) {
		if err := r.VirtualClient.Update(ctx, virtObj); err != nil {
			return reconcile.Result{}, err
		}
	}

	var existing unstructured.Unstructured

	existing.SetGroupVersionKind(r.GVK)

	if err := r.HostClient.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(hostObj), &existing); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("creating the custom resource for the first time on the host cluster")
			return reconcile.Result{}, r.HostClient.Create(ctx, hostObj)
		}

		return reconcile.Result{}, err
	}

	// spec/labels/annotations flow down; everything else stays host-owned
	existing.SetLabels(hostObj.GetLabels())
	existing.SetAnnotations(hostObj.GetAnnotations())

	if spec, ok := hostObj.Object["spec"]; ok {
		existing.Object["spec"] = spec
	}

	if err := r.HostClient.Update(ctx, &existing); err != nil {
		return reconcile.Result{}, err
	}

	// status flows up when requested; the virtual KCM never computes it
	if cfg.SyncStatus {
		if status, ok := existing.Object["status"]; ok {
			virtObj.Object["status"] = status

			if err := r.VirtualClient.Status().Update(ctx, virtObj); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				return reconcile.Result{}, err
			}
		}
	}

	return reconcile.Result{}, nil
}

// translated builds the host counterpart: translator naming/labels, then the
// configured patches with substitution variables resolved.
func (r *CustomResourceReconciler) translated(ctx context.Context, virtObj *unstructured.Unstructured, cfg *v1beta1.CustomResourceSyncConfig) (*unstructured.Unstructured, error) {
	hostObj := virtObj.DeepCopy()
	r.Translator.TranslateTo(hostObj)

	// finalizers/status/managed metadata never flow down
	hostObj.SetFinalizers(nil)
	hostObj.SetResourceVersion("")
	hostObj.SetUID("")
	hostObj.SetOwnerReferences(nil)
	delete(hostObj.Object, "status")

	if len(cfg.Patches) == 0 {
		return hostObj, nil
	}

	vars, err := r.substitutions(ctx, virtObj.GetNamespace())
	if err != nil {
		return nil, err
	}

	for _, p := range cfg.Patches {
		var value interface{}

		if p.Value != nil {
			raw := string(p.Value.Raw)
			for k, v := range vars {
				raw = strings.ReplaceAll(raw, k, v)
			}

			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return nil, fmt.Errorf("patch value for %s: %w", p.Path, err)
			}
		}

		if err := applyPatch(hostObj.Object, p.Op, p.Path, value); err != nil {
			return nil, fmt.Errorf("applying patch %s %s: %w", p.Op, p.Path, err)
		}
	}

	return hostObj, nil
}

func (r *CustomResourceReconciler) substitutions(ctx context.Context, virtNamespace string) (map[string]string, error) {
	vars := map[string]string{
		varVCName: r.ClusterName,
		varHostNS: r.ClusterNamespace,
	}

	// The vc kube-dns ClusterIP is only known at sync time and changes on vc
	// recreation — the reason DNS wiring must be dynamic (the VM-syncer's
	// guest-DNS use case).
	var svc corev1.Service

	dnsName := fmt.Sprintf("k3k-%s-kube-dns", r.ClusterName)
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: dnsName, Namespace: r.ClusterNamespace}, &svc); err == nil {
		vars[varVCDNS] = svc.Spec.ClusterIP
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	_ = virtNamespace // reserved for future per-namespace variables

	return vars, nil
}

// applyPatch applies one add/replace operation at a JSON-pointer path on an
// unstructured object tree. Intermediate maps are created for "add"; slice
// indices and the "-" append marker are supported on existing slices.
func applyPatch(root map[string]interface{}, op, path string, value interface{}) error {
	if op != "add" && op != "replace" {
		return fmt.Errorf("unsupported op %q", op)
	}

	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must start with /")
	}

	segments := strings.Split(path[1:], "/")
	for i, s := range segments {
		segments[i] = strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
	}

	var cur interface{} = root

	for _, seg := range segments[:len(segments)-1] {
		switch node := cur.(type) {
		case map[string]interface{}:
			next, ok := node[seg]
			if !ok {
				if op == "replace" {
					return fmt.Errorf("path segment %q not found", seg)
				}

				created := map[string]interface{}{}
				node[seg] = created
				cur = created

				continue
			}

			cur = next
		case []interface{}:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				return fmt.Errorf("invalid slice index %q", seg)
			}

			cur = node[idx]
		default:
			return fmt.Errorf("cannot traverse %q: not an object or array", seg)
		}
	}

	last := segments[len(segments)-1]

	switch node := cur.(type) {
	case map[string]interface{}:
		if op == "replace" {
			if _, ok := node[last]; !ok {
				return fmt.Errorf("replace target %q not found", last)
			}
		}

		node[last] = value
	case []interface{}:
		return patchSlice(root, segments, node, op, last, value)
	default:
		return fmt.Errorf("cannot patch %q: parent is not an object or array", last)
	}

	return nil
}

// patchSlice handles the final segment pointing into a slice: numeric index
// replacement or "-" append. The parent reference must be rewritten because
// append reallocates.
func patchSlice(root map[string]interface{}, segments []string, node []interface{}, op, last string, value interface{}) error {
	if last == "-" {
		if op != "add" {
			return fmt.Errorf("append needs op add")
		}

		return replaceParentSlice(root, segments[:len(segments)-1], append(node, value))
	}

	idx, err := strconv.Atoi(last)
	if err != nil || idx < 0 || idx >= len(node) {
		return fmt.Errorf("invalid slice index %q", last)
	}

	node[idx] = value

	return nil
}

func replaceParentSlice(root map[string]interface{}, parentSegments []string, newSlice []interface{}) error {
	var cur interface{} = root

	for _, seg := range parentSegments[:len(parentSegments)-1] {
		switch node := cur.(type) {
		case map[string]interface{}:
			cur = node[seg]
		case []interface{}:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				return fmt.Errorf("invalid slice index %q", seg)
			}

			cur = node[idx]
		default:
			return fmt.Errorf("cannot traverse %q", seg)
		}
	}

	parent, ok := cur.(map[string]interface{})
	if !ok {
		return fmt.Errorf("append parent is not an object")
	}

	parent[parentSegments[len(parentSegments)-1]] = newSlice

	return nil
}
