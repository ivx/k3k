package syncer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const (
	crControllerName = "customresource-syncer-controller"
	crFinalizerName  = "customresource.k3k.io/finalizer"

	// Substitution variables usable in patch values.
	varVCDNS       = "$(VC_DNS)"
	varVCName      = "$(VC_NAME)"
	varHostNS      = "$(HOST_NS)"
	varVCNamespace = "$(VC_NAMESPACE)"
)

// CustomResourceReconciler generically syncs one custom resource type from
// the virtual cluster down to the host cluster: new resource types are
// configuration (spec.sync.customResources), not code. The host cluster runs
// the operator; the virtual cluster only holds the API surface. Optionally
// the host object's status flows back to the virtual object.
type CustomResourceReconciler struct {
	*SyncerContext
	GVK      schema.GroupVersionKind
	Recorder record.EventRecorder
}

// AddCustomResourceSyncers registers one syncer controller per enabled
// sync.customResources entry. The entry LIST is read once at kubelet start
// (adding a new type needs a kubelet restart); the enabled flag of a
// registered entry is honored live through the reconciler, with deletions
// still processed for cleanup. Because controller-runtime informers start
// with a full LIST, pre-existing virtual objects are replayed at startup —
// enabling a type and restarting the kubelet backfills everything.
//
// ready limits registration to the GVKs whose type the virtual cluster serves
// (see EnsureCustomResourceDefinitions); nil means all enabled entries.
func AddCustomResourceSyncers(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string, recorder record.EventRecorder, ready map[schema.GroupVersionKind]bool) error {
	var cluster v1beta1.Cluster

	// The manager caches have not started yet — use the direct reader.
	if err := hostMgr.GetAPIReader().Get(ctx, types.NamespacedName{Name: clusterName, Namespace: clusterNamespace}, &cluster); err != nil {
		return fmt.Errorf("customresource syncer: reading cluster: %w", err)
	}

	for _, cfg := range CustomResourceEntries(&cluster) {
		if !cfg.Enabled {
			continue
		}

		if ready != nil {
			gv, err := schema.ParseGroupVersion(cfg.APIVersion)
			if err != nil || !ready[gv.WithKind(cfg.Kind)] {
				continue
			}
		}

		if err := addCustomResourceSyncer(virtMgr, hostMgr, clusterName, clusterNamespace, cfg, recorder); err != nil {
			return fmt.Errorf("customresource syncer for %s/%s: %w", cfg.APIVersion, cfg.Kind, err)
		}
	}

	return nil
}

// pdbEntry is the generic-syncer form of sync.podDisruptionBudgets: a PDB is
// a LabelSelector-carrying object whose host copy must be scoped to the pods
// of its virtual cluster and namespace - exactly what selectors do.
func pdbEntry(cfg v1beta1.PodDisruptionBudgetSyncConfig) v1beta1.CustomResourceSyncConfig {
	return v1beta1.CustomResourceSyncConfig{
		APIVersion: "policy/v1",
		Kind:       "PodDisruptionBudget",
		Enabled:    cfg.Enabled,
		Selector:   cfg.Selector,
		Selectors:  []string{"/spec/selector"},
	}
}

// CustomResourceEntries returns the effective sync.customResources entries of
// a cluster: the configured ones plus the built-in alias for
// sync.podDisruptionBudgets (unless an explicit PodDisruptionBudget entry
// exists, which then wins).
func CustomResourceEntries(cluster *v1beta1.Cluster) []v1beta1.CustomResourceSyncConfig {
	if cluster.Spec.Sync == nil {
		return nil
	}

	entries := slices.Clone(cluster.Spec.Sync.CustomResources)

	explicitPDB := slices.ContainsFunc(entries, func(e v1beta1.CustomResourceSyncConfig) bool {
		return e.APIVersion == "policy/v1" && e.Kind == "PodDisruptionBudget"
	})

	if !explicitPDB {
		entries = append(entries, pdbEntry(cluster.Spec.Sync.PodDisruptionBudgets))
	}

	return entries
}

func addCustomResourceSyncer(virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string, cfg v1beta1.CustomResourceSyncConfig, recorder record.EventRecorder) error {
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
		GVK:      gvk,
		Recorder: recorder,
	}

	virtObj := &unstructured.Unstructured{}
	virtObj.SetGroupVersionKind(gvk)

	hostObj := &unstructured.Unstructured{}
	hostObj.SetGroupVersionKind(gvk)

	name := reconciler.Translator.TranslateName(clusterNamespace, strings.ToLower(cfg.Kind)+"-"+crControllerName)

	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(virtObj, builder.WithPredicates(predicate.NewPredicateFuncs(reconciler.filterResources))).
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
	entries := CustomResourceEntries(cluster)

	for i := range entries {
		cfg := &entries[i]

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

// filterResources applies the entry's label selector to the virtual objects.
// Deletions always pass so that host copies of objects that stopped matching
// (or of a disabled entry) are cleaned up.
func (r *CustomResourceReconciler) filterResources(object ctrlruntimeclient.Object) bool {
	if !object.GetDeletionTimestamp().IsZero() {
		return true
	}

	var cluster v1beta1.Cluster
	if err := r.HostClient.Get(context.Background(), types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return false
	}

	cfg := r.config(&cluster)
	if cfg == nil || !cfg.Enabled {
		return false
	}

	return labels.SelectorFromSet(cfg.Selector).Matches(labels.Set(object.GetLabels()))
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
		if errors.Is(err, ErrRejected) && virtObj.GetDeletionTimestamp().IsZero() {
			return reconcile.Result{}, r.reject(ctx, virtObj, err)
		}

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

	// spec/labels/annotations flow down; everything else stays host-owned.
	// Only write when the virtual side changed (content hash): the host
	// operator's mutating webhooks default fields in spec, and rewriting
	// the virtual spec on every pass would undo them, bump the generation,
	// trigger the host watch and loop (seen with KubeVirt: thousands of
	// generations, virt-launcher re-syncing the domain every second).
	if existing.GetAnnotations()[SpecHashAnnotation] != hostObj.GetAnnotations()[SpecHashAnnotation] {
		existing.SetLabels(hostObj.GetLabels())
		existing.SetAnnotations(hostObj.GetAnnotations())

		if spec, ok := hostObj.Object["spec"]; ok {
			existing.Object["spec"] = spec
		}

		if err := r.HostClient.Update(ctx, &existing); err != nil {
			return reconcile.Result{}, err
		}
	}

	// status flows up when requested; the virtual KCM never computes it
	if cfg.SyncStatus {
		if status, ok := existing.Object["status"]; ok && !equality.Semantic.DeepEqual(status, virtObj.Object["status"]) {
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

	if err := r.applyPatches(ctx, hostObj, virtObj.GetNamespace(), cfg.Patches); err != nil {
		return nil, err
	}

	// Scope selectors to this virtual cluster and refuse fields that would
	// widen what the object can reach. Both run on the translated object so
	// that they also cover values introduced by patches.
	if err := scopeSelectors(hostObj.Object, cfg.Selectors, r.ClusterName, virtObj.GetNamespace()); err != nil {
		return nil, err
	}

	if err := checkRejects(hostObj.Object, cfg.Rejects); err != nil {
		return nil, err
	}

	stampSpecHash(hostObj)

	return hostObj, nil
}

// SpecHashAnnotation records, on the host copy, a hash of what the virtual
// side asked for (labels, annotations, spec). Unchanged hash = no write.
const SpecHashAnnotation = "k3k.io/spec-hash"

// stampSpecHash sets SpecHashAnnotation on obj from its labels, its other
// annotations and its spec.
func stampSpecHash(obj *unstructured.Unstructured) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	delete(annotations, SpecHashAnnotation)

	content := map[string]any{
		"labels":      obj.GetLabels(),
		"annotations": annotations,
		"spec":        obj.Object["spec"],
	}

	// json.Marshal sorts map keys: deterministic across runs
	raw, _ := json.Marshal(content)
	sum := sha256.Sum256(raw)

	annotations[SpecHashAnnotation] = hex.EncodeToString(sum[:])
	obj.SetAnnotations(annotations)
}

func (r *CustomResourceReconciler) applyPatches(ctx context.Context, hostObj *unstructured.Unstructured, virtNamespace string, patches []v1beta1.CustomResourcePatch) error {
	if len(patches) == 0 {
		return nil
	}

	vars, err := substitutionVars(ctx, r.HostClient, r.ClusterName, r.ClusterNamespace, virtNamespace)
	if err != nil {
		return err
	}

	for _, p := range patches {
		var value any

		if p.Value != nil {
			if err := json.Unmarshal([]byte(substitute(string(p.Value.Raw), vars)), &value); err != nil {
				return fmt.Errorf("patch value for %s: %w", p.Path, err)
			}
		}

		if err := applyPatch(hostObj.Object, p.Op, p.Path, value); err != nil {
			return fmt.Errorf("applying patch %s %s: %w", p.Op, p.Path, err)
		}
	}

	return nil
}

// reject handles an object that must not reach the host: the virtual object
// gets a Warning event with the reason, and a host copy from an earlier,
// valid version is removed so that the stale version cannot stay in effect.
// Not an error for the reconciler — retrying changes nothing.
func (r *CustomResourceReconciler) reject(ctx context.Context, virtObj *unstructured.Unstructured, cause error) error {
	log := ctrl.LoggerFrom(ctx)
	log.Info("object rejected, not synced to the host", "name", virtObj.GetName(), "namespace", virtObj.GetNamespace(), "reason", cause.Error())

	if r.Recorder != nil {
		r.Recorder.Event(virtObj, corev1.EventTypeWarning, "SyncRejected", cause.Error())
	}

	hostObj := virtObj.DeepCopy()
	r.Translator.TranslateTo(hostObj)

	if err := r.HostClient.Delete(ctx, hostObj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

// substitutionVars resolves the variables usable in patch values and
// per-namespace templates.
func substitutionVars(ctx context.Context, hostClient ctrlruntimeclient.Client, clusterName, clusterNamespace, virtNamespace string) (map[string]string, error) {
	vars := map[string]string{
		varVCName:      clusterName,
		varHostNS:      clusterNamespace,
		varVCNamespace: virtNamespace,
	}

	// The vc kube-dns ClusterIP is only known at sync time and changes on vc
	// recreation - the reason DNS wiring must be dynamic (the VM-syncer's
	// guest-DNS use case).
	var svc corev1.Service

	dnsName := fmt.Sprintf("k3k-%s-kube-dns", clusterName)
	if err := hostClient.Get(ctx, types.NamespacedName{Name: dnsName, Namespace: clusterNamespace}, &svc); err == nil {
		vars[varVCDNS] = svc.Spec.ClusterIP
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	return vars, nil
}

// substitute replaces every variable in raw.
func substitute(raw string, vars map[string]string) string {
	for k, v := range vars {
		raw = strings.ReplaceAll(raw, k, v)
	}

	return raw
}

// applyPatch applies one add/replace operation at a JSON-pointer path on an
// unstructured object tree. Intermediate maps are created for "add"; slice
// indices and the "-" append marker are supported on existing slices.
func applyPatch(root map[string]any, op, path string, value any) error {
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

	var cur any = root

	for _, seg := range segments[:len(segments)-1] {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[seg]
			if !ok {
				if op == "replace" {
					return fmt.Errorf("path segment %q not found", seg)
				}

				created := map[string]any{}
				node[seg] = created
				cur = created

				continue
			}

			cur = next
		case []any:
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
	case map[string]any:
		if op == "replace" {
			if _, ok := node[last]; !ok {
				return fmt.Errorf("replace target %q not found", last)
			}
		}

		node[last] = value
	case []any:
		return patchSlice(root, segments, node, op, last, value)
	default:
		return fmt.Errorf("cannot patch %q: parent is not an object or array", last)
	}

	return nil
}

// patchSlice handles the final segment pointing into a slice: numeric index
// replacement or "-" append. The parent reference must be rewritten because
// append reallocates.
func patchSlice(root map[string]any, segments []string, node []any, op, last string, value any) error {
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

func replaceParentSlice(root map[string]any, parentSegments []string, newSlice []any) error {
	var cur any = root

	for _, seg := range parentSegments[:len(parentSegments)-1] {
		switch node := cur.(type) {
		case map[string]any:
			cur = node[seg]
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				return fmt.Errorf("invalid slice index %q", seg)
			}

			cur = node[idx]
		default:
			return fmt.Errorf("cannot traverse %q", seg)
		}
	}

	parent, ok := cur.(map[string]any)
	if !ok {
		return fmt.Errorf("append parent is not an object")
	}

	parent[parentSegments[len(parentSegments)-1]] = newSlice

	return nil
}
