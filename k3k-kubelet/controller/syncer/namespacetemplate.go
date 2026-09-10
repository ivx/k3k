package syncer

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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
	namespaceTemplateControllerName = "namespace-template-controller"

	// PerNamespaceLabel marks host objects rendered from a perNamespace template.
	PerNamespaceLabel = "k3k.io/perNamespace"
)

// NamespaceTemplateReconciler renders the perNamespace templates of the
// sync.customResources entries on the host, once per namespace of the
// virtual cluster, and removes them with the namespace. This is how the
// platform ships per-tenant-namespace defaults (a baseline network policy,
// a quota) without a tenant being able to edit them: the objects exist only
// on the host.
type NamespaceTemplateReconciler struct {
	*SyncerContext
}

// AddNamespaceTemplateSyncer registers the controller when at least one entry
// carries perNamespace templates. Like the entry list itself, this is read at
// kubelet start.
func AddNamespaceTemplateSyncer(ctx context.Context, virtMgr, hostMgr manager.Manager, clusterName, clusterNamespace string) error {
	var cluster v1beta1.Cluster
	if err := hostMgr.GetAPIReader().Get(ctx, types.NamespacedName{Name: clusterName, Namespace: clusterNamespace}, &cluster); err != nil {
		return fmt.Errorf("namespace template syncer: reading cluster: %w", err)
	}

	hasTemplates := false

	for _, cfg := range CustomResourceEntries(&cluster) {
		if cfg.Enabled && len(cfg.PerNamespace) > 0 {
			hasTemplates = true
			break
		}
	}

	if !hasTemplates {
		return nil
	}

	reconciler := NamespaceTemplateReconciler{
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
	}

	return ctrl.NewControllerManagedBy(virtMgr).
		Named(reconciler.Translator.TranslateName(clusterNamespace, namespaceTemplateControllerName)).
		For(&corev1.Namespace{}).
		// A template change on the Cluster must re-render every namespace,
		// not wait for the next namespace event.
		WatchesRawSource(source.Kind(hostMgr.GetCache(), ctrlruntimeclient.Object(&v1beta1.Cluster{}),
			handler.EnqueueRequestsFromMapFunc(reconciler.allNamespaces))).
		Complete(&reconciler)
}

// allNamespaces enqueues every namespace of the virtual cluster when its
// Cluster object changes.
func (r *NamespaceTemplateReconciler) allNamespaces(ctx context.Context, obj ctrlruntimeclient.Object) []reconcile.Request {
	if obj.GetName() != r.ClusterName || obj.GetNamespace() != r.ClusterNamespace {
		return nil
	}

	var namespaces corev1.NamespaceList
	if err := r.VirtualClient.List(ctx, &namespaces); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "listing virtual namespaces for template re-render")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(namespaces.Items))
	for _, ns := range namespaces.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: ns.Name}})
	}

	return requests
}

func (r *NamespaceTemplateReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "clusterNamespace", r.ClusterNamespace, "namespace", req.Name)
	ctx = ctrl.LoggerInto(ctx, log)

	var cluster v1beta1.Cluster
	if err := r.HostClient.Get(ctx, types.NamespacedName{Name: r.ClusterName, Namespace: r.ClusterNamespace}, &cluster); err != nil {
		return reconcile.Result{}, err
	}

	var namespace corev1.Namespace

	err := r.VirtualClient.Get(ctx, req.NamespacedName, &namespace)
	gone := apierrors.IsNotFound(err) || (err == nil && !namespace.DeletionTimestamp.IsZero())

	if err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, err
	}

	for _, cfg := range CustomResourceEntries(&cluster) {
		if len(cfg.PerNamespace) == 0 {
			continue
		}

		gv, err := schema.ParseGroupVersion(cfg.APIVersion)
		if err != nil {
			return reconcile.Result{}, err
		}

		gvk := gv.WithKind(cfg.Kind)

		if gone || !cfg.Enabled {
			if err := r.deleteRendered(ctx, gvk, req.Name); err != nil {
				return reconcile.Result{}, err
			}

			continue
		}

		for i, tpl := range cfg.PerNamespace {
			obj, err := r.render(ctx, gvk, tpl, req.Name)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("perNamespace template %d of %s: %w", i, gvk, err)
			}

			if err := controllerutil.SetOwnerReference(&cluster, obj, r.HostClient.Scheme()); err != nil {
				return reconcile.Result{}, err
			}

			if err := r.apply(ctx, obj); err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	return reconcile.Result{}, nil
}

// render builds the host object for one template and namespace: variables
// substituted, then translated exactly like a synced object of that
// namespace (name, labels, annotations) plus the perNamespace marker.
func (r *NamespaceTemplateReconciler) render(ctx context.Context, gvk schema.GroupVersionKind, tpl v1beta1.CustomResourceTemplate, virtNamespace string) (*unstructured.Unstructured, error) {
	vars, err := substitutionVars(ctx, r.HostClient, r.ClusterName, r.ClusterNamespace, virtNamespace)
	if err != nil {
		return nil, err
	}

	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(substitute(string(tpl.Template.Raw), vars)), &obj.Object); err != nil {
		return nil, fmt.Errorf("template is not a JSON object: %w", err)
	}

	if obj.GetKind() == "" {
		obj.SetGroupVersionKind(gvk)
	}

	if obj.GroupVersionKind() != gvk {
		return nil, fmt.Errorf("template kind %s does not match the entry %s", obj.GroupVersionKind(), gvk)
	}

	if obj.GetName() == "" {
		return nil, fmt.Errorf("template has no metadata.name")
	}

	obj.SetNamespace(virtNamespace)
	r.Translator.TranslateTo(obj)

	labels := obj.GetLabels()
	labels[translate.NamespaceNameLabel] = virtNamespace
	labels[PerNamespaceLabel] = "true"
	obj.SetLabels(labels)

	delete(obj.Object, "status")

	return obj, nil
}

func (r *NamespaceTemplateReconciler) apply(ctx context.Context, obj *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())

	if err := r.HostClient.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(obj), existing); err != nil {
		if apierrors.IsNotFound(err) {
			return r.HostClient.Create(ctx, obj)
		}

		return err
	}

	existing.SetLabels(obj.GetLabels())
	existing.SetAnnotations(obj.GetAnnotations())
	existing.SetOwnerReferences(obj.GetOwnerReferences())

	if spec, ok := obj.Object["spec"]; ok {
		existing.Object["spec"] = spec
	}

	// objects without a spec (e.g. ConfigMap-like kinds) carry their payload at top level
	for k, v := range obj.Object {
		if k == "metadata" || k == "apiVersion" || k == "kind" || k == "spec" || k == "status" {
			continue
		}

		existing.Object[k] = v
	}

	return r.HostClient.Update(ctx, existing)
}

// deleteRendered removes every object of gvk rendered for a virtual namespace.
func (r *NamespaceTemplateReconciler) deleteRendered(ctx context.Context, gvk schema.GroupVersionKind, virtNamespace string) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))

	if err := r.HostClient.List(ctx, list,
		ctrlruntimeclient.InNamespace(r.ClusterNamespace),
		ctrlruntimeclient.MatchingLabels{
			translate.ClusterNameLabel:   r.ClusterName,
			translate.NamespaceNameLabel: virtNamespace,
			PerNamespaceLabel:            "true",
		}); err != nil {
		return ctrlruntimeclient.IgnoreNotFound(err)
	}

	for i := range list.Items {
		if err := r.HostClient.Delete(ctx, &list.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}
