package syncer

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const (
	crdControllerName = "crd-syncer-controller"

	// CRDSyncedLabel marks CRDs in the virtual cluster that are copies of host CRDs.
	CRDSyncedLabel = "k3k.io/synced-from-host"

	crdEstablishedTimeout = 60 * time.Second
)

// CRDReconciler keeps the CustomResourceDefinitions that back the configured
// sync.customResources entries in step with the host: the virtual cluster only
// holds the API surface, the host runs the operator, so the schema must be the
// host's — hand-shipped copies drift on every operator upgrade.
type CRDReconciler struct {
	HostClient  ctrlruntimeclient.Client
	VirtClient  ctrlruntimeclient.Client
	ClusterName string
	// wanted maps a host CRD name to the entry it backs.
	wanted map[string]schema.GroupVersionKind
}

// EnsureCustomResourceDefinitions copies the CRD behind every enabled
// sync.customResources entry from the host into the virtual cluster and waits
// until it is established. It runs before the custom-resource syncers are
// registered, so their informers always find the type. The returned map
// (host CRD name -> GVK) feeds the drift watch, see AddCRDSyncer.
func EnsureCustomResourceDefinitions(ctx context.Context, hostReader ctrlruntimeclient.Reader, virtClient ctrlruntimeclient.Client, cluster *v1beta1.Cluster) (map[string]schema.GroupVersionKind, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", cluster.Name)
	wanted := map[string]schema.GroupVersionKind{}

	if cluster.Spec.Sync == nil {
		return wanted, nil
	}

	var crds apiextensionsv1.CustomResourceDefinitionList
	if err := hostReader.List(ctx, &crds); err != nil {
		return nil, fmt.Errorf("listing host CRDs: %w", err)
	}

	for _, cfg := range cluster.Spec.Sync.CustomResources {
		if !cfg.Enabled {
			continue
		}

		gv, err := schema.ParseGroupVersion(cfg.APIVersion)
		if err != nil {
			return nil, fmt.Errorf("customResources entry %s/%s: %w", cfg.APIVersion, cfg.Kind, err)
		}

		gvk := gv.WithKind(cfg.Kind)

		hostCRD := findCRD(crds.Items, gvk)
		if hostCRD == nil {
			// Built-in kinds (e.g. policy/v1 PodDisruptionBudget) have no CRD
			// and are served by the virtual API server already.
			if servedByVirtualCluster(virtClient, gvk) {
				continue
			}

			return nil, fmt.Errorf("customResources entry %s: no CRD on the host serves this kind", gvk)
		}

		wanted[hostCRD.Name] = gvk

		if err := applyCRD(ctx, virtClient, hostCRD); err != nil {
			return nil, fmt.Errorf("copying CRD %s into the virtual cluster: %w", hostCRD.Name, err)
		}

		if err := waitEstablished(ctx, virtClient, hostCRD.Name); err != nil {
			return nil, err
		}

		log.Info("CRD copied from the host", "crd", hostCRD.Name, "gvk", gvk.String())
	}

	return wanted, nil
}

// AddCRDSyncer watches the host CRDs that back configured entries and
// re-applies them into the virtual cluster on change, so an operator upgrade on
// the host reaches the virtual API surface without a kubelet restart.
func AddCRDSyncer(ctx context.Context, hostMgr manager.Manager, virtClient ctrlruntimeclient.Client, clusterName string, wanted map[string]schema.GroupVersionKind) error {
	if len(wanted) == 0 {
		return nil
	}

	reconciler := &CRDReconciler{
		HostClient:  hostMgr.GetClient(),
		VirtClient:  virtClient,
		ClusterName: clusterName,
		wanted:      wanted,
	}

	return ctrl.NewControllerManagedBy(hostMgr).
		Named(clusterName + "-" + crdControllerName).
		For(&apiextensionsv1.CustomResourceDefinition{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj ctrlruntimeclient.Object) bool {
			_, ok := wanted[obj.GetName()]
			return ok
		})).
		Complete(reconciler)
}

func (r *CRDReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("cluster", r.ClusterName, "crd", req.Name)

	var hostCRD apiextensionsv1.CustomResourceDefinition
	if err := r.HostClient.Get(ctx, req.NamespacedName, &hostCRD); err != nil {
		// A deleted host CRD is left alone in the virtual cluster: the syncer
		// for that type cannot work any more, but removing the API surface
		// would cascade-delete every virtual object of that kind.
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	if err := applyCRD(ctx, r.VirtClient, &hostCRD); err != nil {
		return reconcile.Result{}, err
	}

	log.V(1).Info("CRD re-applied from the host")

	return reconcile.Result{}, nil
}

// findCRD returns the host CRD that serves gvk: same group, same kind, and the
// requested version served.
func findCRD(crds []apiextensionsv1.CustomResourceDefinition, gvk schema.GroupVersionKind) *apiextensionsv1.CustomResourceDefinition {
	for i := range crds {
		crd := &crds[i]
		if crd.Spec.Group != gvk.Group || crd.Spec.Names.Kind != gvk.Kind {
			continue
		}

		for _, v := range crd.Spec.Versions {
			if v.Name == gvk.Version && v.Served {
				return crd
			}
		}
	}

	return nil
}

// sanitizeCRD builds the virtual-cluster copy of a host CRD: the spec verbatim
// except for the conversion strategy, which is forced to None — a conversion
// webhook lives on the host and is unreachable from the virtual API server,
// and the virtual cluster never converts anyway (it only stores what the
// syncer reads back in the configured version).
func sanitizeCRD(hostCRD *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.CustomResourceDefinition {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	crd.Name = hostCRD.Name
	crd.Labels = map[string]string{CRDSyncedLabel: "true"}
	crd.Spec = *hostCRD.Spec.DeepCopy()
	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{Strategy: apiextensionsv1.NoneConverter}

	return crd
}

func applyCRD(ctx context.Context, virtClient ctrlruntimeclient.Client, hostCRD *apiextensionsv1.CustomResourceDefinition) error {
	desired := sanitizeCRD(hostCRD)

	existing := &apiextensionsv1.CustomResourceDefinition{}
	existing.Name = desired.Name

	_, err := controllerutil.CreateOrUpdate(ctx, virtClient, existing, func() error {
		if existing.Labels == nil {
			existing.Labels = map[string]string{}
		}

		existing.Labels[CRDSyncedLabel] = "true"
		existing.Spec = desired.Spec

		return nil
	})

	return err
}

func waitEstablished(ctx context.Context, virtClient ctrlruntimeclient.Client, name string) error {
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, crdEstablishedTimeout, true, func(ctx context.Context) (bool, error) {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := virtClient.Get(ctx, ctrlruntimeclient.ObjectKey{Name: name}, &crd); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}

			return false, err
		}

		for _, c := range crd.Status.Conditions {
			if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
				return true, nil
			}
		}

		return false, nil
	})
}

func servedByVirtualCluster(virtClient ctrlruntimeclient.Client, gvk schema.GroupVersionKind) bool {
	mapper := virtClient.RESTMapper()
	if mapper == nil {
		return false
	}

	_, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)

	return err == nil
}
