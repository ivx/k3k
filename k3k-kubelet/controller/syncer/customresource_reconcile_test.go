package syncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

var pdbGVK = schema.GroupVersionKind{Group: "policy", Version: "v1", Kind: "PodDisruptionBudget"}

func newCRTestScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))

	return scheme
}

// newPDBReconciler builds the generic reconciler for the built-in PDB alias.
func newPDBReconciler(t *testing.T, pdbCfg v1beta1.PodDisruptionBudgetSyncConfig, hostObjs, virtObjs []runtime.Object) *CustomResourceReconciler {
	scheme := newCRTestScheme(t)

	cluster := &v1beta1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "ns-1"},
		Spec:       v1beta1.ClusterSpec{Sync: &v1beta1.SyncConfig{PodDisruptionBudgets: pdbCfg}},
	}

	hostObjs = append(hostObjs, cluster)

	return &CustomResourceReconciler{
		SyncerContext: &SyncerContext{
			HostClient:       fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(hostObjs...).Build(),
			VirtualClient:    fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(virtObjs...).Build(),
			Translator:       translate.ToHostTranslator{ClusterName: "mycluster", ClusterNamespace: "ns-1"},
			ClusterName:      "mycluster",
			ClusterNamespace: "ns-1",
		},
		GVK: pdbGVK,
	}
}

func TestCustomResourceEntriesPDBAlias(t *testing.T) {
	cluster := &v1beta1.Cluster{Spec: v1beta1.ClusterSpec{Sync: &v1beta1.SyncConfig{
		PodDisruptionBudgets: v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true, Selector: map[string]string{"tier": "db"}},
		CustomResources:      []v1beta1.CustomResourceSyncConfig{{APIVersion: "cilium.io/v2", Kind: "CiliumNetworkPolicy", Enabled: true}},
	}}}

	entries := CustomResourceEntries(cluster)
	require.Len(t, entries, 2)
	assert.Equal(t, "CiliumNetworkPolicy", entries[0].Kind)
	assert.Equal(t, "PodDisruptionBudget", entries[1].Kind)
	assert.True(t, entries[1].Enabled)
	assert.Equal(t, []string{"/spec/selector"}, entries[1].Selectors)
	assert.Equal(t, map[string]string{"tier": "db"}, entries[1].Selector)

	// an explicit PDB entry wins over the alias
	cluster.Spec.Sync.CustomResources = append(cluster.Spec.Sync.CustomResources,
		v1beta1.CustomResourceSyncConfig{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Enabled: false})
	entries = CustomResourceEntries(cluster)
	require.Len(t, entries, 2)
	assert.False(t, entries[1].Enabled)

	assert.Nil(t, CustomResourceEntries(&v1beta1.Cluster{}))
}

// Ported from the dedicated PDB syncer: the generic reconciler with the alias
// must behave the same.
func TestGenericReconcilePDB(t *testing.T) {
	minAvailable := intstr.FromInt32(1)

	virtPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "web-pdb", Namespace: "team-a", Labels: map[string]string{"app": "web"}},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "web-pdb", Namespace: "team-a"}}

	t.Run("creates scoped host pdb", func(t *testing.T) {
		r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB.DeepCopy()})

		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		hostName := r.Translator.TranslateName("team-a", "web-pdb")

		var hostPDB policyv1.PodDisruptionBudget
		require.NoError(t, r.HostClient.Get(context.Background(), types.NamespacedName{Name: hostName, Namespace: "ns-1"}, &hostPDB))

		assert.Equal(t, map[string]string{
			"app":                        "web",
			translate.ClusterNameLabel:   "mycluster",
			translate.NamespaceNameLabel: "team-a",
		}, hostPDB.Spec.Selector.MatchLabels)
		assert.Equal(t, &minAvailable, hostPDB.Spec.MinAvailable)
		assert.Equal(t, "web-pdb", hostPDB.Annotations[translate.ResourceNameAnnotation])
		assert.Equal(t, "team-a", hostPDB.Annotations[translate.ResourceNamespaceAnnotation])
		assert.Len(t, hostPDB.OwnerReferences, 1)

		var synced policyv1.PodDisruptionBudget
		require.NoError(t, r.VirtualClient.Get(context.Background(), req.NamespacedName, &synced))
		assert.Contains(t, synced.Finalizers, crFinalizerName)
	})

	t.Run("nil selector is pinned to the virtual namespace", func(t *testing.T) {
		noSelector := virtPDB.DeepCopy()
		noSelector.Spec.Selector = nil

		r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{noSelector})

		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		var hostPDB policyv1.PodDisruptionBudget
		require.NoError(t, r.HostClient.Get(context.Background(), types.NamespacedName{Name: r.Translator.TranslateName("team-a", "web-pdb"), Namespace: "ns-1"}, &hostPDB))
		assert.Equal(t, map[string]string{
			translate.ClusterNameLabel:   "mycluster",
			translate.NamespaceNameLabel: "team-a",
		}, hostPDB.Spec.Selector.MatchLabels)
	})

	t.Run("deletion cleans up host pdb and finalizer", func(t *testing.T) {
		r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB.DeepCopy()})

		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		var synced policyv1.PodDisruptionBudget
		require.NoError(t, r.VirtualClient.Get(context.Background(), req.NamespacedName, &synced))
		// the fake client honours finalizers: Delete sets the timestamp
		require.NoError(t, r.VirtualClient.Delete(context.Background(), &synced))

		_, err = r.Reconcile(context.Background(), req)
		require.NoError(t, err)

		var hostPDB policyv1.PodDisruptionBudget

		err = r.HostClient.Get(context.Background(), types.NamespacedName{Name: r.Translator.TranslateName("team-a", "web-pdb"), Namespace: "ns-1"}, &hostPDB)
		assert.True(t, apierrors.IsNotFound(err), "host copy must be gone")

		err = r.VirtualClient.Get(context.Background(), req.NamespacedName, &synced)
		assert.True(t, apierrors.IsNotFound(err), "finalizer must be released")
	})

	t.Run("filter honours enabled and selector", func(t *testing.T) {
		r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true, Selector: map[string]string{"app": "web"}}, nil, nil)
		assert.True(t, r.filterResources(virtPDB.DeepCopy()))

		other := virtPDB.DeepCopy()
		other.Labels = map[string]string{"app": "db"}
		assert.False(t, r.filterResources(other))

		now := metav1.Now()
		other.DeletionTimestamp = &now
		assert.True(t, r.filterResources(other), "deletions always pass")

		disabled := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: false}, nil, nil)
		assert.False(t, disabled.filterResources(virtPDB.DeepCopy()))
	})
}

func TestNamespaceTemplateReconcile(t *testing.T) {
	scheme := newCRTestScheme(t)
	// any kind the fake client knows works; the template mechanics are kind-agnostic

	tpl := v1beta1.CustomResourceTemplate{Template: *rawJSON(t, map[string]any{
		"apiVersion": "policy/v1",
		"kind":       "PodDisruptionBudget",
		"metadata":   map[string]any{"name": "baseline", "annotations": map[string]any{"for": "$(VC_NAMESPACE) in $(VC_NAME)"}},
		"spec":       map[string]any{"minAvailable": 1, "selector": map[string]any{"matchLabels": map[string]any{"k3k.io/namespaceName": "$(VC_NAMESPACE)"}}},
	})}

	cluster := &v1beta1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "ns-1"},
		Spec: v1beta1.ClusterSpec{Sync: &v1beta1.SyncConfig{CustomResources: []v1beta1.CustomResourceSyncConfig{
			{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Enabled: true, PerNamespace: []v1beta1.CustomResourceTemplate{tpl}},
		}}},
	}

	teamA := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}

	r := &NamespaceTemplateReconciler{SyncerContext: &SyncerContext{
		HostClient:       fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build(),
		VirtualClient:    fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(teamA).Build(),
		Translator:       translate.ToHostTranslator{ClusterName: "mycluster", ClusterNamespace: "ns-1"},
		ClusterName:      "mycluster",
		ClusterNamespace: "ns-1",
	}}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "team-a"}}

	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	hostName := r.Translator.TranslateName("team-a", "baseline")

	var rendered policyv1.PodDisruptionBudget
	require.NoError(t, r.HostClient.Get(context.Background(), types.NamespacedName{Name: hostName, Namespace: "ns-1"}, &rendered))
	assert.Equal(t, "team-a in mycluster", rendered.Annotations["for"])
	assert.Equal(t, "team-a", rendered.Spec.Selector.MatchLabels[translate.NamespaceNameLabel])
	assert.Equal(t, "true", rendered.Labels[PerNamespaceLabel])
	assert.Equal(t, "team-a", rendered.Labels[translate.NamespaceNameLabel])
	assert.Equal(t, "mycluster", rendered.Labels[translate.ClusterNameLabel])
	assert.Len(t, rendered.OwnerReferences, 1)

	// second pass is an update, not a conflict
	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	// namespace gone -> rendered object removed
	require.NoError(t, r.VirtualClient.Delete(context.Background(), teamA))

	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	err = r.HostClient.Get(context.Background(), types.NamespacedName{Name: hostName, Namespace: "ns-1"}, &rendered)
	assert.True(t, apierrors.IsNotFound(err), "rendered object must be removed with its namespace")
}

func TestNamespaceTemplateRenderRejectsWrongKind(t *testing.T) {
	scheme := newCRTestScheme(t)
	r := &NamespaceTemplateReconciler{SyncerContext: &SyncerContext{
		HostClient:       fake.NewClientBuilder().WithScheme(scheme).Build(),
		Translator:       translate.ToHostTranslator{ClusterName: "mycluster", ClusterNamespace: "ns-1"},
		ClusterName:      "mycluster",
		ClusterNamespace: "ns-1",
	}}

	tpl := v1beta1.CustomResourceTemplate{Template: *rawJSON(t, map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "x"},
	})}

	_, err := r.render(context.Background(), pdbGVK, tpl, "team-a")
	require.Error(t, err)

	noName := v1beta1.CustomResourceTemplate{Template: *rawJSON(t, map[string]any{"spec": map[string]any{}})}

	_, err = r.render(context.Background(), pdbGVK, noName, "team-a")
	require.Error(t, err)
}
