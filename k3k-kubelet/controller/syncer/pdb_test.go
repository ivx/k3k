package syncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

func maxUnavailablePDB(maxUnavailable intstr.IntOrString, generation, observed int64, desiredHealthy int32) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "cortex", Generation: generation},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
		},
		Status: policyv1.PodDisruptionBudgetStatus{
			ObservedGeneration: observed,
			ExpectedPods:       desiredHealthy + 1,
			DesiredHealthy:     desiredHealthy,
			CurrentHealthy:     desiredHealthy + 1,
			DisruptionsAllowed: 1,
		},
	}
}

func reconcileHostPDB(t *testing.T, r *CustomResourceReconciler) *policyv1.PodDisruptionBudget {
	t.Helper()

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "redis", Namespace: "cortex"}})
	require.NoError(t, err)

	var hostPDB policyv1.PodDisruptionBudget
	require.NoError(t, r.HostClient.Get(context.Background(),
		types.NamespacedName{Name: r.Translator.TranslateName("cortex", "redis"), Namespace: "ns-1"}, &hostPDB))

	return &hostPDB
}

func TestPDBMaxUnavailableBecomesMinAvailable(t *testing.T) {
	virtPDB := maxUnavailablePDB(intstr.FromInt32(1), 1, 1, 2)
	r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB})

	hostPDB := reconcileHostPDB(t, r)

	assert.Nil(t, hostPDB.Spec.MaxUnavailable, "the host cannot count pods for maxUnavailable")
	require.NotNil(t, hostPDB.Spec.MinAvailable)
	assert.Equal(t, intstr.FromInt32(2), *hostPDB.Spec.MinAvailable, "minAvailable = virtual desiredHealthy")
}

func TestPDBFollowsVirtualScale(t *testing.T) {
	virtPDB := maxUnavailablePDB(intstr.FromInt32(1), 1, 1, 2)
	r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB})

	reconcileHostPDB(t, r)

	// the workload scales 3 → 5: the virtual controller updates the status
	var cur policyv1.PodDisruptionBudget
	require.NoError(t, r.VirtualClient.Get(context.Background(), types.NamespacedName{Name: "redis", Namespace: "cortex"}, &cur))
	cur.Status.DesiredHealthy = 4
	cur.Status.ExpectedPods = 5
	require.NoError(t, r.VirtualClient.Status().Update(context.Background(), &cur))

	hostPDB := reconcileHostPDB(t, r)
	assert.Equal(t, intstr.FromInt32(4), *hostPDB.Spec.MinAvailable)
}

func TestPDBPercentageBecomesMinAvailable(t *testing.T) {
	pct := intstr.FromString("50%")
	virtPDB := maxUnavailablePDB(intstr.FromInt32(0), 1, 1, 3)
	virtPDB.Spec.MaxUnavailable = nil
	virtPDB.Spec.MinAvailable = &pct

	r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB})

	hostPDB := reconcileHostPDB(t, r)
	assert.Equal(t, intstr.FromInt32(3), *hostPDB.Spec.MinAvailable)
}

func TestPDBIntegerMinAvailableUnchanged(t *testing.T) {
	two := intstr.FromInt32(2)
	virtPDB := maxUnavailablePDB(intstr.FromInt32(0), 1, 1, 7)
	virtPDB.Spec.MaxUnavailable = nil
	virtPDB.Spec.MinAvailable = &two

	r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB})

	hostPDB := reconcileHostPDB(t, r)
	assert.Equal(t, two, *hostPDB.Spec.MinAvailable, "an integer minAvailable already works on the host")
}

func TestPDBStaleStatusPassesThrough(t *testing.T) {
	// spec changed (generation 2), the virtual controller has not caught up:
	// never derive a budget from a status of an older spec
	virtPDB := maxUnavailablePDB(intstr.FromInt32(1), 2, 1, 0)
	r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB})

	hostPDB := reconcileHostPDB(t, r)
	assert.Nil(t, hostPDB.Spec.MinAvailable)
	require.NotNil(t, hostPDB.Spec.MaxUnavailable, "pass-through blocks evictions on the host, as before")
}

func TestPDBVirtualSyncFailedPassesThrough(t *testing.T) {
	virtPDB := maxUnavailablePDB(intstr.FromInt32(1), 1, 1, 0)
	virtPDB.Status.Conditions = []metav1.Condition{{
		Type: policyv1.DisruptionAllowedCondition, Status: metav1.ConditionFalse, Reason: policyv1.SyncFailedReason,
	}}

	r := newPDBReconciler(t, v1beta1.PodDisruptionBudgetSyncConfig{Enabled: true}, nil, []runtime.Object{virtPDB})

	hostPDB := reconcileHostPDB(t, r)
	assert.Nil(t, hostPDB.Spec.MinAvailable, "a desiredHealthy of 0 from a failed count must not open the budget")
	require.NotNil(t, hostPDB.Spec.MaxUnavailable)
}
