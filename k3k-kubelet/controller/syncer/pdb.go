package syncer

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var podDisruptionBudgetGVK = schema.GroupVersionKind{Group: "policy", Version: "v1", Kind: "PodDisruptionBudget"}

// translatePodDisruptionBudget rewrites the budget of a host PDB copy into an
// integer minAvailable.
//
// On the host, the controller of every synced pod is the k3k Cluster object,
// which has no scale subresource. The host disruption controller therefore
// cannot count the expected pods, and a maxUnavailable (or percentage) budget
// never allows an eviction (SyncFailed, expectedPods 0). The disruption
// controller of the virtual cluster sees the real workload controllers and
// computes status.desiredHealthy; an integer minAvailable of that value gives
// the host the same budget without a scale lookup.
//
// The rewrite only uses a status that belongs to the current spec
// (observedGeneration) and that the virtual controller could compute. In all
// other cases the spec passes through unchanged, which blocks evictions on
// the host until the status is known — never more disruptions than before.
func translatePodDisruptionBudget(virtObj, hostObj *unstructured.Unstructured) error {
	var virtPDB policyv1.PodDisruptionBudget
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(virtObj.Object, &virtPDB); err != nil {
		return err
	}

	if !needsBudgetTranslation(&virtPDB) || !budgetStatusCurrent(&virtPDB) {
		return nil
	}

	spec, ok := hostObj.Object["spec"].(map[string]any)
	if !ok {
		return nil
	}

	delete(spec, "maxUnavailable")
	spec["minAvailable"] = int64(virtPDB.Status.DesiredHealthy)

	return nil
}

// needsBudgetTranslation reports whether the host needs the pod count: every
// budget except an integer minAvailable.
func needsBudgetTranslation(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb.Spec.MaxUnavailable != nil {
		return true
	}

	return pdb.Spec.MinAvailable != nil && pdb.Spec.MinAvailable.Type == intstr.String // a percentage
}

// budgetStatusCurrent reports whether the virtual disruption controller has
// computed the status for the current spec.
func budgetStatusCurrent(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb.Status.ObservedGeneration != pdb.Generation {
		return false
	}

	for _, c := range pdb.Status.Conditions {
		if c.Type == policyv1.DisruptionAllowedCondition && c.Status == metav1.ConditionFalse && c.Reason == policyv1.SyncFailedReason {
			return false
		}
	}

	return true
}
