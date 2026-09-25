package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func failedPod(created time.Time, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "webhook", Namespace: "cattle-system", Annotations: annotations,
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: corev1.PodStatus{
			Phase:   corev1.PodPending,
			Reason:  providerFailedReason,
			Message: "failed to create the host pod: connection refused",
		},
	}
}

func runPodRetry(t *testing.T, pod *corev1.Pod, now time.Time) (reconcile.Result, *corev1.Pod) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pod).Build()
	r := &PodRetryReconciler{VirtualClient: c, Interval: 2 * time.Minute, Now: func() time.Time { return now }}

	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}})
	require.NoError(t, err)

	var got corev1.Pod
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &got))

	return res, &got
}

func TestPodRetryStampsAfterInterval(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

	res, got := runPodRetry(t, failedPod(now.Add(-10*time.Minute), nil), now)

	assert.Equal(t, now.Format(time.RFC3339), got.Annotations[ProviderRetryAnnotation], "annotation change re-enqueues the pod in virtual-kubelet")
	assert.Equal(t, 2*time.Minute, res.RequeueAfter)
}

func TestPodRetryWaitsForInterval(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

	// fresh failure: the pod controller's own retries first
	res, got := runPodRetry(t, failedPod(now.Add(-30*time.Second), nil), now)
	assert.Empty(t, got.Annotations[ProviderRetryAnnotation])
	assert.Equal(t, 90*time.Second, res.RequeueAfter)

	// retried 1 minute ago
	last := map[string]string{ProviderRetryAnnotation: now.Add(-time.Minute).Format(time.RFC3339)}
	res, got = runPodRetry(t, failedPod(now.Add(-time.Hour), last), now)
	assert.Equal(t, last[ProviderRetryAnnotation], got.Annotations[ProviderRetryAnnotation])
	assert.Equal(t, time.Minute, res.RequeueAfter)
}

func TestPodRetryIgnoresOtherPods(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

	running := failedPod(now.Add(-time.Hour), nil)
	running.Status.Phase = corev1.PodRunning
	running.Status.Reason = ""

	res, got := runPodRetry(t, running, now)
	assert.Empty(t, got.Annotations)
	assert.Zero(t, res.RequeueAfter)

	neverRestart := failedPod(now.Add(-time.Hour), nil)
	neverRestart.Status.Phase = corev1.PodFailed // RestartPolicy Never: terminal by design

	_, got = runPodRetry(t, neverRestart, now)
	assert.Empty(t, got.Annotations)
}
