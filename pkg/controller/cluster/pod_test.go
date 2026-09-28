package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const podTestVC = "vc"

func podTestScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))

	return scheme
}

func hostPodFor(virtName, virtNS, virtUID string) *corev1.Pod {
	yes := true

	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: virtName + "-host", Namespace: podTestVC,
		Labels:          map[string]string{translate.ClusterNameLabel: podTestVC},
		Annotations:     map[string]string{translate.ResourceNameAnnotation: virtName, translate.ResourceNamespaceAnnotation: virtNS, translate.VirtualUIDAnnotation: virtUID},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "Cluster", Name: podTestVC, Controller: &yes}},
		Finalizers:      []string{"test/keep"}, // keeps the host Pod while it terminates
	}}
}

func virtPodWithGrace(name, ns, uid string) *corev1.Pod {
	grace := int64(900)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid), Finalizers: []string{"test/keep"}},
		Spec:       corev1.PodSpec{TerminationGracePeriodSeconds: &grace},
	}
}

type podTestEnv struct {
	r    *PodReconciler
	host ctrlruntimeclient.Client
	virt ctrlruntimeclient.Client
}

func newPodTestEnv(t *testing.T, hostObjs, virtObjs []runtime.Object) *podTestEnv {
	scheme := podTestScheme(t)
	env := &podTestEnv{
		host: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(hostObjs...).Build(),
		virt: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(virtObjs...).Build(),
	}
	env.r = &PodReconciler{
		Client: env.host,
		VirtualClientFor: func(context.Context, ctrlruntimeclient.Client, string, string) (ctrlruntimeclient.Client, error) {
			return env.virt, nil
		},
	}

	return env
}

func (e *podTestEnv) reconcile(t *testing.T, hostName string) {
	t.Helper()

	_, err := e.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: hostName, Namespace: podTestVC}})
	require.NoError(t, err)
}

// deleteHostPod starts the graceful deletion of the host Pod and then
// removes it completely (the containers stopped).
func (e *podTestEnv) startHostDeletion(t *testing.T, hostName string) {
	t.Helper()

	var hp corev1.Pod
	require.NoError(t, e.host.Get(context.Background(), types.NamespacedName{Name: hostName, Namespace: podTestVC}, &hp))
	require.NoError(t, e.host.Delete(context.Background(), &hp))
}

func (e *podTestEnv) finishHostDeletion(t *testing.T, hostName string) {
	t.Helper()

	var hp corev1.Pod
	require.NoError(t, e.host.Get(context.Background(), types.NamespacedName{Name: hostName, Namespace: podTestVC}, &hp))
	hp.Finalizers = nil
	require.NoError(t, e.host.Update(context.Background(), &hp))
}

func TestPodControllerRemovesVirtualPodWhenHostPodIsGone(t *testing.T) {
	env := newPodTestEnv(t, []runtime.Object{hostPodFor("rabbit-2", "rabbitmq", "uid-old")}, []runtime.Object{virtPodWithGrace("rabbit-2", "rabbitmq", "uid-old")})

	var deletes []ctrlruntimeclient.DeleteOptions

	env.virt = interceptor.NewClient(env.virt.(ctrlruntimeclient.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c ctrlruntimeclient.WithWatch, obj ctrlruntimeclient.Object, opts ...ctrlruntimeclient.DeleteOption) error {
			o := ctrlruntimeclient.DeleteOptions{}
			o.ApplyOptions(opts)
			deletes = append(deletes, o)

			return c.Delete(ctx, obj, opts...)
		},
	})

	// drain: the host Pod is evicted → the virtual Pod is deleted (graceful)
	env.startHostDeletion(t, "rabbit-2-host")
	env.reconcile(t, "rabbit-2-host")

	require.Len(t, deletes, 1)
	assert.Nil(t, deletes[0].GracePeriodSeconds, "first delete: the Pod's own grace period")

	// the host Pod is gone: the virtual Pod does not wait for its 900 s
	env.finishHostDeletion(t, "rabbit-2-host")
	env.reconcile(t, "rabbit-2-host")

	require.Len(t, deletes, 2)
	require.NotNil(t, deletes[1].GracePeriodSeconds)
	assert.Equal(t, int64(0), *deletes[1].GracePeriodSeconds)
	require.NotNil(t, deletes[1].Preconditions)
	assert.Equal(t, types.UID("uid-old"), *deletes[1].Preconditions.UID, "only the incarnation of the host Pod")

	// the entry is consumed: a second NotFound does nothing
	env.reconcile(t, "rabbit-2-host")
	assert.Len(t, deletes, 2)
}

func TestPodControllerKeepsReplacementPod(t *testing.T) {
	env := newPodTestEnv(t, []runtime.Object{hostPodFor("rabbit-2", "rabbitmq", "uid-old")}, []runtime.Object{virtPodWithGrace("rabbit-2", "rabbitmq", "uid-old")})

	env.startHostDeletion(t, "rabbit-2-host")
	env.reconcile(t, "rabbit-2-host")

	// the old virtual Pod went away by itself, the StatefulSet created a new one
	var old corev1.Pod
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "rabbit-2", Namespace: "rabbitmq"}, &old))
	old.Finalizers = nil
	require.NoError(t, env.virt.Update(context.Background(), &old))

	require.Eventually(t, func() bool {
		return apierrors.IsNotFound(env.virt.Get(context.Background(), types.NamespacedName{Name: "rabbit-2", Namespace: "rabbitmq"}, &corev1.Pod{}))
	}, time.Second, 10*time.Millisecond)

	replacement := virtPodWithGrace("rabbit-2", "rabbitmq", "uid-new")
	replacement.Finalizers = nil
	require.NoError(t, env.virt.Create(context.Background(), replacement))

	env.finishHostDeletion(t, "rabbit-2-host")
	env.reconcile(t, "rabbit-2-host")

	var cur corev1.Pod
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "rabbit-2", Namespace: "rabbitmq"}, &cur))
	assert.Equal(t, types.UID("uid-new"), cur.UID)
	assert.True(t, cur.DeletionTimestamp.IsZero(), "the replacement Pod is not touched")
}

func TestPodControllerIgnoresUnknownHostPod(t *testing.T) {
	env := newPodTestEnv(t, nil, []runtime.Object{virtPodWithGrace("web", "app", "uid-1")})

	// a host Pod this controller never saw terminating: nothing to do
	env.reconcile(t, "unknown-host")

	var vp corev1.Pod
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "web", Namespace: "app"}, &vp))
	assert.True(t, vp.DeletionTimestamp.IsZero())
}
