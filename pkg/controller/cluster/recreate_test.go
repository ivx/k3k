package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func serverSTS(policy appsv1.PodManagementPolicyType) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "k3k-vc-server", Namespace: "vc", UID: "sts-uid", Finalizers: []string{etcdPodFinalizerName}},
		Spec:       appsv1.StatefulSetSpec{PodManagementPolicy: policy},
	}
}

func TestRecreateServerStatefulSetOnPolicyChange(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))

	var propagation *metav1.DeletionPropagation

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(serverSTS(appsv1.OrderedReadyPodManagement)).
		WithInterceptorFuncs(interceptor.Funcs{Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			o := &client.DeleteOptions{}
			o.ApplyOptions(opts)
			propagation = o.PropagationPolicy

			return cl.Delete(ctx, obj, opts...)
		}}).Build()

	r := &ClusterReconciler{Client: c}
	expected := serverSTS(appsv1.ParallelPodManagement)

	recreating, err := r.recreateServerStatefulSetIfImmutableChanged(context.Background(), expected)
	assert.True(t, recreating)
	require.ErrorIs(t, err, errServerStatefulSetRecreating)
	require.NotNil(t, propagation)
	assert.Equal(t, metav1.DeletePropagationOrphan, *propagation, "the server pods must keep running")

	// the finalizer holds the old StatefulSet: keep waiting, no second delete
	propagation = nil
	recreating, err = r.recreateServerStatefulSetIfImmutableChanged(context.Background(), expected)
	assert.True(t, recreating)
	require.ErrorIs(t, err, errServerStatefulSetRecreating)
	assert.Nil(t, propagation)
}

func TestRecreateServerStatefulSetNoop(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))

	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(serverSTS(appsv1.ParallelPodManagement)).Build()}

	recreating, err := r.recreateServerStatefulSetIfImmutableChanged(context.Background(), serverSTS(appsv1.ParallelPodManagement))
	require.NoError(t, err)
	assert.False(t, recreating)

	// no StatefulSet yet: CreateOrUpdate creates it
	r = &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	recreating, err = r.recreateServerStatefulSetIfImmutableChanged(context.Background(), serverSTS(appsv1.ParallelPodManagement))
	require.NoError(t, err)
	assert.False(t, recreating)
}
