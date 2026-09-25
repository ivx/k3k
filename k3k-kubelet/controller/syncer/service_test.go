package syncer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

const (
	svcTestCluster   = "mycluster"
	svcTestNamespace = "ns-1"
)

func newSvcTestScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))

	return scheme
}

func svcTestClusterObj() *v1beta1.Cluster {
	return &v1beta1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: svcTestCluster, Namespace: svcTestNamespace, UID: "cluster-uid"},
		Spec:       v1beta1.ClusterSpec{Sync: &v1beta1.SyncConfig{Services: v1beta1.ServiceSyncConfig{Enabled: true}}},
	}
}

type svcTestEnv struct {
	r        *ServiceReconciler
	host     ctrlruntimeclient.Client
	virt     ctrlruntimeclient.Client
	recorder *record.FakeRecorder
}

func newSvcTestEnv(t *testing.T, hostObjs, virtObjs []runtime.Object, hostFuncs, virtFuncs *interceptor.Funcs) *svcTestEnv {
	scheme := newSvcTestScheme(t)

	hostBuilder := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(append(hostObjs, svcTestClusterObj())...).WithStatusSubresource(&corev1.Service{})
	if hostFuncs != nil {
		hostBuilder = hostBuilder.WithInterceptorFuncs(*hostFuncs)
	}

	virtBuilder := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(virtObjs...).WithStatusSubresource(&corev1.Service{})
	if virtFuncs != nil {
		virtBuilder = virtBuilder.WithInterceptorFuncs(*virtFuncs)
	}

	env := &svcTestEnv{
		host:     hostBuilder.Build(),
		virt:     virtBuilder.Build(),
		recorder: record.NewFakeRecorder(16),
	}

	env.r = &ServiceReconciler{SyncerContext: &SyncerContext{
		ClusterName:      svcTestCluster,
		ClusterNamespace: svcTestNamespace,
		HostClient:       env.host,
		VirtualClient:    env.virt,
		Translator:       translate.ToHostTranslator{ClusterName: svcTestCluster, ClusterNamespace: svcTestNamespace},
		Recorder:         env.recorder,
	}}

	return env
}

func (e *svcTestEnv) reconcile(t *testing.T, name, namespace string) error {
	t.Helper()

	_, err := e.r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}})

	return err
}

func (e *svcTestEnv) hostCopy(t *testing.T, name, namespace string) (*corev1.Service, error) {
	t.Helper()

	var svc corev1.Service

	err := e.host.Get(context.Background(), types.NamespacedName{Name: e.r.Translator.TranslateName(namespace, name), Namespace: svcTestNamespace}, &svc)

	return &svc, err
}

func (e *svcTestEnv) events() []string {
	var out []string

	for {
		select {
		case ev := <-e.recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func virtSvc(name, namespace, clusterIP string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("virt-" + name)},
		Spec: corev1.ServiceSpec{
			Type:       corev1.ServiceTypeClusterIP,
			ClusterIP:  clusterIP,
			ClusterIPs: []string{clusterIP},
			Ports:      []corev1.ServicePort{{Name: "http", Port: 80}},
		},
	}
}

// hostCopyOf builds the host copy a previous syncer run left behind.
func hostCopyOf(t *testing.T, v *corev1.Service, clusterIP string) *corev1.Service {
	t.Helper()

	tr := translate.ToHostTranslator{ClusterName: svcTestCluster, ClusterNamespace: svcTestNamespace}
	h := v.DeepCopy()
	tr.TranslateTo(h)
	h.UID = types.UID("host-" + v.Name)
	h.Spec.ClusterIP = clusterIP
	h.Spec.ClusterIPs = []string{clusterIP}
	h.OwnerReferences = []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "Cluster", Name: svcTestCluster, UID: "cluster-uid"}}

	return h
}

func allocatedErr(path *field.Path, value any, msg string) error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Service"}, "svc", field.ErrorList{field.Invalid(path, value, msg)})
}

func TestServiceSyncCreatesHostCopyWithHash(t *testing.T) {
	env := newSvcTestEnv(t, nil, []runtime.Object{virtSvc("web", "app", "10.0.0.5")}, nil, nil)

	require.NoError(t, env.reconcile(t, "web", "app"))

	host, err := env.hostCopy(t, "web", "app")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.5", host.Spec.ClusterIP)
	assert.NotEmpty(t, host.Annotations[ContentHashAnnotation])
	assert.Equal(t, svcTestCluster, host.Labels[translate.ClusterNameLabel])
	assert.True(t, env.r.isSyncedCopy(host), "a created copy must match the sweeper's criteria")
}

func TestServiceSyncSkipsUnchangedCopy(t *testing.T) {
	v := virtSvc("web", "app", "10.0.0.5")
	env := newSvcTestEnv(t, nil, []runtime.Object{v}, nil, nil)

	require.NoError(t, env.reconcile(t, "web", "app"))

	// a host controller adds an annotation; with an unchanged virtual side the
	// syncer must not write (no fight with host controllers)
	host, err := env.hostCopy(t, "web", "app")
	require.NoError(t, err)

	host.Annotations["host.example/owned"] = "yes"
	require.NoError(t, env.host.Update(context.Background(), host))

	require.NoError(t, env.reconcile(t, "web", "app"))

	host, err = env.hostCopy(t, "web", "app")
	require.NoError(t, err)
	assert.Equal(t, "yes", host.Annotations["host.example/owned"])

	// a change on the virtual side is written
	var cur corev1.Service
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "web", Namespace: "app"}, &cur))
	cur.Labels = map[string]string{"tier": "front"}
	require.NoError(t, env.virt.Update(context.Background(), &cur))

	require.NoError(t, env.reconcile(t, "web", "app"))

	host, err = env.hostCopy(t, "web", "app")
	require.NoError(t, err)
	assert.Equal(t, "front", host.Labels["tier"])
}

func TestServiceSyncRecreatesCopyWithStaleClusterIP(t *testing.T) {
	v := virtSvc("rancher-webhook", "cattle-system", "10.195.114.70")
	stale := hostCopyOf(t, v, "10.195.94.5")

	env := newSvcTestEnv(t, []runtime.Object{stale}, []runtime.Object{v}, nil, nil)

	// pass 1: the stale copy is removed and the event names both IPs
	require.NoError(t, env.reconcile(t, "rancher-webhook", "cattle-system"))

	_, err := env.hostCopy(t, "rancher-webhook", "cattle-system")
	assert.True(t, apierrors.IsNotFound(err), "stale copy must be deleted, got %v", err)

	events := env.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], ReasonHostCopyRecreated)
	assert.Contains(t, events[0], "10.195.94.5")
	assert.Contains(t, events[0], "10.195.114.70")

	// pass 2 (the host delete event requeues): a copy with the virtual IP
	require.NoError(t, env.reconcile(t, "rancher-webhook", "cattle-system"))

	host, err := env.hostCopy(t, "rancher-webhook", "cattle-system")
	require.NoError(t, err)
	assert.Equal(t, "10.195.114.70", host.Spec.ClusterIP)
}

func TestServiceSyncClusterIPConflictReallocatesVirtualService(t *testing.T) {
	v := virtSvc("db", "app", "10.195.0.42")
	v.Finalizers = []string{serviceFinalizerName}
	v.Labels = map[string]string{"app": "db"}

	hostFuncs := &interceptor.Funcs{
		Create: func(ctx context.Context, c ctrlruntimeclient.WithWatch, obj ctrlruntimeclient.Object, opts ...ctrlruntimeclient.CreateOption) error {
			if svc, ok := obj.(*corev1.Service); ok && svc.Spec.ClusterIP == "10.195.0.42" {
				return allocatedErr(field.NewPath("spec", "clusterIPs").Index(0), svc.Spec.ClusterIPs,
					"failed to allocate IP 10.195.0.42: provided IP is already allocated")
			}

			return c.Create(ctx, obj, opts...)
		},
	}

	var deleted, created int

	virtFuncs := &interceptor.Funcs{
		Delete: func(ctx context.Context, c ctrlruntimeclient.WithWatch, obj ctrlruntimeclient.Object, opts ...ctrlruntimeclient.DeleteOption) error {
			deleted++
			return c.Delete(ctx, obj, opts...)
		},
		Create: func(ctx context.Context, c ctrlruntimeclient.WithWatch, obj ctrlruntimeclient.Object, opts ...ctrlruntimeclient.CreateOption) error {
			created++
			return c.Create(ctx, obj, opts...)
		},
	}

	env := newSvcTestEnv(t, nil, []runtime.Object{v}, hostFuncs, virtFuncs)

	require.NoError(t, env.reconcile(t, "db", "app"))

	assert.Equal(t, 1, deleted, "the virtual service is deleted once")
	assert.Equal(t, 1, created, "and created again")

	var fresh corev1.Service
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "db", Namespace: "app"}, &fresh))
	assert.Empty(t, fresh.Spec.ClusterIP, "the new virtual service must not request the rejected IP")
	assert.Empty(t, fresh.Spec.ClusterIPs)
	assert.Equal(t, "db", fresh.Labels["app"])
	assert.Equal(t, []corev1.ServicePort{{Name: "http", Port: 80}}, fresh.Spec.Ports)

	_, err := env.hostCopy(t, "db", "app")
	assert.True(t, apierrors.IsNotFound(err))

	events := env.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "ClusterIPConflict")
	assert.Contains(t, events[0], "10.195.0.42")
}

func TestServiceSyncNodePortConflictClearsPort(t *testing.T) {
	v := virtSvc("web", "app", "10.0.0.5")
	v.Spec.Type = corev1.ServiceTypeNodePort
	v.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 80, NodePort: 30080}, {Name: "https", Port: 443, NodePort: 30443}}

	hostFuncs := &interceptor.Funcs{
		Create: func(ctx context.Context, c ctrlruntimeclient.WithWatch, obj ctrlruntimeclient.Object, opts ...ctrlruntimeclient.CreateOption) error {
			if svc, ok := obj.(*corev1.Service); ok && svc.Spec.Ports[1].NodePort == 30443 {
				return allocatedErr(field.NewPath("spec", "ports").Index(1).Child("nodePort"), 30443, "provided port is already allocated")
			}

			return c.Create(ctx, obj, opts...)
		},
	}

	env := newSvcTestEnv(t, nil, []runtime.Object{v}, hostFuncs, nil)

	require.NoError(t, env.reconcile(t, "web", "app"))

	var cur corev1.Service
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "web", Namespace: "app"}, &cur))
	assert.Equal(t, int32(30080), cur.Spec.Ports[0].NodePort, "an accepted port stays")
	assert.Equal(t, int32(0), cur.Spec.Ports[1].NodePort, "the rejected port is cleared for reallocation")

	events := env.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "NodePortConflict")
	assert.Contains(t, events[0], "30443")
}

func TestServiceSyncReportsOtherRejections(t *testing.T) {
	v := virtSvc("web", "app", "10.0.0.5")

	hostFuncs := &interceptor.Funcs{
		Create: func(ctx context.Context, c ctrlruntimeclient.WithWatch, obj ctrlruntimeclient.Object, opts ...ctrlruntimeclient.CreateOption) error {
			if _, ok := obj.(*corev1.Service); ok {
				return allocatedErr(field.NewPath("spec", "clusterIPs").Index(0), "10.0.0.5",
					"failed to allocate IP 10.0.0.5: the provided IP (10.0.0.5) is not in the valid range")
			}

			return c.Create(ctx, obj, opts...)
		},
	}

	env := newSvcTestEnv(t, nil, []runtime.Object{v}, hostFuncs, nil)

	err := env.reconcile(t, "web", "app")
	require.Error(t, err, "a rejection that reallocation cannot fix is retried")

	events := env.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], ReasonSyncFailed)
	assert.Contains(t, events[0], "not in the valid range")

	// the virtual service is left alone
	var cur corev1.Service
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "web", Namespace: "app"}, &cur))
	assert.Equal(t, "10.0.0.5", cur.Spec.ClusterIP)
}

func TestServiceSyncCopiesLoadBalancerStatus(t *testing.T) {
	v := virtSvc("gw", "app", "10.0.0.7")
	v.Spec.Type = corev1.ServiceTypeLoadBalancer

	env := newSvcTestEnv(t, nil, []runtime.Object{v}, nil, nil)
	require.NoError(t, env.reconcile(t, "gw", "app"))

	host, err := env.hostCopy(t, "gw", "app")
	require.NoError(t, err)

	host.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}}
	require.NoError(t, env.host.Status().Update(context.Background(), host))

	// the host status change requeues the virtual service (host watch)
	require.NoError(t, env.reconcile(t, "gw", "app"))

	var cur corev1.Service
	require.NoError(t, env.virt.Get(context.Background(), types.NamespacedName{Name: "gw", Namespace: "app"}, &cur))
	require.Len(t, cur.Status.LoadBalancer.Ingress, 1)
	assert.Equal(t, "203.0.113.10", cur.Status.LoadBalancer.Ingress[0].IP)
}

func TestMapHostToVirtual(t *testing.T) {
	sc := &SyncerContext{ClusterName: svcTestCluster}

	v := virtSvc("web", "app", "10.0.0.5")
	host := hostCopyOf(t, v, "10.0.0.5")

	reqs := sc.mapHostToVirtual(context.Background(), host)
	require.Len(t, reqs, 1)
	assert.Equal(t, types.NamespacedName{Name: "web", Namespace: "app"}, reqs[0].NamespacedName)

	other := host.DeepCopy()
	other.Labels[translate.ClusterNameLabel] = "other"
	assert.Empty(t, sc.mapHostToVirtual(context.Background(), other), "copies of other clusters are ignored")

	controllerSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "k3k-mycluster-kube-dns", Namespace: svcTestNamespace}}
	assert.Empty(t, sc.mapHostToVirtual(context.Background(), controllerSvc), "controller objects are ignored")
}

func TestOrphanSweep(t *testing.T) {
	scheme := newSvcTestScheme(t)

	live := virtSvc("live", "app", "10.0.0.1")
	gone := virtSvc("gone", "app", "10.0.0.2")

	liveCopy := hostCopyOf(t, live, "10.0.0.1")
	goneCopy := hostCopyOf(t, gone, "10.0.0.2")

	// k3k controller object: owned by the Cluster, but no translation markers
	controllerSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "k3k-mycluster-kube-dns", Namespace: svcTestNamespace,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "Cluster", Name: svcTestCluster}},
	}}

	// host operator object that copied the markers but is not owned by the Cluster
	operatorSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "claim-conn", Namespace: svcTestNamespace,
		Labels:      map[string]string{translate.ClusterNameLabel: svcTestCluster},
		Annotations: map[string]string{translate.ResourceNameAnnotation: "claim", translate.ResourceNamespaceAnnotation: "app"},
	}}

	// orphaned PVC: holds data, reported only
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-gone", Namespace: svcTestNamespace,
		Labels:          map[string]string{translate.ClusterNameLabel: svcTestCluster},
		Annotations:     map[string]string{translate.ResourceNameAnnotation: "data", translate.ResourceNamespaceAnnotation: "app"},
		OwnerReferences: liveCopy.OwnerReferences,
	}}

	host := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(liveCopy, goneCopy, controllerSvc, operatorSecret, pvc).Build()
	virt := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(live).Build()

	sweeper := &OrphanSweeper{
		SyncerContext: &SyncerContext{ClusterName: svcTestCluster, ClusterNamespace: svcTestNamespace, HostClient: host, VirtualClient: virt},
		HostReader:    host,
		VirtualReader: virt,
		Kinds:         BuiltinSweptKinds(),
	}

	require.NoError(t, sweeper.Sweep(context.Background()))

	exists := func(obj ctrlruntimeclient.Object, name string) bool {
		err := host.Get(context.Background(), types.NamespacedName{Name: name, Namespace: svcTestNamespace}, obj)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}

		return err == nil
	}

	assert.True(t, exists(&corev1.Service{}, liveCopy.Name), "copy of a live object stays")
	assert.False(t, exists(&corev1.Service{}, goneCopy.Name), "orphaned copy is deleted")
	assert.True(t, exists(&corev1.Service{}, controllerSvc.Name), "controller objects stay")
	assert.True(t, exists(&corev1.Secret{}, operatorSecret.Name), "objects not owned by the Cluster stay")
	assert.True(t, exists(&corev1.PersistentVolumeClaim{}, pvc.Name), "orphaned PVCs are only reported")
}

func TestAllocationConflicts(t *testing.T) {
	ip, ports := allocationConflicts(allocatedErr(field.NewPath("spec", "clusterIPs").Index(0), "x", "failed to allocate IP 10.0.0.1: provided IP is already allocated"))
	assert.True(t, ip)
	assert.Empty(t, ports)

	ip, ports = allocationConflicts(allocatedErr(field.NewPath("spec", "ports").Index(2).Child("nodePort"), 30000, "provided port is already allocated"))
	assert.False(t, ip)
	assert.Equal(t, []int{2}, ports)

	ip, ports = allocationConflicts(apierrors.NewConflict(schema.GroupResource{Resource: "services"}, "x", nil))
	assert.False(t, ip)
	assert.Empty(t, ports)

	ip, _ = allocationConflicts(allocatedErr(field.NewPath("spec", "clusterIPs").Index(0), "x", strings.Repeat("no", 3)))
	assert.False(t, ip, "only 'already allocated' counts")
}
