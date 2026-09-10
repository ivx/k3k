package syncer

import (
	"context"
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

func rawJSON(t *testing.T, v any) *runtime.RawExtension {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	return &runtime.RawExtension{Raw: b}
}

func TestApplyPatchAddCreatesIntermediateMaps(t *testing.T) {
	obj := map[string]any{"spec": map[string]any{}}

	if err := applyPatch(obj, "add", "/spec/template/spec/dnsPolicy", "None"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _, _ := unstructured.NestedString(obj, "spec", "template", "spec", "dnsPolicy")
	if got != "None" {
		t.Fatalf("expected None, got %q", got)
	}
}

func TestApplyPatchReplaceMissingPathFails(t *testing.T) {
	obj := map[string]any{"spec": map[string]any{}}

	if err := applyPatch(obj, "replace", "/spec/missing/leaf", "x"); err == nil {
		t.Fatal("expected error for replace on a missing path")
	}
}

func TestApplyPatchSliceAppend(t *testing.T) {
	obj := map[string]any{
		"spec": map[string]any{
			"args": []any{"a"},
		},
	}

	if err := applyPatch(obj, "add", "/spec/args/-", "b"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	args, _, _ := unstructured.NestedSlice(obj, "spec", "args")
	if len(args) != 2 || args[1] != "b" {
		t.Fatalf("expected appended slice, got %v", args)
	}
}

func TestApplyPatchUnsupportedOp(t *testing.T) {
	if err := applyPatch(map[string]any{}, "remove", "/spec", nil); err == nil {
		t.Fatal("expected error for unsupported op")
	}
}

// The full down-translation: name/namespace collapse, labels, patches with
// variable substitution ($(VC_DNS) from the host kube-dns service), and
// status stripping.
func TestTranslatedAppliesPatchesAndSubstitution(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	dnsSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "k3k-mycluster-kube-dns", Namespace: "host-ns"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.96.99.99"},
	}

	hostClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(dnsSvc).Build()

	r := &CustomResourceReconciler{
		SyncerContext: &SyncerContext{
			ClusterName:      "mycluster",
			ClusterNamespace: "host-ns",
			HostClient:       hostClient,
			Translator: translate.ToHostTranslator{
				ClusterName:      "mycluster",
				ClusterNamespace: "host-ns",
			},
		},
		GVK: schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachine"},
	}

	virtObj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata": map[string]any{
			"name":      "testvm",
			"namespace": "default",
		},
		"spec":   map[string]any{"runStrategy": "Always"},
		"status": map[string]any{"ready": true},
	}}

	cfg := &v1beta1.CustomResourceSyncConfig{
		APIVersion: "kubevirt.io/v1",
		Kind:       "VirtualMachine",
		Enabled:    true,
		Patches: []v1beta1.CustomResourcePatch{
			{Op: "add", Path: "/spec/template/spec/dnsPolicy", Value: rawJSON(t, "None")},
			{Op: "add", Path: "/spec/template/spec/dnsConfig", Value: rawJSON(t, map[string]any{
				"nameservers": []string{"$(VC_DNS)"},
				"searches":    []string{"default.svc.cluster.local"},
			})},
			{Op: "add", Path: "/metadata/annotations/vc-name", Value: rawJSON(t, "$(VC_NAME)")},
		},
	}

	hostObj, err := r.translated(context.Background(), virtObj, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hostObj.GetNamespace() != "host-ns" {
		t.Fatalf("expected host namespace, got %q", hostObj.GetNamespace())
	}

	if hostObj.GetLabels()[translate.ClusterNameLabel] != "mycluster" {
		t.Fatal("cluster name label missing on host object")
	}

	if _, ok := hostObj.Object["status"]; ok {
		t.Fatal("status must not flow down")
	}

	ns, _, _ := unstructured.NestedStringSlice(hostObj.Object, "spec", "template", "spec", "dnsConfig", "nameservers")
	if len(ns) != 1 || ns[0] != "10.96.99.99" {
		t.Fatalf("expected substituted VC_DNS, got %v", ns)
	}

	if hostObj.GetAnnotations()["vc-name"] != "mycluster" {
		t.Fatalf("expected substituted VC_NAME, got %q", hostObj.GetAnnotations()["vc-name"])
	}

	// the virtual object must be untouched by translation
	if _, ok := virtObj.Object["status"]; !ok {
		t.Fatal("virtual object was mutated")
	}
}
