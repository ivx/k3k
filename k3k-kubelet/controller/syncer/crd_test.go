package syncer

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

func testCRD(name, group, kind string, versions ...string) apiextensionsv1.CustomResourceDefinition {
	crd := apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: kind, Plural: name[:len(name)-len(group)-1]},
			Scope: apiextensionsv1.NamespaceScoped,
			Conversion: &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.WebhookConverter,
				Webhook:  &apiextensionsv1.WebhookConversion{ConversionReviewVersions: []string{"v1"}},
			},
		},
	}

	for _, v := range versions {
		crd.Spec.Versions = append(crd.Spec.Versions, apiextensionsv1.CustomResourceDefinitionVersion{Name: v, Served: true})
	}

	return crd
}

func TestFindCRD(t *testing.T) {
	crds := []apiextensionsv1.CustomResourceDefinition{
		testCRD("ciliumnetworkpolicies.cilium.io", "cilium.io", "CiliumNetworkPolicy", "v2"),
		testCRD("virtualmachines.kubevirt.io", "kubevirt.io", "VirtualMachine", "v1alpha3", "v1"),
	}

	tests := []struct {
		name string
		gvk  schema.GroupVersionKind
		want string
	}{
		{"exact match", schema.GroupVersionKind{Group: "cilium.io", Version: "v2", Kind: "CiliumNetworkPolicy"}, "ciliumnetworkpolicies.cilium.io"},
		{"second served version", schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachine"}, "virtualmachines.kubevirt.io"},
		{"version not served", schema.GroupVersionKind{Group: "cilium.io", Version: "v1", Kind: "CiliumNetworkPolicy"}, ""},
		{"kind not found", schema.GroupVersionKind{Group: "cilium.io", Version: "v2", Kind: "CiliumEndpoint"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findCRD(crds, tt.gvk)
			if tt.want == "" {
				if got != nil {
					t.Fatalf("expected no match, got %s", got.Name)
				}

				return
			}

			if got == nil || got.Name != tt.want {
				t.Fatalf("expected %s, got %v", tt.want, got)
			}
		})
	}
}

func TestSanitizeCRDDropsWebhookConversionAndStatus(t *testing.T) {
	host := testCRD("ciliumnetworkpolicies.cilium.io", "cilium.io", "CiliumNetworkPolicy", "v2")
	host.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue}}
	host.ResourceVersion = "42"
	host.UID = "abc"

	got := sanitizeCRD(&host)

	if got.Spec.Conversion == nil || got.Spec.Conversion.Strategy != apiextensionsv1.NoneConverter || got.Spec.Conversion.Webhook != nil {
		t.Fatalf("conversion not forced to None: %+v", got.Spec.Conversion)
	}

	if got.ResourceVersion != "" || got.UID != "" || len(got.Status.Conditions) != 0 {
		t.Fatal("host metadata/status leaked into the copy")
	}

	if got.Labels[CRDSyncedLabel] != "true" {
		t.Fatal("synced label missing")
	}

	if got.Spec.Group != "cilium.io" || got.Spec.Names.Kind != "CiliumNetworkPolicy" || len(got.Spec.Versions) != 1 {
		t.Fatalf("spec not copied: %+v", got.Spec)
	}
}

func TestApplyCRDCreatesThenUpdates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	virt := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	host := testCRD("virtualmachines.kubevirt.io", "kubevirt.io", "VirtualMachine", "v1")

	if err := applyCRD(context.Background(), virt, &host); err != nil {
		t.Fatalf("create: %v", err)
	}

	// host gains a version -> virtual copy follows
	host.Spec.Versions = append(host.Spec.Versions, apiextensionsv1.CustomResourceDefinitionVersion{Name: "v2", Served: true})

	if err := applyCRD(context.Background(), virt, &host); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got apiextensionsv1.CustomResourceDefinition
	if err := virt.Get(context.Background(), ctrlruntimeclient.ObjectKey{Name: host.Name}, &got); err != nil {
		t.Fatal(err)
	}

	if len(got.Spec.Versions) != 2 {
		t.Fatalf("expected 2 versions after update, got %d", len(got.Spec.Versions))
	}

	if got.Spec.Conversion.Strategy != apiextensionsv1.NoneConverter {
		t.Fatal("conversion strategy not None after update")
	}
}

func TestEnsureCustomResourceDefinitionsMissingHostCRDFails(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	hostCRD := testCRD("virtualmachines.kubevirt.io", "kubevirt.io", "VirtualMachine", "v1")
	host := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&hostCRD).Build()
	virt := fakeclient.NewClientBuilder().WithScheme(scheme).Build()

	cluster := &v1beta1.Cluster{Spec: v1beta1.ClusterSpec{Sync: &v1beta1.SyncConfig{CustomResources: []v1beta1.CustomResourceSyncConfig{
		{APIVersion: "cilium.io/v2", Kind: "CiliumNetworkPolicy", Enabled: true},
	}}}}

	if _, err := EnsureCustomResourceDefinitions(context.Background(), host, virt, cluster); err == nil {
		t.Fatal("expected an error for a kind without a host CRD")
	}

	// disabled entries are ignored entirely
	cluster.Spec.Sync.CustomResources[0].Enabled = false

	wanted, err := EnsureCustomResourceDefinitions(context.Background(), host, virt, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(wanted) != 0 {
		t.Fatalf("expected no wanted CRDs, got %v", wanted)
	}
}
