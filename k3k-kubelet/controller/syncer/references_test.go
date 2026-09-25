package syncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

// kubevirtReferences are the paths the virtual-cluster template configures
// for the kubevirtSync entry.
var kubevirtReferences = []string{
	"/spec/template/spec/volumes/*/persistentVolumeClaim/claimName",
	"/spec/template/spec/volumes/*/dataVolume/name",
	"/spec/template/spec/volumes/*/cloudInitNoCloud/secretRef/name",
	"/spec/template/spec/volumes/*/cloudInitNoCloud/networkDataSecretRef/name",
	"/spec/template/spec/volumes/*/configMap/name",
	"/spec/template/spec/volumes/*/secret/secretName",
	"/spec/dataVolumeTemplates/*/metadata/name",
}

func testVM() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata":   map[string]any{"name": "worker", "namespace": "iwfm"},
		"spec": map[string]any{
			"dataVolumeTemplates": []any{
				map[string]any{"metadata": map[string]any{"name": "worker-root"}},
			},
			"template": map[string]any{"spec": map[string]any{
				"volumes": []any{
					map[string]any{"name": "root", "dataVolume": map[string]any{"name": "worker-root"}},
					map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "worker-data"}},
					map[string]any{"name": "cloudinit", "cloudInitNoCloud": map[string]any{
						"secretRef":            map[string]any{"name": "worker-userdata"},
						"networkDataSecretRef": map[string]any{"name": "worker-netdata"},
					}},
					map[string]any{"name": "cfg", "configMap": map[string]any{"name": "worker-cfg"}},
					map[string]any{"name": "creds", "secret": map[string]any{"secretName": "db-app"}},
					map[string]any{"name": "disk", "containerDisk": map[string]any{"image": "ghcr.io/ivx/vm:1"}},
				},
			}},
		},
	}}
}

func TestTranslateReferences(t *testing.T) {
	vm := testVM()

	require.NoError(t, translateReferences(vm.Object, kubevirtReferences, func(name string) string { return "h-" + name }))

	volumes, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes")
	get := func(i int, fields ...string) string {
		v, _, _ := unstructured.NestedString(volumes[i].(map[string]any), fields...)
		return v
	}

	assert.Equal(t, "h-worker-root", get(0, "dataVolume", "name"))
	assert.Equal(t, "h-worker-data", get(1, "persistentVolumeClaim", "claimName"))
	assert.Equal(t, "h-worker-userdata", get(2, "cloudInitNoCloud", "secretRef", "name"))
	assert.Equal(t, "h-worker-netdata", get(2, "cloudInitNoCloud", "networkDataSecretRef", "name"))
	assert.Equal(t, "h-worker-cfg", get(3, "configMap", "name"))
	assert.Equal(t, "h-db-app", get(4, "secret", "secretName"))
	assert.Equal(t, "ghcr.io/ivx/vm:1", get(5, "containerDisk", "image"), "unreferenced fields stay")

	// volume names are not references
	assert.Equal(t, "root", get(0, "name"))

	dvts, _, _ := unstructured.NestedSlice(vm.Object, "spec", "dataVolumeTemplates")
	dvName, _, _ := unstructured.NestedString(dvts[0].(map[string]any), "metadata", "name")
	assert.Equal(t, "h-worker-root", dvName, "a DataVolume template and its volume reference get the same name")
}

func TestTranslateReferencesEdgeCases(t *testing.T) {
	obj := map[string]any{
		"spec": map[string]any{
			"list":  []any{"a", "b"},
			"num":   int64(3),
			"empty": "",
		},
	}

	up := func(name string) string { return "h-" + name }

	require.NoError(t, translateReferences(obj, []string{"/spec/list/1", "/spec/num", "/spec/empty", "/spec/missing/x"}, up))

	spec := obj["spec"].(map[string]any)
	assert.Equal(t, []any{"a", "h-b"}, spec["list"], "list index")
	assert.Equal(t, int64(3), spec["num"], "non-strings stay")
	assert.Equal(t, "", spec["empty"], "empty names stay")

	require.NoError(t, translateReferences(obj, []string{"/spec/list/*"}, up))
	assert.Equal(t, []any{"h-a", "h-h-b"}, spec["list"], "list wildcard")

	assert.Error(t, translateReferences(obj, []string{"spec/list"}, up), "paths must start with /")
}

func TestTranslatedTranslatesReferences(t *testing.T) {
	tr := translate.ToHostTranslator{ClusterName: "mycluster", ClusterNamespace: "host-ns"}

	r := &CustomResourceReconciler{
		SyncerContext: &SyncerContext{ClusterName: "mycluster", ClusterNamespace: "host-ns", Translator: tr},
		GVK:           schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachine"},
	}

	hostObj, err := r.translated(context.Background(), testVM(), &v1beta1.CustomResourceSyncConfig{
		APIVersion: "kubevirt.io/v1", Kind: "VirtualMachine", Enabled: true, References: kubevirtReferences,
	})
	require.NoError(t, err)

	volumes, _, _ := unstructured.NestedSlice(hostObj.Object, "spec", "template", "spec", "volumes")
	claim, _, _ := unstructured.NestedString(volumes[1].(map[string]any), "persistentVolumeClaim", "claimName")
	secret, _, _ := unstructured.NestedString(volumes[2].(map[string]any), "cloudInitNoCloud", "secretRef", "name")

	// exactly the names the PVC and Secret syncers give the host copies
	assert.Equal(t, tr.TranslateName("iwfm", "worker-data"), claim)
	assert.Equal(t, tr.TranslateName("iwfm", "worker-userdata"), secret)
	assert.LessOrEqual(t, len(claim), 63)
}
