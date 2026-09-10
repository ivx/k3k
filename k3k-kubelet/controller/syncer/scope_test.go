package syncer

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

func TestScopeSelector(t *testing.T) {
	tests := []struct {
		name      string
		selector  map[string]any
		want      map[string]any
		wantError bool
	}{
		{
			name:     "empty selector is pinned to own namespace",
			selector: map[string]any{},
			want: map[string]any{"matchLabels": map[string]any{
				"k3k.io/clusterName": "vc1", "k3k.io/namespaceName": "team-a",
			}},
		},
		{
			name:     "app labels are kept, scope added",
			selector: map[string]any{"matchLabels": map[string]any{"app": "web"}},
			want: map[string]any{"matchLabels": map[string]any{
				"app": "web", "k3k.io/clusterName": "vc1", "k3k.io/namespaceName": "team-a",
			}},
		},
		{
			name: "namespace reference is rewritten",
			selector: map[string]any{"matchLabels": map[string]any{
				"io.kubernetes.pod.namespace": "team-b", "app": "db",
			}},
			want: map[string]any{"matchLabels": map[string]any{
				"app": "db", "k3k.io/clusterName": "vc1", "k3k.io/namespaceName": "team-b",
			}},
		},
		{
			name: "cilium source prefix on the namespace reference is handled",
			selector: map[string]any{"matchLabels": map[string]any{
				"k8s:io.kubernetes.pod.namespace": "team-b",
			}},
			want: map[string]any{"matchLabels": map[string]any{
				"k3k.io/clusterName": "vc1", "k3k.io/namespaceName": "team-b",
			}},
		},
		{
			name: "tenant cannot escape the cluster scope via matchLabels",
			selector: map[string]any{"matchLabels": map[string]any{
				"k3k.io/clusterName": "other-vc",
			}},
			want: map[string]any{"matchLabels": map[string]any{
				"k3k.io/clusterName": "vc1", "k3k.io/namespaceName": "team-a",
			}},
		},
		{
			name: "namespace In expression is rewritten and suppresses the default pin",
			selector: map[string]any{"matchExpressions": []any{
				map[string]any{"key": "io.kubernetes.pod.namespace", "operator": "In", "values": []any{"team-b", "team-c"}},
			}},
			want: map[string]any{
				"matchLabels": map[string]any{"k3k.io/clusterName": "vc1"},
				"matchExpressions": []any{
					map[string]any{"key": "k3k.io/namespaceName", "operator": "In", "values": []any{"team-b", "team-c"}},
				},
			},
		},
		{
			name: "namespace Exists expression means every namespace of this cluster",
			selector: map[string]any{"matchExpressions": []any{
				map[string]any{"key": "io.kubernetes.pod.namespace", "operator": "Exists"},
			}},
			want: map[string]any{
				"matchLabels": map[string]any{"k3k.io/clusterName": "vc1"},
				"matchExpressions": []any{
					map[string]any{"key": "k3k.io/namespaceName", "operator": "Exists"},
				},
			},
		},
		{
			name: "namespace NotIn expression stays inside the cluster scope",
			selector: map[string]any{"matchExpressions": []any{
				map[string]any{"key": "io.kubernetes.pod.namespace", "operator": "NotIn", "values": []any{"team-b"}},
			}},
			want: map[string]any{
				"matchLabels": map[string]any{"k3k.io/clusterName": "vc1"},
				"matchExpressions": []any{
					map[string]any{"key": "k3k.io/namespaceName", "operator": "NotIn", "values": []any{"team-b"}},
				},
			},
		},
		{
			name: "namespace label selector is rejected",
			selector: map[string]any{"matchLabels": map[string]any{
				"io.kubernetes.pod.namespace.labels.tier": "frontend",
			}},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := scopeSelector(tt.selector, "vc1", "team-a")
			if tt.wantError {
				if !errors.Is(err, ErrRejected) {
					t.Fatalf("expected ErrRejected, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			assertEqualJSON(t, tt.want, tt.selector)
		})
	}
}

func TestScopeSelectorsWildcardPaths(t *testing.T) {
	cnp := map[string]any{
		"spec": map[string]any{
			"endpointSelector": map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"ingress": []any{
				map[string]any{"fromEndpoints": []any{
					map[string]any{"matchLabels": map[string]any{"app": "lb"}},
					map[string]any{"matchLabels": map[string]any{"io.kubernetes.pod.namespace": "team-b"}},
				}},
				map[string]any{"fromEntities": []any{"world"}},
			},
		},
	}

	paths := []string{"/spec/endpointSelector", "/spec/ingress/*/fromEndpoints/*", "/spec/egress/*/toEndpoints/*"}

	if err := scopeSelectors(cnp, paths, "vc1", "team-a"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spec := cnp["spec"].(map[string]any)
	ep := spec["endpointSelector"].(map[string]any)["matchLabels"].(map[string]any)

	if ep["k3k.io/clusterName"] != "vc1" || ep["k3k.io/namespaceName"] != "team-a" || ep["app"] != "web" {
		t.Fatalf("endpointSelector not scoped: %v", ep)
	}

	from := spec["ingress"].([]any)[0].(map[string]any)["fromEndpoints"].([]any)

	first := from[0].(map[string]any)["matchLabels"].(map[string]any)
	if first["k3k.io/namespaceName"] != "team-a" || first["app"] != "lb" {
		t.Fatalf("same-namespace peer not pinned: %v", first)
	}

	second := from[1].(map[string]any)["matchLabels"].(map[string]any)
	if second["k3k.io/namespaceName"] != "team-b" || second["k3k.io/clusterName"] != "vc1" {
		t.Fatalf("cross-namespace peer not translated: %v", second)
	}

	if _, leaked := second["io.kubernetes.pod.namespace"]; leaked {
		t.Fatal("original namespace key not removed")
	}
}

func TestCheckRejects(t *testing.T) {
	rejects := []v1beta1.CustomResourceReject{
		{Path: "/spec/egress/*/toEntities", Allow: []string{"world"}},
		{Path: "/spec/egress/*/toCIDR"},
		{Path: "/spec/nodeSelector"},
	}

	tests := []struct {
		name   string
		spec   map[string]any
		reject bool
	}{
		{"nothing set", map[string]any{"egress": []any{map[string]any{"toPorts": []any{}}}}, false},
		{"allowed entity", map[string]any{"egress": []any{map[string]any{"toEntities": []any{"world"}}}}, false},
		{"empty list is fine", map[string]any{"egress": []any{map[string]any{"toCIDR": []any{}}}}, false},
		{"forbidden entity", map[string]any{"egress": []any{map[string]any{"toEntities": []any{"world", "cluster"}}}}, true},
		{"cidr set", map[string]any{"egress": []any{map[string]any{"toCIDR": []any{"10.0.0.0/8"}}}}, true},
		{"node selector set", map[string]any{"nodeSelector": map[string]any{"matchLabels": map[string]any{"a": "b"}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkRejects(map[string]any{"spec": tt.spec}, rejects)
			if tt.reject && !errors.Is(err, ErrRejected) {
				t.Fatalf("expected ErrRejected, got %v", err)
			}

			if !tt.reject && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestFindNodes(t *testing.T) {
	root := map[string]any{
		"specs": []any{
			map[string]any{"egress": []any{map[string]any{"toEntities": []any{"world"}}}},
			map[string]any{"egress": []any{map[string]any{"toEntities": []any{"host"}}, map[string]any{}}},
		},
	}

	nodes, err := findNodes(root, "/specs/*/egress/*/toEntities")
	if err != nil {
		t.Fatal(err)
	}

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	nodes, err = findNodes(root, "/specs/1/egress/0/toEntities/0")
	if err != nil || len(nodes) != 1 || nodes[0] != "host" {
		t.Fatalf("indexed lookup failed: %v %v", nodes, err)
	}

	nodes, _ = findNodes(root, "/missing/*/x")
	if len(nodes) != 0 {
		t.Fatal("missing path must match nothing")
	}

	if _, err := findNodes(root, "no-slash"); err == nil {
		t.Fatal("path without leading slash must fail")
	}
}

// A previously valid object that turns invalid loses its host copy and the
// tenant gets an event.
func TestRejectRemovesHostCopyAndRecordsEvent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	gvk := schema.GroupVersionKind{Group: "cilium.io", Version: "v2", Kind: "CiliumNetworkPolicy"}

	translator := translate.ToHostTranslator{ClusterName: "vc1", ClusterNamespace: "host-ns"}

	hostCopy := &unstructured.Unstructured{}
	hostCopy.SetGroupVersionKind(gvk)
	hostCopy.SetName(translator.TranslateName("team-a", "allow-db"))
	hostCopy.SetNamespace("host-ns")

	hostClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(hostCopy).Build()
	recorder := record.NewFakeRecorder(4)

	r := &CustomResourceReconciler{
		SyncerContext: &SyncerContext{
			ClusterName:      "vc1",
			ClusterNamespace: "host-ns",
			HostClient:       hostClient,
			Translator:       translator,
		},
		GVK:      gvk,
		Recorder: recorder,
	}

	virtObj := &unstructured.Unstructured{}
	virtObj.SetGroupVersionKind(gvk)
	virtObj.SetName("allow-db")
	virtObj.SetNamespace("team-a")

	if err := r.reject(context.Background(), virtObj, errors.New("rejected: /spec/egress/*/toCIDR may not be set")); err != nil {
		t.Fatalf("reject: %v", err)
	}

	var gone unstructured.Unstructured

	gone.SetGroupVersionKind(gvk)

	if err := hostClient.Get(context.Background(), ctrlruntimeclient.ObjectKeyFromObject(hostCopy), &gone); err == nil {
		t.Fatal("host copy still present after rejection")
	}

	select {
	case e := <-recorder.Events:
		if e == "" {
			t.Fatal("empty event")
		}
	default:
		t.Fatal("no event recorded")
	}
}

func TestTranslatedScopesAndRejects(t *testing.T) {
	r := &CustomResourceReconciler{
		SyncerContext: &SyncerContext{
			ClusterName:      "vc1",
			ClusterNamespace: "host-ns",
			Translator:       translate.ToHostTranslator{ClusterName: "vc1", ClusterNamespace: "host-ns"},
		},
		GVK: schema.GroupVersionKind{Group: "policy", Version: "v1", Kind: "PodDisruptionBudget"},
	}

	cfg := &v1beta1.CustomResourceSyncConfig{
		APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Enabled: true,
		Selectors: []string{"/spec/selector"},
	}

	pdb := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
		"metadata": map[string]any{"name": "web", "namespace": "team-a"},
		"spec":     map[string]any{"minAvailable": int64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "web"}}},
	}}

	hostObj, err := r.translated(context.Background(), pdb, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sel, _, _ := unstructured.NestedStringMap(hostObj.Object, "spec", "selector", "matchLabels")
	if sel["app"] != "web" || sel[translate.ClusterNameLabel] != "vc1" || sel[translate.NamespaceNameLabel] != "team-a" {
		t.Fatalf("PDB selector not scoped: %v", sel)
	}

	if hostObj.GetName() == "web" || hostObj.GetNamespace() != "host-ns" {
		t.Fatalf("name/namespace not translated: %s/%s", hostObj.GetNamespace(), hostObj.GetName())
	}
}

func assertEqualJSON(t *testing.T, want, got any) {
	t.Helper()

	w := &unstructured.Unstructured{Object: map[string]any{"x": want}}
	g := &unstructured.Unstructured{Object: map[string]any{"x": got}}

	wj, _ := w.MarshalJSON()
	gj, _ := g.MarshalJSON()

	if string(wj) != string(gj) {
		t.Fatalf("mismatch\nwant %s\ngot  %s", wj, gj)
	}
}

func TestScopeSelectorsCreatesMissingSelector(t *testing.T) {
	// no spec.selector at all: must end up pinned, not wide open
	pdb := map[string]any{"spec": map[string]any{"minAvailable": int64(1)}}

	if err := scopeSelectors(pdb, []string{"/spec/selector"}, "vc1", "team-a"); err != nil {
		t.Fatal(err)
	}

	sel := pdb["spec"].(map[string]any)["selector"].(map[string]any)["matchLabels"].(map[string]any)
	if sel["k3k.io/clusterName"] != "vc1" || sel["k3k.io/namespaceName"] != "team-a" {
		t.Fatalf("missing selector not created and scoped: %v", sel)
	}

	// wildcard tails are not created: a rule without fromEndpoints stays without
	cnp := map[string]any{"spec": map[string]any{"ingress": []any{map[string]any{"fromEntities": []any{"world"}}}}}

	if err := scopeSelectors(cnp, []string{"/spec/ingress/*/fromEndpoints/*"}, "vc1", "team-a"); err != nil {
		t.Fatal(err)
	}

	rule := cnp["spec"].(map[string]any)["ingress"].([]any)[0].(map[string]any)
	if _, created := rule["fromEndpoints"]; created {
		t.Fatal("wildcard tail must not be created")
	}
}
