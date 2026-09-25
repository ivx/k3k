package provider

import (
	"context"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/rancher/k3k/k3k-kubelet/translate"
)

func Test_mergeEnvVars(t *testing.T) {
	type args struct {
		orig []corev1.EnvVar
		new  []corev1.EnvVar
	}

	tests := []struct {
		name string
		args args
		want []corev1.EnvVar
	}{
		{
			name: "orig and new are empty",
			args: args{
				orig: []corev1.EnvVar{},
				new:  []corev1.EnvVar{},
			},
			want: []corev1.EnvVar{},
		},
		{
			name: "only orig is empty",
			args: args{
				orig: []corev1.EnvVar{},
				new:  []corev1.EnvVar{{Name: "FOO", Value: "new_val"}},
			},
			want: []corev1.EnvVar{{Name: "FOO", Value: "new_val"}},
		},
		{
			name: "orig has a matching element",
			args: args{
				orig: []corev1.EnvVar{{Name: "FOO", Value: "old_val"}},
				new:  []corev1.EnvVar{{Name: "FOO", Value: "new_val"}},
			},
			want: []corev1.EnvVar{{Name: "FOO", Value: "new_val"}},
		},
		{
			name: "orig have multiple elements",
			args: args{
				orig: []corev1.EnvVar{{Name: "FOO_0", Value: "old_val_0"}, {Name: "FOO_1", Value: "old_val_1"}},
				new:  []corev1.EnvVar{{Name: "FOO_1", Value: "new_val_1"}},
			},
			want: []corev1.EnvVar{{Name: "FOO_0", Value: "old_val_0"}, {Name: "FOO_1", Value: "new_val_1"}},
		},
		{
			name: "orig and new have multiple elements and some not matching",
			args: args{
				orig: []corev1.EnvVar{{Name: "FOO_0", Value: "old_val_0"}, {Name: "FOO_1", Value: "old_val_1"}},
				new:  []corev1.EnvVar{{Name: "FOO_1", Value: "new_val_1"}, {Name: "FOO_2", Value: "val_1"}},
			},
			want: []corev1.EnvVar{{Name: "FOO_0", Value: "old_val_0"}, {Name: "FOO_1", Value: "new_val_1"}, {Name: "FOO_2", Value: "val_1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeEnvVars(tt.args.orig, tt.args.new); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeEnvVars() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_configureEnv(t *testing.T) {
	virtualPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod",
			Namespace: "my-namespace",
		},
	}

	tests := []struct {
		name       string
		virtualPod *corev1.Pod
		envs       []corev1.EnvVar
		want       []corev1.EnvVar
	}{
		{
			name:       "empty envs",
			virtualPod: virtualPod,
			envs:       []corev1.EnvVar{},
			want:       []corev1.EnvVar{},
		},
		{
			name:       "simple env var",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{Name: "MY_VAR", Value: "my-value"},
			},
			want: []corev1.EnvVar{
				{Name: "MY_VAR", Value: "my-value"},
			},
		},
		{
			name:       "metadata.name field ref",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{
					Name: "POD_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.name",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{Name: "POD_NAME", Value: "my-pod"},
			},
		},
		{
			name:       "metadata.namespace field ref",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{
					Name: "POD_NAMESPACE",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.namespace",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{Name: "POD_NAMESPACE", Value: "my-namespace"},
			},
		},
		{
			name:       "other field ref",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{
					Name: "NODE_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.nodeName",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{
					Name: "NODE_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.nodeName",
						},
					},
				},
			},
		},
		{
			name:       "secret key ref",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{
					Name: "SECRET_VAR",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
							Key:                  "my-key",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{
					Name: "SECRET_VAR",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret-my-namespace-c-test-6d792d7365637265742b6d792d6-887db"},
							Key:                  "my-key",
						},
					},
				},
			},
		},
		{
			name:       "configmap key ref",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{
					Name: "CONFIG_VAR",
					ValueFrom: &corev1.EnvVarSource{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
							Key:                  "my-key",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{
					Name: "CONFIG_VAR",
					ValueFrom: &corev1.EnvVarSource{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap-my-namespace-c-test-6d792d636f6e6669676d6170-301f6"},
							Key:                  "my-key",
						},
					},
				},
			},
		},
		{
			name:       "resource field ref",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{
					Name: "CPU_LIMIT",
					ValueFrom: &corev1.EnvVarSource{
						ResourceFieldRef: &corev1.ResourceFieldSelector{
							ContainerName: "my-container",
							Resource:      "limits.cpu",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{
					Name: "CPU_LIMIT",
					ValueFrom: &corev1.EnvVarSource{
						ResourceFieldRef: &corev1.ResourceFieldSelector{
							ContainerName: "my-container",
							Resource:      "limits.cpu",
						},
					},
				},
			},
		},
		{
			name:       "mixed env vars",
			virtualPod: virtualPod,
			envs: []corev1.EnvVar{
				{Name: "MY_VAR", Value: "my-value"},
				{
					Name: "POD_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.name",
						},
					},
				},
				{
					Name: "POD_NAMESPACE",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.namespace",
						},
					},
				},
				{
					Name: "NODE_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.nodeName",
						},
					},
				},
			},
			want: []corev1.EnvVar{
				{Name: "MY_VAR", Value: "my-value"},
				{Name: "POD_NAME", Value: "my-pod"},
				{Name: "POD_NAMESPACE", Value: "my-namespace"},
				{
					Name: "NODE_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.nodeName",
						},
					},
				},
			},
		},
	}

	p := Provider{
		Translator: translate.ToHostTranslator{
			ClusterName:      "c-test",
			ClusterNamespace: "ns-test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.configureEnv(tt.virtualPod, tt.envs)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestGetPods_ScopedToAgent pins the behavior that GetPods returns only the Pods synced by this
// k3k-kubelet agent, identified by the AgentNameLabel and scoped to this cluster's host namespace.
// Pods synced by another agent, or living in another namespace, are excluded. Pods are returned
// regardless of whether their virtual counterpart still exists, so the virtual-kubelet startup
// reconciliation can still clean up genuine orphans.
func TestGetPods_ScopedToAgent(t *testing.T) {
	const (
		clusterName      = "c-test"
		clusterNamespace = "ns-test"
		agentName        = "node-a"
	)

	// host Pods carry the tracking metadata TranslateFrom reads to recover the virtual identity,
	// plus the AgentNameLabel recording which agent synced them.
	newHostPod := func(name, agent, namespace string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					translate.ClusterNameLabel: clusterName,
					translate.AgentNameLabel:   agent,
				},
				Annotations: map[string]string{
					translate.ResourceNameAnnotation:      name,
					translate.ResourceNamespaceAnnotation: "default",
				},
			},
		}
	}

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	hostClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			newHostPod("a1", agentName, clusterNamespace), // synced by this agent -> returned
			newHostPod("b1", "node-b", clusterNamespace),  // synced by another agent -> excluded
			newHostPod("d1", agentName, "ns-other"),       // another namespace -> excluded
		).
		Build()

	p := Provider{
		Host: ClusterContext{Client: hostClient},
		Translator: translate.ToHostTranslator{
			ClusterName:      clusterName,
			ClusterNamespace: clusterNamespace,
		},
		ClusterName:      clusterName,
		ClusterNamespace: clusterNamespace,
		agentHostname:    agentName,
		logger:           logr.Discard(),
	}

	pods, err := p.GetPods(context.Background())
	require.NoError(t, err)

	names := map[string]bool{}
	for _, pod := range pods {
		names[pod.Name] = true
	}

	// only a1 (synced by this agent, in this namespace) is returned.
	assert.Equal(t, map[string]bool{"a1": true}, names)
}

func TestUpdateMetadata(t *testing.T) {
	hostPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "host-pod",
			Namespace: "ns-test",
			Labels: map[string]string{
				translate.ClusterNameLabel: "c-test",
				translate.AgentNameLabel:   "node-a",
				"app":                      "nginx",
			},
			Annotations: map[string]string{
				translate.ResourceNameAnnotation:      "my-pod",
				translate.ResourceNamespaceAnnotation: "default",
			},
		},
	}

	virtualPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod",
			Namespace: "default",
			Labels:    map[string]string{"app": "nginx"},
		},
	}

	updateMetadata(hostPod, virtualPod)

	assert.Equal(t, "c-test", hostPod.Labels[translate.ClusterNameLabel])
	assert.Equal(t, "node-a", hostPod.Labels[translate.AgentNameLabel])
	assert.Equal(t, "my-pod", hostPod.Annotations[translate.ResourceNameAnnotation])
	assert.Equal(t, "default", hostPod.Annotations[translate.ResourceNamespaceAnnotation])
}

func Test_configureDNS(t *testing.T) {
	const dnsIP = "10.197.77.73"

	vcSearches := []string{"tenant.svc.cluster.local", "svc.cluster.local", "cluster.local"}
	ndots := func(v string) corev1.PodDNSConfigOption {
		return corev1.PodDNSConfigOption{Name: "ndots", Value: new(v)}
	}
	pod := func(policy corev1.DNSPolicy, cfg *corev1.PodDNSConfig) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "tenant"},
			Spec:       corev1.PodSpec{DNSPolicy: policy, DNSConfig: cfg},
		}
	}

	tests := []struct {
		name       string
		virtualPod *corev1.Pod
		wantPolicy corev1.DNSPolicy
		wantConfig *corev1.PodDNSConfig
	}{
		{
			name:       "no dnsConfig gets the virtual cluster DNS",
			virtualPod: pod(corev1.DNSClusterFirst, nil),
			wantPolicy: corev1.DNSNone,
			wantConfig: &corev1.PodDNSConfig{
				Nameservers: []string{dnsIP},
				Searches:    vcSearches,
				Options:     []corev1.PodDNSConfigOption{ndots("5")},
			},
		},
		{
			name: "dnsConfig with options only keeps the options and still gets the virtual cluster DNS",
			virtualPod: pod(corev1.DNSClusterFirst, &corev1.PodDNSConfig{
				Options: []corev1.PodDNSConfigOption{ndots("1")},
			}),
			wantPolicy: corev1.DNSNone,
			wantConfig: &corev1.PodDNSConfig{
				Nameservers: []string{dnsIP},
				Searches:    vcSearches,
				Options:     []corev1.PodDNSConfigOption{ndots("1")},
			},
		},
		{
			name: "extra nameservers and searches are appended after the virtual cluster ones",
			virtualPod: pod(corev1.DNSClusterFirst, &corev1.PodDNSConfig{
				Nameservers: []string{"192.0.2.53", dnsIP},
				Searches:    []string{"example.internal", "svc.cluster.local"},
				Options:     []corev1.PodDNSConfigOption{{Name: "timeout", Value: new("2")}},
			}),
			wantPolicy: corev1.DNSNone,
			wantConfig: &corev1.PodDNSConfig{
				Nameservers: []string{dnsIP, "192.0.2.53"},
				Searches:    append(vcSearches, "example.internal"),
				Options:     []corev1.PodDNSConfigOption{{Name: "timeout", Value: new("2")}, ndots("5")},
			},
		},
		{
			name: "nameservers are capped at three",
			virtualPod: pod(corev1.DNSClusterFirst, &corev1.PodDNSConfig{
				Nameservers: []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"},
			}),
			wantPolicy: corev1.DNSNone,
			wantConfig: &corev1.PodDNSConfig{
				Nameservers: []string{dnsIP, "192.0.2.1", "192.0.2.2"},
				Searches:    vcSearches,
				Options:     []corev1.PodDNSConfigOption{ndots("5")},
			},
		},
		{
			name: "dnsPolicy None is left untouched",
			virtualPod: pod(corev1.DNSNone, &corev1.PodDNSConfig{
				Nameservers: []string{"192.0.2.53"},
			}),
			wantPolicy: corev1.DNSNone,
			wantConfig: &corev1.PodDNSConfig{
				Nameservers: []string{"192.0.2.53"},
			},
		},
		{
			name: "the coredns pod is left untouched",
			virtualPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "coredns",
					Namespace: metav1.NamespaceSystem,
					Labels:    map[string]string{"k8s-app": "kube-dns"},
				},
				Spec: corev1.PodSpec{DNSPolicy: corev1.DNSDefault},
			},
			wantPolicy: corev1.DNSDefault,
			wantConfig: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostPod := tt.virtualPod.DeepCopy()

			configureDNS(hostPod, tt.virtualPod, dnsIP)

			assert.Equal(t, tt.wantPolicy, hostPod.Spec.DNSPolicy)
			assert.Equal(t, tt.wantConfig, hostPod.Spec.DNSConfig)
		})
	}
}

func TestConfigureScheduling(t *testing.T) {
	const agentName = "node-a"

	preferAgentNode := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
				Weight: 100,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{agentName},
					}},
				},
			}},
		},
	}

	pinToAgentNode := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchFields: []corev1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{agentName},
					}},
				}},
			},
		},
	}

	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}}

	// ownAffinity has every kind of constraint the virtual scheduler already evaluated
	ownAffinity := func() *corev1.Affinity {
		return &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "topology.kubernetes.io/zone",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"zone-1"},
						}},
					}},
				},
			},
			PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					LabelSelector: selector,
					TopologyKey:   "topology.kubernetes.io/zone",
				}},
			},
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					LabelSelector: selector,
					TopologyKey:   "kubernetes.io/hostname",
				}},
			},
		}
	}

	spread := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     selector,
	}}

	tolerations := []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "db", Effect: corev1.TaintEffectNoSchedule}}
	nodeSelector := map[string]string{"pool": "db"}

	newPod := func(affinity *corev1.Affinity, withSpread bool) *corev1.Pod {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				NodeName:     agentName,
				Affinity:     affinity,
				Tolerations:  tolerations,
				NodeSelector: nodeSelector,
			},
		}
		if withSpread {
			pod.Spec.TopologySpreadConstraints = spread
		}

		return pod
	}

	tests := []struct {
		name            string
		mirrorHostNodes bool
		pod             *corev1.Pod
		wantAffinity    *corev1.Affinity
		wantSpread      []corev1.TopologySpreadConstraint
	}{
		{
			name:         "without mirrorHostNodes, no own affinity: prefer the agent node",
			pod:          newPod(nil, true),
			wantAffinity: preferAgentNode,
			wantSpread:   spread,
		},
		{
			name:         "without mirrorHostNodes, own affinity: keep it, no preference",
			pod:          newPod(ownAffinity(), true),
			wantAffinity: ownAffinity(),
			wantSpread:   spread,
		},
		{
			name:            "with mirrorHostNodes, no own affinity: pin to the agent node",
			mirrorHostNodes: true,
			pod:             newPod(nil, false),
			wantAffinity:    pinToAgentNode,
		},
		{
			name:            "with mirrorHostNodes, own affinity and spread: pin only, own constraints dropped",
			mirrorHostNodes: true,
			pod:             newPod(ownAffinity(), true),
			wantAffinity:    pinToAgentNode,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Provider{agentHostname: agentName, mirrorHostNodes: tt.mirrorHostNodes}

			p.configureScheduling(tt.pod)

			assert.Empty(t, tt.pod.Spec.NodeName)
			assert.Equal(t, tt.wantAffinity, tt.pod.Spec.Affinity)
			assert.Equal(t, tt.wantSpread, tt.pod.Spec.TopologySpreadConstraints)
			// tolerations and the nodeSelector are not changed by the placement rules
			assert.Equal(t, tolerations, tt.pod.Spec.Tolerations)
			assert.Equal(t, nodeSelector, tt.pod.Spec.NodeSelector)
		})
	}
}

// TestDeletePod_OnlyOwnIncarnation pins that an agent only deletes the host Pod incarnation it is
// responsible for. Host Pod names are derived from the virtual name only, so a delete for an old
// incarnation (e.g. a StatefulSet Pod that moved to another node) must not remove the new copy.
func TestDeletePod_OnlyOwnIncarnation(t *testing.T) {
	const (
		clusterName      = "c-test"
		clusterNamespace = "ns-test"
		agentName        = "node-a"
		virtualNamespace = "default"
		virtualName      = "db-1"
	)

	translator := translate.ToHostTranslator{ClusterName: clusterName, ClusterNamespace: clusterNamespace}
	hostPodName := translator.TranslateName(virtualNamespace, virtualName)

	newHostPod := func(agent, virtualUID string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        hostPodName,
				Namespace:   clusterNamespace,
				UID:         types.UID("host-uid"),
				Labels:      map[string]string{translate.ClusterNameLabel: clusterName},
				Annotations: map[string]string{},
			},
		}
		if agent != "" {
			pod.Labels[translate.AgentNameLabel] = agent
		}

		if virtualUID != "" {
			pod.Annotations[translate.VirtualUIDAnnotation] = virtualUID
		}

		return pod
	}

	newVirtualPod := func(uid string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: virtualName, Namespace: virtualNamespace, UID: types.UID(uid)},
		}
	}

	tests := []struct {
		name        string
		hostPod     *corev1.Pod
		virtualPod  *corev1.Pod
		wantDeleted bool
	}{
		{
			name:        "own copy of this incarnation is deleted",
			hostPod:     newHostPod(agentName, "virt-uid-1"),
			virtualPod:  newVirtualPod("virt-uid-1"),
			wantDeleted: true,
		},
		{
			name:        "copy synced by another agent is kept",
			hostPod:     newHostPod("node-b", "virt-uid-2"),
			virtualPod:  newVirtualPod("virt-uid-1"),
			wantDeleted: false,
		},
		{
			name:        "copy synced by another agent is kept, also without a virtual UID (GetPod/GetPods path)",
			hostPod:     newHostPod("node-b", "virt-uid-2"),
			virtualPod:  newVirtualPod(""),
			wantDeleted: false,
		},
		{
			name:        "own copy of another incarnation is kept",
			hostPod:     newHostPod(agentName, "virt-uid-2"),
			virtualPod:  newVirtualPod("virt-uid-1"),
			wantDeleted: false,
		},
		{
			name:        "own copy is deleted when the virtual Pod has no UID (GetPod/GetPods path)",
			hostPod:     newHostPod(agentName, "virt-uid-1"),
			virtualPod:  newVirtualPod(""),
			wantDeleted: true,
		},
		{
			name:        "copy of an older k3k-kubelet without ownership metadata is deleted",
			hostPod:     newHostPod("", ""),
			virtualPod:  newVirtualPod("virt-uid-1"),
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := k8sfake.NewClientset(tt.hostPod)

			// the fake object tracker ignores preconditions, so record the delete options
			var deletes []k8stesting.DeleteActionImpl

			clientset.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				deletes = append(deletes, action.(k8stesting.DeleteActionImpl))
				return false, nil, nil
			})

			p := Provider{
				Host:             ClusterContext{CoreClient: clientset.CoreV1()},
				Translator:       translator,
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
				agentHostname:    agentName,
				logger:           logr.Discard(),
			}

			require.NoError(t, p.deletePod(context.Background(), tt.virtualPod))

			_, err := clientset.CoreV1().Pods(clusterNamespace).Get(context.Background(), hostPodName, metav1.GetOptions{})

			if !tt.wantDeleted {
				require.NoError(t, err, "host pod must still exist")
				assert.Empty(t, deletes, "no delete must be sent")

				return
			}

			assert.True(t, apierrors.IsNotFound(err), "host pod must be deleted")
			require.Len(t, deletes, 1)
			require.NotNil(t, deletes[0].DeleteOptions.Preconditions)
			require.NotNil(t, deletes[0].DeleteOptions.Preconditions.UID)
			assert.Equal(t, tt.hostPod.UID, *deletes[0].DeleteOptions.Preconditions.UID)
		})
	}
}

func TestDeletePod_HostPodNotFound(t *testing.T) {
	p := Provider{
		Host:             ClusterContext{CoreClient: k8sfake.NewClientset().CoreV1()},
		Translator:       translate.ToHostTranslator{ClusterName: "c-test", ClusterNamespace: "ns-test"},
		ClusterNamespace: "ns-test",
		agentHostname:    "node-a",
		logger:           logr.Discard(),
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "db-1", Namespace: "default", UID: "virt-uid-1"}}

	assert.NoError(t, p.deletePod(context.Background(), pod))
}
