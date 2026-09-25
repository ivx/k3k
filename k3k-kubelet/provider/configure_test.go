package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMirrorNodeDropsHostPodCIDRs(t *testing.T) {
	host := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "srv-01", Labels: map[string]string{"topology.kubernetes.io/zone": "01"}},
		Spec: corev1.NodeSpec{
			PodCIDR:       "172.31.0.0/24",
			PodCIDRs:      []string{"172.31.0.0/24", "fd00::/64"},
			Unschedulable: true,
			Taints:        []corev1.Taint{{Key: "maintenance", Effect: corev1.TaintEffectNoSchedule}},
		},
		Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.34.10+rke2r1"}},
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "srv-01"}}
	mirrorNode(node, host, 50774, "v1.34.9-k3s1")

	assert.Empty(t, node.Spec.PodCIDR, "the virtual cluster allocates its own pod CIDR")
	assert.Nil(t, node.Spec.PodCIDRs)
	assert.True(t, node.Spec.Unschedulable, "cordon is mirrored")
	assert.Equal(t, host.Spec.Taints, node.Spec.Taints)
	assert.Equal(t, "01", node.Labels["topology.kubernetes.io/zone"])
	assert.Equal(t, int32(50774), node.Status.DaemonEndpoints.KubeletEndpoint.Port)
	assert.Equal(t, "v1.34.9-k3s1", node.Status.NodeInfo.KubeletVersion)
	assert.Equal(t, "172.31.0.0/24", host.Spec.PodCIDR, "the host node is not changed")
}
