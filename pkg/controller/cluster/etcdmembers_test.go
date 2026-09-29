package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/k3k/pkg/controller/cluster/server"
)

func etcdServerPod(name, ip string, started time.Time, annotation string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "vc"},
		Status: corev1.PodStatus{
			PodIP: ip,
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}},
			}},
		},
	}

	if annotation != "" {
		pod.Annotations = map[string]string{server.EtcdMembersAnnotation: annotation}
	}

	return pod
}

func member(id uint64, name, peerURL string) *etcdserverpb.Member {
	return &etcdserverpb.Member{ID: id, Name: name, PeerURLs: []string{peerURL}}
}

func TestPlanEtcdRepair(t *testing.T) {
	now := time.Unix(2000000000, 0)
	started := now.Add(-5 * time.Minute)

	tests := []struct {
		name       string
		pod        *corev1.Pod
		members    []*etcdserverpb.Member
		wantUpdate *peerURLUpdate
		wantValue  string
	}{
		{
			name: "member with an earlier pod IP gets the current IP",
			pod:  etcdServerPod("k3k-vc-server-2", "10.0.0.9", started, ""),
			members: []*etcdserverpb.Member{
				member(1, "k3k-vc-server-0-aaaa", "https://10.0.0.1:2380"),
				member(3, "k3k-vc-server-2-cccc", "https://10.0.0.3:2380"),
			},
			wantUpdate: &peerURLUpdate{memberID: 3, peerURL: "https://10.0.0.9:2380"},
			wantValue:  "2000000000 k3k-vc-server-0-aaaa,k3k-vc-server-2-cccc",
		},
		{
			name: "member with the current IP: no update",
			pod:  etcdServerPod("k3k-vc-server-2", "10.0.0.9", started, ""),
			members: []*etcdserverpb.Member{
				member(3, "k3k-vc-server-2-cccc", "https://10.0.0.9:2380"),
			},
			wantValue: "2000000000 k3k-vc-server-2-cccc",
		},
		{
			name: "member removed: only the list (the script requests the rejoin)",
			pod:  etcdServerPod("k3k-vc-server-1", "10.0.0.9", started, ""),
			members: []*etcdserverpb.Member{
				member(10, "k3k-vc-server-10-dddd", "https://10.0.0.10:2380"),
				member(1, "k3k-vc-server-0-aaaa", "https://10.0.0.1:2380"),
			},
			wantValue: "2000000000 k3k-vc-server-0-aaaa,k3k-vc-server-10-dddd",
		},
		{
			name: "two members with the pod name: no update",
			pod:  etcdServerPod("k3k-vc-server-2", "10.0.0.9", started, ""),
			members: []*etcdserverpb.Member{
				member(3, "k3k-vc-server-2-cccc", "https://10.0.0.3:2380"),
				member(4, "k3k-vc-server-2-eeee", "https://10.0.0.4:2380"),
			},
			wantValue: "2000000000 k3k-vc-server-2-cccc,k3k-vc-server-2-eeee",
		},
		{
			name: "learner that has not started (no name) is not listed",
			pod:  etcdServerPod("k3k-vc-server-2", "fd00::9", started, ""),
			members: []*etcdserverpb.Member{
				member(3, "k3k-vc-server-2-cccc", "https://[fd00::3]:2380"),
				{ID: 5, PeerURLs: []string{"https://10.0.0.5:2380"}},
			},
			wantUpdate: &peerURLUpdate{memberID: 3, peerURL: "https://[fd00::9]:2380"},
			wantValue:  "2000000000 k3k-vc-server-2-cccc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repair := planEtcdRepair(tt.pod, tt.members, now)
			assert.Equal(t, tt.wantUpdate, repair.update)
			assert.Equal(t, tt.wantValue, repair.annotation)
		})
	}
}

// The annotation keeps its time while the names do not change: a new time in each reconcile
// would patch the pod again and again.
func TestEtcdMembersValueIsStable(t *testing.T) {
	now := time.Unix(2000000000, 0)
	started := now.Add(-5 * time.Minute)
	written := now.Add(-time.Minute).Unix()

	current := "1999999940 a,b"

	assert.Equal(t, written, int64(1999999940))

	// same names, written after the container start: kept
	assert.Equal(t, current, etcdMembersValue(etcdServerPod("s", "10.0.0.1", started, current), "a,b", now))

	// other names: new value
	assert.Equal(t, "2000000000 a,c", etcdMembersValue(etcdServerPod("s", "10.0.0.1", started, current), "a,c", now))

	// written before the container start (a restart since): new value
	restarted := etcdServerPod("s", "10.0.0.1", now.Add(-30*time.Second), current)
	assert.Equal(t, "2000000000 a,b", etcdMembersValue(restarted, "a,b", now))

	// no or broken annotation: new value
	assert.Equal(t, "2000000000 a,b", etcdMembersValue(etcdServerPod("s", "10.0.0.1", started, ""), "a,b", now))
	assert.Equal(t, "2000000000 a,b", etcdMembersValue(etcdServerPod("s", "10.0.0.1", started, "x a,b"), "a,b", now))
}

func TestRunningSince(t *testing.T) {
	now := time.Now()

	assert.True(t, runningSince(etcdServerPod("s", "", now.Add(-3*time.Minute), ""), now.Add(-2*time.Minute)))
	assert.False(t, runningSince(etcdServerPod("s", "", now.Add(-time.Minute), ""), now.Add(-2*time.Minute)))
	assert.False(t, runningSince(&corev1.Pod{}, now))

	waiting := etcdServerPod("s", "", now.Add(-time.Hour), "")
	waiting.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
	assert.False(t, runningSince(waiting, now))
}
