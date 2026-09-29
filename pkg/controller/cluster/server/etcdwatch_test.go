package server

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

func renderStartCommand(t *testing.T) string {
	t.Helper()

	cluster := &v1beta1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "ns"},
		Spec:       v1beta1.ClusterSpec{Servers: ptr.To[int32](3), Mode: v1beta1.SharedClusterMode},
	}

	cmd, err := New(cluster, nil, "token", "rancher/k3s:v1.34.9-k3s1", "IfNotPresent", nil).setupStartCommand()
	require.NoError(t, err)

	return cmd
}

func TestStartCommandSyntax(t *testing.T) {
	cmd := renderStartCommand(t)

	out, err := exec.Command("bash", "-n", "-c", cmd).CombinedOutput()
	require.NoError(t, err, string(out))

	// the liveness probe reads /var/log/k3s.log: every k3s server must write to it
	assert.NotContains(t, cmd, "k3s.info")
	assert.Contains(t, cmd, "watch_etcd_membership &")
	assert.Contains(t, cmd, "rm -f "+k3kRejoinFile)
}

// The liveness probe restarts the server on a rejoin request of the startup script and on the
// removal message of etcd.
func TestLivenessProbeReadsRejoinRequest(t *testing.T) {
	s := New(&v1beta1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}, nil, "token", "img", "", nil)
	probe := s.podSpec(t.Context(), "img", "k3k-c-server", false, "").Containers[0].LivenessProbe

	cmd := strings.Join(probe.Exec.Command, " ")
	assert.Contains(t, cmd, "/var/log/k3s.log")
	assert.Contains(t, cmd, k3kRejoinFile)

	// the message of k3s after it wrote the tombstone restarts the server; the rejoin message and
	// the etcd error (no tombstone yet) must not
	assert.Contains(t, "This node has been removed from the cluster - please restart k3s to rejoin the cluster", removedFromClusterText)
	assert.NotContains(t, "tombstone file has been detected, removing ${datadir}/server/db to rejoin the cluster", removedFromClusterText)
	assert.NotContains(t, "the member has been permanently removed from the cluster", removedFromClusterText)
	assert.Contains(t, "the member has been permanently removed from the cluster", etcdRemovedText)
}

// The watcher writes the tombstone only if a member list that is newer than the container start
// does not contain the member name of the server.
func TestWatchEtcdMembership(t *testing.T) {
	cmd := renderStartCommand(t)

	start := strings.Index(cmd, "watch_etcd_membership() {")
	require.GreaterOrEqual(t, start, 0)

	end := strings.Index(cmd[start:], "\n}\n")
	require.Greater(t, end, 0)

	function := cmd[start : start+end+3]

	now := time.Now().Unix()

	etcdError := `{"level":"warn","msg":"server error","error":"the member has been permanently removed from the cluster"}`

	tests := []struct {
		name          string
		annotation    string
		k3sLog        string
		wantTombstone bool
	}{
		{name: "own member removed", annotation: fmt.Sprintf("%d s-0-aaaa,s-1-bbbb", now+100), wantTombstone: true},
		{name: "own member listed", annotation: fmt.Sprintf("%d s-0-aaaa,s-2-cccc,s-1-bbbb", now+100)},
		{name: "list older than the container", annotation: fmt.Sprintf("%d s-0-aaaa,s-1-bbbb", now-100)},
		{name: "no list"},
		{name: "etcd reports the removal", k3sLog: etcdError, wantTombstone: true},
		{name: "etcd reports the removal, own member listed", annotation: fmt.Sprintf("%d s-2-cccc", now+100), k3sLog: etcdError, wantTombstone: true},
		{name: "rejoin message of k3s is no signal", k3sLog: "tombstone file has been detected, removing ${datadir}/server/db to rejoin the cluster"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			etcdDir := filepath.Join(dir, "etcd")
			metaDir := filepath.Join(dir, "meta")
			logFile := filepath.Join(dir, "k3k-rejoin")

			require.NoError(t, os.MkdirAll(etcdDir, 0o755))
			require.NoError(t, os.MkdirAll(metaDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(etcdDir, "name"), []byte("s-2-cccc"), 0o644))

			annotations := `other="x"` + "\n"
			if tt.annotation != "" {
				annotations += fmt.Sprintf("%s=%q\n", EtcdMembersAnnotation, tt.annotation)
			}

			require.NoError(t, os.WriteFile(filepath.Join(metaDir, "annotations"), []byte(annotations), 0o644))

			k3sLogFile := filepath.Join(dir, "k3s.log")
			require.NoError(t, os.WriteFile(k3sLogFile, []byte(tt.k3sLog+"\n"), 0o644))

			f := strings.NewReplacer(k3sETCDDataDir, etcdDir, k3kMetaDir, metaDir, k3kRejoinFile, logFile, "/var/log/k3s.log", k3sLogFile).Replace(function)

			// three passes of the loop without waiting
			script := "info() { echo \"$@\"; }\nn=0\nsleep() { n=$((n+1)); [ $n -le 3 ]; }\n" + f + "watch_etcd_membership\n"

			out, err := exec.Command("bash", "-c", script).CombinedOutput()
			require.NoError(t, err, string(out))

			_, statErr := os.Stat(filepath.Join(etcdDir, "tombstone"))
			logged, _ := os.ReadFile(logFile)

			if tt.wantTombstone {
				assert.NoError(t, statErr, "tombstone must exist")
				assert.Contains(t, string(logged), removedFromClusterText, "the liveness probe must see the request")

				return
			}

			assert.True(t, os.IsNotExist(statErr), "no tombstone")
			assert.Empty(t, logged)
		})
	}
}
