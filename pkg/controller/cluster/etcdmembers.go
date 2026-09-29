package cluster

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
	"github.com/rancher/k3k/pkg/controller/cluster/server"
)

const (
	// etcdRepairDelay is how long the container of a server pod must run without the pod
	// becoming ready before its etcd membership is repaired. A normal start or join is ready
	// much earlier.
	etcdRepairDelay = 2 * time.Minute

	// etcdRepairInterval is the requeue interval while a server pod is not ready.
	etcdRepairInterval = 30 * time.Second
)

// peerURLUpdate is an etcd member whose peer URL must change to the current pod IP.
type peerURLUpdate struct {
	memberID uint64
	peerURL  string
}

// etcdRepair is the repair for one server pod that stays not ready.
type etcdRepair struct {
	// update is set if the member of the pod still has the peer URL of an earlier pod IP. The
	// pod IP changes when the pod sandbox is made again (node restart) without a pod delete, and
	// k3s does not update its own peer URL: its startup check waits for a member with the
	// current IP forever.
	update *peerURLUpdate

	// annotation is the value for server.EtcdMembersAnnotation: the time and the names of the
	// current members. The startup script requests a rejoin if the member of the pod is not in
	// the list (the member was removed while its etcd did not run, so no tombstone exists).
	annotation string
}

// reconcileEtcdMembers repairs the etcd membership of server pods that stay not ready, and
// removes the members annotation from ready server pods. It needs a ready server (quorum): if
// all servers are down, a restart with new pod IPs needs a cluster reset.
func (p *StatefulSetReconciler) reconcileEtcdMembers(ctx context.Context, cluster *v1beta1.Cluster, pods []corev1.Pod) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	now := time.Now()

	var (
		stuck   []*corev1.Pod
		waiting bool
		ready   bool
	)

	for i := range pods {
		pod := &pods[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}

		if isPodReady(pod) {
			ready = true

			if err := p.setEtcdMembersAnnotation(ctx, pod, ""); err != nil {
				return reconcile.Result{}, err
			}

			continue
		}

		waiting = true

		if pod.Status.PodIP != "" && runningSince(pod, now.Add(-etcdRepairDelay)) {
			stuck = append(stuck, pod)
		}
	}

	if len(stuck) == 0 || !ready {
		if waiting {
			return reconcile.Result{RequeueAfter: etcdRepairInterval}, nil
		}

		return reconcile.Result{}, nil
	}

	client, err := p.etcdClient(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, err
	}
	defer func() { _ = client.Close() }()

	listCtx, cancel := context.WithTimeout(ctx, memberRemovalTimeout)
	defer cancel()

	members, err := client.MemberList(listCtx)
	if err != nil {
		return reconcile.Result{}, err
	}

	for _, pod := range stuck {
		repair := planEtcdRepair(pod, members.Members, now)

		if repair.update != nil {
			log.Info("Updating the etcd peer URL of a server pod with a new IP", "pod", pod.Name, "member", strconv.FormatUint(repair.update.memberID, 16), "peerURL", repair.update.peerURL)

			updateCtx, cancel := context.WithTimeout(ctx, memberRemovalTimeout)
			_, err := client.MemberUpdate(updateCtx, repair.update.memberID, []string{repair.update.peerURL})

			cancel()

			if err != nil {
				return reconcile.Result{}, err
			}
		}

		if err := p.setEtcdMembersAnnotation(ctx, pod, repair.annotation); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{RequeueAfter: etcdRepairInterval}, nil
}

// planEtcdRepair returns the repair for a server pod that stays not ready. The etcd member of a
// server pod has the name "<pod name>-<random suffix>" (k3s).
func planEtcdRepair(pod *corev1.Pod, members []*etcdserverpb.Member, now time.Time) etcdRepair {
	var (
		names []string
		own   []*etcdserverpb.Member
	)

	for _, member := range members {
		if member.Name == "" {
			continue
		}

		names = append(names, member.Name)

		if strings.HasPrefix(member.Name, pod.Name+"-") {
			own = append(own, member)
		}
	}

	slices.Sort(names)

	repair := etcdRepair{
		annotation: etcdMembersValue(pod, strings.Join(names, ","), now),
	}

	// more than one member with the name of this pod: not clear which is the current one
	if len(own) != 1 || len(own[0].PeerURLs) == 0 {
		return repair
	}

	peerURL := "https://" + net.JoinHostPort(pod.Status.PodIP, "2380")

	u, err := url.Parse(own[0].PeerURLs[0])
	if err != nil || u.Hostname() == pod.Status.PodIP {
		return repair
	}

	repair.update = &peerURLUpdate{memberID: own[0].ID, peerURL: peerURL}

	return repair
}

// etcdMembersValue returns the annotation value for the member list. It keeps the current value
// if it has the same names and was written after the start of the container: the startup script
// needs only a list that is newer than its start, and a new time in each reconcile would patch
// the pod (and start the next reconcile) again and again.
func etcdMembersValue(pod *corev1.Pod, names string, now time.Time) string {
	if current, ok := pod.Annotations[server.EtcdMembersAnnotation]; ok {
		listed, currentNames, _ := strings.Cut(current, " ")

		unix, err := strconv.ParseInt(listed, 10, 64)
		if err == nil && currentNames == names && startedBefore(pod, time.Unix(unix, 0)) {
			return current
		}
	}

	return fmt.Sprintf("%d %s", now.Unix(), names)
}

// setEtcdMembersAnnotation sets the members annotation of a server pod, or removes it if value
// is empty.
func (p *StatefulSetReconciler) setEtcdMembersAnnotation(ctx context.Context, pod *corev1.Pod, value string) error {
	current, found := pod.Annotations[server.EtcdMembersAnnotation]
	if current == value && (found || value == "") {
		return nil
	}

	patch := ctrlruntimeclient.MergeFrom(pod.DeepCopy())

	if value == "" {
		delete(pod.Annotations, server.EtcdMembersAnnotation)
	} else {
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}

		pod.Annotations[server.EtcdMembersAnnotation] = value
	}

	return ctrlruntimeclient.IgnoreNotFound(p.Client.Patch(ctx, pod, patch))
}

func isPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

// startedBefore reports whether all containers of the pod run and started before t (whole
// seconds, as in the annotation).
func startedBefore(pod *corev1.Pod, t time.Time) bool {
	return runningSince(pod, t.Add(-time.Second))
}

// runningSince reports whether all containers of the pod run and started before t.
func runningSince(pod *corev1.Pod, t time.Time) bool {
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}

	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Running == nil || status.State.Running.StartedAt.After(t) {
			return false
		}
	}

	return true
}
