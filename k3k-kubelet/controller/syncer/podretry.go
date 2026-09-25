package syncer

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podRetryControllerName = "pod-retry-controller"

	// ProviderRetryAnnotation is stamped on a virtual pod whose host pod could
	// not be created, to make the pod controller try again.
	ProviderRetryAnnotation = "k3k.io/provider-retry"

	// providerFailedReason is the status reason the virtual-kubelet pod
	// controller sets when the provider (the host) rejected the pod.
	providerFailedReason = "ProviderFailed"

	podRetryInterval = 2 * time.Minute
)

// PodRetryReconciler brings back pods that failed to start on the host.
//
// The virtual-kubelet pod controller marks such a pod Pending/ProviderFailed
// and retries with a growing backoff, but only a limited number of times;
// then it forgets the pod. Status updates and informer resyncs do not start
// a new attempt, only a change of the pod spec, labels or annotations does.
// A transient host failure (API outage, admission webhook down, quota) thus
// left the pod stuck until someone deleted it. This controller stamps an
// annotation on such pods at a fixed interval, which starts a new attempt.
type PodRetryReconciler struct {
	VirtualClient ctrlruntimeclient.Client
	Interval      time.Duration
	Now           func() time.Time
}

// AddPodRetryController registers the controller with the virtual manager
// (leader election applies; the pod controller of the agent that owns the
// pod reacts to the annotation).
func AddPodRetryController(virtMgr manager.Manager, clusterName, clusterNamespace string) error {
	reconciler := &PodRetryReconciler{
		VirtualClient: virtMgr.GetClient(),
		Interval:      podRetryInterval,
		Now:           time.Now,
	}

	name := clusterName + "-" + clusterNamespace + "-" + podRetryControllerName

	return ctrl.NewControllerManagedBy(virtMgr).
		Named(name).
		For(&corev1.Pod{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj ctrlruntimeclient.Object) bool {
			pod, ok := obj.(*corev1.Pod)
			return ok && providerFailed(pod)
		}))).
		Complete(reconciler)
}

func providerFailed(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp.IsZero() &&
		pod.Status.Phase == corev1.PodPending &&
		pod.Status.Reason == providerFailedReason
}

func (r *PodRetryReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var pod corev1.Pod
	if err := r.VirtualClient.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, &pod); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	if !providerFailed(&pod) {
		return reconcile.Result{}, nil
	}

	now := r.Now()

	// Wait one interval after the last retry, or after the pod creation
	// (the first failure gets the pod controller's own quick retries; a pod
	// that never started has no status.startTime).
	last := pod.CreationTimestamp.Time
	if t, err := time.Parse(time.RFC3339, pod.Annotations[ProviderRetryAnnotation]); err == nil {
		last = t
	}

	if wait := last.Add(r.Interval).Sub(now); wait > 0 {
		return reconcile.Result{RequeueAfter: wait}, nil
	}

	ctrl.LoggerFrom(ctx).Info("retrying a pod that failed to start on the host", "pod", req.String(), "message", pod.Status.Message)

	patch := ctrlruntimeclient.MergeFrom(pod.DeepCopy())

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	pod.Annotations[ProviderRetryAnnotation] = now.UTC().Format(time.RFC3339)

	if err := r.VirtualClient.Patch(ctx, &pod, patch); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	// if the attempt fails again, the status update brings the pod back here
	return reconcile.Result{RequeueAfter: r.Interval}, nil
}
