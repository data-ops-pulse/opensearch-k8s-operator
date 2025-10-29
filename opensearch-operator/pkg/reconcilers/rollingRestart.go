package reconcilers

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	opsterv1 "github.com/Opster/opensearch-k8s-operator/opensearch-operator/api/v1"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/opensearch-gateway/services"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/pkg/builders"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/pkg/helpers"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/k8s"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"github.com/Opster/opensearch-k8s-operator/opensearch-operator/pkg/reconcilers/util"
	"github.com/cisco-open/operator-tools/pkg/reconciler"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	componentName    = "RollingRestart"
	statusInProgress = "InProgress"
	statusFinished   = "Finished"
	statusFailed     = "Failed"
	statusPaused     = "Paused"
	statusRecovering = "Recovering"
)

// Rolling restart configuration constants
const (
	DefaultMaxRetries           = 5
	DefaultRetryInterval        = 30 * time.Second
	DefaultMaxFailureDuration   = 10 * time.Minute
)

// candidate represents a pod that is a candidate for rolling restart
type candidate struct {
	podName  string
	podNS    string
	sts      appsv1.StatefulSet
	nodePool opsterv1.NodePool
	isMaster bool
	ordinal  int
}

// RollingRestartFailureHandler manages failure scenarios and recovery
type RollingRestartFailureHandler struct {
	MaxRetries           int
	RetryInterval        time.Duration
	MaxFailureDuration   time.Duration
	FailureCount         int
	FirstFailureTime     time.Time
	LastFailureType      string
	AutoRecoveryEnabled  bool
}

type RollingRestartReconciler struct {
	client            k8s.K8sClient
	ctx               context.Context
	instance          *opsterv1.OpenSearchCluster
	logger            logr.Logger
	osClient          *services.OsClusterClient
	recorder          record.EventRecorder
	reconcilerContext *ReconcilerContext
	failureHandler    *RollingRestartFailureHandler
}

func NewRollingRestartReconciler(
	client client.Client,
	ctx context.Context,
	recorder record.EventRecorder,
	reconcilerContext *ReconcilerContext,
	instance *opsterv1.OpenSearchCluster,
	opts ...reconciler.ResourceReconcilerOption,
) *RollingRestartReconciler {
	return &RollingRestartReconciler{
		client:            k8s.NewK8sClient(client, ctx, append(opts, reconciler.WithLog(log.FromContext(ctx).WithValues("reconciler", "restart")))...),
		ctx:               ctx,
		instance:          instance,
		logger:            log.FromContext(ctx).WithValues("reconciler", "restart"),
		recorder:          recorder,
		reconcilerContext: reconcilerContext,
		failureHandler: &RollingRestartFailureHandler{
			MaxRetries:          DefaultMaxRetries,
			RetryInterval:       DefaultRetryInterval,
			MaxFailureDuration:  DefaultMaxFailureDuration,
			AutoRecoveryEnabled: true,
		},
	}
}

func (r *RollingRestartReconciler) Reconcile() (ctrl.Result, error) {
	// Check if rolling restart is paused
	if r.isPaused() {
		r.logger.Info("Rolling restart is paused via annotation")
		return ctrl.Result{Requeue: true, RequeueAfter: 30 * time.Second}, nil
	}

	// Check if we're in recovery mode
	if r.instance.Annotations["opensearch.opster.io/rolling-restart-failed"] == "true" {
		if r.instance.Annotations["opensearch.opster.io/rolling-restart-recovery"] == "true" {
			r.logger.Info("Attempting recovery from previous failure")
			if err := r.emergencyRecovery(); err != nil {
				return r.handleFailure(err, "recovery_failed")
			}
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
		}
		r.logger.Info("Rolling restart is in failed state, waiting for manual intervention")
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Minute}, nil
	}

	// We should never get to this while an upgrade is in progress
	// but put a defensive check in
	if r.instance.Status.Version != "" && r.instance.Status.Version != r.instance.Spec.General.Version {
		r.logger.V(1).Info("Upgrade in progress, skipping rolling restart")
		return ctrl.Result{}, nil
	}

	status := r.findStatus()
	var pendingUpdate bool

	// Check that all nodes are ready before doing work
	// Also check if there are pending updates for all nodes.
	for _, nodePool := range r.instance.Spec.NodePools {
		sts, err := r.client.GetStatefulSet(builders.StsName(r.instance, &nodePool), r.instance.Namespace)
		if err != nil {
			return r.handleFailure(err, "statefulset_retrieval_failed")
		}

		// Check for pending updates
		if sts.Status.UpdateRevision != "" &&
			sts.Status.UpdatedReplicas != ptr.Deref(sts.Spec.Replicas, int32(1)) {
			pendingUpdate = true
		} else if sts.Status.UpdateRevision != "" &&
			sts.Status.CurrentRevision != sts.Status.UpdateRevision {
			// If all pods in sts are updated to spec.replicas but current version is not updated.
			err := r.client.UdateObjectStatus(&sts, func(object client.Object) {
				instance := object.(*appsv1.StatefulSet)
				instance.Status.CurrentRevision = sts.Status.UpdateRevision
			})
			if err != nil {
				r.logger.Error(err, "failed to update status")
				return r.handleFailure(err, "status_update_failed")
			}
		}

		if sts.Status.ReadyReplicas != ptr.Deref(sts.Spec.Replicas, 1) {
			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: 10 * time.Second,
			}, nil
		}
	}

	if !pendingUpdate {
		// Check if we had a restart running that is finished so that we can reactivate shard allocation
		if status != nil && status.Status == statusInProgress {
			osClient, err := util.CreateClientForCluster(r.client, r.ctx, r.instance, nil)
			if err != nil {
				return ctrl.Result{Requeue: true}, err
			}
			if err = services.ReactivateShardAllocation(osClient); err != nil {
				r.logger.V(1).Info("Restart complete. Reactivating shard allocation")
				return ctrl.Result{Requeue: true}, err
			}
			r.recorder.AnnotatedEventf(r.instance, map[string]string{"cluster-name": r.instance.GetName()}, "Normal", "RollingRestart", "Rolling restart completed")
			if err = r.updateStatus(statusFinished); err != nil {
				return ctrl.Result{Requeue: true}, err
			}
		}
		r.logger.V(1).Info("No pods pending restart")
		return ctrl.Result{}, nil
	}

	// Skip a rolling restart if the cluster hasn't finished initializing
	if !r.instance.Status.Initialized {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: 10 * time.Second,
		}, nil
	}

	if err := r.updateStatus(statusInProgress); err != nil {
		return ctrl.Result{Requeue: true}, err
	}
	r.recorder.AnnotatedEventf(r.instance, map[string]string{"cluster-name": r.instance.GetName()}, "Normal", "RollingRestart", "Starting rolling restart")

	// If there is work to do create an Opensearch Client
	var err error

	r.osClient, err = util.CreateClientForCluster(r.client, r.ctx, r.instance, nil)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Use global candidate selection for rolling restart
	return r.globalCandidateRollingRestart()
}

// globalCandidateRollingRestart aggregates candidates across all StatefulSets,
// orders them and restarts at most one pod per reconciliation.
func (r *RollingRestartReconciler) globalCandidateRollingRestart() (ctrl.Result, error) {
	r.logger.Info("Starting global candidate rolling restart")

	// Build candidate list across all node pools
	var candidates []candidate

	for _, np := range r.instance.Spec.NodePools {
		sts, err := r.client.GetStatefulSet(builders.StsName(r.instance, &np), r.instance.Namespace)
		if err != nil {
			r.logger.Error(err, "Failed to get StatefulSet", "nodePool", np.Component)
			return ctrl.Result{}, fmt.Errorf("failed to get StatefulSet for nodePool %s: %w", np.Component, err)
		}

		r.logger.V(1).Info("Checking StatefulSet for pending updates",
			"nodePool", np.Component,
			"updateRevision", sts.Status.UpdateRevision,
			"updatedReplicas", sts.Status.UpdatedReplicas,
			"desiredReplicas", ptr.Deref(sts.Spec.Replicas, 1))

		if sts.Status.UpdateRevision == "" || sts.Status.UpdatedReplicas == ptr.Deref(sts.Spec.Replicas, 1) {
			r.logger.V(1).Info("StatefulSet has no pending updates", "nodePool", np.Component)
			continue
		}

		pod, err := helpers.GetPodWithOlderRevision(r.client, &sts)
		if err != nil {
			r.logger.Error(err, "Failed to get pod with older revision", "nodePool", np.Component)
			return ctrl.Result{}, fmt.Errorf("failed to get pod with older revision for nodePool %s: %w", np.Component, err)
		}
		if pod == nil {
			r.logger.V(1).Info("No pod with older revision found", "nodePool", np.Component)
			continue
		}

		ord := parseOrdinalFromName(pod.Name)
		isMaster := helpers.HasManagerRole(&np)
		r.logger.Info("Found candidate pod",
			"pod", pod.Name,
			"nodePool", np.Component,
			"isMaster", isMaster,
			"ordinal", ord)

		candidates = append(candidates, candidate{
			podName:  pod.Name,
			podNS:    pod.Namespace,
			sts:      sts,
			nodePool: np,
			isMaster: isMaster,
			ordinal:  ord,
		})
	}

	r.logger.Info("Found candidates for rolling restart", "count", len(candidates))

	if len(candidates) == 0 {
		r.logger.Info("No candidates found, rolling restart complete")
		return ctrl.Result{}, nil
	}

	// Sort: prefer non-masters, then by StatefulSet name ASC, then ordinal DESC
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].isMaster != candidates[j].isMaster {
			return !candidates[i].isMaster && candidates[j].isMaster
		}
		if candidates[i].sts.Name != candidates[j].sts.Name {
			return candidates[i].sts.Name < candidates[j].sts.Name
		}
		return candidates[i].ordinal > candidates[j].ordinal
	})

	r.logger.V(1).Info("Sorted candidates", "candidates", func() []string {
		var names []string
		for _, c := range candidates {
			names = append(names, fmt.Sprintf("%s(isMaster:%v,ordinal:%d)", c.podName, c.isMaster, c.ordinal))
		}
		return names
	}())

	// Enforce master quorum: if next is master, ensure safe to disrupt
	next := candidates[0]
	r.logger.Info("Selected candidate for restart",
		"pod", next.podName,
		"nodePool", next.nodePool.Component,
		"isMaster", next.isMaster,
		"ordinal", next.ordinal)

	if next.isMaster {
		totalMasters, readyMasters, err := r.countMasters()
		if err != nil {
			r.logger.Error(err, "Failed to count masters")
			return ctrl.Result{}, fmt.Errorf("failed to count masters: %w", err)
		}
		minRequired := (totalMasters + 1) / 2
		r.logger.Info("Checking master quorum",
			"totalMasters", totalMasters,
			"readyMasters", readyMasters,
			"minRequired", minRequired)

		if readyMasters <= minRequired {
			r.logger.Info("Master quorum unsafe, looking for non-master candidate")
			// Try to pick the first non-master candidate instead, if any
			for _, c := range candidates {
				if !c.isMaster {
					next = c
					r.logger.Info("Switched to non-master candidate",
						"pod", next.podName,
						"nodePool", next.nodePool.Component)
					break
				}
			}
			// If still master and unsafe, requeue and wait
			if next.isMaster && readyMasters <= minRequired {
				r.logger.Info("No safe non-master candidates, requeuing to wait for quorum")
				return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
			}
		} else {
			r.logger.Info("Master quorum is safe, proceeding with restart")
		}
	}

	// Restart exactly this candidate pod
	res, err := r.restartSpecificPod(next)
	if err != nil {
		return ctrl.Result{}, err
	}
	if res.Requeue {
		// restartSpecificPod needs to wait (cluster not ready, pod not ready, etc.)
		return res, nil
	}
	// Pod deleted successfully, continue to next reconciliation to check for more candidates
	return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
}

func parseOrdinalFromName(name string) int {
	// expects name like <stsName>-<ordinal>
	parts := strings.Split(name, "-")
	if len(parts) == 0 {
		return -1
	}
	ord, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return -1
	}
	return ord
}

func (r *RollingRestartReconciler) countMasters() (int32, int32, error) {
	var total, ready int32
	for _, np := range r.instance.Spec.NodePools {
		if !helpers.HasManagerRole(&np) {
			continue
		}
		sts, err := r.client.GetStatefulSet(builders.StsName(r.instance, &np), r.instance.Namespace)
		if err != nil {
			return 0, 0, err
		}
		total += ptr.Deref(sts.Spec.Replicas, 1)
		ready += sts.Status.ReadyReplicas
	}
	return total, ready, nil
}

// restartSpecificPod performs the prechecks and deletes a specific pod
func (r *RollingRestartReconciler) restartSpecificPod(cand interface{}) (ctrl.Result, error) {
	c := cand.(candidate)

	dataCount := util.DataNodesCount(r.client, r.instance)
	if dataCount == 2 && r.instance.Spec.General.DrainDataNodes {
		r.logger.Info("Only 2 data nodes and drain is set, some shards may not drain")
	}

	ready, message, err := services.CheckClusterStatusForRestart(r.osClient, r.instance.Spec.General.DrainDataNodes)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cluster health check failed: %w", err)
	}
	if !ready {
		r.logger.Info(fmt.Sprintf("Couldn't proceed with rolling restart for Pod %s because %s", c.podName, message))
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	r.logger.Info(fmt.Sprintf("Preparing to restart pod %s", c.podName))
	ready, err = services.PreparePodForDelete(r.osClient, r.logger, c.podName, r.instance.Spec.General.DrainDataNodes, dataCount)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("pod preparation failed: %w", err)
	}
	if !ready {
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	if err := r.client.DeletePod(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: c.podName, Namespace: c.podNS}}); err != nil {
		return ctrl.Result{}, fmt.Errorf("pod deletion failed: %w", err)
	}

	if r.instance.Spec.General.DrainDataNodes {
		_, err = services.RemoveExcludeNodeHost(r.osClient, c.podName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to remove exclude node host: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

func (r *RollingRestartReconciler) updateStatus(status string) error {
	return UpdateComponentStatus(r.client, r.instance, &opsterv1.ComponentStatus{
		Component:   componentName,
		Status:      status,
		Description: "",
	})
}

func (r *RollingRestartReconciler) findStatus() *opsterv1.ComponentStatus {
	comp := r.instance.Status.ComponentsStatus
	_, found := helpers.FindFirstPartial(comp, opsterv1.ComponentStatus{
		Component: componentName,
	}, helpers.GetByComponent)
	if found {
		return &comp[0]
	}
	return nil
}

// isPaused checks if rolling restart is paused via annotation
func (r *RollingRestartReconciler) isPaused() bool {
	return r.instance.Annotations["opensearch.opster.io/rolling-restart-paused"] == "true"
}

// handleFailure manages failure scenarios with retry limits and rollback
func (r *RollingRestartReconciler) handleFailure(err error, failureType string) (ctrl.Result, error) {
	r.failureHandler.FailureCount++
	r.failureHandler.LastFailureType = failureType

	if r.failureHandler.FailureCount == 1 {
		r.failureHandler.FirstFailureTime = time.Now()
	}

	r.logger.Error(err, "Rolling restart failure", 
		"failureType", failureType,
		"failureCount", r.failureHandler.FailureCount,
		"maxRetries", r.failureHandler.MaxRetries)

	// Check if we've exceeded retry limits
	if r.failureHandler.FailureCount > r.failureHandler.MaxRetries ||
		time.Since(r.failureHandler.FirstFailureTime) > r.failureHandler.MaxFailureDuration {

		return r.triggerRollback(failureType, err)
	}

	// Exponential backoff for retries
	backoffDuration := time.Duration(r.failureHandler.FailureCount) * r.failureHandler.RetryInterval
	r.logger.Info("Retrying rolling restart after backoff", 
		"backoffDuration", backoffDuration,
		"failureCount", r.failureHandler.FailureCount)

	return ctrl.Result{Requeue: true, RequeueAfter: backoffDuration}, nil
}

// triggerRollback initiates rollback procedures
func (r *RollingRestartReconciler) triggerRollback(failureType string, err error) (ctrl.Result, error) {
	r.logger.Error(err, "Rolling restart failed, triggering rollback", "failureType", failureType)

	// 1. Pause rolling restart
	if updateErr := r.updateStatus(statusFailed); updateErr != nil {
		r.logger.Error(updateErr, "Failed to update status to failed")
	}

	// 2. Reactivate shard allocation
	if r.osClient != nil {
		if reactivateErr := services.ReactivateShardAllocation(r.osClient); reactivateErr != nil {
			r.logger.Error(reactivateErr, "Failed to reactivate shard allocation during rollback")
		}
	}

	// 3. Emit failure event
	r.recorder.AnnotatedEventf(r.instance, 
		map[string]string{
			"cluster-name": r.instance.GetName(), 
			"failure-type": failureType,
			"failure-count": fmt.Sprintf("%d", r.failureHandler.FailureCount),
		}, 
		"Warning", "RollingRestart", "Rolling restart failed: %s", err.Error())

	// 4. Set failure annotation for manual intervention
	r.setFailureAnnotation(failureType, err.Error())

	// 5. Requeue for manual intervention
	return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Minute}, nil
}

// setFailureAnnotation sets failure information in cluster annotations
func (r *RollingRestartReconciler) setFailureAnnotation(failureType, errorMessage string) {
	annotations := map[string]string{
		"opensearch.opster.io/rolling-restart-failed":     "true",
		"opensearch.opster.io/rolling-restart-failure-type": failureType,
		"opensearch.opster.io/rolling-restart-error":      errorMessage,
		"opensearch.opster.io/rolling-restart-failure-time": time.Now().Format(time.RFC3339),
		"opensearch.opster.io/rolling-restart-failure-count": fmt.Sprintf("%d", r.failureHandler.FailureCount),
	}

	// Update the cluster resource using the proper method
	if err := r.client.UpdateOpenSearchClusterStatus(client.ObjectKeyFromObject(r.instance), func(instance *opsterv1.OpenSearchCluster) {
		if instance.Annotations == nil {
			instance.Annotations = make(map[string]string)
		}
		for k, v := range annotations {
			instance.Annotations[k] = v
		}
	}); err != nil {
		r.logger.Error(err, "Failed to update cluster annotations")
	}
}


// emergencyRecovery attempts emergency recovery procedures
func (r *RollingRestartReconciler) emergencyRecovery() error {
	r.logger.Info("Attempting emergency recovery")

	// 1. Update status to recovering
	if err := r.updateStatus(statusRecovering); err != nil {
		r.logger.Error(err, "Failed to update status to recovering")
	}

	// 2. Force shard allocation to all
	if r.osClient != nil {
		if err := r.forceShardAllocation(); err != nil {
			r.logger.Error(err, "Failed to force shard allocation")
			return err
		}
	}

	// 3. Reset failure handler
	r.failureHandler.FailureCount = 0
	r.failureHandler.FirstFailureTime = time.Time{}
	r.failureHandler.LastFailureType = ""

	// 4. Clear failure annotations
	r.clearFailureAnnotations()

	// 5. Reset status
	if err := r.updateStatus(statusFinished); err != nil {
		r.logger.Error(err, "Failed to update status to finished after recovery")
	}

	r.logger.Info("Emergency recovery completed successfully")
	return nil
}

// forceShardAllocation forces shard allocation to all
func (r *RollingRestartReconciler) forceShardAllocation() error {
	if r.osClient == nil {
		return fmt.Errorf("no OpenSearch client available")
	}

	// Force shard allocation to all
	if err := services.SetClusterShardAllocation(r.osClient, services.ClusterSettingsAllocationAll); err != nil {
		return fmt.Errorf("failed to set shard allocation to all: %w", err)
	}

	r.logger.Info("Forced shard allocation to all")
	return nil
}

// clearFailureAnnotations removes failure-related annotations
func (r *RollingRestartReconciler) clearFailureAnnotations() {
	failureKeys := []string{
		"opensearch.opster.io/rolling-restart-failed",
		"opensearch.opster.io/rolling-restart-failure-type",
		"opensearch.opster.io/rolling-restart-error",
		"opensearch.opster.io/rolling-restart-failure-time",
		"opensearch.opster.io/rolling-restart-failure-count",
	}

	// Update the cluster resource using the proper method
	if err := r.client.UpdateOpenSearchClusterStatus(client.ObjectKeyFromObject(r.instance), func(instance *opsterv1.OpenSearchCluster) {
		if instance.Annotations != nil {
			for _, key := range failureKeys {
				delete(instance.Annotations, key)
			}
		}
	}); err != nil {
		r.logger.Error(err, "Failed to clear failure annotations")
	}
}

func (r *RollingRestartReconciler) DeleteResources() (ctrl.Result, error) {
	result := reconciler.CombinedResult{}
	return result.Result, result.Err
}
