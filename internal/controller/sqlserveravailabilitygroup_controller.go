/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
	"github.com/anocentino/sql-on-k8s-operator/internal/sqlutil"
)

// SQLServerAvailabilityGroupReconciler reconciles a SQLServerAvailabilityGroup object.
type SQLServerAvailabilityGroupReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubernetes.Interface
	RestConfig *rest.Config

	// reseatFirstFailureTime records when the first consecutive 41104 failure was
	// observed for each pod. Key: "namespace/agname/podname".
	// When Msg 41104 is seen the operator simply retries SET(ROLE=SECONDARY)
	// on each reconcile until SQL Server's internal AG resource manager clears
	// the previous error autonomously. Cleared on success.
	reseatFirstFailureTime sync.Map
}

// effectiveClusterType returns the string value of the cluster type, defaulting to NONE
// when the spec field is empty (backward compatibility with CRs that predate the field).
func effectiveClusterType(ct sqlv1alpha1.AGClusterType) string {
	if ct == "" {
		return string(sqlv1alpha1.AGClusterTypeNone)
	}
	return string(ct)
}

// Label keys used to tag AG pod roles so Service selectors can dynamically route traffic.
const (
	// labelAGRole is set on every AG pod to reflect its current SQL Server AG role.
	// Values: "primary", "readable-secondary", "secondary".
	labelAGRole = "sql.mssql.microsoft.com/ag-role"
	// agRolePrimary is the SQL Server AG role string returned by DMV queries for the primary replica.
	agRolePrimary = "PRIMARY"
	// agRoleSecondary is the SQL Server AG role string returned by DMV queries for secondary replicas.
	agRoleSecondary = "SECONDARY"
	// agRoleResolving is the SQL Server AG role string for replicas in transition (e.g. after JOIN or restart).
	agRoleResolving = "RESOLVING"
	// agClusterTypeNone is the T-SQL cluster type string for standalone (read-scale) mode.
	// Defined as a named constant to satisfy goconst and give callsites a self-documenting name.
	agClusterTypeNone = "NONE"
)

// +kubebuilder:rbac:groups=sql.mssql.microsoft.com,resources=sqlserveravailabilitygroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sql.mssql.microsoft.com,resources=sqlserveravailabilitygroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sql.mssql.microsoft.com,resources=sqlserveravailabilitygroups/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;patch

// Reconcile drives the desired state of a SQLServerAvailabilityGroup.
func (r *SQLServerAvailabilityGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	ag := &sqlv1alpha1.SQLServerAvailabilityGroup{}
	if err := r.Get(ctx, req.NamespacedName, ag); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling SQLServerAvailabilityGroup", "name", ag.Name)

	// --- Finalizer bookkeeping ---
	// The finalizer blocks Kubernetes from fully deleting the CR until we have a
	// chance to clean up in-memory controller state keyed off this AG (41104 and
	// re-seat timers in r.reseatFirstFailureTime). Kubernetes owner references
	// garbage-collect the StatefulSet, Services, and ConfigMap, but the sync.Map
	// entries are controller-local and otherwise leak for the life of the pod.
	if ag.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(ag, agFinalizer) {
			controllerutil.AddFinalizer(ag, agFinalizer)
			if err := r.Update(ctx, ag); err != nil {
				return ctrl.Result{}, fmt.Errorf("could not add finalizer: %w", err)
			}
			// Re-fetch so subsequent patch bases reflect the new ResourceVersion.
			if err := r.Get(ctx, req.NamespacedName, ag); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		if controllerutil.ContainsFinalizer(ag, agFinalizer) {
			r.clearAllRecoveryState(ag)
			controllerutil.RemoveFinalizer(ag, agFinalizer)
			if err := r.Update(ctx, ag); err != nil {
				return ctrl.Result{}, fmt.Errorf("could not remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// 1. Reconcile shared mssql.conf ConfigMap (hadr.hadrenabled = 1)
	if err := r.reconcileAGConfigMap(ctx, ag); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile AG ConfigMap: %w", err)
	}

	// 2. Reconcile the StatefulSet (all replicas in one set)
	if err := r.reconcileAGStatefulSet(ctx, ag); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile AG StatefulSet: %w", err)
	}

	// 3. Reconcile headless service for pod DNS
	if err := r.reconcileAGHeadlessService(ctx, ag); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile AG headless service: %w", err)
	}

	// 4. Reconcile listener services
	if ag.Spec.Listener != nil {
		if err := r.reconcileListenerService(ctx, ag); err != nil {
			return ctrl.Result{}, fmt.Errorf("could not reconcile AG listener service: %w", err)
		}
	}
	if ag.Spec.ReadOnlyListener != nil {
		if err := r.reconcileReadOnlyListenerService(ctx, ag); err != nil {
			return ctrl.Result{}, fmt.Errorf("could not reconcile AG read-only listener service: %w", err)
		}
	}

	// 5. Bootstrap the AG (idempotent): certificates, endpoints, CREATE AG, JOIN
	if !ag.Status.InitializationComplete {
		result, err := r.bootstrapAG(ctx, ag)
		if err != nil {
			log.Error(err, "AG bootstrap failed; will retry")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if result.RequeueAfter > 0 {
			return result, nil
		}
		// Re-fetch after bootstrap so updateAGStatus starts with the latest
		// ResourceVersion (the bootstrap status patch bumped it).
		if err := r.Get(ctx, req.NamespacedName, ag); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 6. Update AG status (replica roles, sync states)
	if err := r.updateAGStatus(ctx, ag); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not update AG status: %w", err)
	}

	// 7. Automatic failover detection (CLUSTER_TYPE = EXTERNAL only).
	// reconcileFailover checks whether the primary pod is NotReady and, if it has
	// been NotReady for longer than the configured threshold, promotes the best
	// synchronous secondary. It returns a non-zero RequeueAfter while it is waiting.
	if result, err := r.reconcileFailover(ctx, ag); err != nil {
		return ctrl.Result{}, err
	} else if result.RequeueAfter > 0 {
		return result, nil
	}

	// 8. RESOLVING replica detection (CLUSTER_TYPE = EXTERNAL only).
	// After an unplanned failover the killed primary pod restarts and reconnects to
	// the AG but lands in RESOLVING state. Without an explicit SET (ROLE = SECONDARY)
	// it stays there indefinitely. reconcileResolvingReplicas detects such replicas
	// on the primary and transitions each one to SECONDARY. When Msg 41104 prevents
	// the transition, the reconcile loop simply retries until SQL Server's internal
	// AG resource manager clears the previous error autonomously.
	var hadResolving bool
	if effectiveClusterType(ag.Spec.ClusterType) != agClusterTypeNone {
		var rserr error
		hadResolving, rserr = r.reconcileResolvingReplicas(ctx, ag)
		if rserr != nil {
			log.Error(rserr, "Could not reconcile RESOLVING replicas; will retry on next poll")
		}
	}

	// 8b. NOT SYNCHRONIZING replica detection (CLUSTER_TYPE = EXTERNAL only).
	// After a planned or unplanned failover, secondaries that were connected to the
	// old primary sometimes lose database synchronization with the new primary. Their
	// replica role is SECONDARY but their databases are stuck in NOT SYNCHRONIZING state.
	// Re-issuing SET (ROLE = SECONDARY) forces them to re-establish the database
	// mirroring session with the new primary. If that doesn't clear it, the operator
	// escalates: secondary-only endpoint restart at 15s, bilateral restart at 30s.
	var hadNotSynchronizing bool
	if effectiveClusterType(ag.Spec.ClusterType) != agClusterTypeNone {
		var nserr error
		hadNotSynchronizing, nserr = r.reconcileNotSynchronizingReplicas(ctx, ag)
		if nserr != nil {
			log.Error(nserr, "Could not reconcile NOT SYNCHRONIZING replicas; will retry on next poll")
		}
	}

	// 9. Reconcile PV reclaim policy for all replica PVCs.
	if err := r.reconcilePVCReclaimPolicy(ctx, ag); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile PV reclaim policy: %w", err)
	}

	// Requeue sooner when RESOLVING or NOT SYNCHRONIZING replicas are active so the
	// verify-retry loop converges faster than the normal 60s poll interval.
	// 5s gives the endpoint restart escalation logic (15s→secondary, 30s→bilateral)
	// visibility into recovery progress and catches successful autonomous transitions
	// (RESOLVING→SECONDARY) quickly without hammering SQL.
	if hadResolving || hadNotSynchronizing {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	// Requeue periodically to keep status fresh; failover detection is event-driven
	// via pod readiness watches (see SetupWithManager).
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// reconcileAGConfigMap ensures an mssql.conf ConfigMap with hadr.hadrenabled=1 exists.
func (r *SQLServerAvailabilityGroupReconciler) reconcileAGConfigMap(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	cmName := ag.Name + "-mssql-conf"
	conf := make(map[string]string)
	maps.Copy(conf, ag.Spec.MSSQLConf)
	// Always enable HADR
	conf["hadr.hadrenabled"] = "1"

	desired := buildMSSQLConf(conf)
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: ag.Namespace}, cm)
	if errors.IsNotFound(err) {
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: ag.Namespace},
			Data:       map[string]string{"mssql.conf": desired},
		}
		if err2 := controllerutil.SetControllerReference(ag, newCM, r.Scheme); err2 != nil {
			return err2
		}
		return r.Create(ctx, newCM)
	}
	if err != nil {
		return err
	}
	cm.Data = map[string]string{"mssql.conf": desired}
	return r.Update(ctx, cm)
}

// reconcileAGStatefulSet ensures the StatefulSet for all AG replicas exists and is correct.
func (r *SQLServerAvailabilityGroupReconciler) reconcileAGStatefulSet(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: ag.Name, Namespace: ag.Namespace}, sts)
	desired := r.buildAGStatefulSet(ag)
	if errors.IsNotFound(err) {
		if err2 := controllerutil.SetControllerReference(ag, desired, r.Scheme); err2 != nil {
			return err2
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Reconcile mutable fields from the desired StatefulSet into the live object.
	// VolumeClaimTemplates and Selector are immutable after creation so they are
	// intentionally left unchanged.
	replicas := int32(len(ag.Spec.Replicas))
	sts.Spec.Replicas = &replicas
	sts.Spec.Template.Spec.Containers[0].Image = desired.Spec.Template.Spec.Containers[0].Image
	sts.Spec.Template.Spec.Containers[0].Resources = desired.Spec.Template.Spec.Containers[0].Resources
	sts.Spec.Template.Spec.Containers[0].Env = desired.Spec.Template.Spec.Containers[0].Env
	sts.Spec.Template.Spec.Containers[0].Ports = desired.Spec.Template.Spec.Containers[0].Ports
	sts.Spec.Template.Spec.Containers[0].Lifecycle = desired.Spec.Template.Spec.Containers[0].Lifecycle
	sts.Spec.Template.Spec.NodeSelector = desired.Spec.Template.Spec.NodeSelector
	sts.Spec.Template.Spec.Tolerations = desired.Spec.Template.Spec.Tolerations
	return r.Update(ctx, sts)
}

// reconcileAGHeadlessService creates the headless Service for AG pod DNS.
func (r *SQLServerAvailabilityGroupReconciler) reconcileAGHeadlessService(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	labels := map[string]string{"app": ag.Name, "role": "mssql-ag"}
	svcName := ag.Name + "-headless"
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: svcName, Namespace: ag.Namespace}, svc)
	desiredPorts := []corev1.ServicePort{
		{Name: "mssql", Port: 1433, TargetPort: intstr.FromInt32(1433)},
		{Name: "hadr", Port: ag.Spec.EndpointPort, TargetPort: intstr.FromInt32(ag.Spec.EndpointPort)},
	}
	if errors.IsNotFound(err) {
		newSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ag.Namespace, Labels: labels},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector:  labels,
				Ports:     desiredPorts,
			},
		}
		if err2 := controllerutil.SetControllerReference(ag, newSvc, r.Scheme); err2 != nil {
			return err2
		}
		return r.Create(ctx, newSvc)
	}
	if err != nil {
		return err
	}
	// Patch the existing headless service so the selector and ports stay in sync
	// (e.g. when spec.endpointPort changes).
	patch := client.MergeFrom(svc.DeepCopy())
	svc.Spec.Selector = labels
	svc.Spec.Ports = desiredPorts
	return r.Patch(ctx, svc, patch)
}

// reconcileListenerService creates or updates the read-write AG listener Service.
// The selector targets only pods labelled sql.mssql.microsoft.com/ag-role=primary so
// that all client traffic is always directed at the current primary replica, regardless
// of which pod holds that role after a failover.
func (r *SQLServerAvailabilityGroupReconciler) reconcileListenerService(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	l := ag.Spec.Listener
	ownerLabels := map[string]string{"app": ag.Name, "role": "mssql-ag"}
	// Selector targets ONLY the pod currently holding the PRIMARY role.
	selector := map[string]string{
		"app":       ag.Name,
		labelAGRole: "primary",
	}
	listenerPort := l.Port
	if listenerPort == 0 {
		listenerPort = 1433
	}
	svcType := l.ServiceType
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: l.Name, Namespace: ag.Namespace, Labels: ownerLabels},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: selector,
			// PublishNotReadyAddresses keeps the endpoint dialable even when the
			// sole backing pod briefly toggles NotReady during a failover. Without
			// this, clients see connection resets until the new primary receives its
			// ag-role=primary label AND passes its readiness probe. The selector is
			// already narrowed to ag-role=primary, so expanding to not-ready pods
			// cannot accidentally route traffic to a secondary.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{Name: "mssql", Port: listenerPort, TargetPort: intstr.FromInt32(listenerPort)},
			},
		},
	}
	if err2 := controllerutil.SetControllerReference(ag, desired, r.Scheme); err2 != nil {
		return err2
	}
	existing := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: l.Name, Namespace: ag.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Patch the existing service so the selector, port, and type stay in sync.
	patch := client.MergeFrom(existing.DeepCopy())
	existing.Spec.Selector = selector
	existing.Spec.Type = svcType
	existing.Spec.Ports = desired.Spec.Ports
	return r.Patch(ctx, existing, patch)
}

// reconcileReadOnlyListenerService creates or updates the read-only AG listener Service.
// It selects only pods labelled sql.mssql.microsoft.com/ag-role=readable-secondary and
// enables ClientIP session affinity so each client connection is pinned to the same
// replica for the lifetime of the connection (important for read-scale workloads).
func (r *SQLServerAvailabilityGroupReconciler) reconcileReadOnlyListenerService(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	l := ag.Spec.ReadOnlyListener
	ownerLabels := map[string]string{"app": ag.Name, "role": "mssql-ag"}
	selector := map[string]string{
		"app":       ag.Name,
		labelAGRole: "readable-secondary",
	}
	listenerPort := l.Port
	if listenerPort == 0 {
		listenerPort = 1433
	}
	svcType := l.ServiceType
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}
	// 5-minute timeout keeps long-running read queries on the same replica.
	affinityTimeoutSec := int32(300)
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: l.Name, Namespace: ag.Namespace, Labels: ownerLabels},
		Spec: corev1.ServiceSpec{
			Type:            svcType,
			Selector:        selector,
			SessionAffinity: corev1.ServiceAffinityClientIP,
			SessionAffinityConfig: &corev1.SessionAffinityConfig{
				ClientIP: &corev1.ClientIPConfig{TimeoutSeconds: &affinityTimeoutSec},
			},
			// See comment on the read-write listener — the same rationale applies to
			// read-only clients.
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{Name: "mssql", Port: listenerPort, TargetPort: intstr.FromInt32(listenerPort)},
			},
		},
	}
	if err2 := controllerutil.SetControllerReference(ag, desired, r.Scheme); err2 != nil {
		return err2
	}
	existing := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: l.Name, Namespace: ag.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	patch := client.MergeFrom(existing.DeepCopy())
	existing.Spec.Selector = selector
	existing.Spec.Type = svcType
	existing.Spec.SessionAffinity = corev1.ServiceAffinityClientIP
	existing.Spec.SessionAffinityConfig = desired.Spec.SessionAffinityConfig
	existing.Spec.Ports = desired.Spec.Ports
	return r.Patch(ctx, existing, patch)
}

// reconcilePodLabels patches the sql.mssql.microsoft.com/ag-role label on every AG replica pod
// so that the listener and read-only Services route to the correct pods.
// Label values:
//   - "primary"            — the pod currently holding the PRIMARY role
//   - "readable-secondary" — a non-primary pod whose spec marks it as readableSecondary: true
//   - "secondary"          — all other non-primary pods
//
// Primary detection uses the live SQL query result (replicaRoles map) first, then falls back
// to ag.Status.PrimaryReplica so that secondary pod labeling still works even when the DMV
// query returns an empty result on a secondary (e.g. when synchronization_health is NOT_HEALTHY).
func (r *SQLServerAvailabilityGroupReconciler) reconcilePodLabels(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup, replicaRoles map[string]string) error {
	log := logf.FromContext(ctx)

	// Determine primary pod: prefer live SQL query result, fall back to status field.
	primaryPod := ag.Status.PrimaryReplica
	for podName, role := range replicaRoles {
		if role == agRolePrimary {
			primaryPod = podName
			break
		}
	}

	// Build a lookup from pod name → readableSecondary flag from the spec.
	readableByPod := make(map[string]bool, len(ag.Spec.Replicas))
	for i := range ag.Spec.Replicas {
		podName := podNameForReplica(ag, i)
		readableByPod[podName] = ag.Spec.Replicas[i].ReadableSecondary
	}

	patchPodLabel := func(podName, desiredLabel string) error {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: ag.Namespace}, pod); err != nil {
			return nil // pod not yet scheduled — skip silently
		}
		if pod.Labels[labelAGRole] == desiredLabel {
			return nil
		}
		patch := client.MergeFrom(pod.DeepCopy())
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[labelAGRole] = desiredLabel
		if err := r.Patch(ctx, pod, patch); err != nil {
			log.Error(err, "Could not patch pod AG role label", "pod", podName, "label", desiredLabel)
			return err
		}
		log.Info("Updated pod AG role label", "pod", podName, "role", desiredLabel)
		return nil
	}

	// Pass 1: promote the new primary FIRST. This ordering matters during failover:
	// the listener Service selects only pods with ag-role=primary. If we demoted the
	// old primary first, the Service endpoints would flap to empty until the new
	// primary is labelled. Labelling the new primary first guarantees at least one
	// backend is always present (may briefly be two during role transitions, which
	// is harmless because only the replica truly in PRIMARY role will accept writes).
	if primaryPod != "" {
		if err := patchPodLabel(primaryPod, "primary"); err != nil {
			return err
		}
	}

	// Pass 2: demote all non-primaries.
	for i := range ag.Spec.Replicas {
		podName := podNameForReplica(ag, i)
		if podName == primaryPod {
			continue
		}
		var desiredLabel string
		if readableByPod[podName] {
			desiredLabel = "readable-secondary"
		} else {
			desiredLabel = "secondary"
		}
		if err := patchPodLabel(podName, desiredLabel); err != nil {
			return err
		}
	}
	return nil
}

// joinSecondaries runs ALTER AVAILABILITY GROUP JOIN on every secondary pod (index 1..N-1)
// and, for CLUSTER_TYPE = EXTERNAL, immediately transitions each replica from RESOLVING
// to SECONDARY via ALTER AVAILABILITY GROUP SET (ROLE = SECONDARY).
//
// This is extracted from bootstrapAG to keep that function's cyclomatic complexity within
// the linter threshold while keeping the logic easy to read.
func (r *SQLServerAvailabilityGroupReconciler) joinSecondaries(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	exec *sqlutil.Executor,
	saPassword, clusterType string,
) ctrl.Result {
	log := logf.FromContext(ctx)
	replicas := ag.Spec.Replicas
	for i := 1; i < len(replicas); i++ {
		secondaryPod := podNameForReplica(ag, i)
		if _, err := exec.ExecSQL(ctx, ag.Namespace, secondaryPod, containerName, saPassword,
			sqlutil.JoinAGSQL(ag.Spec.AGName, clusterType)); err != nil {
			log.Error(err, "Could not join AG on secondary", "pod", secondaryPod)
			return ctrl.Result{RequeueAfter: 15 * time.Second}
		}
		log.Info("Joined AG on secondary", "pod", secondaryPod)
		if clusterType != agClusterTypeNone {
			// Transition from RESOLVING → SECONDARY so SQL Server reports the replica
			// as SECONDARY+CONNECTED and the operator's health monitoring can proceed.
			// The AG resource on the secondary may not be online immediately after JOIN,
			// so retry a few times with a short delay (Msg 41104).
			var setRoleErr error
			for attempt := 1; attempt <= 3; attempt++ {
				if _, setRoleErr = exec.ExecSQL(ctx, ag.Namespace, secondaryPod, containerName, saPassword,
					sqlutil.SetRoleToSecondarySQL(ag.Spec.AGName)); setRoleErr == nil {
					break
				}
				log.Info("SET(ROLE=SECONDARY) not ready yet, retrying", "pod", secondaryPod, "attempt", attempt, "err", setRoleErr)
				time.Sleep(time.Duration(attempt*5) * time.Second)
			}
			if setRoleErr != nil {
				log.Error(setRoleErr, "Could not set secondary role on secondary", "pod", secondaryPod)
				return ctrl.Result{RequeueAfter: 15 * time.Second}
			}
			log.Info("Set SECONDARY role on secondary", "pod", secondaryPod)
		}
	}
	return ctrl.Result{}
}

// joinDatabasesOnSecondaries queries the primary for databases in the AG and
// explicitly joins each one on every secondary using the idempotent
// JoinDatabaseToAGSQL command (which includes an IF NOT EXISTS guard).
//
// This ensures that databases added to the AG externally (e.g. via
// ALTER AVAILABILITY GROUP ADD DATABASE) are properly joined on secondaries
// without producing the noisy Error 41145 on re-runs.
func (r *SQLServerAvailabilityGroupReconciler) joinDatabasesOnSecondaries(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	exec *sqlutil.Executor,
	primaryPod, saPassword string,
) {
	log := logf.FromContext(ctx)

	// Discover databases in the AG from the primary.
	result, err := exec.ExecSQL(ctx, ag.Namespace, primaryPod, containerName, saPassword,
		sqlutil.AGDatabasesSQL(ag.Spec.AGName))
	if err != nil {
		log.Info("Could not query AG databases from primary; skipping database join", "err", err)
		return
	}

	agPrefix := ag.Name + "-"
	var databases []string
	for line := range strings.SplitSeq(result.Stdout, "\n") {
		dbName := strings.TrimSpace(line)
		// Filter out empty lines, column headers, and separator lines.
		if dbName == "" || strings.HasPrefix(dbName, agPrefix) || strings.HasPrefix(dbName, "-") || dbName == "name" {
			continue
		}
		databases = append(databases, dbName)
	}
	if len(databases) == 0 {
		return
	}

	// For each secondary, join each database using the idempotent command.
	for i := 1; i < len(ag.Spec.Replicas); i++ {
		secondaryPod := podNameForReplica(ag, i)
		for _, dbName := range databases {
			if _, joinErr := exec.ExecSQL(ctx, ag.Namespace, secondaryPod, containerName, saPassword,
				sqlutil.JoinDatabaseToAGSQL(dbName, ag.Spec.AGName)); joinErr != nil {
				log.Info("Could not join database on secondary (may not be restored yet)",
					"database", dbName, "pod", secondaryPod, "err", joinErr)
			} else {
				log.Info("Ensured database joined on secondary", "database", dbName, "pod", secondaryPod)
			}
		}
	}
}

// bootstrapAG orchestrates the T-SQL AG setup across all replica pods.
// Steps:
//  1. Wait until all pods are ready
//  2. On each replica: create master key, certificate, HADR endpoint, backup cert
//  3. Transfer each cert to every other pod via SPDY streaming (ReadFileFromPod/WriteFileToPod)
//  4. Cross-restore peer certs, create logins, grant endpoint connect
//  5. On primary (pod-0): CREATE AVAILABILITY GROUP
//  6. On secondaries: JOIN AVAILABILITY GROUP
func (r *SQLServerAvailabilityGroupReconciler) bootstrapAG(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not get SA password: %w", err)
	}

	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}
	endpointPort := ag.Spec.EndpointPort
	if endpointPort == 0 {
		endpointPort = 5022
	}
	replicas := ag.Spec.Replicas
	headlessDomain := fmt.Sprintf("%s-headless.%s.svc.cluster.local", ag.Name, ag.Namespace)
	masterKeyPw := saPassword + "_mk"

	// Step 1: Wait for all pods to be ready
	for i := range replicas {
		podName := podNameForReplica(ag, i)
		if !exec.IsReady(ctx, ag.Namespace, podName, containerName, saPassword) {
			log.Info("Pod not yet ready, requeuing", "pod", podName)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	// Step 2: Create master key, certificate, and HADR endpoint on every replica.
	// Backup the DER-encoded cert to a well-known path on that pod's own PVC.
	for i := range replicas {
		podName := podNameForReplica(ag, i)
		certName := certNameForPod(podName)
		backupPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", certName)
		for _, q := range []string{
			sqlutil.CreateMasterKeySQL(masterKeyPw),
			sqlutil.CreateCertificateSQL(certName, podName+" AG Certificate", backupPath),
			sqlutil.CreateEndpointSQL(agEndpointName, certName, endpointPort),
		} {
			if _, err := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword, q); err != nil {
				return ctrl.Result{}, fmt.Errorf("cert/endpoint setup failed on %s: %w", podName, err)
			}
		}
		log.Info("Created cert and endpoint", "pod", podName)
	}

	// Step 3: For each (source, target) pair, transfer the source cert file into the
	// target pod using SPDY streaming (ReadFileFromPod → WriteFileToPod).
	// This avoids any shared-storage requirement between pods.
	for j := range replicas {
		sourcePod := podNameForReplica(ag, j)
		sourceCertName := certNameForPod(sourcePod)
		sourceCertPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", sourceCertName)

		certBytes, err := exec.ReadFileFromPod(ctx, ag.Namespace, sourcePod, containerName, sourceCertPath)
		if err != nil {
			log.Error(err, "Could not read cert from pod", "pod", sourcePod)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		for i := range replicas {
			if i == j {
				continue
			}
			targetPod := podNameForReplica(ag, i)
			destPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", sourceCertName)
			if err := exec.WriteFileToPod(ctx, ag.Namespace, targetPod, containerName, destPath, certBytes); err != nil {
				log.Error(err, "Could not write cert to pod", "source", sourcePod, "target", targetPod)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		}
	}

	// Step 4: On each target pod, import all peer public certs, create cert-based
	// server logins, and grant CONNECT on the HADR endpoint.
	// SQL Server HADR auth flow: connecting peer presents its cert → local instance
	// validates it against the stored public cert → grants access via cert login.
	for i := range replicas {
		targetPod := podNameForReplica(ag, i)
		for j := range replicas {
			if i == j {
				continue
			}
			sourcePod := podNameForReplica(ag, j)
			sourceCertName := certNameForPod(sourcePod)
			destPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", sourceCertName)
			loginName := fmt.Sprintf("%s_login", strings.ReplaceAll(sourcePod, "-", "_"))
			for _, q := range []string{
				sqlutil.RestoreCertificateSQL(sourceCertName, destPath),
				sqlutil.CreateLoginFromCertSQL(loginName, sourceCertName),
				sqlutil.GrantEndpointConnectSQL(agEndpointName, loginName),
			} {
				if _, err := exec.ExecSQL(ctx, ag.Namespace, targetPod, containerName, saPassword, q); err != nil {
					log.Error(err, "Could not set up peer cert auth", "target", targetPod, "source", sourcePod)
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
			}
		}
	}

	// Step 5: Verify all replica HADR endpoints are TCP-reachable before creating the AG.
	// SQL Server (CLUSTER_TYPE = NONE) attempts DNS resolution and TCP connection to each
	// secondary's HADR endpoint URL the instant CREATE AVAILABILITY GROUP runs. If DNS
	// has not yet propagated (common on Docker Desktop ARM64 where pod DNS entries can
	// take 15–30 s to appear in CoreDNS after the pod becomes Ready), the primary records
	// error 11001 "No such host is known" and does NOT retry — leaving the replicas
	// permanently DISCONNECTED. By verifying TCP connectivity from the controller pod
	// (which uses the same CoreDNS) before issuing CREATE AG, we guarantee that DNS is
	// already available when SQL Server makes its first connection attempt.
	primaryPod := fmt.Sprintf("%s-0", ag.Name)
	hadrPort := fmt.Sprintf("%d", endpointPort)
	for i := range replicas {
		podName := podNameForReplica(ag, i)
		fqdn := fmt.Sprintf("%s.%s", podName, headlessDomain)
		addr := net.JoinHostPort(fqdn, hadrPort)
		conn, dialErr := net.DialTimeout("tcp", addr, 3*time.Second)
		if dialErr != nil {
			log.Info("HADR endpoint not yet reachable, requeuing", "pod", podName, "addr", addr, "err", dialErr)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		_ = conn.Close()
		log.Info("HADR endpoint reachable", "pod", podName, "addr", addr)
	}

	replicaInputs := make([]sqlutil.AGReplicaInput, len(replicas))
	for i, rep := range replicas {
		podName := podNameForReplica(ag, i)
		replicaInputs[i] = sqlutil.AGReplicaInput{
			PodName:           podName,
			EndpointFQDN:      fmt.Sprintf("%s.%s", podName, headlessDomain),
			AvailabilityMode:  string(rep.AvailabilityMode),
			FailoverMode:      string(rep.FailoverMode),
			ReadableSecondary: rep.ReadableSecondary,
		}
	}
	// CREATE AVAILABILITY GROUP cannot be wrapped in BEGIN...END in SQL Server.
	// Check existence in Go and issue a plain CREATE only when the AG is absent.
	existResult, err := exec.ExecSQL(ctx, ag.Namespace, primaryPod, containerName, saPassword,
		sqlutil.AGExistsSQL(ag.Spec.AGName))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("could not check AG existence: %w", err)
	}
	agAlreadyExists := false
	for line := range strings.SplitSeq(existResult.Stdout, "\n") {
		line = strings.TrimSpace(line)
		var n int
		if _, scanErr := fmt.Sscanf(line, "%d", &n); scanErr == nil {
			agAlreadyExists = n > 0
			break
		}
	}

	if !agAlreadyExists {
		clusterType := effectiveClusterType(ag.Spec.ClusterType)
		createSQL := sqlutil.CreateAGSQL(ag.Spec.AGName, clusterType, replicaInputs, endpointPort)
		log.Info("Creating Availability Group", "ag", ag.Spec.AGName, "clusterType", clusterType)
		if _, createErr := exec.ExecSQL(ctx, ag.Namespace, primaryPod, containerName, saPassword, createSQL); createErr != nil {
			return ctrl.Result{}, fmt.Errorf("could not create AG on primary: %w", createErr)
		}
	} else {
		log.Info("Availability Group already exists, skipping CREATE", "ag", ag.Spec.AGName)
	}

	// Step 6: JOIN on each secondary and, for EXTERNAL clusters, transition from RESOLVING.
	clusterType := effectiveClusterType(ag.Spec.ClusterType)
	if result := r.joinSecondaries(ctx, ag, exec, saPassword, clusterType); result.RequeueAfter > 0 {
		return result, nil
	}

	// Step 6b: Idempotent database join on each secondary.
	// Query the primary for databases already in the AG, then explicitly join
	// each one on every secondary using a guarded command. This eliminates
	// the noisy Error 41145 ("The database has already joined the AG") that
	// SQL Server logs when automatic seeding re-encounters an existing database.
	r.joinDatabasesOnSecondaries(ctx, ag, exec, primaryPod, saPassword)

	// Step 7: Wait for all secondaries to reach SECONDARY+CONNECTED before declaring
	// bootstrap complete. If this times out the HADR transport failed to establish —
	// typically because SQL Server's internal HADR engine attempted its first connection
	// before the secondaries' HADR transport was fully initialised.  Recovery: drop the AG
	// on ALL replicas so every instance starts clean. Without dropping on the secondaries,
	// they retain stale replica-ID state from the failed AG; when the primary creates a
	// new AG with fresh replica GUIDs, the secondaries reject the connection (ID mismatch)
	// causing another timeout.  After the full cleanup the next reconcile will recreate
	// the AG from scratch, and the TCP-reachability check (Step 5) will ensure the HADR
	// endpoints are reachable before CREATE AVAILABILITY GROUP runs.
	if !r.waitForSecondariesReady(ctx, exec, ag, primaryPod, saPassword, len(replicas)) {
		r.dropAGOnAllReplicas(ctx, exec, ag, primaryPod, saPassword, len(replicas))
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	log.Info("AG bootstrap complete", "ag", ag.Spec.AGName)
	patch := client.MergeFrom(ag.DeepCopy())
	ag.Status.InitializationComplete = true
	ag.Status.Phase = sqlv1alpha1.AGPhaseRunning
	ag.Status.PrimaryReplica = primaryPod
	return ctrl.Result{}, r.Status().Patch(ctx, ag, patch)
}

// dropAGOnAllReplicas drops the named AG on every replica so each instance
// starts with a clean slate before the next CREATE AVAILABILITY GROUP attempt.
// Secondaries are dropped first to remove their stale replica-ID state; if they
// are left joined to an AG that no longer exists on the primary, the new primary
// (with fresh replica GUIDs) cannot establish the HADR transport because the ID
// mismatch causes the secondary to reject the connection, manifesting as a
// 10-second timeout on the primary.  Errors are logged but not returned — the
// caller always requeues after this function regardless of outcome.
func (r *SQLServerAvailabilityGroupReconciler) dropAGOnAllReplicas(
	ctx context.Context,
	sqlExec *sqlutil.Executor,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	primaryPod, saPassword string,
	replicaCount int,
) {
	log := logf.FromContext(ctx)
	log.Info("Dropping AG on all replicas for clean recreation", "ag", ag.Spec.AGName)
	dropSQL := fmt.Sprintf("DROP AVAILABILITY GROUP [%s]", ag.Spec.AGName)
	for i := 1; i < replicaCount; i++ {
		secondaryPod := podNameForReplica(ag, i)
		if _, err := sqlExec.ExecSQL(ctx, ag.Namespace, secondaryPod, containerName, saPassword, dropSQL); err != nil {
			log.Error(err, "Could not drop AG on secondary, continuing", "pod", secondaryPod)
		}
	}
	if _, err := sqlExec.ExecSQL(ctx, ag.Namespace, primaryPod, containerName, saPassword, dropSQL); err != nil {
		log.Error(err, "Could not drop AG on primary, will requeue anyway")
	}
}

// waitForSecondariesReady polls the primary until all secondary replicas report
// both role_desc = 'SECONDARY' AND connected_state_desc = 'CONNECTED', waiting
// up to 10 minutes (60 × 10 s). The sleep between each attempt is
// context-aware: if the context is cancelled (operator restart, CR deletion,
// etc.) the function returns false immediately rather than blocking.
//
// Both conditions are required because:
//   - role_desc = 'SECONDARY' is set by the protocol JOIN message and can be
//     true even when the HADR TCP transport is DISCONNECTED (e.g. the primary's
//     first connection attempt failed due to a DNS lookup race right after
//     CREATE AVAILABILITY GROUP).
//   - connected_state_desc = 'CONNECTED' confirms the HADR transport session is
//     actively established, so reads/replication are actually flowing.
//
// Consecutive-success requirement:
//   - CLUSTER_TYPE = NONE: 3 consecutive polls (30 s). Guards against transient
//     "blips" where DNS or certificates drop the session briefly after CREATE AG
//     (observed on Docker Desktop ARM64).
//   - CLUSTER_TYPE = EXTERNAL: 1 poll. With EXTERNAL the HADR handshake goes
//     through a more complex quorum negotiation phase, so the connection oscillates
//     briefly between polls even though transport is established (evidenced by
//     seeding proceeding in the SQL Server log). Once all secondaries are observed
//     CONNECTED the operator's continuous reconciliation loop takes over health
//     monitoring, making the 30-second stability window unnecessary.
func (r *SQLServerAvailabilityGroupReconciler) waitForSecondariesReady(
	ctx context.Context,
	sqlExec *sqlutil.Executor,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	primaryPod, saPassword string,
	replicaCount int,
) bool {
	log := logf.FromContext(ctx)
	wantSecondaries := replicaCount - 1
	const maxAttempts = 60 // 60 × 10 s = 10 min total
	// With EXTERNAL the operator acts as the cluster manager; one confirmed
	// CONNECTED observation is sufficient — health monitoring continues after bootstrap.
	requiredConsecutive := 3
	if effectiveClusterType(ag.Spec.ClusterType) != "NONE" {
		requiredConsecutive = 1
	}
	consecutiveOK := 0
	log.Info("Waiting for secondaries to reach SECONDARY+CONNECTED state",
		"ag", ag.Spec.AGName, "expected", wantSecondaries, "requiredConsecutive", requiredConsecutive)
	for attempt := range maxAttempts {
		result, qErr := sqlExec.ExecSQL(ctx, ag.Namespace, primaryPod, containerName, saPassword,
			sqlutil.SecondaryCountSQL(ag.Spec.AGName))
		if qErr != nil {
			consecutiveOK = 0
			log.Info("Could not query secondary state, will retry", "attempt", attempt, "err", qErr)
			select {
			case <-ctx.Done():
				log.Info("Context cancelled while waiting for secondaries", "ag", ag.Spec.AGName)
				return false
			case <-time.After(10 * time.Second):
			}
			continue
		}
		var secondaryCount int
		for line := range strings.SplitSeq(result.Stdout, "\n") {
			line = strings.TrimSpace(line)
			if _, scanErr := fmt.Sscanf(line, "%d", &secondaryCount); scanErr == nil {
				break
			}
		}
		if secondaryCount >= wantSecondaries {
			consecutiveOK++
			log.Info("Secondaries in SECONDARY+CONNECTED state",
				"ag", ag.Spec.AGName, "count", secondaryCount,
				"consecutiveOK", consecutiveOK, "required", requiredConsecutive)
			if consecutiveOK >= requiredConsecutive {
				log.Info("Secondaries stable, bootstrap complete",
					"ag", ag.Spec.AGName, "stablePolls", consecutiveOK)
				return true
			}
		} else {
			if consecutiveOK > 0 {
				log.Info("Secondaries dropped below required count, resetting consecutive counter",
					"ag", ag.Spec.AGName, "count", secondaryCount, "expected", wantSecondaries)
			} else {
				log.Info("Secondaries not yet ready, retrying",
					"count", secondaryCount, "expected", wantSecondaries, "attempt", attempt)
			}
			consecutiveOK = 0
		}
		select {
		case <-ctx.Done():
			log.Info("Context cancelled while waiting for secondaries", "ag", ag.Spec.AGName)
			return false
		case <-time.After(10 * time.Second):
		}
	}
	log.Info("Timed out waiting for secondaries to reach SECONDARY+CONNECTED state", "ag", ag.Spec.AGName)
	return false
}

// certNameForPod returns the SQL certificate name for a given pod name.
func certNameForPod(podName string) string {
	return fmt.Sprintf("%s_cert", strings.ReplaceAll(podName, "-", "_"))
}

// getSAPassword reads the SA password from the referenced Secret.
// SecretKeySelector is always namespace-scoped to the CR's namespace.
func (r *SQLServerAvailabilityGroupReconciler) getSAPassword(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) (string, error) {
	secret := &corev1.Secret{}
	ref := ag.Spec.SAPasswordSecretRef
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ag.Namespace}, secret); err != nil {
		return "", err
	}
	pw, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s", ref.Key, ag.Namespace, ref.Name)
	}
	return string(pw), nil
}

// updateAGStatus refreshes replica roles and sync state from all pods.
// LastSeenPrimary is only updated for the replica that is currently PRIMARY
// to avoid creating a constant status diff that would trigger a reconcile storm.
func (r *SQLServerAvailabilityGroupReconciler) updateAGStatus(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return err
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}

	// Build a map of existing statuses so we can preserve LastSeenPrimary for
	// replicas that haven't changed role.
	existing := make(map[string]sqlv1alpha1.AGReplicaStatus, len(ag.Status.ReplicaStatuses))
	for _, s := range ag.Status.ReplicaStatuses {
		existing[s.Name] = s
	}

	patch := client.MergeFrom(ag.DeepCopy())

	// Collect roles for all replicas in one pass so we can update pod labels atomically.
	replicaRoles := make(map[string]string, len(ag.Spec.Replicas))
	var statuses []sqlv1alpha1.AGReplicaStatus
	foundPrimary := false
	for i := range ag.Spec.Replicas {
		podName := podNameForReplica(ag, i)
		role, _ := exec.GetAGRole(ctx, ag.Namespace, podName, containerName, saPassword, ag.Spec.AGName)
		syncState, _ := exec.GetAGSyncState(ctx, ag.Namespace, podName, containerName, saPassword, ag.Spec.AGName)
		replicaRoles[podName] = role
		if role == agRolePrimary {
			ag.Status.PrimaryReplica = podName
			foundPrimary = true
		}

		// Only stamp LastSeenPrimary when this pod is the active primary.
		// Preserving the previous value avoids an always-dirty patch.
		var lastSeenPrimary *metav1.Time
		if role == agRolePrimary {
			now := metav1.Now()
			lastSeenPrimary = &now
		} else if prev, ok := existing[podName]; ok {
			lastSeenPrimary = prev.LastSeenPrimary
		}

		statuses = append(statuses, sqlv1alpha1.AGReplicaStatus{
			Name:                 podName,
			Role:                 role,
			SynchronizationState: syncState,
			Connected:            role != "",
			LastSeenPrimary:      lastSeenPrimary,
		})
	}
	ag.Status.ReplicaStatuses = statuses

	// If no live pod reported PRIMARY but the recorded primary is itself reporting
	// SECONDARY, the status.primaryReplica is stale — this happens after an
	// externally-initiated planned failover (e.g., ALTER AG FAILOVER run by the
	// preStop hook or by a test script) when the controller has not yet observed
	// the new primary.
	// Recover by identifying the unreachable pod as the inferred primary so that
	// reconcileFailover can correctly detect the pod failure and start the timer.
	if !foundPrimary && replicaRoles[ag.Status.PrimaryReplica] == agRoleSecondary {
		log := logf.FromContext(ctx)
		for i := range ag.Spec.Replicas {
			podName := podNameForReplica(ag, i)
			if podName == ag.Status.PrimaryReplica {
				continue
			}
			if replicaRoles[podName] == "" { // GetAGRole failed → pod unreachable
				log.Info("Correcting stale primary: recorded primary is SECONDARY, inferred primary from unreachable pod",
					"stale", ag.Status.PrimaryReplica, "inferred", podName)
				ag.Status.PrimaryReplica = podName
				break
			}
		}
	}

	// Update pod labels so the listener and read-only Services route correctly.
	// This is best-effort: a labelling failure should not prevent status from being saved.
	if labelErr := r.reconcilePodLabels(ctx, ag, replicaRoles); labelErr != nil {
		logf.FromContext(ctx).Error(labelErr, "Could not reconcile pod AG role labels")
	}

	// Set standard status conditions so users can use kubectl wait --for=condition=...
	if ag.Status.InitializationComplete {
		apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
			Type:               "Initialized",
			Status:             metav1.ConditionTrue,
			Reason:             "BootstrapComplete",
			Message:            "AG bootstrap completed successfully",
			ObservedGeneration: ag.Generation,
			LastTransitionTime: metav1.Now(),
		})
	}
	if foundPrimary {
		// Count healthy replicas to determine overall availability.
		healthyCount := 0
		for _, s := range statuses {
			if s.Connected && s.SynchronizationState == "HEALTHY" {
				healthyCount++
			}
		}
		msg := fmt.Sprintf("Primary is %s; %d/%d replicas healthy", ag.Status.PrimaryReplica, healthyCount, len(statuses))
		apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
			Type:               sqlv1alpha1.ConditionAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             "PrimaryHealthy",
			Message:            msg,
			ObservedGeneration: ag.Generation,
			LastTransitionTime: metav1.Now(),
		})
	} else {
		apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
			Type:               sqlv1alpha1.ConditionAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             "NoPrimary",
			Message:            "No replica is currently serving as PRIMARY",
			ObservedGeneration: ag.Generation,
			LastTransitionTime: metav1.Now(),
		})
	}

	return r.Status().Patch(ctx, ag, patch)
}

// buildAGStatefulSet constructs the StatefulSet for all AG replicas.
// All replicas share the same image, config, and storage spec. PodAntiAffinity
// ensures replicas are spread across nodes in multi-node clusters.
func (r *SQLServerAvailabilityGroupReconciler) buildAGStatefulSet(ag *sqlv1alpha1.SQLServerAvailabilityGroup) *appsv1.StatefulSet {
	labels := map[string]string{
		"app":                          ag.Name,
		"role":                         "mssql-ag",
		"app.kubernetes.io/name":       "sqlserveravailabilitygroup",
		"app.kubernetes.io/instance":   ag.Name,
		"app.kubernetes.io/managed-by": "sql-on-k8s-operator",
	}
	replicas := int32(len(ag.Spec.Replicas))
	port := int32(1433)
	endpointPort := ag.Spec.EndpointPort
	if endpointPort == 0 {
		endpointPort = 5022
	}
	edition := ag.Spec.Edition
	if edition == "" {
		edition = "Developer"
	}
	acceptEULA := ag.Spec.AcceptEULA
	if acceptEULA == "" {
		acceptEULA = "Y"
	}

	accessModes := ag.Spec.Storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	dataSize := ag.Spec.Storage.DataVolumeSize
	if dataSize.IsZero() {
		dataSize = resource.MustParse("10Gi")
	}

	// Both SA_PASSWORD (legacy) and MSSQL_SA_PASSWORD (current) are set from the
	// same secret so the image works regardless of SQL Server version. Microsoft
	// deprecated SA_PASSWORD starting with SQL Server 2019 CU; MSSQL_SA_PASSWORD
	// is the preferred name going forward.
	envVars := []corev1.EnvVar{
		{Name: "ACCEPT_EULA", Value: acceptEULA},
		{Name: "MSSQL_PID", Value: edition},
		{Name: "MSSQL_AGENT_ENABLED", Value: "true"},
		{Name: "AG_NAME", Value: ag.Spec.AGName},
		{
			Name: "SA_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &ag.Spec.SAPasswordSecretRef,
			},
		},
		{
			Name: "MSSQL_SA_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &ag.Spec.SAPasswordSecretRef,
			},
		},
	}

	// PodAntiAffinity: prefer spreading replicas across different nodes
	antiAffinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": ag.Name, "role": "mssql-ag"},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ag.Name,
			Namespace: ag.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			ServiceName:         ag.Name + "-headless",
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// fsGroup 10001 is the mssql group; required so the SQL Server
					// process can read/write data files on the mounted PVC volume.
					// See: https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-kubernetes-best-practices-statefulsets
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup: func() *int64 { g := int64(10001); return &g }(),
					},
					Affinity:     antiAffinity,
					NodeSelector: ag.Spec.NodeSelector,
					Tolerations:  ag.Spec.Tolerations,
					Containers: []corev1.Container{
						{
							Name:      "mssql",
							Image:     ag.Spec.Image,
							Env:       envVars,
							Resources: ag.Spec.Resources,
							Ports: []corev1.ContainerPort{
								{Name: "mssql", ContainerPort: port, Protocol: corev1.ProtocolTCP},
								{Name: "hadr", ContainerPort: endpointPort, Protocol: corev1.ProtocolTCP},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "mssql-data", MountPath: "/var/opt/mssql"},
								{Name: "mssql-conf", MountPath: "/var/opt/mssql/mssql.conf", SubPath: "mssql.conf"},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/bash", "-c", probeScript},
									},
								},
								InitialDelaySeconds: 90,
								PeriodSeconds:       30,
								FailureThreshold:    5,
								TimeoutSeconds:      15,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/bash", "-c", probeScript},
									},
								},
								InitialDelaySeconds: 45,
								PeriodSeconds:       15,
								FailureThreshold:    3,
								TimeoutSeconds:      10,
							},
							Lifecycle: &corev1.Lifecycle{
								PreStop: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/bash", "-c", preStopScript},
									},
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "mssql-conf",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: ag.Name + "-mssql-conf",
									},
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "mssql-data"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:      accessModes,
						StorageClassName: ag.Spec.Storage.StorageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: dataSize,
							},
						},
					},
				},
			},
		},
	}
}

// isPodReady returns true if the pod has a Ready condition with status True.
func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// selectFailoverTarget picks the best synchronous secondary for an unplanned failover.
//
// Selection is done in two passes:
//  1. Prefer replicas whose synchronization_health_desc = HEALTHY (all databases SYNCHRONIZED).
//     This guarantees zero data loss for synchronous-commit replicas.
//  2. If no HEALTHY replica is found (e.g. primary just died and secondaries are still
//     transitioning, or SQL Server is temporarily unresponsive), fall back to the first
//     configured SynchronousCommit/Automatic replica. SQL Server will accept FAILOVER on
//     a synchronized replica even when the primary is offline (it checks the persisted
//     synchronization state). If the replica was not synchronized, SQL Server returns
//     error 41142 and the operator retries on the next reconcile.
//
// A replica is considered for either pass only when:
//   - AvailabilityMode = SynchronousCommit — async replicas are never auto-failed over to.
//   - FailoverMode = Automatic — only replicas explicitly configured for automatic failover.
//
// NOTE: We intentionally do NOT pre-filter candidates by SQL reachability. After an unplanned
// primary failure, secondaries may be temporarily unresponsive to SELECT 1 (e.g. RESOLVING
// state transitions, redo thread activity, or transient kubelet exec latency). Skipping them
// entirely would leave the AG in a headless state indefinitely. Instead, we capture them as
// best-effort candidates and let the subsequent FAILOVER command retry until the target
// is ready to accept commands (or SQL Server rejects with 41142 if not synchronized).
//
// NOTE: synchronization_state_desc (SYNCHRONIZED/SYNCHRONIZING) lives in
// sys.dm_hadr_DATABASE_replica_states, not sys.dm_hadr_availability_replica_states.
// We use GetAGSyncState which reads synchronization_health_desc (HEALTHY/PARTIALLY_HEALTHY/
// NOT_HEALTHY) from the replica-level DMV — the correct aggregate for this decision.
func (r *SQLServerAvailabilityGroupReconciler) selectFailoverTarget(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) (string, error) {
	log := logf.FromContext(ctx)
	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return "", err
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}

	var bestEffort string // first configured sync/auto replica — used as fallback
	for i, rep := range ag.Spec.Replicas {
		podName := podNameForReplica(ag, i)
		if podName == ag.Status.PrimaryReplica {
			continue
		}
		if rep.AvailabilityMode != sqlv1alpha1.SynchronousCommit {
			continue // never auto-failover to asynchronous replicas
		}
		if rep.FailoverMode != sqlv1alpha1.FailoverModeAutomatic {
			continue // only promote replicas explicitly configured for automatic failover
		}

		// Check replica-level synchronization health.
		// If the check fails (SQL Server temporarily unreachable), keep the candidate as
		// best-effort rather than skipping it entirely. The FAILOVER command will be
		// retried and will succeed once the target's SQL Server is ready to accept commands.
		health, hErr := exec.GetAGSyncState(ctx, ag.Namespace, podName, containerName, saPassword, ag.Spec.AGName)
		if hErr != nil {
			log.Info("Could not check sync health for candidate; keeping as best-effort fallback",
				"pod", podName, "err", hErr)
			if bestEffort == "" {
				bestEffort = podName
			}
			continue
		}

		log.Info("Candidate synchronous replica sync health", "pod", podName, "health", health)
		if health == "HEALTHY" {
			// All databases SYNCHRONIZED — ideal target, no data loss.
			return podName, nil
		}

		// Reachable but not HEALTHY (e.g. primary just died, replica is transitioning).
		// Record as best-effort fallback; keep searching for a HEALTHY candidate.
		if bestEffort == "" {
			bestEffort = podName
		}
		log.Info("Candidate not HEALTHY; keeping as best-effort fallback", "pod", podName, "health", health)
	}

	if bestEffort != "" {
		log.Info("No HEALTHY replica found; using best-effort replica for force failover",
			"pod", bestEffort)
		return bestEffort, nil
	}

	return "", fmt.Errorf("no SynchronousCommit replica with failoverMode=Automatic available for automatic failover")
}

// reconcileResolvingReplicas detects secondaries stuck in RESOLVING role (CLUSTER_TYPE = EXTERNAL)
// and transitions each one to SECONDARY by issuing ALTER AVAILABILITY GROUP SET (ROLE = SECONDARY).
//
// This handles the pod-restart case: after an unplanned failover, the killed primary pod
// restarts and reconnects to the AG but remains in RESOLVING state indefinitely — SQL Server
// requires the external cluster manager to explicitly grant it the SECONDARY role.
//
// When SET(ROLE=SECONDARY) fails with Msg 41104 ("previous error"), the operator
// issues ALTER AG OFFLINE + bilateral endpoint restart (on both the stuck replica
// and the current primary) to clear stale transport state and force reconnection.
func (r *SQLServerAvailabilityGroupReconciler) reconcileResolvingReplicas(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) (hadAny bool, err error) {
	log := logf.FromContext(ctx)
	if !ag.Status.InitializationComplete || ag.Status.PrimaryReplica == "" {
		return false, nil
	}
	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return false, err
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}
	result, err := exec.ExecSQL(ctx, ag.Namespace, ag.Status.PrimaryReplica, containerName, saPassword,
		sqlutil.ResolvingReplicasSQL(ag.Spec.AGName))
	if err != nil {
		log.Info("Could not query RESOLVING replicas from primary", "err", err)
		return false, nil
	}

	agPrefix := ag.Name + "-"
	for line := range strings.SplitSeq(result.Stdout, "\n") {
		replicaName := strings.TrimSpace(line)
		if replicaName == "" || !strings.HasPrefix(replicaName, agPrefix) {
			continue
		}
		podName := replicaName

		// Guard: never re-seat the primary — same rationale as in
		// reconcileNotSynchronizingReplicas.
		if podName == ag.Status.PrimaryReplica {
			log.Info("Skipping re-seat: pod is the current primary", "pod", podName)
			continue
		}

		hadAny = true

		log.Info("Detected RESOLVING secondary; transitioning to SECONDARY", "pod", podName)

		if _, setErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
			sqlutil.SetRoleToSecondarySQL(ag.Spec.AGName)); setErr != nil {
			if sqlutil.HasSQLError(sqlutil.ExecResult{}, setErr, sqlutil.ErrCodePreviousError) {
				r.handle41104(ctx, ag, exec, saPassword, podName)
			} else {
				log.Error(setErr, "Could not set SECONDARY role on RESOLVING replica", "pod", podName)
			}
		} else {
			r.clear41104Tracking(ag, podName)
			log.Info("Set SECONDARY role on RESOLVING replica", "pod", podName)
		}
	}
	return hadAny, nil
}

// reconcileNotSynchronizingReplicas detects SECONDARY replicas whose databases are stuck
// in NOT SYNCHRONIZING state (CLUSTER_TYPE = EXTERNAL) and re-seats them by issuing
// ALTER AVAILABILITY GROUP SET (ROLE = SECONDARY).
//
// When SET(ROLE=SECONDARY) fails with Msg 41104, the operator delegates to handle41104
// which issues ALTER AG OFFLINE + bilateral endpoint restart.
//
// When re-seats succeed but the replica remains NOT SYNCHRONIZING, the operator escalates:
// - At 15s: restart the secondary's HADR endpoint (clears collateral damage).
// - At 30s: escalate to bilateral restart (secondary + primary endpoints).
// This handles both direct connectivity failures and bystander damage from primary
// endpoint restarts triggered by recovery of other replicas.
func (r *SQLServerAvailabilityGroupReconciler) reconcileNotSynchronizingReplicas(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) (hadAny bool, err error) {
	log := logf.FromContext(ctx)
	if !ag.Status.InitializationComplete || ag.Status.PrimaryReplica == "" {
		return false, nil
	}
	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return false, err
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}

	actualRole, roleErr := exec.GetAGRole(ctx, ag.Namespace, ag.Status.PrimaryReplica, containerName, saPassword, ag.Spec.AGName)
	if roleErr != nil {
		log.Info("Could not verify primary role; skipping NOT SYNCHRONIZING check", "pod", ag.Status.PrimaryReplica, "err", roleErr)
		return false, nil
	}
	if actualRole != agRolePrimary {
		log.Info("Recorded primary is not in PRIMARY role; skipping NOT SYNCHRONIZING re-seat",
			"pod", ag.Status.PrimaryReplica, "role", actualRole)
		return false, nil
	}

	notSyncPods := r.queryNotSynchronizingPods(ctx, ag, exec, saPassword)
	if len(notSyncPods) == 0 {
		return false, nil
	}

	// Clean up stale /notSyncStuck tracking keys for pods that recovered.
	notSyncSet := make(map[string]struct{}, len(notSyncPods))
	for _, p := range notSyncPods {
		notSyncSet[p] = struct{}{}
	}
	prefix := ag.Namespace + "/" + ag.Name + "/"
	r.reseatFirstFailureTime.Range(func(key, _ any) bool {
		s, ok := key.(string)
		if !ok || !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, "/notSyncStuck") {
			return true
		}
		pod := strings.TrimPrefix(s, prefix)
		pod = strings.TrimSuffix(pod, "/notSyncStuck")
		if _, still := notSyncSet[pod]; !still {
			r.reseatFirstFailureTime.Delete(key)
		}
		return true
	})

	agDBNames := r.queryAGDatabaseNames(ctx, ag, exec, saPassword)

	for _, podName := range notSyncPods {
		// Guard: never re-seat the primary. After a planned failover the
		// outgoing primary's DMVs can briefly report the new primary as a
		// NOT SYNCHRONIZING SECONDARY before the role change propagates.
		// If the query ran against a stale PrimaryReplica value the new
		// primary ends up in notSyncPods. Re-seating it triggers Msg 41104
		// → ALTER AG OFFLINE, which wrecks the primary.
		if podName == ag.Status.PrimaryReplica {
			log.Info("Skipping re-seat: pod is the current primary", "pod", podName)
			continue
		}

		if len(agDBNames) > 0 && r.isSeedingInProgress(ctx, ag, exec, saPassword, podName, agDBNames) {
			log.Info("Automatic seeding in progress; skipping re-seat", "pod", podName)
			continue
		}

		hadAny = true
		log.Info("Detected NOT SYNCHRONIZING secondary; re-seating with SET(ROLE=SECONDARY)", "pod", podName)

		if _, setErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
			sqlutil.SetRoleToSecondarySQL(ag.Spec.AGName)); setErr != nil {
			if sqlutil.HasSQLError(sqlutil.ExecResult{}, setErr, sqlutil.ErrCodePreviousError) {
				r.handle41104(ctx, ag, exec, saPassword, podName)
			} else {
				log.Error(setErr, "Could not re-seat NOT SYNCHRONIZING replica", "pod", podName)
			}
		} else {
			r.clear41104Tracking(ag, podName)
			log.Info("Re-seated NOT SYNCHRONIZING replica", "pod", podName)

			// Track how long this pod has been persistently NOT SYNCHRONIZING
			// despite successful re-seats. If transport is stale (e.g. collateral
			// damage from a bilateral endpoint restart on the primary for another
			// replica), restart this secondary's endpoint to force reconnection.
			// If secondary-only restart doesn't help, escalate to bilateral restart.
			stuckKey := ag.Namespace + "/" + ag.Name + "/" + podName + "/notSyncStuck"
			now := time.Now()
			firstRaw, _ := r.reseatFirstFailureTime.LoadOrStore(stuckKey, now)
			firstTime := firstRaw.(time.Time)
			stuckDur := now.Sub(firstTime)
			if stuckDur >= 30*time.Second {
				// Secondary-only restart already tried; escalate to bilateral
				log.Info("Secondary persistently NOT SYNCHRONIZING; escalating to bilateral endpoint restart",
					"pod", podName, "stuckFor", stuckDur.Round(time.Second))
				if _, epErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
					sqlutil.RestartHADREndpointSQL()); epErr != nil {
					log.Error(epErr, "Could not restart HADR endpoint on stuck secondary", "pod", podName)
				}
				if ag.Status.PrimaryReplica != "" && ag.Status.PrimaryReplica != podName {
					log.Info("Restarting HADR endpoint on primary to clear bilateral transport state",
						"primary", ag.Status.PrimaryReplica)
					if _, epErr := exec.ExecSQL(ctx, ag.Namespace, ag.Status.PrimaryReplica, containerName, saPassword,
						sqlutil.RestartHADREndpointSQL()); epErr != nil {
						log.Error(epErr, "Could not restart HADR endpoint on primary", "pod", ag.Status.PrimaryReplica)
					}
				}
				// Reset timer — next cycle: secondary at 15s, bilateral at 30s
				r.reseatFirstFailureTime.Store(stuckKey, now)
			} else if stuckDur >= 15*time.Second {
				log.Info("Secondary persistently NOT SYNCHRONIZING despite successful re-seats; restarting HADR endpoint",
					"pod", podName, "stuckFor", stuckDur.Round(time.Second))
				if _, epErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
					sqlutil.RestartHADREndpointSQL()); epErr != nil {
					log.Error(epErr, "Could not restart HADR endpoint on stuck secondary", "pod", podName)
				}
				// Don't reset timer — allow escalation to bilateral at 30s
			}
		}
	}
	return hadAny, nil
}

// handle41104 manages Msg 41104 failures ("The Availability Group resource manager
// is waiting for a previous error to be resolved").
//
// Recovery strategy depends on the pod's local AG role:
//
//  1. LOCAL ROLE = PRIMARY (split-brain after planned failover):
//     Issue ALTER AVAILABILITY GROUP … OFFLINE to reset the AG resource state
//     from PRIMARY → RESOLVING. On the next reconcile SET(ROLE=SECONDARY) succeeds.
//
//  2. LOCAL ROLE = RESOLVING (unplanned failover — pod killed and restarted):
//     The AG resource manager has stale primary metadata and a latched connection
//     error. After 15 s, issue ALTER AG OFFLINE + bilateral endpoint restart
//     (restart endpoints on both the stuck replica AND the current primary).
//     The mirroring transport caches connection state on both sides — restarting
//     only one side is insufficient. OFFLINE clears stale primary_replica metadata;
//     bilateral endpoint restart forces fresh connections in both directions.
//     If the first attempt fails, retry every 90 s.
//
// No DROP/JOIN, no REMOVE/ADD — those risk triggering a full re-seed if a log
// backup truncates the chain between operations.
func (r *SQLServerAvailabilityGroupReconciler) handle41104(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	exec *sqlutil.Executor,
	saPassword, podName string,
) {
	log := logf.FromContext(ctx)
	cooldownKey := ag.Namespace + "/" + ag.Name + "/" + podName

	now := time.Now()
	firstRaw, _ := r.reseatFirstFailureTime.LoadOrStore(cooldownKey, now)
	firstTime := firstRaw.(time.Time)
	stuckDuration := now.Sub(firstTime)

	// Check local role: after a planned failover the old primary may still report
	// PRIMARY locally even though the actual primary has changed.  Running OFFLINE
	// resets the AG resource from PRIMARY → RESOLVING, allowing a subsequent
	// SET(ROLE=SECONDARY) to succeed.
	localRole, roleErr := exec.GetAGRole(ctx, ag.Namespace, podName, containerName, saPassword, ag.Spec.AGName)
	if roleErr == nil && localRole == agRolePrimary {
		log.Info("Msg 41104; pod still reports PRIMARY locally — issuing ALTER AG OFFLINE to reset state",
			"pod", podName, "stuckFor", stuckDuration.Round(time.Second))
		if _, offErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
			sqlutil.SetAGOfflineSQL(ag.Spec.AGName)); offErr != nil {
			log.Error(offErr, "Could not take AG OFFLINE on stuck primary", "pod", podName)
		}
		return
	}

	// RESOLVING case (unplanned failover): the AG resource has stale primary
	// metadata and a latched connection error. After 30 s, issue OFFLINE +
	// endpoint restart. The transport needs ~30 s to reconnect; the next
	// reconcile cycle will retry SET(ROLE=SECONDARY).
	// If the first attempt didn't work, retry every 90 s.
	offlineKey := cooldownKey + "/offlineRestart"
	lastRaw, offlineDone := r.reseatFirstFailureTime.Load(offlineKey)
	canRetry := false
	if offlineDone {
		lastTime := lastRaw.(time.Time)
		canRetry = now.Sub(lastTime) >= 90*time.Second
	}

	if (!offlineDone || canRetry) && stuckDuration >= 15*time.Second {
		r.reseatFirstFailureTime.Store(offlineKey, now)
		log.Info("Msg 41104; pod in RESOLVING — issuing ALTER AG OFFLINE + bilateral endpoint restart",
			"pod", podName, "stuckFor", stuckDuration.Round(time.Second), "retry", canRetry)
		if _, offErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
			sqlutil.SetAGOfflineSQL(ag.Spec.AGName)); offErr != nil {
			log.Error(offErr, "Could not take AG OFFLINE on RESOLVING replica", "pod", podName)
		}
		// Restart endpoint on the SECONDARY (stuck replica) to clear stale connections.
		if _, epErr := exec.ExecSQL(ctx, ag.Namespace, podName, containerName, saPassword,
			sqlutil.RestartHADREndpointSQL()); epErr != nil {
			log.Error(epErr, "Could not restart HADR endpoint on RESOLVING replica", "pod", podName)
		}
		// Restart endpoint on the PRIMARY too — the transport timeout is bilateral;
		// the primary also caches stale connection state for the returning replica
		// and won't attempt a fresh connection until its own endpoint is cycled.
		if ag.Status.PrimaryReplica != "" && ag.Status.PrimaryReplica != podName {
			log.Info("Restarting HADR endpoint on primary to clear bilateral transport state",
				"primary", ag.Status.PrimaryReplica)
			if _, epErr := exec.ExecSQL(ctx, ag.Namespace, ag.Status.PrimaryReplica, containerName, saPassword,
				sqlutil.RestartHADREndpointSQL()); epErr != nil {
				log.Error(epErr, "Could not restart HADR endpoint on primary", "pod", ag.Status.PrimaryReplica)
			}
		}
		return
	}

	log.Info("Msg 41104; waiting for AG resource manager to clear previous error",
		"pod", podName, "stuckFor", stuckDuration.Round(time.Second))
}

// clear41104Tracking clears the 41104 failure tracking for a pod (called on success).
func (r *SQLServerAvailabilityGroupReconciler) clear41104Tracking(
	ag *sqlv1alpha1.SQLServerAvailabilityGroup, podName string,
) {
	cooldownKey := ag.Namespace + "/" + ag.Name + "/" + podName
	r.reseatFirstFailureTime.Delete(cooldownKey)
	r.reseatFirstFailureTime.Delete(cooldownKey + "/offlineRestart")
}

// queryAGDatabaseNames returns the database names in the AG as seen from the primary.
// Returns nil if the query fails (treated as unknown — seeding check is skipped).
func (r *SQLServerAvailabilityGroupReconciler) queryAGDatabaseNames(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	exec *sqlutil.Executor,
	saPassword string,
) []string {
	res, err := exec.ExecSQL(ctx, ag.Namespace, ag.Status.PrimaryReplica, containerName, saPassword,
		sqlutil.AGDatabaseNamesSQL(ag.Spec.AGName))
	if err != nil {
		return nil
	}
	var names []string
	for line := range strings.SplitSeq(res.Stdout, "\n") {
		name := strings.TrimSpace(line)
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// isSeedingInProgress returns true when the secondary pod is missing at least
// one database that exists in the AG on the primary, indicating that automatic
// seeding has not yet completed for that database.
//
// Automatic seeding creates the database on the secondary only after all pages
// have been transferred. While seeding is in progress, sys.databases on the
// secondary does not contain the database being seeded, so it will be absent
// from the secondary's online user-database list.
//
// Database names are resolved with DB_NAME() on both sides for consistent
// comparison, avoiding false positives when non-AG user databases exist on
// a secondary.
func (r *SQLServerAvailabilityGroupReconciler) isSeedingInProgress(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	exec *sqlutil.Executor,
	saPassword string,
	secondaryPod string,
	agDatabaseNames []string,
) bool {
	res, err := exec.ExecSQL(ctx, ag.Namespace, secondaryPod, containerName, saPassword,
		sqlutil.SecondaryUserDatabaseNamesSQL())
	if err != nil {
		// Cannot connect to secondary; assume seeding is not in progress so
		// the normal re-seat logic can handle a genuinely stuck replica.
		return false
	}
	secDBs := make(map[string]struct{})
	for line := range strings.SplitSeq(res.Stdout, "\n") {
		name := strings.TrimSpace(line)
		if name != "" {
			secDBs[name] = struct{}{}
		}
	}
	for _, agDB := range agDatabaseNames {
		if _, exists := secDBs[agDB]; !exists {
			return true
		}
	}
	return false
}

// queryNotSynchronizingPods returns the pod names of secondaries with databases
// in NOT SYNCHRONIZING state, as seen from the primary.
func (r *SQLServerAvailabilityGroupReconciler) queryNotSynchronizingPods(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	exec *sqlutil.Executor,
	saPassword string,
) []string {
	result, err := exec.ExecSQL(ctx, ag.Namespace, ag.Status.PrimaryReplica, containerName, saPassword,
		sqlutil.NotSynchronizingReplicasSQL(ag.Spec.AGName))
	if err != nil {
		logf.FromContext(ctx).Info("Could not query NOT SYNCHRONIZING replicas from primary", "err", err)
		return nil
	}
	agPrefix := ag.Name + "-"
	var pods []string
	for line := range strings.SplitSeq(result.Stdout, "\n") {
		replicaName := strings.TrimSpace(line)
		if replicaName == "" || !strings.HasPrefix(replicaName, agPrefix) {
			continue
		}
		pods = append(pods, replicaName)
	}
	return pods
}

// failoverHeadlessAG handles the "headless AG" scenario: the recorded primary pod
// is K8s-Ready but is serving as SECONDARY/RESOLVING (e.g. after an unplanned kill +
// pod restart), leaving the AG with no active primary.
//
// Issues ALTER AVAILABILITY GROUP FAILOVER (not FORCE_FAILOVER_ALLOW_DATA_LOSS) on the
// best synchronous secondary. SQL Server verifies synchronization state locally and rejects
// the command with error 41142 if the target was not synchronized; the operator retries on
// the next reconcile rather than forcing a potentially lossy failover.
//
// Returns a non-empty ctrl.Result when it handled the situation (corrected status, issued
// failover, or queued a retry). Returns ctrl.Result{} when the pod is genuinely
// serving as PRIMARY (or SQL is unreachable — we treat that conservatively as healthy).
func (r *SQLServerAvailabilityGroupReconciler) failoverHeadlessAG(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	primaryPodName string,
	patch client.Patch,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	saPassword, pwErr := r.getSAPassword(ctx, ag)
	if pwErr != nil {
		return ctrl.Result{}, nil // can't verify — treat conservatively as healthy
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}

	actualRole, _ := exec.GetAGRole(ctx, ag.Namespace, primaryPodName, containerName, saPassword, ag.Spec.AGName)
	if actualRole != agRoleSecondary && actualRole != agRoleResolving {
		return ctrl.Result{}, nil // pod IS primary (or SQL unreachable) — nothing to do
	}

	// Pod is K8s-Ready but NOT serving as primary. Check if another pod is.
	for i := range ag.Spec.Replicas {
		candidatePod := podNameForReplica(ag, i)
		if candidatePod == primaryPodName {
			continue
		}
		role, _ := exec.GetAGRole(ctx, ag.Namespace, candidatePod, containerName, saPassword, ag.Spec.AGName)
		if role == agRolePrimary {
			log.Info("Correcting stale primary record; another pod is PRIMARY",
				"old", primaryPodName, "new", candidatePod)
			ag.Status.PrimaryReplica = candidatePod
			ag.Status.PrimaryNotReadySince = nil
			if sErr := r.Status().Patch(ctx, ag, patch); sErr != nil {
				return ctrl.Result{}, fmt.Errorf("could not update primary record: %w", sErr)
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// No pod is PRIMARY — the AG is headless. Trigger immediate failover.
	log.Info("AG is headless (no primary); triggering immediate failover",
		"recordedPrimary", primaryPodName)

	targetPod, selErr := r.selectFailoverTarget(ctx, ag)
	if selErr != nil || targetPod == "" {
		log.Info("No eligible failover target for headless AG; will retry", "err", selErr)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	foRes, foErr := exec.ExecSQL(ctx, ag.Namespace, targetPod, containerName, saPassword,
		sqlutil.FailoverSQL(ag.Spec.AGName))
	if foErr != nil {
		if isUnsynchronizedError(foRes, foErr) {
			log.Info("Failover target not synchronized (error 41142); will retry", "target", targetPod)
		} else {
			log.Error(foErr, "Failover command failed for headless AG; will retry", "target", targetPod)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	log.Info("Headless AG failover succeeded", "newPrimary", targetPod, "formerPrimary", primaryPodName)
	now := metav1.Now()
	ag.Status.PrimaryReplica = targetPod
	ag.Status.PrimaryNotReadySince = nil
	ag.Status.Phase = sqlv1alpha1.AGPhaseRunning
	apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
		Type:               "Failover",
		Status:             metav1.ConditionTrue,
		Reason:             "HeadlessAGFailover",
		Message:            fmt.Sprintf("Force failover from headless AG: %s → %s", primaryPodName, targetPod),
		ObservedGeneration: ag.Generation,
		LastTransitionTime: now,
	})
	if sErr := r.Status().Patch(ctx, ag, patch); sErr != nil {
		return ctrl.Result{}, fmt.Errorf("could not update status after headless failover: %w", sErr)
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// isUnsynchronizedError returns true when a failover exec result contains SQL Server
// error 41142, which indicates the target replica was not synchronized with the primary
// at the time of the failover and SQL Server rejected the FAILOVER command rather than
// risking data loss. The operator retries on the next reconcile cycle.
//
// This matches Microsoft's mssql-server-ha behaviour: ag-helper checks for this error
// and returns OCF_ERR_GENERIC to Pacemaker, never falling back to FORCE_FAILOVER.
func isUnsynchronizedError(res sqlutil.ExecResult, err error) bool {
	return sqlutil.HasSQLError(res, err, sqlutil.ErrCodeNotSynchronized)
}

// primaryDiagsUnhealthy runs sp_server_diagnostics on the named pod and returns true
// when the instance health fails the configured HealthThreshold. It matches Microsoft's
// OpenDBWithHealthCheck + Diagnose pattern from mssql-server-ha.
//
// Returns false (healthy) in all error cases — if SQL Server is genuinely unreachable,
// the Kubernetes pod readiness probe will catch it and set the pod to NotReady, which
// is the primary liveness signal for the failover timer.
func (r *SQLServerAvailabilityGroupReconciler) primaryDiagsUnhealthy(
	ctx context.Context,
	ag *sqlv1alpha1.SQLServerAvailabilityGroup,
	podName string,
) bool {
	log := logf.FromContext(ctx)

	threshold := "system" // matches Microsoft's default ServerCriticalError
	if ag.Spec.AutomaticFailover != nil && ag.Spec.AutomaticFailover.HealthThreshold != "" {
		threshold = ag.Spec.AutomaticFailover.HealthThreshold
	}

	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		log.Info("Could not retrieve SA password for diagnostics check; treating as healthy", "err", err)
		return false
	}

	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}
	diag, err := exec.CheckServerDiagnostics(ctx, ag.Namespace, podName, containerName, saPassword)
	if err != nil {
		// SQL Server temporarily unreachable — treat conservatively as healthy.
		// The K8s readiness probe will flip the pod to NotReady if it truly fails.
		log.Info("Could not run sp_server_diagnostics; treating primary as healthy", "pod", podName, "err", err)
		return false
	}

	healthy := diag.IsHealthyAt(threshold)
	if !healthy {
		log.Info("sp_server_diagnostics reports primary unhealthy",
			"pod", podName,
			"threshold", threshold,
			"system", diag.System,
			"resource", diag.Resource,
			"queryProcessing", diag.QueryProcessing)
	}
	return !healthy
}

// reconcileFailover detects primary pod failures and automatically promotes a synchronous
// secondary. It is a no-op unless AutomaticFailover is enabled.
//
// With CLUSTER_TYPE = EXTERNAL the operator acts as the external cluster manager and issues
// ALTER AVAILABILITY GROUP FAILOVER (prefixed with sp_set_session_context to authorize the DDL).
// This mirrors the promote action in Microsoft's mssql-server-ha ag-helper, which always
// issues FAILOVER and surfaces error 41142 back to Pacemaker if the target is not synchronized,
// rather than falling back to FORCE_FAILOVER_ALLOW_DATA_LOSS.
//
// State machine:
//  1. Primary pod is Ready           → clear PrimaryNotReadySince, return.
//  2. Primary pod is NotReady        → record PrimaryNotReadySince (first observation), requeue.
//  3. NotReady for < threshold       → wait, requeue with remaining time.
//  4. NotReady for >= threshold      → select best synchronous target, issue FAILOVER,
//     update status.PrimaryReplica and clear PrimaryNotReadySince.
//     If target returns 41142 (not synchronized), log and retry next reconcile.
func (r *SQLServerAvailabilityGroupReconciler) reconcileFailover(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	af := ag.Spec.AutomaticFailover
	if af == nil || !af.Enabled {
		return ctrl.Result{}, nil
	}
	if !ag.Status.InitializationComplete || ag.Status.PrimaryReplica == "" {
		return ctrl.Result{}, nil
	}

	primaryPodName := ag.Status.PrimaryReplica
	primaryPod := &corev1.Pod{}
	podGetErr := r.Get(ctx, types.NamespacedName{Name: primaryPodName, Namespace: ag.Namespace}, primaryPod)
	if podGetErr != nil && !errors.IsNotFound(podGetErr) {
		return ctrl.Result{}, podGetErr
	}
	podReady := podGetErr == nil && isPodReady(primaryPod)

	patch := client.MergeFrom(ag.DeepCopy())

	if podReady {
		// Supplementary SQL Server health check via sp_server_diagnostics.
		// Catches internal failures (non-yielding schedulers, memory pressure, worker
		// deadlocks) that don't immediately flip the pod to K8s-NotReady — matching
		// Microsoft's OpenDBWithHealthCheck + Diagnose pattern from mssql-server-ha.
		//
		// Conservative: if the exec fails (SQL temporarily unreachable), we treat the
		// pod as healthy and let the K8s readiness probe handle the real liveness signal.
		if r.primaryDiagsUnhealthy(ctx, ag, primaryPodName) {
			// Pod is K8s-Ready but SQL Server internally unhealthy — fall through to
			// the NotReady timer logic below so the same threshold/timer path is used.
			log.Info("Primary is K8s-Ready but sp_server_diagnostics reports unhealthy; treating as degraded",
				"pod", primaryPodName)
		} else {
			// The primary pod is K8s-Ready AND SQL is healthy, but it may have restarted
			// after being killed and come back as SECONDARY/RESOLVING, leaving the AG with
			// no active primary ("headless AG"). failoverHeadlessAG detects this via SQL
			// and issues an immediate failover without waiting for the NotReady timer.
			if result, hErr := r.failoverHeadlessAG(ctx, ag, primaryPodName, patch); hErr != nil || result != (ctrl.Result{}) {
				return result, hErr
			}

			// Pod is genuinely serving as PRIMARY (or SQL is unreachable — treated conservatively).
			if ag.Status.PrimaryNotReadySince != nil {
				log.Info("Primary pod recovered; clearing failover timer", "pod", primaryPodName)
				ag.Status.PrimaryNotReadySince = nil
				if err := r.Status().Patch(ctx, ag, patch); err != nil {
					return ctrl.Result{}, fmt.Errorf("could not clear PrimaryNotReadySince: %w", err)
				}
			}
			return ctrl.Result{}, nil
		}
	}

	// Primary is NotReady (or missing).
	threshold := max(time.Duration(af.FailoverThresholdSeconds)*time.Second, 10*time.Second)
	now := metav1.Now()

	if ag.Status.PrimaryNotReadySince == nil {
		log.Info("Primary pod NotReady; starting failover threshold timer",
			"pod", primaryPodName, "threshold", threshold)
		ag.Status.PrimaryNotReadySince = &now
		if err := r.Status().Patch(ctx, ag, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("could not set PrimaryNotReadySince: %w", err)
		}
		return ctrl.Result{RequeueAfter: threshold}, nil
	}

	elapsed := time.Since(ag.Status.PrimaryNotReadySince.Time)
	if elapsed < threshold {
		remaining := threshold - elapsed
		log.Info("Primary pod NotReady; waiting for threshold",
			"pod", primaryPodName, "elapsed", elapsed.Round(time.Second), "remaining", remaining.Round(time.Second))
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	// Threshold exceeded — select and promote the best synchronous secondary.
	log.Info("Primary NotReady threshold exceeded; selecting failover target",
		"pod", primaryPodName, "elapsed", elapsed.Round(time.Second))

	targetPod, selErr := r.selectFailoverTarget(ctx, ag)
	if selErr != nil || targetPod == "" {
		log.Info("No eligible failover target; will retry", "err", selErr)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return ctrl.Result{}, err
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}
	foRes, foErr := exec.ExecSQL(ctx, ag.Namespace, targetPod, containerName, saPassword,
		sqlutil.FailoverSQL(ag.Spec.AGName))
	if foErr != nil {
		if isUnsynchronizedError(foRes, foErr) {
			log.Info("Failover target not synchronized (error 41142); will retry with another candidate next reconcile", "target", targetPod)
		} else {
			log.Error(foErr, "Failover command failed; will retry", "target", targetPod)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	log.Info("Automatic unplanned failover succeeded", "newPrimary", targetPod, "formerPrimary", primaryPodName)
	ag.Status.PrimaryReplica = targetPod
	ag.Status.PrimaryNotReadySince = nil
	ag.Status.Phase = sqlv1alpha1.AGPhaseRunning
	apimeta.SetStatusCondition(&ag.Status.Conditions, metav1.Condition{
		Type:               "Failover",
		Status:             metav1.ConditionTrue,
		Reason:             "UnplannedFailover",
		Message:            fmt.Sprintf("Automatic failover from %s to %s after primary was NotReady for %s", primaryPodName, targetPod, elapsed.Round(time.Second)),
		ObservedGeneration: ag.Generation,
		LastTransitionTime: now,
	})
	if err := r.Status().Patch(ctx, ag, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not update status after failover: %w", err)
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// mapPodToAG maps an AG replica pod back to a reconcile request for its owning
// SQLServerAvailabilityGroup CR. AG pods carry the label "app: <agName>" set by
// buildAGStatefulSet. If the label is absent or the AG is not found the pod is
// ignored (returns nil).
func (r *SQLServerAvailabilityGroupReconciler) mapPodToAG(ctx context.Context, obj client.Object) []reconcile.Request {
	agName, ok := obj.GetLabels()["app"]
	if !ok {
		return nil
	}
	// Confirm the named AG actually exists before enqueuing.
	ag := &sqlv1alpha1.SQLServerAvailabilityGroup{}
	if err := r.Get(ctx, types.NamespacedName{Name: agName, Namespace: obj.GetNamespace()}, ag); err != nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: agName, Namespace: obj.GetNamespace()}}}
}

// reconcilePVCReclaimPolicy lists all PVCs for the AG's replicas (matched by the
// "app: <agName>" label the StatefulSet applies), then patches each bound PV's
// reclaimPolicy to match spec.storage.reclaimPolicy. Defaults to Retain when the
// field is unset so data is never silently deleted on CR removal.
func (r *SQLServerAvailabilityGroupReconciler) reconcilePVCReclaimPolicy(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	log := logf.FromContext(ctx)
	desired := ag.Spec.Storage.ReclaimPolicy
	if desired == "" {
		desired = corev1.PersistentVolumeReclaimRetain
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList,
		client.InNamespace(ag.Namespace),
		client.MatchingLabels{"app": ag.Name},
	); err != nil {
		return fmt.Errorf("could not list PVCs: %w", err)
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
			continue
		}
		pv := &corev1.PersistentVolume{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return fmt.Errorf("could not get PV %s: %w", pvc.Spec.VolumeName, err)
		}
		if pv.Spec.PersistentVolumeReclaimPolicy == desired {
			continue
		}
		patch := client.MergeFrom(pv.DeepCopy())
		pv.Spec.PersistentVolumeReclaimPolicy = desired
		if err := r.Patch(ctx, pv, patch); err != nil {
			return fmt.Errorf("could not patch PV %s reclaimPolicy: %w", pv.Name, err)
		}
		log.Info("Patched PV reclaimPolicy", "pv", pv.Name, "policy", desired)
	}
	return nil
}

// SetupWithManager sets up the AG controller with the Manager.
func (r *SQLServerAvailabilityGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// GenerationChangedPredicate suppresses status-only update events.
	// Status subresource patches never increment metadata.generation, so this
	// predicate breaks the status-patch → watch-event → reconcile feedback loop
	// that would otherwise cause ~1 Hz runaway reconciliation.
	genChanged := predicate.GenerationChangedPredicate{}

	// isManagedAGPod is true for pods created by this operator's AG StatefulSets.
	// These pods carry the "app.kubernetes.io/managed-by" label set in buildAGStatefulSet.
	// Watching them lets the controller react quickly to primary pod readiness changes
	// without waiting for the 60-second RequeueAfter poll interval.
	isManagedAGPod := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()["app.kubernetes.io/managed-by"] == "sql-on-k8s-operator" &&
			obj.GetLabels()["role"] == "mssql-ag"
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&sqlv1alpha1.SQLServerAvailabilityGroup{}, builder.WithPredicates(genChanged)).
		Owns(&appsv1.StatefulSet{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.Service{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.ConfigMap{}, builder.WithPredicates(genChanged)).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodToAG),
			builder.WithPredicates(isManagedAGPod),
		).
		Named("sqlserveravailabilitygroup").
		Complete(r)
}
