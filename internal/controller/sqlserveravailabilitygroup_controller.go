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
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create

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
	// on the primary and transitions each one to SECONDARY.
	if effectiveClusterType(ag.Spec.ClusterType) != agClusterTypeNone {
		if err := r.reconcileResolvingReplicas(ctx, ag); err != nil {
			log.Error(err, "Could not reconcile RESOLVING replicas; will retry on next poll")
		}
	}

	// Requeue periodically to keep status fresh and detect failovers
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
	replicas := int32(len(ag.Spec.Replicas))
	sts.Spec.Replicas = &replicas
	sts.Spec.Template.Spec.Containers[0].Image = ag.Spec.Image
	sts.Spec.Template.Spec.Containers[0].Resources = ag.Spec.Resources
	return r.Update(ctx, sts)
}

// reconcileAGHeadlessService creates the headless Service for AG pod DNS.
func (r *SQLServerAvailabilityGroupReconciler) reconcileAGHeadlessService(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	labels := map[string]string{"app": ag.Name, "role": "mssql-ag"}
	svcName := ag.Name + "-headless"
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: svcName, Namespace: ag.Namespace}, svc)
	if errors.IsNotFound(err) {
		newSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ag.Namespace, Labels: labels},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector:  labels,
				Ports: []corev1.ServicePort{
					{Name: "mssql", Port: 1433, TargetPort: intstr.FromInt32(1433)},
					{Name: "hadr", Port: ag.Spec.EndpointPort, TargetPort: intstr.FromInt32(ag.Spec.EndpointPort)},
				},
			},
		}
		if err2 := controllerutil.SetControllerReference(ag, newSvc, r.Scheme); err2 != nil {
			return err2
		}
		return r.Create(ctx, newSvc)
	}
	return err
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
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
		readableByPod[podName] = ag.Spec.Replicas[i].ReadableSecondary
	}

	for i := range ag.Spec.Replicas {
		podName := fmt.Sprintf("%s-%d", ag.Name, i)

		// Assign label based on primary identity (SQL-confirmed or status-derived) and spec.
		var desiredLabel string
		if podName == primaryPod {
			desiredLabel = "primary"
		} else if readableByPod[podName] {
			desiredLabel = "readable-secondary"
		} else {
			desiredLabel = "secondary"
		}

		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: ag.Namespace}, pod); err != nil {
			continue // pod not yet scheduled — skip
		}
		if pod.Labels[labelAGRole] == desiredLabel {
			continue // already correct
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
		secondaryPod := fmt.Sprintf("%s-%d", ag.Name, i)
		if _, err := exec.ExecSQL(ctx, ag.Namespace, secondaryPod, "mssql", saPassword,
			sqlutil.JoinAGSQL(ag.Spec.AGName, clusterType)); err != nil {
			log.Error(err, "Could not join AG on secondary", "pod", secondaryPod)
			return ctrl.Result{RequeueAfter: 15 * time.Second}
		}
		log.Info("Joined AG on secondary", "pod", secondaryPod)
		if clusterType != agClusterTypeNone {
			// Transition from RESOLVING → SECONDARY so SQL Server reports the replica
			// as SECONDARY+CONNECTED and the operator's health monitoring can proceed.
			if _, err := exec.ExecSQL(ctx, ag.Namespace, secondaryPod, "mssql", saPassword,
				sqlutil.SetRoleToSecondarySQL(ag.Spec.AGName)); err != nil {
				log.Error(err, "Could not set secondary role on secondary", "pod", secondaryPod)
				return ctrl.Result{RequeueAfter: 15 * time.Second}
			}
			log.Info("Set SECONDARY role on secondary", "pod", secondaryPod)
		}
	}
	return ctrl.Result{}
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
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
		if !exec.IsReady(ctx, ag.Namespace, podName, "mssql", saPassword) {
			log.Info("Pod not yet ready, requeuing", "pod", podName)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	// Step 2: Create master key, certificate, and HADR endpoint on every replica.
	// Backup the DER-encoded cert to a well-known path on that pod's own PVC.
	for i := range replicas {
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
		certName := certNameForPod(podName)
		backupPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", certName)
		for _, q := range []string{
			sqlutil.CreateMasterKeySQL(masterKeyPw),
			sqlutil.CreateCertificateSQL(certName, podName+" AG Certificate", backupPath),
			sqlutil.CreateEndpointSQL("AGEP", certName, endpointPort),
		} {
			if _, err := exec.ExecSQL(ctx, ag.Namespace, podName, "mssql", saPassword, q); err != nil {
				return ctrl.Result{}, fmt.Errorf("cert/endpoint setup failed on %s: %w", podName, err)
			}
		}
		log.Info("Created cert and endpoint", "pod", podName)
	}

	// Step 3: For each (source, target) pair, transfer the source cert file into the
	// target pod using SPDY streaming (ReadFileFromPod → WriteFileToPod).
	// This avoids any shared-storage requirement between pods.
	for j := range replicas {
		sourcePod := fmt.Sprintf("%s-%d", ag.Name, j)
		sourceCertName := certNameForPod(sourcePod)
		sourceCertPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", sourceCertName)

		certBytes, err := exec.ReadFileFromPod(ctx, ag.Namespace, sourcePod, "mssql", sourceCertPath)
		if err != nil {
			log.Error(err, "Could not read cert from pod", "pod", sourcePod)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		for i := range replicas {
			if i == j {
				continue
			}
			targetPod := fmt.Sprintf("%s-%d", ag.Name, i)
			destPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", sourceCertName)
			if err := exec.WriteFileToPod(ctx, ag.Namespace, targetPod, "mssql", destPath, certBytes); err != nil {
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
		targetPod := fmt.Sprintf("%s-%d", ag.Name, i)
		for j := range replicas {
			if i == j {
				continue
			}
			sourcePod := fmt.Sprintf("%s-%d", ag.Name, j)
			sourceCertName := certNameForPod(sourcePod)
			destPath := fmt.Sprintf("/var/opt/mssql/data/%s.cer", sourceCertName)
			loginName := fmt.Sprintf("%s_login", strings.ReplaceAll(sourcePod, "-", "_"))
			for _, q := range []string{
				sqlutil.RestoreCertificateSQL(sourceCertName, destPath),
				sqlutil.CreateLoginFromCertSQL(loginName, sourceCertName),
				sqlutil.GrantEndpointConnectSQL("AGEP", loginName),
			} {
				if _, err := exec.ExecSQL(ctx, ag.Namespace, targetPod, "mssql", saPassword, q); err != nil {
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
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
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
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
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
	existResult, err := exec.ExecSQL(ctx, ag.Namespace, primaryPod, "mssql", saPassword,
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
		if _, createErr := exec.ExecSQL(ctx, ag.Namespace, primaryPod, "mssql", saPassword, createSQL); createErr != nil {
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
		secondaryPod := fmt.Sprintf("%s-%d", ag.Name, i)
		if _, err := sqlExec.ExecSQL(ctx, ag.Namespace, secondaryPod, "mssql", saPassword, dropSQL); err != nil {
			log.Error(err, "Could not drop AG on secondary, continuing", "pod", secondaryPod)
		}
	}
	if _, err := sqlExec.ExecSQL(ctx, ag.Namespace, primaryPod, "mssql", saPassword, dropSQL); err != nil {
		log.Error(err, "Could not drop AG on primary, will requeue anyway")
	}
}

// waitForSecondariesReady polls the primary until all secondary replicas report
// both role_desc = 'SECONDARY' AND connected_state_desc = 'CONNECTED', waiting
// up to 10 minutes (60 × 10 s).
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
		result, qErr := sqlExec.ExecSQL(ctx, ag.Namespace, primaryPod, "mssql", saPassword,
			sqlutil.SecondaryCountSQL(ag.Spec.AGName))
		if qErr != nil {
			consecutiveOK = 0
			log.Info("Could not query secondary state, will retry", "attempt", attempt, "err", qErr)
			time.Sleep(10 * time.Second)
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
		time.Sleep(10 * time.Second)
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
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
		role, _ := exec.GetAGRole(ctx, ag.Namespace, podName, "mssql", saPassword, ag.Spec.AGName)
		syncState, _ := exec.GetAGSyncState(ctx, ag.Namespace, podName, "mssql", saPassword, ag.Spec.AGName)
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
	// externally-initiated planned failover (e.g., ALTER AG FAILOVER run by a test)
	// when the controller has not yet observed the new primary.
	// Recover by identifying the unreachable pod as the inferred primary so that
	// reconcileFailover can correctly detect the pod failure and start the timer.
	if !foundPrimary && replicaRoles[ag.Status.PrimaryReplica] == agRoleSecondary {
		log := logf.FromContext(ctx)
		for i := range ag.Spec.Replicas {
			podName := fmt.Sprintf("%s-%d", ag.Name, i)
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

	envVars := []corev1.EnvVar{
		{Name: "ACCEPT_EULA", Value: acceptEULA},
		{Name: "MSSQL_PID", Value: edition},
		{Name: "MSSQL_AGENT_ENABLED", Value: "true"},
		{
			Name: "SA_PASSWORD",
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
										Command: []string{
											"/bin/bash", "-c",
											`/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" -C -b 2>&1 | grep -q "1"`,
										},
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
										Command: []string{
											"/bin/bash", "-c",
											`/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" -C -b 2>&1 | grep -q "1"`,
										},
									},
								},
								InitialDelaySeconds: 45,
								PeriodSeconds:       15,
								FailureThreshold:    3,
								TimeoutSeconds:      10,
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
//     configured SynchronousCommit/Automatic replica. The FORCE_FAILOVER_ALLOW_DATA_LOSS
//     command on a synchronous replica that was SYNCHRONIZED before the primary died incurs
//     no actual data loss despite the command name.
//
// A replica is considered for either pass only when:
//   - AvailabilityMode = SynchronousCommit — async replicas are never auto-failed over to.
//   - FailoverMode = Automatic — only replicas explicitly configured for automatic failover.
//
// NOTE: We intentionally do NOT pre-filter candidates by SQL reachability. After an unplanned
// primary failure, secondaries may be temporarily unresponsive to SELECT 1 (e.g. RESOLVING
// state transitions, redo thread activity, or transient kubelet exec latency). Skipping them
// entirely would leave the AG in a headless state indefinitely. Instead, we capture them as
// best-effort candidates and let the subsequent FORCE_FAILOVER_ALLOW_DATA_LOSS command fail
// and retry until the target is ready to accept commands.
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
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
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
		// best-effort rather than skipping it entirely. The FORCE_FAILOVER command will be
		// retried and will succeed once the target's SQL Server is ready to accept commands.
		health, hErr := exec.GetAGSyncState(ctx, ag.Namespace, podName, "mssql", saPassword, ag.Spec.AGName)
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
// requires the external cluster manager to explicitly grant it the SECONDARY role. This is
// analogous to Pacemaker's "start" action in the mssql-server-ha OCF agent.
//
// The function queries the current primary for non-local replicas in RESOLVING state and
// issues the transition command directly on each affected pod's SQL connection.
func (r *SQLServerAvailabilityGroupReconciler) reconcileResolvingReplicas(ctx context.Context, ag *sqlv1alpha1.SQLServerAvailabilityGroup) error {
	log := logf.FromContext(ctx)
	if !ag.Status.InitializationComplete || ag.Status.PrimaryReplica == "" {
		return nil
	}
	saPassword, err := r.getSAPassword(ctx, ag)
	if err != nil {
		return err
	}
	exec := &sqlutil.Executor{Client: r.KubeClient, RestConfig: r.RestConfig}
	result, err := exec.ExecSQL(ctx, ag.Namespace, ag.Status.PrimaryReplica, "mssql", saPassword,
		sqlutil.ResolvingReplicasSQL(ag.Spec.AGName))
	if err != nil {
		// Primary may be mid-failover; log and return nil to avoid blocking the reconcile loop.
		log.Info("Could not query RESOLVING replicas from primary", "err", err)
		return nil
	}
	// Filter output lines to valid pod names.
	// sqlcmd output (without -h -1) includes a column-header line (e.g.
	// "replica_server_name") and a separator line ("----------...") before the
	// data rows.  Pod names in this operator are always prefixed with the AG
	// name followed by a hyphen (e.g. "mssql-ag-0"), so any other line is noise.
	agPrefix := ag.Name + "-"
	for line := range strings.SplitSeq(result.Stdout, "\n") {
		replicaName := strings.TrimSpace(line)
		if replicaName == "" || !strings.HasPrefix(replicaName, agPrefix) {
			continue // skip blank lines, column headers, and separator lines
		}
		// replica_server_name matches the pod name (@@SERVERNAME = short pod hostname).
		podName := replicaName
		log.Info("Detected RESOLVING secondary; transitioning to SECONDARY", "pod", podName)
		if _, setErr := exec.ExecSQL(ctx, ag.Namespace, podName, "mssql", saPassword,
			sqlutil.SetRoleToSecondarySQL(ag.Spec.AGName)); setErr != nil {
			log.Error(setErr, "Could not set SECONDARY role on RESOLVING replica", "pod", podName)
		} else {
			log.Info("Set SECONDARY role on RESOLVING replica", "pod", podName)
		}
	}
	return nil
}

// forceFailoverHeadlessAG handles the "headless AG" scenario: the recorded primary pod
// is K8s-Ready but is serving as SECONDARY/RESOLVING (e.g. after an unplanned kill +
// pod restart), leaving the AG with no active primary.
//
// Returns a non-empty ctrl.Result when it handled the situation (corrected status, issued
// force failover, or queued a retry). Returns ctrl.Result{} when the pod is genuinely
// serving as PRIMARY (or SQL is unreachable — we treat that conservatively as healthy).
func (r *SQLServerAvailabilityGroupReconciler) forceFailoverHeadlessAG(
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

	actualRole, _ := exec.GetAGRole(ctx, ag.Namespace, primaryPodName, "mssql", saPassword, ag.Spec.AGName)
	if actualRole != agRoleSecondary && actualRole != agRoleResolving {
		return ctrl.Result{}, nil // pod IS primary (or SQL unreachable) — nothing to do
	}

	// Pod is K8s-Ready but NOT serving as primary. Check if another pod is.
	for i := range ag.Spec.Replicas {
		candidatePod := fmt.Sprintf("%s-%d", ag.Name, i)
		if candidatePod == primaryPodName {
			continue
		}
		role, _ := exec.GetAGRole(ctx, ag.Namespace, candidatePod, "mssql", saPassword, ag.Spec.AGName)
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

	// No pod is PRIMARY — the AG is headless. Trigger immediate force failover.
	log.Info("AG is headless (no primary); triggering immediate force failover",
		"recordedPrimary", primaryPodName)

	targetPod, selErr := r.selectFailoverTarget(ctx, ag)
	if selErr != nil || targetPod == "" {
		log.Info("No eligible failover target for headless AG; will retry", "err", selErr)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if _, foErr := exec.ExecSQL(ctx, ag.Namespace, targetPod, "mssql", saPassword,
		sqlutil.ForcedFailoverSQL(ag.Spec.AGName)); foErr != nil {
		log.Error(foErr, "Forced failover command failed for headless AG; will retry", "target", targetPod)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	log.Info("Headless AG force failover succeeded", "newPrimary", targetPod, "formerPrimary", primaryPodName)
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

// reconcileFailover detects primary pod failures and automatically promotes a synchronous
// secondary. It is a no-op unless AutomaticFailover is enabled.
//
// With CLUSTER_TYPE = EXTERNAL the operator acts as the external cluster manager and issues
// FORCE_FAILOVER_ALLOW_DATA_LOSS (prefixed with sp_set_session_context to authorize the DDL).
// For a synchronous replica that was SYNCHRONIZED at the time of the primary failure, no
// data is actually lost despite the command name.
//
// State machine:
//  1. Primary pod is Ready           → clear PrimaryNotReadySince, return.
//  2. Primary pod is NotReady        → record PrimaryNotReadySince (first observation), requeue.
//  3. NotReady for < threshold       → wait, requeue with remaining time.
//  4. NotReady for >= threshold      → select best synchronous target, issue FORCE_FAILOVER,
//     update status.PrimaryReplica and clear PrimaryNotReadySince.
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
		// The primary pod is K8s-Ready, but it may have restarted after being killed and
		// come back as SECONDARY/RESOLVING, leaving the AG with no active primary ("headless
		// AG"). forceFailoverHeadlessAG detects this via SQL and issues an immediate force
		// failover without waiting for the NotReady timer.
		if result, hErr := r.forceFailoverHeadlessAG(ctx, ag, primaryPodName, patch); hErr != nil || result != (ctrl.Result{}) {
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

	// Primary is NotReady (or missing).
	threshold := time.Duration(af.FailoverThresholdSeconds) * time.Second
	if threshold < 10*time.Second {
		threshold = 30 * time.Second
	}
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
	if _, foErr := exec.ExecSQL(ctx, ag.Namespace, targetPod, "mssql", saPassword,
		sqlutil.ForcedFailoverSQL(ag.Spec.AGName)); foErr != nil {
		log.Error(foErr, "Forced failover command failed; will retry", "target", targetPod)
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
