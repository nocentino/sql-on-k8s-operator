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
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

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

// Label keys used to tag AG pod roles so Service selectors can dynamically route traffic.
const (
	// labelAGRole is set on every AG pod to reflect its current SQL Server AG role.
	// Values: "primary", "readable-secondary", "secondary".
	labelAGRole = "sql.mssql.microsoft.com/ag-role"
	// agRolePrimary is the SQL Server AG role string returned by DMV queries for the primary replica.
	agRolePrimary = "PRIMARY"
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

	// Step 5: CREATE AVAILABILITY GROUP on pod-0 (primary).
	// PodName must match @@SERVERNAME (short hostname); EndpointFQDN is used in ENDPOINT_URL.
	primaryPod := fmt.Sprintf("%s-0", ag.Name)
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
		createSQL := sqlutil.CreateAGSQL(ag.Spec.AGName, "NONE", replicaInputs, endpointPort)
		log.Info("Creating Availability Group", "ag", ag.Spec.AGName)
		if _, createErr := exec.ExecSQL(ctx, ag.Namespace, primaryPod, "mssql", saPassword, createSQL); createErr != nil {
			return ctrl.Result{}, fmt.Errorf("could not create AG on primary: %w", createErr)
		}
		// Wait for the primary's HADR transport to initialise connection sessions
		// before secondaries join.  Without this pause the JOIN races the primary's
		// initial connection attempt and the replicas end up permanently DISCONNECTED
		// (observed on Docker Desktop ARM64 with SQL Server 2022).
		log.Info("Waiting for HADR transport to initialise before joining secondaries")
		time.Sleep(10 * time.Second)
	} else {
		log.Info("Availability Group already exists, skipping CREATE", "ag", ag.Spec.AGName)
	}

	// Step 6: JOIN on each secondary.
	for i := 1; i < len(replicas); i++ {
		secondaryPod := fmt.Sprintf("%s-%d", ag.Name, i)
		if _, err := exec.ExecSQL(ctx, ag.Namespace, secondaryPod, "mssql", saPassword,
			sqlutil.JoinAGSQL(ag.Spec.AGName, "NONE")); err != nil {
			log.Error(err, "Could not join AG on secondary", "pod", secondaryPod)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	log.Info("AG bootstrap complete", "ag", ag.Spec.AGName)
	patch := client.MergeFrom(ag.DeepCopy())
	ag.Status.InitializationComplete = true
	ag.Status.Phase = sqlv1alpha1.AGPhaseRunning
	ag.Status.PrimaryReplica = primaryPod
	return ctrl.Result{}, r.Status().Patch(ctx, ag, patch)
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
	for i := range ag.Spec.Replicas {
		podName := fmt.Sprintf("%s-%d", ag.Name, i)
		role, _ := exec.GetAGRole(ctx, ag.Namespace, podName, "mssql", saPassword, ag.Spec.AGName)
		syncState, _ := exec.GetAGSyncState(ctx, ag.Namespace, podName, "mssql", saPassword, ag.Spec.AGName)
		replicaRoles[podName] = role
		if role == agRolePrimary {
			ag.Status.PrimaryReplica = podName
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

// SetupWithManager sets up the AG controller with the Manager.
func (r *SQLServerAvailabilityGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// GenerationChangedPredicate suppresses status-only update events.
	// Status subresource patches never increment metadata.generation, so this
	// predicate breaks the status-patch → watch-event → reconcile feedback loop
	// that would otherwise cause ~1 Hz runaway reconciliation.
	genChanged := predicate.GenerationChangedPredicate{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&sqlv1alpha1.SQLServerAvailabilityGroup{}, builder.WithPredicates(genChanged)).
		Owns(&appsv1.StatefulSet{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.Service{}, builder.WithPredicates(genChanged)).
		Owns(&corev1.ConfigMap{}, builder.WithPredicates(genChanged)).
		Named("sqlserveravailabilitygroup").
		Complete(r)
}
