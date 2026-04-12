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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
)

// SQLServerInstanceReconciler reconciles a SQLServerInstance object.
type SQLServerInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=sql.mssql.microsoft.com,resources=sqlserverinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sql.mssql.microsoft.com,resources=sqlserverinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sql.mssql.microsoft.com,resources=sqlserverinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create

// Reconcile drives the desired state of a SQLServerInstance.
func (r *SQLServerInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the SQLServerInstance resource
	instance := &sqlv1alpha1.SQLServerInstance{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling SQLServerInstance", "name", instance.Name)

	// --- 1. Reconcile ConfigMap (mssql.conf) ---
	if err := r.reconcileConfigMap(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile ConfigMap: %w", err)
	}

	// --- 2. Reconcile StatefulSet ---
	if err := r.reconcileStatefulSet(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile StatefulSet: %w", err)
	}

	// --- 3. Reconcile Services ---
	if err := r.reconcileServices(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile Services: %w", err)
	}

	// --- 4. Reconcile PV reclaim policy ---
	if err := r.reconcilePVCReclaimPolicy(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not reconcile PV reclaim policy: %w", err)
	}

	// --- 5. Update status ---
	if err := r.updateStatus(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("could not update status: %w", err)
	}

	return ctrl.Result{}, nil
}

// reconcileConfigMap ensures the mssql.conf ConfigMap exists and is up to date.
func (r *SQLServerInstanceReconciler) reconcileConfigMap(ctx context.Context, instance *sqlv1alpha1.SQLServerInstance) error {
	cm := &corev1.ConfigMap{}
	cmName := instance.Name + "-mssql-conf"
	err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: instance.Namespace}, cm)

	desired := buildMSSQLConf(instance.Spec.MSSQLConf)
	if errors.IsNotFound(err) {
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: instance.Namespace,
			},
			Data: map[string]string{"mssql.conf": desired},
		}
		if err := controllerutil.SetControllerReference(instance, newCM, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, newCM)
	}
	if err != nil {
		return err
	}

	cm.Data = map[string]string{"mssql.conf": desired}
	return r.Update(ctx, cm)
}

// reconcileStatefulSet ensures the StatefulSet for the SQL Server instance exists and matches spec.
func (r *SQLServerInstanceReconciler) reconcileStatefulSet(ctx context.Context, instance *sqlv1alpha1.SQLServerInstance) error {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, sts)

	desired := r.buildStatefulSet(instance)
	if errors.IsNotFound(err) {
		if err2 := controllerutil.SetControllerReference(instance, desired, r.Scheme); err2 != nil {
			return err2
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update image for rolling upgrades and resource changes
	sts.Spec.Template.Spec.Containers[0].Image = instance.Spec.Image
	sts.Spec.Template.Spec.Containers[0].Resources = instance.Spec.Resources
	return r.Update(ctx, sts)
}

// reconcileServices ensures the headless Service (for StatefulSet DNS) and
// an optional ClusterIP service exist.
func (r *SQLServerInstanceReconciler) reconcileServices(ctx context.Context, instance *sqlv1alpha1.SQLServerInstance) error {
	// Headless service for StatefulSet stable DNS
	headless := &corev1.Service{}
	headlessName := instance.Name + "-headless"
	err := r.Get(ctx, types.NamespacedName{Name: headlessName, Namespace: instance.Namespace}, headless)
	if errors.IsNotFound(err) {
		svc := r.buildHeadlessService(instance)
		if err2 := controllerutil.SetControllerReference(instance, svc, r.Scheme); err2 != nil {
			return err2
		}
		if err2 := r.Create(ctx, svc); err2 != nil {
			return err2
		}
	} else if err != nil {
		return err
	}

	// Client-facing service — create if missing, update type if it has drifted.
	clientSvc := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, clientSvc)
	if errors.IsNotFound(err) {
		svc := r.buildClusterIPService(instance)
		if err2 := controllerutil.SetControllerReference(instance, svc, r.Scheme); err2 != nil {
			return err2
		}
		return r.Create(ctx, svc)
	}
	if err != nil {
		return err
	}
	desiredType := instance.Spec.ServiceType
	if desiredType == "" {
		desiredType = corev1.ServiceTypeClusterIP
	}
	if clientSvc.Spec.Type != desiredType {
		clientSvc.Spec.Type = desiredType
		if err2 := r.Update(ctx, clientSvc); err2 != nil {
			return err2
		}
	}
	return nil
}

// updateStatus refreshes the SQLServerInstance status subresource.
func (r *SQLServerInstanceReconciler) updateStatus(ctx context.Context, instance *sqlv1alpha1.SQLServerInstance) error {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, sts); err != nil {
		return err
	}

	patch := client.MergeFrom(instance.DeepCopy())
	instance.Status.ReadyReplicas = sts.Status.ReadyReplicas
	instance.Status.CurrentImage = instance.Spec.Image
	instance.Status.ServiceName = instance.Name + "-headless"

	if sts.Status.ReadyReplicas > 0 {
		instance.Status.Phase = sqlv1alpha1.PhaseRunning
		setCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               sqlv1alpha1.ConditionAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             "Running",
			Message:            "SQL Server is ready",
			LastTransitionTime: metav1.Now(),
		})
	} else {
		instance.Status.Phase = sqlv1alpha1.PhasePending
		setCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               sqlv1alpha1.ConditionAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             "NotReady",
			Message:            "Waiting for SQL Server pod to become ready",
			LastTransitionTime: metav1.Now(),
		})
	}

	return r.Status().Patch(ctx, instance, patch)
}

// buildStatefulSet constructs the desired StatefulSet for a SQLServerInstance.
func (r *SQLServerInstanceReconciler) buildStatefulSet(instance *sqlv1alpha1.SQLServerInstance) *appsv1.StatefulSet {
	labels := map[string]string{
		"app":                          instance.Name,
		"app.kubernetes.io/name":       "sqlserverinstance",
		"app.kubernetes.io/instance":   instance.Name,
		"app.kubernetes.io/managed-by": "sql-on-k8s-operator",
	}
	port := instance.Spec.Port
	if port == 0 {
		port = 1433
	}
	edition := instance.Spec.Edition
	if edition == "" {
		edition = "Developer"
	}
	acceptEULA := instance.Spec.AcceptEULA
	if acceptEULA == "" {
		acceptEULA = "Y"
	}

	// Build environment variables.
	// Both SA_PASSWORD (legacy) and MSSQL_SA_PASSWORD (current) are set from the
	// same secret so the image works regardless of SQL Server version. Microsoft
	// deprecated SA_PASSWORD starting with SQL Server 2019 CU; MSSQL_SA_PASSWORD
	// is the preferred name going forward.
	envVars := []corev1.EnvVar{
		{Name: "ACCEPT_EULA", Value: acceptEULA},
		{Name: "MSSQL_PID", Value: edition},
		{
			Name: "SA_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &instance.Spec.SAPasswordSecretRef,
			},
		},
		{
			Name: "MSSQL_SA_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &instance.Spec.SAPasswordSecretRef,
			},
		},
		{Name: "MSSQL_TCP_PORT", Value: fmt.Sprintf("%d", port)},
	}
	if instance.Spec.Timezone != "" {
		envVars = append(envVars, corev1.EnvVar{Name: "TZ", Value: instance.Spec.Timezone})
	}
	envVars = append(envVars, instance.Spec.AdditionalEnvVars...)

	// Storage access modes
	accessModes := instance.Spec.Storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	dataSize := instance.Spec.Storage.DataVolumeSize
	if dataSize.IsZero() {
		dataSize = resource.MustParse("10Gi")
	}

	one := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &one,
			ServiceName:         instance.Name + "-headless",
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
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
					NodeSelector: instance.Spec.NodeSelector,
					Tolerations:  instance.Spec.Tolerations,
					Containers: []corev1.Container{
						{
							Name:      "mssql",
							Image:     instance.Spec.Image,
							Env:       envVars,
							Resources: instance.Spec.Resources,
							Ports: []corev1.ContainerPort{
								{Name: "mssql", ContainerPort: port, Protocol: corev1.ProtocolTCP},
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
											`/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" -C -b 2>&1 | grep -q "1"`,
										},
									},
								},
								InitialDelaySeconds: 60,
								PeriodSeconds:       30,
								FailureThreshold:    5,
								TimeoutSeconds:      10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/bash", "-c",
											`/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" -C -b 2>&1 | grep -q "1"`,
										},
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
								FailureThreshold:    3,
								TimeoutSeconds:      5,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "mssql-conf",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: instance.Name + "-mssql-conf",
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
						StorageClassName: instance.Spec.Storage.StorageClassName,
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
	return sts
}

// buildHeadlessService creates the headless Service used by the StatefulSet for stable DNS names.
func (r *SQLServerInstanceReconciler) buildHeadlessService(instance *sqlv1alpha1.SQLServerInstance) *corev1.Service {
	labels := map[string]string{"app": instance.Name}
	port := instance.Spec.Port
	if port == 0 {
		port = 1433
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name + "-headless",
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{Name: "mssql", Port: port, TargetPort: intstr.FromInt32(port)},
			},
		},
	}
}

// buildClientService creates the client-facing Service for SQL Server access.
// The service type defaults to ClusterIP but respects spec.serviceType when set.
func (r *SQLServerInstanceReconciler) buildClusterIPService(instance *sqlv1alpha1.SQLServerInstance) *corev1.Service {
	labels := map[string]string{"app": instance.Name}
	port := instance.Spec.Port
	if port == 0 {
		port = 1433
	}
	svcType := instance.Spec.ServiceType
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{Name: "mssql", Port: port, TargetPort: intstr.FromInt32(port)},
			},
		},
	}
}

// buildMSSQLConf generates an mssql.conf INI file from a key-value map.
// Keys are in "section.key" format (e.g., "memory.memorylimitmb").
// Each unique [section] is emitted once with all its key=value pairs.
func buildMSSQLConf(conf map[string]string) string {
	sections := map[string][]string{}
	var toplevel []string

	for k, v := range conf {
		// Split on first dot to separate section from key
		idx := -1
		for i, c := range k {
			if c == '.' {
				idx = i
				break
			}
		}
		if idx < 0 {
			toplevel = append(toplevel, fmt.Sprintf("%s = %s", k, v))
			continue
		}
		section := k[:idx]
		key := k[idx+1:]
		sections[section] = append(sections[section], fmt.Sprintf("%s = %s", key, v))
	}

	var result strings.Builder
	for _, line := range toplevel {
		result.WriteString(line + "\n")
	}
	for section, entries := range sections {
		result.WriteString(fmt.Sprintf("[%s]\n", section))
		for _, e := range entries {
			result.WriteString(e + "\n")
		}
		result.WriteString("\n")
	}
	return result.String()
}

// setCondition updates or appends a condition in a condition list.
func setCondition(conditions *[]metav1.Condition, cond metav1.Condition) {
	for i, c := range *conditions {
		if c.Type == cond.Type {
			(*conditions)[i] = cond
			return
		}
	}
	*conditions = append(*conditions, cond)
}

// reconcilePVCReclaimPolicy finds every PVC owned by the instance's StatefulSet and
// patches the bound PersistentVolume's reclaimPolicy to match spec.storage.reclaimPolicy.
// This is performed on every reconcile so drift (e.g. a StorageClass default overriding
// the desired policy) is continuously corrected.
func (r *SQLServerInstanceReconciler) reconcilePVCReclaimPolicy(ctx context.Context, instance *sqlv1alpha1.SQLServerInstance) error {
	log := logf.FromContext(ctx)
	desired := instance.Spec.Storage.ReclaimPolicy
	if desired == "" {
		desired = corev1.PersistentVolumeReclaimRetain
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList,
		client.InNamespace(instance.Namespace),
		client.MatchingLabels{"app": instance.Name},
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

// SetupWithManager sets up the controller with the Manager.
func (r *SQLServerInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sqlv1alpha1.SQLServerInstance{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Named("sqlserverinstance").
		Complete(r)
}
