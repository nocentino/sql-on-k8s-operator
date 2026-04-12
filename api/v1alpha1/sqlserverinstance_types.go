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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SQLServerInstanceSpec defines the desired state of a standalone SQL Server instance.
type SQLServerInstanceSpec struct {
	// Image is the SQL Server container image to use (e.g. mcr.microsoft.com/mssql/server:2022-latest).
	// +kubebuilder:validation:Required
	// +kubebuilder:default="mcr.microsoft.com/mssql/server:2022-latest"
	Image string `json:"image"`

	// Edition is the SQL Server edition (Developer, Express, Standard, Enterprise, EnterpriseCore).
	// +kubebuilder:validation:Enum=Developer;Express;Standard;Enterprise;EnterpriseCore
	// +kubebuilder:default=Developer
	Edition string `json:"edition,omitempty"`

	// SAPasswordSecretRef references a Kubernetes Secret containing the SA_PASSWORD key.
	// +kubebuilder:validation:Required
	SAPasswordSecretRef corev1.SecretKeySelector `json:"saPasswordSecretRef"`

	// Storage defines the persistent volume configuration for SQL Server data files.
	// +kubebuilder:validation:Required
	Storage SQLServerStorageSpec `json:"storage"`

	// MSSQLConf contains key-value pairs written to /var/opt/mssql/mssql.conf.
	// +optional
	MSSQLConf map[string]string `json:"mssqlConf,omitempty"`

	// Resources defines CPU and memory requests/limits for the SQL Server container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Port is the TCP port SQL Server listens on. Defaults to 1433.
	// +kubebuilder:default=1433
	// +optional
	Port int32 `json:"port,omitempty"`

	// ServiceType controls how the client-facing Service is exposed.
	// ClusterIP (default) exposes the instance inside the cluster only.
	// NodePort exposes it on each node's IP at a static port.
	// LoadBalancer provisions an external load balancer (cloud providers).
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	// +optional
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// AcceptEULA indicates acceptance of the SQL Server EULA (must be "Y").
	// +kubebuilder:validation:Enum=Y;y
	// +kubebuilder:default=Y
	AcceptEULA string `json:"acceptEula,omitempty"`

	// Timezone sets the TZ environment variable for the container.
	// +optional
	Timezone string `json:"timezone,omitempty"`

	// AdditionalEnvVars are extra environment variables injected into the SQL Server pod.
	// +optional
	AdditionalEnvVars []corev1.EnvVar `json:"additionalEnvVars,omitempty"`

	// NodeSelector constrains scheduling to nodes matching these labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow the pod to be scheduled on tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// SQLServerStorageSpec defines how persistent storage is provisioned for SQL Server.
type SQLServerStorageSpec struct {
	// StorageClassName is the name of the StorageClass to use (e.g. standard, managed-csi).
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// DataVolumeSize is the size of the PVC for SQL Server data and log files.
	// +kubebuilder:default="10Gi"
	DataVolumeSize resource.Quantity `json:"dataVolumeSize,omitempty"`

	// AccessModes defines the access mode for the PVC.
	// +kubebuilder:default={"ReadWriteOnce"}
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// SQLServerInstanceStatus defines the observed state of SQLServerInstance.
type SQLServerInstanceStatus struct {
	// ReadyReplicas is the number of SQL Server pods that are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Phase summarizes the lifecycle phase of this instance (Pending, Running, Failed, Upgrading).
	// +optional
	Phase string `json:"phase,omitempty"`

	// CurrentImage is the image currently running on the StatefulSet pods.
	// +optional
	CurrentImage string `json:"currentImage,omitempty"`

	// ServiceName is the name of the headless Service for the StatefulSet.
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// Conditions represent the current state of the SQLServerInstance resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition type constants
const (
	ConditionAvailable   = "Available"
	ConditionProgressing = "Progressing"
	ConditionDegraded    = "Degraded"

	PhaseRunning   = "Running"
	PhasePending   = "Pending"
	PhaseFailed    = "Failed"
	PhaseUpgrading = "Upgrading"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sqli
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=".status.currentImage"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// SQLServerInstance is the Schema for the sqlserverinstances API.
// It manages the full lifecycle of a standalone SQL Server on Linux deployment.
type SQLServerInstance struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec SQLServerInstanceSpec `json:"spec"`

	// +optional
	Status SQLServerInstanceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SQLServerInstanceList contains a list of SQLServerInstance
type SQLServerInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SQLServerInstance `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SQLServerInstance{}, &SQLServerInstanceList{})
}
