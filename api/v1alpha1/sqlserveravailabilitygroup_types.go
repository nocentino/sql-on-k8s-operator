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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AvailabilityMode specifies synchronous or asynchronous replication for an AG replica.
// +kubebuilder:validation:Enum=SynchronousCommit;AsynchronousCommit
type AvailabilityMode string

const (
	SynchronousCommit  AvailabilityMode = "SynchronousCommit"
	AsynchronousCommit AvailabilityMode = "AsynchronousCommit"
)

// FailoverMode specifies how failover is performed.
// +kubebuilder:validation:Enum=Automatic;Manual
type FailoverMode string

const (
	AutomaticFailover FailoverMode = "Automatic"
	ManualFailover    FailoverMode = "Manual"
)

// AGReplicaSpec defines the desired state of one AG replica.
type AGReplicaSpec struct {
	// Name is the hostname / pod name suffix for this replica (e.g. primary, secondary-1).
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// AvailabilityMode controls synchronous or asynchronous commit.
	// +kubebuilder:default=SynchronousCommit
	AvailabilityMode AvailabilityMode `json:"availabilityMode,omitempty"`

	// FailoverMode controls automatic or manual failover.
	// +kubebuilder:default=Automatic
	FailoverMode FailoverMode `json:"failoverMode,omitempty"`

	// ReadableSecondary enables secondary replicas to serve read queries.
	// +optional
	ReadableSecondary bool `json:"readableSecondary,omitempty"`
}

// ListenerSpec defines the AG listener configuration.
type ListenerSpec struct {
	// Name is the DNS name for the listener (used as the Kubernetes Service name).
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Port is the TCP port the listener serves on. Defaults to 1433.
	// +kubebuilder:default=1433
	Port int32 `json:"port,omitempty"`

	// ServiceType controls how the listener is exposed (ClusterIP or LoadBalancer).
	// +kubebuilder:validation:Enum=ClusterIP;LoadBalancer
	// +kubebuilder:default=ClusterIP
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`
}

// SQLServerAvailabilityGroupSpec defines the desired state of an Always On AG.
type SQLServerAvailabilityGroupSpec struct {
	// AGName is the name of the SQL Server Availability Group (T-SQL).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=128
	AGName string `json:"agName"`

	// Image is the SQL Server container image for all replicas.
	// +kubebuilder:validation:Required
	// +kubebuilder:default="mcr.microsoft.com/mssql/server:2022-latest"
	Image string `json:"image"`

	// Edition is the SQL Server edition.
	// +kubebuilder:validation:Enum=Developer;Express;Standard;Enterprise;EnterpriseCore
	// +kubebuilder:default=Developer
	Edition string `json:"edition,omitempty"`

	// SAPasswordSecretRef references the Secret containing the SA_PASSWORD key.
	// +kubebuilder:validation:Required
	SAPasswordSecretRef corev1.SecretKeySelector `json:"saPasswordSecretRef"`

	// Replicas defines the set of AG replicas (primary + secondaries).
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=9
	Replicas []AGReplicaSpec `json:"replicas"`

	// Storage defines PVC settings shared by all replica pods.
	// +kubebuilder:validation:Required
	Storage SQLServerStorageSpec `json:"storage"`

	// MSSQLConf contains mssql.conf settings applied to all replicas.
	// hadr.hadrenabled is automatically set to 1.
	// +optional
	MSSQLConf map[string]string `json:"mssqlConf,omitempty"`

	// EndpointPort is the TCP port for the AG mirroring endpoint. Defaults to 5022.
	// +kubebuilder:default=5022
	// +optional
	EndpointPort int32 `json:"endpointPort,omitempty"`

	// Listener configures the read-write AG listener Service.
	// The service selector is automatically updated to point only at the current primary replica.
	// +optional
	Listener *ListenerSpec `json:"listener,omitempty"`

	// ReadOnlyListener configures a separate read-only Service for readable secondary replicas.
	// The selector targets only pods that are currently in the SECONDARY role and have
	// readableSecondary: true in their replica spec.
	// ClientIP session affinity keeps each client connection pinned to the same replica
	// for the lifetime of the connection.
	// +optional
	ReadOnlyListener *ListenerSpec `json:"readOnlyListener,omitempty"`

	// Resources defines CPU and memory requests/limits for each replica.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// AcceptEULA must be "Y" to accept the SQL Server EULA.
	// +kubebuilder:validation:Enum=Y;y
	// +kubebuilder:default=Y
	AcceptEULA string `json:"acceptEula,omitempty"`

	// NodeSelector constrains all replica pods to nodes matching these labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for all replica pods.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// AGReplicaStatus captures the observed state of a single replica.
type AGReplicaStatus struct {
	// Name is the replica pod name.
	Name string `json:"name"`

	// Role is the current AG role (PRIMARY, SECONDARY, RESOLVING).
	Role string `json:"role,omitempty"`

	// SynchronizationState reflects the current data synchronization state.
	SynchronizationState string `json:"synchronizationState,omitempty"`

	// Connected indicates whether this replica is connected to the primary.
	Connected bool `json:"connected,omitempty"`

	// LastSeenPrimary is the timestamp when this replica last saw a healthy primary.
	// +optional
	LastSeenPrimary *metav1.Time `json:"lastSeenPrimary,omitempty"`
}

// SQLServerAvailabilityGroupStatus defines the observed state of the AG.
type SQLServerAvailabilityGroupStatus struct {
	// Phase summarizes the overall lifecycle phase.
	// +optional
	Phase string `json:"phase,omitempty"`

	// PrimaryReplica is the name of the current primary replica pod.
	// +optional
	PrimaryReplica string `json:"primaryReplica,omitempty"`

	// InitializationComplete indicates whether the AG T-SQL bootstrap has completed.
	// +optional
	InitializationComplete bool `json:"initializationComplete,omitempty"`

	// ReplicaStatuses holds per-replica observed state.
	// +optional
	ReplicaStatuses []AGReplicaStatus `json:"replicaStatuses,omitempty"`

	// Conditions represent the current state of the AG.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// AG phase constants
const (
	AGPhaseInitializing = "Initializing"
	AGPhaseRunning      = "Running"
	AGPhaseFailed       = "Failed"
	AGPhaseFailingOver  = "FailingOver"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sqlag
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=".status.primaryReplica"
// +kubebuilder:printcolumn:name="Init",type=boolean,JSONPath=".status.initializationComplete"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// SQLServerAvailabilityGroup is the Schema for the sqlserveravailabilitygroups API.
// It manages an Always On Availability Group across multiple SQL Server replicas.
type SQLServerAvailabilityGroup struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec SQLServerAvailabilityGroupSpec `json:"spec"`

	// +optional
	Status SQLServerAvailabilityGroupStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SQLServerAvailabilityGroupList contains a list of SQLServerAvailabilityGroup
type SQLServerAvailabilityGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SQLServerAvailabilityGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SQLServerAvailabilityGroup{}, &SQLServerAvailabilityGroupList{})
}
