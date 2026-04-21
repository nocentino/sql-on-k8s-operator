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
	"context"
	"regexp"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
)

var sqlserveravailabilitygrouplog = logf.Log.WithName("sqlserveravailabilitygroup-resource")

// agNameRegex matches valid SQL identifiers for the Availability Group name.
// The CRD schema enforces this pattern at the OpenAPI level; we re-check here
// for defense in depth and to short-circuit before reconciliation.
var agNameRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// SetupSQLServerAvailabilityGroupWebhookWithManager registers the validating
// webhook for SQLServerAvailabilityGroup. This guards against silent drift of
// fields the reconciler cannot safely change on a live AG (e.g., agName,
// clusterType, removing replicas).
func SetupSQLServerAvailabilityGroupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &sqlv1alpha1.SQLServerAvailabilityGroup{}).
		WithValidator(&SQLServerAvailabilityGroupCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-sql-mssql-microsoft-com-v1alpha1-sqlserveravailabilitygroup,mutating=false,failurePolicy=fail,sideEffects=None,groups=sql.mssql.microsoft.com,resources=sqlserveravailabilitygroups,verbs=create;update,versions=v1alpha1,name=vsqlserveravailabilitygroup-v1alpha1.kb.io,admissionReviewVersions=v1

// SQLServerAvailabilityGroupCustomValidator enforces admission-time invariants
// for the SQLServerAvailabilityGroup CRD.
type SQLServerAvailabilityGroupCustomValidator struct{}

var sqlAGGK = schema.GroupKind{Group: "sql.mssql.microsoft.com", Kind: "SQLServerAvailabilityGroup"}

// ValidateCreate runs cross-field checks not expressible in the CRD schema.
func (v *SQLServerAvailabilityGroupCustomValidator) ValidateCreate(_ context.Context, obj *sqlv1alpha1.SQLServerAvailabilityGroup) (admission.Warnings, error) {
	sqlserveravailabilitygrouplog.Info("Validate create", "name", obj.GetName())
	errs := validateAGSpec(obj)
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(sqlAGGK, obj.Name, errs)
}

// ValidateUpdate rejects mutations that the reconciler cannot safely apply to
// a running AG: renaming the AG, switching cluster types, changing storage
// topology, or removing replicas (which requires explicit scale-down handling).
func (v *SQLServerAvailabilityGroupCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *sqlv1alpha1.SQLServerAvailabilityGroup) (admission.Warnings, error) {
	sqlserveravailabilitygrouplog.Info("Validate update", "name", newObj.GetName())
	errs := validateAGSpec(newObj)

	oldS := oldObj.Spec
	newS := newObj.Spec
	if oldS.AGName != newS.AGName {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "agName"),
			"agName is immutable after creation"))
	}
	if oldS.ClusterType != newS.ClusterType {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "clusterType"),
			"clusterType is immutable after creation; switching NONE↔EXTERNAL on a live AG is unsupported"))
	}
	if oldS.Edition != newS.Edition {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "edition"),
			"edition is immutable after creation"))
	}
	if !storageClassEqual(oldS.Storage.StorageClassName, newS.Storage.StorageClassName) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "storage", "storageClassName"),
			"storageClassName is immutable after creation"))
	}
	if !oldS.Storage.DataVolumeSize.Equal(newS.Storage.DataVolumeSize) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "storage", "dataVolumeSize"),
			"dataVolumeSize is immutable after creation"))
	}
	if !accessModesEqual(oldS.Storage.AccessModes, newS.Storage.AccessModes) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "storage", "accessModes"),
			"accessModes is immutable after creation"))
	}
	// Replica removal needs explicit scale-down handling (drop from AG, remove
	// endpoint grants, optionally delete PVC). Reject via webhook so users do
	// not silently lose a replica the reconciler would leave orphaned.
	oldNames := make(map[string]struct{}, len(oldS.Replicas))
	for _, r := range oldS.Replicas {
		oldNames[r.Name] = struct{}{}
	}
	newNames := make(map[string]struct{}, len(newS.Replicas))
	for _, r := range newS.Replicas {
		newNames[r.Name] = struct{}{}
	}
	for name := range oldNames {
		if _, ok := newNames[name]; !ok {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "replicas"),
				"removing replicas is not supported; replica "+name+" must be left in place"))
		}
	}
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(sqlAGGK, newObj.Name, errs)
}

// ValidateDelete is a no-op; controller finalizers handle cleanup.
func (v *SQLServerAvailabilityGroupCustomValidator) ValidateDelete(_ context.Context, _ *sqlv1alpha1.SQLServerAvailabilityGroup) (admission.Warnings, error) {
	return nil, nil
}

func validateAGSpec(obj *sqlv1alpha1.SQLServerAvailabilityGroup) field.ErrorList {
	var errs field.ErrorList
	if !agNameRegex.MatchString(obj.Spec.AGName) {
		errs = append(errs, field.Invalid(field.NewPath("spec", "agName"), obj.Spec.AGName,
			"agName must match ^[A-Za-z_][A-Za-z0-9_]*$"))
	}
	if len(obj.Spec.Replicas) < 1 {
		errs = append(errs, field.Required(field.NewPath("spec", "replicas"),
			"at least one replica is required"))
	}
	if obj.Spec.AutomaticFailover != nil && obj.Spec.AutomaticFailover.FailoverThresholdSeconds != 0 &&
		obj.Spec.AutomaticFailover.FailoverThresholdSeconds < 10 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "automaticFailover", "failoverThresholdSeconds"),
			obj.Spec.AutomaticFailover.FailoverThresholdSeconds,
			"failoverThresholdSeconds must be >= 10 to avoid spurious failovers"))
	}
	for i, r := range obj.Spec.Replicas {
		if r.Name == "" {
			errs = append(errs, field.Required(
				field.NewPath("spec", "replicas").Index(i).Child("name"),
				"replica name is required"))
		}
	}
	if obj.Spec.Listener != nil && obj.Spec.Listener.Port != 0 &&
		(obj.Spec.Listener.Port < 1 || obj.Spec.Listener.Port > 65535) {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "listener", "port"), obj.Spec.Listener.Port,
			"port must be in range 1-65535"))
	}
	if obj.Spec.SAPasswordSecretRef.Name == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "saPasswordSecretRef", "name"),
			"saPasswordSecretRef.name is required"))
	}
	if obj.Spec.SAPasswordSecretRef.Key == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "saPasswordSecretRef", "key"),
			"saPasswordSecretRef.key is required"))
	}
	return errs
}
