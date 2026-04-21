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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
)

var sqlserverinstancelog = logf.Log.WithName("sqlserverinstance-resource")

// SetupSQLServerInstanceWebhookWithManager registers the validating webhook for
// SQLServerInstance with the controller-runtime manager. Immutable-field
// rejection prevents silent drift that the reconciler cannot recover from.
func SetupSQLServerInstanceWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &sqlv1alpha1.SQLServerInstance{}).
		WithValidator(&SQLServerInstanceCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-sql-mssql-microsoft-com-v1alpha1-sqlserverinstance,mutating=false,failurePolicy=fail,sideEffects=None,groups=sql.mssql.microsoft.com,resources=sqlserverinstances,verbs=create;update,versions=v1alpha1,name=vsqlserverinstance-v1alpha1.kb.io,admissionReviewVersions=v1

// SQLServerInstanceCustomValidator enforces admission-time invariants for the
// SQLServerInstance CRD. CRD OpenAPI schema covers required fields and basic
// patterns; this webhook covers cross-field and immutable-field rules that
// cannot be expressed in the schema.
type SQLServerInstanceCustomValidator struct{}

var sqlInstanceGK = schema.GroupKind{Group: "sql.mssql.microsoft.com", Kind: "SQLServerInstance"}

// ValidateCreate runs the shared spec checks at admission time.
func (v *SQLServerInstanceCustomValidator) ValidateCreate(_ context.Context, obj *sqlv1alpha1.SQLServerInstance) (admission.Warnings, error) {
	sqlserverinstancelog.Info("Validate create", "name", obj.GetName())
	errs := validateInstanceSpec(obj)
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(sqlInstanceGK, obj.Name, errs)
}

// ValidateUpdate rejects mutations to fields that the controller cannot safely
// reconcile on a live instance (e.g., storage size or class changes require
// PVC recreation; edition changes require a SQL Server reinstall).
func (v *SQLServerInstanceCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *sqlv1alpha1.SQLServerInstance) (admission.Warnings, error) {
	sqlserverinstancelog.Info("Validate update", "name", newObj.GetName())
	errs := validateInstanceSpec(newObj)

	oldS := oldObj.Spec
	newS := newObj.Spec
	if oldS.Edition != newS.Edition {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "edition"),
			"edition is immutable after creation; recreate the SQLServerInstance to change it"))
	}
	if !storageClassEqual(oldS.Storage.StorageClassName, newS.Storage.StorageClassName) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "storage", "storageClassName"),
			"storageClassName is immutable after creation"))
	}
	if !oldS.Storage.DataVolumeSize.Equal(newS.Storage.DataVolumeSize) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "storage", "dataVolumeSize"),
			"dataVolumeSize is immutable after creation; PVC expansion is not yet supported"))
	}
	if !accessModesEqual(oldS.Storage.AccessModes, newS.Storage.AccessModes) {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "storage", "accessModes"),
			"accessModes is immutable after creation"))
	}
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(sqlInstanceGK, newObj.Name, errs)
}

// ValidateDelete is a no-op; controller finalizers handle cleanup.
func (v *SQLServerInstanceCustomValidator) ValidateDelete(_ context.Context, _ *sqlv1alpha1.SQLServerInstance) (admission.Warnings, error) {
	return nil, nil
}

func validateInstanceSpec(obj *sqlv1alpha1.SQLServerInstance) field.ErrorList {
	var errs field.ErrorList
	if obj.Spec.Port != 0 && (obj.Spec.Port < 1 || obj.Spec.Port > 65535) {
		errs = append(errs, field.Invalid(field.NewPath("spec", "port"), obj.Spec.Port,
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

func storageClassEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func accessModesEqual(a, b []corev1.PersistentVolumeAccessMode) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[corev1.PersistentVolumeAccessMode]struct{}, len(a))
	for _, m := range a {
		set[m] = struct{}{}
	}
	for _, m := range b {
		if _, ok := set[m]; !ok {
			return false
		}
	}
	return true
}
