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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
)

var _ = Describe("SQLServerInstance Webhook", func() {
	var (
		obj       *sqlv1alpha1.SQLServerInstance
		oldObj    *sqlv1alpha1.SQLServerInstance
		validator SQLServerInstanceCustomValidator
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		validator = SQLServerInstanceCustomValidator{}
		obj = &sqlv1alpha1.SQLServerInstance{
			Spec: sqlv1alpha1.SQLServerInstanceSpec{
				Image:   "mcr.microsoft.com/mssql/server:2022-latest",
				Edition: "Developer",
				SAPasswordSecretRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "sa"},
					Key:                  "SA_PASSWORD",
				},
				Storage: sqlv1alpha1.SQLServerStorageSpec{
					DataVolumeSize: resource.MustParse("10Gi"),
				},
				Port: 1433,
			},
		}
		oldObj = obj.DeepCopy()
	})

	Context("ValidateCreate", func() {
		It("accepts a valid instance", func() {
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
		It("rejects missing saPasswordSecretRef.name", func() {
			obj.Spec.SAPasswordSecretRef.Name = ""
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects out-of-range port", func() {
			obj.Spec.Port = 70000
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("ValidateUpdate", func() {
		It("rejects edition change", func() {
			obj.Spec.Edition = "Standard"
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects dataVolumeSize change", func() {
			obj.Spec.Storage.DataVolumeSize = resource.MustParse("20Gi")
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects storageClassName change", func() {
			sc := "fast-ssd"
			obj.Spec.Storage.StorageClassName = &sc
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
		})
		It("accepts identical update", func() {
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
