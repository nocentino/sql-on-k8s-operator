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

var _ = Describe("SQLServerAvailabilityGroup Webhook", func() {
	var (
		obj       *sqlv1alpha1.SQLServerAvailabilityGroup
		oldObj    *sqlv1alpha1.SQLServerAvailabilityGroup
		validator SQLServerAvailabilityGroupCustomValidator
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		validator = SQLServerAvailabilityGroupCustomValidator{}
		obj = &sqlv1alpha1.SQLServerAvailabilityGroup{
			Spec: sqlv1alpha1.SQLServerAvailabilityGroupSpec{
				AGName:      "AG1",
				Image:       "mcr.microsoft.com/mssql/server:2022-latest",
				Edition:     "Developer",
				ClusterType: sqlv1alpha1.AGClusterTypeExternal,
				SAPasswordSecretRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "sa"},
					Key:                  "SA_PASSWORD",
				},
				Replicas: []sqlv1alpha1.AGReplicaSpec{
					{Name: "r0"}, {Name: "r1"}, {Name: "r2"},
				},
				Storage: sqlv1alpha1.SQLServerStorageSpec{
					DataVolumeSize: resource.MustParse("10Gi"),
				},
			},
		}
		oldObj = obj.DeepCopy()
	})

	Context("ValidateCreate", func() {
		It("accepts a valid AG", func() {
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
		It("rejects invalid agName", func() {
			obj.Spec.AGName = "1-bad-name"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects empty replicas", func() {
			obj.Spec.Replicas = nil
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects low failover threshold", func() {
			obj.Spec.AutomaticFailover = &sqlv1alpha1.AutomaticFailoverSpec{
				Enabled:                  true,
				FailoverThresholdSeconds: 5,
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("ValidateUpdate", func() {
		It("rejects agName change", func() {
			obj.Spec.AGName = "AG2"
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects clusterType change", func() {
			obj.Spec.ClusterType = sqlv1alpha1.AGClusterTypeNone
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
		})
		It("rejects replica removal", func() {
			obj.Spec.Replicas = obj.Spec.Replicas[:2]
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
		})
		It("accepts identical update", func() {
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
