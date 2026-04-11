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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
)

// ── buildMSSQLConf unit tests (pure function, no envtest needed) ─────────────

var _ = Describe("buildMSSQLConf", func() {
	It("produces no duplicate sections when user supplies sqlagent key", func() {
		conf := map[string]string{
			"memory.memorylimitmb": "2048",
			"sqlagent.enabled":     "false",
		}
		out := buildMSSQLConf(conf)
		// Count occurrences of "[sqlagent]" — must be exactly 1
		Expect(strings.Count(out, "[sqlagent]")).To(Equal(1),
			"mssql.conf must not have duplicate [sqlagent] sections")
	})

	It("groups keys correctly under their sections", func() {
		conf := map[string]string{
			"memory.memorylimitmb": "4096",
			"network.tlsprotocols": "1.2",
		}
		out := buildMSSQLConf(conf)
		Expect(out).To(ContainSubstring("[memory]"))
		Expect(out).To(ContainSubstring("memorylimitmb = 4096"))
		Expect(out).To(ContainSubstring("[network]"))
		Expect(out).To(ContainSubstring("tlsprotocols = 1.2"))
	})

	It("handles top-level keys without a dot separator", func() {
		conf := map[string]string{"somekey": "somevalue"}
		out := buildMSSQLConf(conf)
		Expect(out).To(ContainSubstring("somekey = somevalue"))
		Expect(out).NotTo(ContainSubstring("[somekey]"))
	})

	It("returns empty string for empty config", func() {
		Expect(buildMSSQLConf(nil)).To(Equal(""))
		Expect(buildMSSQLConf(map[string]string{})).To(Equal(""))
	})
})

// ── SQLServerInstance controller integration tests (envtest) ─────────────────

var _ = Describe("SQLServerInstance Controller", func() {
	const (
		instanceName = "test-sqli"
		namespace    = "default"
		timeout      = 30 * time.Second
		interval     = 500 * time.Millisecond
	)

	ctx := context.Background()
	nn := types.NamespacedName{Name: instanceName, Namespace: namespace}

	var instance *sqlv1alpha1.SQLServerInstance
	var reconciler *SQLServerInstanceReconciler

	BeforeEach(func() {
		instance = &sqlv1alpha1.SQLServerInstance{
			ObjectMeta: metav1.ObjectMeta{Name: instanceName, Namespace: namespace},
			Spec: sqlv1alpha1.SQLServerInstanceSpec{
				Image:   "mcr.microsoft.com/mssql/server:2022-latest",
				Edition: "Developer",
				SAPasswordSecretRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "mssql-test-secret"},
					Key:                  "SA_PASSWORD",
				},
				Storage: sqlv1alpha1.SQLServerStorageSpec{
					DataVolumeSize: resource.MustParse("1Gi"),
				},
				MSSQLConf: map[string]string{
					"memory.memorylimitmb": "512",
				},
				Port: 1433,
			},
		}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())

		reconciler = &SQLServerInstanceReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
	})

	AfterEach(func() {
		// Clean up instance (ignore not-found in case test deleted it)
		si := &sqlv1alpha1.SQLServerInstance{}
		if err := k8sClient.Get(ctx, nn, si); err == nil {
			_ = k8sClient.Delete(ctx, si)
		}
		// Clean up dependent resources
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: instanceName + "-mssql-conf", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: instanceName, Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: instanceName, Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: instanceName + "-headless", Namespace: namespace}})
	})

	It("reconciles without error", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates the mssql.conf ConfigMap with correct INI content", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		cm := &corev1.ConfigMap{}
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{
				Name: instanceName + "-mssql-conf", Namespace: namespace}, cm)
		}, timeout, interval).Should(Succeed())

		conf := cm.Data["mssql.conf"]
		Expect(conf).To(ContainSubstring("[memory]"))
		Expect(conf).To(ContainSubstring("memorylimitmb = 512"))
		// Must NOT have duplicate sections
		Expect(strings.Count(conf, "[memory]")).To(Equal(1))
	})

	It("creates a StatefulSet with correct image and port", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
		c := sts.Spec.Template.Spec.Containers[0]
		Expect(c.Image).To(Equal("mcr.microsoft.com/mssql/server:2022-latest"))
		Expect(c.Ports).To(ContainElement(
			HaveField("ContainerPort", BeEquivalentTo(1433))))
	})

	It("creates a StatefulSet with liveness and readiness probes", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		c := sts.Spec.Template.Spec.Containers[0]
		Expect(c.LivenessProbe).NotTo(BeNil())
		Expect(c.ReadinessProbe).NotTo(BeNil())
		Expect(c.LivenessProbe.Exec).NotTo(BeNil())
		Expect(c.ReadinessProbe.Exec).NotTo(BeNil())
	})

	It("creates a StatefulSet with a PVC template for mssql-data", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))
		Expect(sts.Spec.VolumeClaimTemplates[0].Name).To(Equal("mssql-data"))
	})

	It("creates both the headless and ClusterIP services", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		headless := &corev1.Service{}
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{
				Name: instanceName + "-headless", Namespace: namespace}, headless)
		}, timeout, interval).Should(Succeed())
		Expect(headless.Spec.ClusterIP).To(Equal("None"))

		clusterSvc := &corev1.Service{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, clusterSvc)
		}, timeout, interval).Should(Succeed())
		Expect(clusterSvc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
	})

	It("sets SA_PASSWORD from the secret reference", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		c := sts.Spec.Template.Spec.Containers[0]
		var saEnv *corev1.EnvVar
		for i := range c.Env {
			if c.Env[i].Name == "SA_PASSWORD" {
				saEnv = &c.Env[i]
				break
			}
		}
		Expect(saEnv).NotTo(BeNil())
		Expect(saEnv.ValueFrom).NotTo(BeNil())
		Expect(saEnv.ValueFrom.SecretKeyRef.Name).To(Equal("mssql-test-secret"))
		Expect(saEnv.ValueFrom.SecretKeyRef.Key).To(Equal("SA_PASSWORD"))
	})

	It("sets the ACCEPT_EULA and MSSQL_PID env vars", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		envMap := map[string]string{}
		for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
			if e.Value != "" {
				envMap[e.Name] = e.Value
			}
		}
		Expect(envMap["ACCEPT_EULA"]).To(Equal("Y"))
		Expect(envMap["MSSQL_PID"]).To(Equal("Developer"))
	})

	It("is idempotent: reconciling twice does not error", func() {
		for i := range 2 {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred(), "reconcile %d should not error", i+1)
		}
	})
})
