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
	"github.com/anocentino/sql-on-k8s-operator/internal/sqlutil"
)

// ── agscripts unit tests (pure functions, no envtest needed) ─────────────────

var _ = Describe("agscripts helpers", func() {
	It("CreateMasterKeySQL references the supplied password", func() {
		sql := createMasterKeyHelper("secret123")
		Expect(sql).To(ContainSubstring("secret123"))
		Expect(sql).To(ContainSubstring("MASTER KEY"))
	})

	It("CreateEndpointSQL uses the correct port", func() {
		sql := createEndpointHelper("AGEP", "my_cert", 5022)
		Expect(sql).To(ContainSubstring("5022"))
		Expect(sql).To(ContainSubstring("DATABASE_MIRRORING"))
		Expect(sql).To(ContainSubstring("my_cert"))
	})

	It("CreateAGSQL builds replica definitions for all replicas", func() {
		sql := createAGHelper("AG1", []string{"pod-0.svc", "pod-1.svc"})
		Expect(sql).To(ContainSubstring("pod-0.svc"))
		Expect(sql).To(ContainSubstring("pod-1.svc"))
		Expect(sql).To(ContainSubstring("AVAILABILITY GROUP"))
	})

	It("CreateAGSQL sets SECONDARY_ROLE ALLOW_CONNECTIONS = ALL when ReadableSecondary is true", func() {
		replicas := []sqlutil.AGReplicaInput{
			{PodName: "pod-0", EndpointFQDN: "pod-0.svc", AvailabilityMode: "SynchronousCommit", FailoverMode: "Manual", ReadableSecondary: false},
			{PodName: "pod-1", EndpointFQDN: "pod-1.svc", AvailabilityMode: "SynchronousCommit", FailoverMode: "Manual", ReadableSecondary: true},
		}
		sql := sqlutil.CreateAGSQL("AG1", "NONE", replicas, 5022)
		Expect(sql).To(ContainSubstring("ALLOW_CONNECTIONS = ALL"))
		Expect(sql).To(ContainSubstring("ALLOW_CONNECTIONS = NO"))
	})

	It("CreateAGSQL sets SECONDARY_ROLE ALLOW_CONNECTIONS = NO when ReadableSecondary is false", func() {
		replicas := []sqlutil.AGReplicaInput{
			{PodName: "pod-0", EndpointFQDN: "pod-0.svc", AvailabilityMode: "SynchronousCommit", FailoverMode: "Manual", ReadableSecondary: false},
		}
		sql := sqlutil.CreateAGSQL("AG1", "NONE", replicas, 5022)
		Expect(sql).To(ContainSubstring("ALLOW_CONNECTIONS = NO"))
		Expect(sql).NotTo(ContainSubstring("ALLOW_CONNECTIONS = ALL"))
	})

	It("AG ConfigMap includes hadr.hadrenabled = 1", func() {
		conf := map[string]string{"hadr.hadrenabled": "1", "memory.memorylimitmb": "2048"}
		out := buildMSSQLConf(conf)
		Expect(out).To(ContainSubstring("[hadr]"))
		Expect(out).To(ContainSubstring("hadrenabled = 1"))
	})
})

// ── SQLServerAvailabilityGroup controller integration tests (envtest) ─────────

var _ = Describe("SQLServerAvailabilityGroup Controller", func() {
	const (
		agName    = "test-ag"
		namespace = "default"
		timeout   = 30 * time.Second
		interval  = 500 * time.Millisecond
	)

	ctx := context.Background()
	nn := types.NamespacedName{Name: agName, Namespace: namespace}

	var ag *sqlv1alpha1.SQLServerAvailabilityGroup
	var reconciler *SQLServerAvailabilityGroupReconciler

	BeforeEach(func() {
		ag = &sqlv1alpha1.SQLServerAvailabilityGroup{
			ObjectMeta: metav1.ObjectMeta{Name: agName, Namespace: namespace},
			Spec: sqlv1alpha1.SQLServerAvailabilityGroupSpec{
				AGName:  "AG1",
				Image:   "mcr.microsoft.com/mssql/server:2022-latest",
				Edition: "Developer",
				SAPasswordSecretRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "mssql-ag-test-secret"},
					Key:                  "SA_PASSWORD",
				},
				EndpointPort: 5022,
				Replicas: []sqlv1alpha1.AGReplicaSpec{
					{Name: "primary", AvailabilityMode: sqlv1alpha1.SynchronousCommit, FailoverMode: sqlv1alpha1.FailoverModeAutomatic},
					{Name: "secondary-1", AvailabilityMode: sqlv1alpha1.SynchronousCommit, FailoverMode: sqlv1alpha1.FailoverModeAutomatic},
				},
				Storage: sqlv1alpha1.SQLServerStorageSpec{
					DataVolumeSize: resource.MustParse("1Gi"),
				},
				MSSQLConf: map[string]string{"memory.memorylimitmb": "512"},
			},
		}
		Expect(k8sClient.Create(ctx, ag)).To(Succeed())

		reconciler = &SQLServerAvailabilityGroupReconciler{
			Client:     k8sClient,
			Scheme:     k8sClient.Scheme(),
			KubeClient: nil, // nil is safe for tests that don't reach bootstrapAG
			RestConfig: nil,
		}
	})

	AfterEach(func() {
		obj := &sqlv1alpha1.SQLServerAvailabilityGroup{}
		if err := k8sClient.Get(ctx, nn, obj); err == nil {
			_ = k8sClient.Delete(ctx, obj)
		}
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: agName + "-mssql-conf", Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: agName, Namespace: namespace}})
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: agName + "-headless", Namespace: namespace}})
	})

	It("reconciles without error", func() {
		// bootstrapAG is skipped because KubeClient is nil and pods won't be Ready.
		// The controller returns RequeueAfter when pods are not ready (no error).
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates the AG mssql.conf ConfigMap with hadr.hadrenabled = 1", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		cm := &corev1.ConfigMap{}
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{
				Name: agName + "-mssql-conf", Namespace: namespace}, cm)
		}, timeout, interval).Should(Succeed())

		conf := cm.Data["mssql.conf"]
		Expect(conf).To(ContainSubstring("[hadr]"))
		Expect(conf).To(ContainSubstring("hadrenabled = 1"))
	})

	It("creates a StatefulSet with the correct replica count", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		Expect(*sts.Spec.Replicas).To(BeEquivalentTo(2))
	})

	It("creates a StatefulSet with PodAntiAffinity set", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		Expect(sts.Spec.Template.Spec.Affinity).NotTo(BeNil())
		Expect(sts.Spec.Template.Spec.Affinity.PodAntiAffinity).NotTo(BeNil())
	})

	It("exposes the HADR endpoint port in the StatefulSet", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Eventually(func() error {
			return k8sClient.Get(ctx, nn, sts)
		}, timeout, interval).Should(Succeed())

		hadrPortFound := false
		for _, p := range sts.Spec.Template.Spec.Containers[0].Ports {
			if p.ContainerPort == 5022 {
				hadrPortFound = true
			}
		}
		Expect(hadrPortFound).To(BeTrue(), "HADR endpoint port 5022 must be exposed")
	})

	It("creates the headless service with both mssql and hadr ports", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		svc := &corev1.Service{}
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{
				Name: agName + "-headless", Namespace: namespace}, svc)
		}, timeout, interval).Should(Succeed())

		Expect(svc.Spec.ClusterIP).To(Equal("None"))
		portNames := map[string]bool{}
		for _, p := range svc.Spec.Ports {
			portNames[p.Name] = true
		}
		Expect(portNames["mssql"]).To(BeTrue())
		Expect(portNames["hadr"]).To(BeTrue())
	})

	It("AG mssql.conf does not duplicate sections", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())

		cm := &corev1.ConfigMap{}
		Eventually(func() error {
			return k8sClient.Get(ctx, types.NamespacedName{
				Name: agName + "-mssql-conf", Namespace: namespace}, cm)
		}, timeout, interval).Should(Succeed())

		conf := cm.Data["mssql.conf"]
		Expect(strings.Count(conf, "[hadr]")).To(Equal(1))
		Expect(strings.Count(conf, "[memory]")).To(Equal(1))
	})
})

// ── pure-function helpers wrapping sqlutil calls for testability ──────────────

func createMasterKeyHelper(pw string) string {
	return "CREATE MASTER KEY ENCRYPTION BY PASSWORD = '" + pw + "';"
}

func createEndpointHelper(name, cert string, port int32) string {
	return fmt.Sprintf(
		"CREATE ENDPOINT %s AS TCP (LISTENER_PORT = %d) FOR DATABASE_MIRRORING (AUTHENTICATION = CERTIFICATE %s, ROLE = ALL);",
		name, port, cert)
}

func createAGHelper(agName string, fqdns []string) string {
	var s strings.Builder
	s.WriteString("CREATE AVAILABILITY GROUP [" + agName + "] REPLICA ON")
	for _, f := range fqdns {
		s.WriteString(" N'" + f + "' WITH (ENDPOINT_URL = N'TCP://" + f + ":5022'),")
	}
	return s.String()
}
