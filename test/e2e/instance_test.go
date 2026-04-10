//go:build e2e
// +build e2e

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

package e2e

import (
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/anocentino/sql-on-k8s-operator/test/utils"
)

var _ = Describe("SQLServerInstance", Ordered, Label("instance"), func() {
	const instancePod = "mssql-e2e-0"

	AfterEach(func() { captureArtifacts("SQLServerInstance " + CurrentSpecReport().FullText()) })

	BeforeAll(func() {
		By("ensuring the SA password secret exists")
		cmd := exec.Command("kubectl", "create", "secret", "generic", instanceSecretName,
			"--from-literal=SA_PASSWORD="+sqlPassword,
			"-n", agNamespace, "--dry-run=client", "-o", "yaml")
		yaml, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		applyCmd := exec.Command("kubectl", "apply", "-f", "-")
		applyCmd.Stdin = stringReader(yaml)
		_, err = utils.Run(applyCmd)
		Expect(err).NotTo(HaveOccurred())

		By("applying the SQLServerInstance CR")
		f, err := os.CreateTemp("", "instance-cr-*.yaml")
		Expect(err).NotTo(HaveOccurred())
		_, err = f.WriteString(instanceCRYAML)
		Expect(err).NotTo(HaveOccurred())
		f.Close()
		cmd = exec.Command("kubectl", "apply", "-f", f.Name())
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		By("deleting the SQLServerInstance CR")
		_ = exec.Command("kubectl", "delete", "sqli", "mssql-e2e", "-n", agNamespace, "--ignore-not-found=true").Run()
		_ = exec.Command("kubectl", "delete", "pvc", "-l", "app=mssql-e2e", "-n", agNamespace).Run()
	})

	It("should create the StatefulSet pod", func() {
		By("waiting for mssql-e2e-0 to become Ready (up to 5m)")
		waitForPodReady(instancePod, 5*time.Minute)
	})

	It("should respond to SQL queries", func() {
		By("executing SELECT 1 on the instance pod")
		Eventually(func(g Gomega) {
			out, err := execSQL(instancePod, "SELECT 1 AS alive")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("1"), "SELECT 1 did not return expected output")
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("should create and query a user database", func() {
		By("creating database e2edb and inserting a row")
		_, err := execSQL(instancePod, "CREATE DATABASE e2edb")
		Expect(err).NotTo(HaveOccurred())

		_, err = execSQL(instancePod, "CREATE TABLE e2edb.dbo.items (id INT IDENTITY, name NVARCHAR(64))")
		Expect(err).NotTo(HaveOccurred())

		_, err = execSQL(instancePod, "INSERT INTO e2edb.dbo.items (name) VALUES ('hello')")
		Expect(err).NotTo(HaveOccurred())

		out, err := execSQL(instancePod, "SET NOCOUNT ON; SELECT name FROM e2edb.dbo.items")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("hello"))
	})

	It("should report Ready in the CR status", func() {
		By("checking SQLServerInstance status for Ready condition")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "sqli", "mssql-e2e", "-n", agNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"), "SQLServerInstance not Ready")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})
})

// stringReader wraps strings.NewReader; defined here to satisfy instance_test usage.
func stringReader(s string) *strings.Reader { return strings.NewReader(s) }
