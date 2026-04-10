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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/anocentino/sql-on-k8s-operator/test/utils"
)

var _ = Describe("AG Bootstrap", Ordered, Label("ag", "bootstrap"), func() {
	AfterEach(func() { captureArtifacts("AG Bootstrap " + CurrentSpecReport().FullText()) })

	BeforeAll(func() {
		By("ensuring the AG secret exists")
		_ = exec.Command("kubectl", "create", "secret", "generic", agSecretName,
			"--from-literal=SA_PASSWORD="+sqlPassword,
			"-n", agNamespace).Run()

		By("applying the 3-replica AG CR")
		f, err := os.CreateTemp("", "ag-cr-*.yaml")
		Expect(err).NotTo(HaveOccurred())
		_, _ = f.WriteString(agCRYAML)
		f.Close()
		cmd := exec.Command("kubectl", "apply", "-f", f.Name())
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		By("deleting the AG CR and PVCs")
		_ = exec.Command("kubectl", "delete", "sqlag", "mssql-ag", "-n", agNamespace, "--ignore-not-found=true").Run()
		_ = exec.Command("kubectl", "delete", "pvc", "-l", "app=mssql-ag", "-n", agNamespace).Run()
	})

	It("should start all 3 replica pods", func() {
		for i := 0; i < 3; i++ {
			waitForPodReady(fmt.Sprintf("mssql-ag-%d", i), 6*time.Minute)
		}
	})

	It("should complete AG bootstrap (initializationComplete=true)", func() {
		By("waiting up to 8 minutes for bootstrap to complete")
		waitForAGBootstrap("mssql-ag", 8*time.Minute)
	})

	It("should report a primary replica in the CR status", func() {
		primary := getAGPrimary("mssql-ag")
		Expect(primary).NotTo(BeEmpty(), "primaryReplica should be set in status")
	})

	It("should have pod-0 report PRIMARY AG role via SQL", func() {
		Eventually(func(g Gomega) {
			out, err := execSQL("mssql-ag-0", `
SET NOCOUNT ON;
SELECT rs.role_desc
FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("PRIMARY"))
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	It("should have secondaries report SECONDARY AG role via SQL", func() {
		for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
			Eventually(func(g Gomega) {
				out, err := execSQL(pod, `
SET NOCOUNT ON;
SELECT rs.role_desc
FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("SECONDARY"), "pod %s not secondary", pod)
			}, 3*time.Minute, 10*time.Second).Should(Succeed())
		}
	})

	It("should have synchronous replicas in SYNCHRONIZED state", func() {
		Eventually(func(g Gomega) {
			out, err := execSQL("mssql-ag-0", `
SET NOCOUNT ON;
SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states rs
JOIN sys.availability_replicas r ON rs.replica_id = r.replica_id
WHERE rs.synchronization_state_desc = 'SYNCHRONIZED'
  AND r.availability_mode = 1`)
			g.Expect(err).NotTo(HaveOccurred())
			var count int
			for _, line := range strings.Split(out, "\n") {
				line = strings.TrimSpace(line)
				if _, scanErr := fmt.Sscanf(line, "%d", &count); scanErr == nil {
					break
				}
			}
			// primary + 1 synchronous secondary = at least 2
			g.Expect(count).To(BeNumerically(">=", 2), "Expected >=2 SYNCHRONIZED replicas")
		}, 3*time.Minute, 10*time.Second).Should(Succeed())
	})

	It("should expose the AG listener service", func() {
		cmd := exec.Command("kubectl", "get", "svc", "mssql-ag-listener", "-n", agNamespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "listener service not found")
	})
})
