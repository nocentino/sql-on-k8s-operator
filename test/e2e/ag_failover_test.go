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
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// failoverToReplica issues a manual failover to the named target pod via T-SQL.
func failoverToReplica(targetPod, targetFQDN string) error {
	q := fmt.Sprintf("ALTER AVAILABILITY GROUP AG1 FAILOVER;") // must run ON the target
	_, err := execSQL(targetPod, q)
	return err
}

var _ = Describe("AG Failover", Ordered, Label("ag", "failover"), func() {
	var primaryPod string
	const rowCount = 50

	BeforeAll(func() {
		By("waiting for AG to be bootstrapped")
		waitForAGBootstrap("mssql-ag", 10*time.Minute)
		primaryPod = getAGPrimary("mssql-ag")
		Expect(primaryPod).NotTo(BeEmpty())

		By("creating test database and table for data-loss validation")
		_, err := execSQL(primaryPod, createTestDB)
		Expect(err).NotTo(HaveOccurred())
		_, err = execSQL(primaryPod, createTestTable)
		Expect(err).NotTo(HaveOccurred())

		By("adding testdb to AG")
		_, err = execSQL(primaryPod, addDBtoAGSQL)
		Expect(err).NotTo(HaveOccurred())

		// Join on secondaries
		for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
			_, err = execSQL(pod, joinDBOnSecondarySQL)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() { captureArtifacts("AG Failover " + CurrentSpecReport().FullText()) })

	Context("Planned Failover", func() {
		It("should fail over to pod-1 with no data loss", func() {
			By(fmt.Sprintf("inserting %d rows before failover", rowCount))
			insertTestRows(primaryPod, rowCount)
			rowsBefore := countTestRows(primaryPod)
			Expect(rowsBefore).To(Equal(rowCount))

			By("waiting for pod-1 to be SYNCHRONIZED before failover")
			Eventually(func(g Gomega) {
				out, err := execSQL("mssql-ag-1", `
SET NOCOUNT ON;
SELECT synchronization_state_desc
FROM sys.dm_hadr_availability_replica_states
WHERE is_local = 1`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("SYNCHRONIZED"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("initiating planned failover to mssql-ag-1")
			err := failoverToReplica("mssql-ag-1", "")
			Expect(err).NotTo(HaveOccurred())

			By("waiting for mssql-ag-1 to become PRIMARY")
			Eventually(func(g Gomega) {
				out, _ := execSQL("mssql-ag-1", `
SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
				g.Expect(strings.TrimSpace(out)).To(Equal("PRIMARY"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("validating no data loss after planned failover")
			rowsAfter := countTestRows("mssql-ag-1")
			Expect(rowsAfter).To(Equal(rowsBefore), "Data loss detected after planned failover!")
		})
	})

	Context("Unplanned Failover (pod kill)", func() {
		It("should elect a new primary and lose no committed data", func() {
			currentPrimary := getAGPrimary("mssql-ag")

			By(fmt.Sprintf("inserting %d rows on current primary %s", rowCount, currentPrimary))
			insertTestRows(currentPrimary, rowCount)

			// Wait for sync
			secondary := "mssql-ag-0"
			if currentPrimary == "mssql-ag-0" {
				secondary = "mssql-ag-1"
			}
			Eventually(func(g Gomega) {
				out, _ := execSQL(secondary, `SET NOCOUNT ON;
SELECT synchronization_state_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1`)
				g.Expect(out).To(ContainSubstring("SYNCHRONIZED"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			rowsBefore := countTestRows(currentPrimary)

			By("killing the primary pod to simulate unplanned failure")
			err := exec.Command("kubectl", "delete", "pod", currentPrimary, "-n", agNamespace, "--grace-period=0", "--force").Run()
			Expect(err).NotTo(HaveOccurred())

			By("waiting for a new primary to be elected")
			var newPrimary string
			Eventually(func(g Gomega) {
				for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
					if pod == currentPrimary {
						continue
					}
					out, err := execSQL(pod, `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
					if err == nil && strings.TrimSpace(out) == "PRIMARY" {
						newPrimary = pod
						return
					}
				}
				g.Expect(newPrimary).NotTo(BeEmpty(), "No new primary elected yet")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("validating committed rows are present on new primary")
			rowsAfter := countTestRows(newPrimary)
			Expect(rowsAfter).To(BeNumerically(">=", rowsBefore),
				"Data loss detected after unplanned failover!")

			By("waiting for killed pod to restart and rejoin")
			waitForPodReady(currentPrimary, 5*time.Minute)
		})
	})
})
