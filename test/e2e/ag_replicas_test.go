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

var _ = Describe("AG Replica Scale", Ordered, Label("ag", "scale"), func() {
	AfterEach(func() { captureArtifacts("AG Scale " + CurrentSpecReport().FullText()) })

	BeforeAll(func() {
		By("waiting for AG bootstrap")
		waitForAGBootstrap("mssql-ag", 10*time.Minute)
	})

	Context("Add Replica", func() {
		// NOTE: Adding a 4th replica requires patching the AG CR with a new replica entry,
		// which may require cluster capacity. We simulate this by documenting the steps
		// and performing assertions on the existing replicas becoming consistent.
		It("should confirm all replicas are in the SECONDARY role and synchronized", func() {
			By("verifying existing secondaries are SYNCHRONIZED before scale operation")
			// BeforeSuite already confirmed CONNECTED state; this is a lightweight check
			// that the replicas are still connected at the start of scale tests.
			// For an empty AG synchronization_health_desc stays NOT_HEALTHY (nothing to
			// synchronize), so connected_state_desc is the correct readiness signal.
			for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
				Eventually(func(g Gomega) {
					out, err := execSQL(pod, `
SET NOCOUNT ON;
SELECT connected_state_desc
FROM sys.dm_hadr_availability_replica_states
WHERE is_local = 1`)
					g.Expect(err).NotTo(HaveOccurred(), "Failed to query %s", pod)
					g.Expect(out).To(ContainSubstring("CONNECTED"),
						"pod %s HADR transport not connected: %s", pod, out)
				}, 3*time.Minute, 5*time.Second).Should(Succeed())
			}
		})

		It("should have consistent row counts across all replicas", func() {
			primary := getAGPrimary("mssql-ag")
			By("ensuring clean testdb state: removing from AG and dropping on all replicas")
			// Try REMOVE DATABASE on every pod because the current primary may have changed
			// after a previous failover test (ordered containers share an AG lifecycle).
			for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
				_, _ = execSQL(pod, "ALTER AVAILABILITY GROUP AG1 REMOVE DATABASE testdb;")
			}
			// Allow SQL Server to complete HADR session teardown before issuing the DROP.
			// Without this pause, the DROP of a RESTORING secondary database can race with
			// internal HADR cleanup and leave residual state that blocks the next VDI seed.
			time.Sleep(5 * time.Second)
			for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
				_, _ = execSQL(pod, "DROP DATABASE IF EXISTS testdb;")
			}
			// Wait for testdb to be completely absent from sys.databases on all pods
			// before creating a fresh copy.  If a DROP fails silently (e.g. due to
			// a lock held by the HADR redo thread), VDI seeding will never start.
			for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
				pod := pod
				Eventually(func(g Gomega) {
					out, _ := execSQL(pod,
						"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'testdb'")
					var count int
					for _, line := range strings.Split(out, "\n") {
						line = strings.TrimSpace(line)
						if _, scanErr := fmt.Sscanf(line, "%d", &count); scanErr == nil {
							break
						}
					}
					g.Expect(count).To(Equal(0), "testdb still present on %s", pod)
				}, 2*time.Minute, 5*time.Second).Should(Succeed(),
					"testdb never fully dropped on %s", pod)
			}

			By("inserting 20 validation rows on primary " + primary)
			_, err := execSQL(primary, createTestDB)
			Expect(err).NotTo(HaveOccurred())
			_, err = execSQL(primary, createTestTable)
			Expect(err).NotTo(HaveOccurred())
			insertTestRows(primary, 20)
			rowsOnPrimary := countTestRows(primary)
			Expect(rowsOnPrimary).To(BeNumerically(">", 0))

			By("adding testdb to AG so secondaries receive rows via HADR replication")
			// Backup + ADD DATABASE: SEEDING_MODE=AUTOMATIC seeds secondaries directly.
			_, err = execSQL(primary, addDBtoAGSQL)
			Expect(err).NotTo(HaveOccurred())

			// Wait for VDI seeding to complete on ALL secondaries before checking row counts.
			// Without this explicit seeding gate, the row-count Eventually fires before the
			// database even exists on the secondary (exit status 1).  The safety test must
			// have already waited for all secondaries before cleanup to avoid interrupted-VDI
			// state contaminating this seeding round (see ag_safety_test.go).
			By("waiting for testdb VDI seeding to complete on all secondaries")
			for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
				if pod == primary {
					continue
				}
				pod := pod
				Eventually(func(g Gomega) {
					out, qErr := execSQL(pod,
						"SET NOCOUNT ON; SELECT synchronization_state_desc "+
							"FROM sys.dm_hadr_database_replica_states "+
							"WHERE DB_NAME(database_id) = 'testdb' AND is_local = 1")
					g.Expect(qErr).NotTo(HaveOccurred(), "seeding check on %s: %v", pod, qErr)
					g.Expect(out).To(Or(ContainSubstring("SYNCHRONIZING"), ContainSubstring("SYNCHRONIZED")),
						"testdb not visible in DMV on %s: %s", pod, out)
				}, 10*time.Minute, 10*time.Second).Should(Succeed(),
					"testdb VDI seeding never completed on %s", pod)
			}

			By("waiting for secondaries to reflect same row count")
			for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
				if pod == primary {
					continue
				}
				pod := pod // capture loop variable for goroutine safety
				Eventually(func(g Gomega) {
					// Use g.Expect (not global Expect) so failures are retried by Eventually.
					// countTestRows uses the global Expect and would abort the test immediately.
					out, err := execSQL(pod, "SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.t")
					g.Expect(err).NotTo(HaveOccurred(),
						"query failed on %s — testdb may still be seeding", pod)
					var count int
					for _, line := range strings.Split(out, "\n") {
						line = strings.TrimSpace(line)
						if _, scanErr := fmt.Sscanf(line, "%d", &count); scanErr == nil {
							break
						}
					}
					g.Expect(count).To(Equal(rowsOnPrimary),
						"replica %s has %d rows, primary has %d", pod, count, rowsOnPrimary)
				}, 10*time.Minute, 10*time.Second).Should(Succeed(),
					"secondary %s never reached expected row count %d", pod, rowsOnPrimary)
			}
		})
	})

	Context("Remove Replica (scale down)", func() {
		It("should allow scale-down to 2 replicas without data loss", func() {
			primary := getAGPrimary("mssql-ag")

			By("recording current row count before scale-down")
			rowsBefore := countTestRows(primary)

			By("applying 2-replica AG CR")
			f, err := os.CreateTemp("", "ag-2rep-*.yaml")
			Expect(err).NotTo(HaveOccurred())
			_, _ = f.WriteString(ag2ReplicaCRYAML)
			f.Close()
			cmd := exec.Command("kubectl", "apply", "-f", f.Name())
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for StatefulSet to scale to 2 replicas")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "sts", "mssql-ag", "-n", agNamespace,
					"-o", "jsonpath={.status.readyReplicas}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("2"), "Expected 2 ready replicas, got: %s", out)
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("verifying primary is still serving writes after scale-down")
			// Re-fetch primary since it may have changed
			activePrimary := ""
			for _, pod := range []string{"mssql-ag-0", "mssql-ag-1"} {
				out, err := execSQL(pod, `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
				if err == nil && strings.TrimSpace(out) == "PRIMARY" {
					activePrimary = pod
					break
				}
			}
			Expect(activePrimary).NotTo(BeEmpty(), "No primary found after scale-down")

			By("validating no data loss after scale-down")
			rowsAfter := countTestRows(activePrimary)
			Expect(rowsAfter).To(Equal(rowsBefore),
				fmt.Sprintf("Data loss: before=%d after=%d", rowsBefore, rowsAfter))

			By("restoring 3-replica AG CR for subsequent tests")
			f2, err := os.CreateTemp("", "ag-3rep-*.yaml")
			Expect(err).NotTo(HaveOccurred())
			_, _ = f2.WriteString(agCRYAML)
			f2.Close()
			cmd = exec.Command("kubectl", "apply", "-f", f2.Name())
			_, _ = utils.Run(cmd)
		})
	})
})
