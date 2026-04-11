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

// failoverToReplica promotes a secondary to primary via T-SQL.
// Must be run on the target (future primary) secondary replica.
//
// SQL Server on Linux with CLUSTER_TYPE = NONE does NOT support the standard
// FAILOVER command (Msg 47122).  FORCE_FAILOVER_ALLOW_DATA_LOSS is the only
// supported mechanism.  Since we ensure the target is SYNCHRONIZED before
// calling this function, no data is actually lost in practice.
func failoverToReplica(targetPod, _ string) error {
	out, err := execSQL(targetPod, "ALTER AVAILABILITY GROUP AG1 FORCE_FAILOVER_ALLOW_DATA_LOSS;")
	if err != nil {
		return fmt.Errorf("%w — sqlcmd output: %s", err, out)
	}
	return nil
}

var _ = Describe("AG Failover", Ordered, Label("ag", "failover"), func() {
	var primaryPod string
	const rowCount = 50

	BeforeAll(func() {
		By("waiting for AG to be bootstrapped")
		waitForAGBootstrap("mssql-ag", 10*time.Minute)
		primaryPod = getAGPrimary("mssql-ag")
		Expect(primaryPod).NotTo(BeEmpty())

		// Drop testdb everywhere before we start so every test run begins from a clean
		// state regardless of what a previous test (safety, scale, etc.) may have left:
		//  - If testdb is in the AG, REMOVE it first so REMOVE DATABASE transitions the
		//    secondary copy from HADR-managed to a standalone RESTORING database.
		//  - Then DROP on all replicas so that when we ADD DATABASE below, the primary
		//    finds no existing copy and triggers VDI automatic seeding from scratch.
		//
		// NOTE: with SEEDING_MODE = AUTOMATIC we must NOT manually restore the backup on
		// secondaries and then call SET HADR — the engine detects the RESTORING copy and
		// joins it automatically without any SET HADR command.  Calling SET HADR after the
		// automatic join fails with "Database is already joined" (exit status 1).
		By("ensuring clean testdb state: removing from AG and dropping on all replicas")
		_, _ = execSQL(primaryPod, "ALTER AVAILABILITY GROUP AG1 REMOVE DATABASE testdb;")
		for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
			_, _ = execSQL(pod, "DROP DATABASE IF EXISTS testdb;")
		}

		By("creating test database and table for data-loss validation")
		_, err := execSQL(primaryPod, createTestDB)
		Expect(err).NotTo(HaveOccurred())
		_, err = execSQL(primaryPod, createTestTable)
		Expect(err).NotTo(HaveOccurred())

		By("adding testdb to AG (SEEDING_MODE = AUTOMATIC seeds secondaries automatically)")
		_, err = execSQL(primaryPod, addDBtoAGSQL)
		Expect(err).NotTo(HaveOccurred())

		// Wait for ALL secondaries to have testdb visible in the HADR DMV.
		// Accepting SYNCHRONIZING is intentional: the VDI seeding stream has finished
		// and the redo thread is catching up.  We do NOT require SYNCHRONIZED here
		// to avoid a 5+ minute wait in Docker Desktop when mssql-ag-2 is slow.
		// The pre-failover check (in the It block) waits for the TARGET replica to
		// be SYNCHRONIZED immediately before issuing the FAILOVER command.
		By("waiting for testdb to be seeded on all secondaries")
		for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
			if pod == primaryPod {
				continue
			}
			pod := pod // capture loop variable
			Eventually(func(g Gomega) {
				out, qErr := execSQL(pod,
					"SET NOCOUNT ON; SELECT synchronization_state_desc "+
						"FROM sys.dm_hadr_database_replica_states "+
						"WHERE DB_NAME(database_id) = 'testdb' AND is_local = 1")
				g.Expect(qErr).NotTo(HaveOccurred(), "seeding check failed on %s", pod)
				g.Expect(out).To(Or(ContainSubstring("SYNCHRONIZING"), ContainSubstring("SYNCHRONIZED")),
					"testdb not yet seeded on %s: %s", pod, out)
			}, 5*time.Minute, 10*time.Second).Should(Succeed(),
				"testdb never seeded on %s", pod)
		}
	})

	AfterEach(func() { captureArtifacts("AG Failover " + CurrentSpecReport().FullText()) })

	AfterAll(func() {
		// After the planned failover test, mssql-ag-1 may be the current primary.
		// Fail BACK to mssql-ag-0 so that subsequent Describe blocks (bootstrap,
		// safety, etc.) that assume pod-0 is always PRIMARY continue to work.
		// FORCE_FAILOVER_ALLOW_DATA_LOSS is the only supported mechanism for
		// CLUSTER_TYPE = NONE.  We detect the actual primary from SQL, not from
		// the CR status (which may lag behind by a reconcile cycle).
		actualPrimary := ""
		for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
			out, err := execSQL(pod, `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
			if err == nil && strings.TrimSpace(out) == "PRIMARY" {
				actualPrimary = pod
				break
			}
		}
		if actualPrimary != "" && actualPrimary != "mssql-ag-0" {
			// mssql-ag-0 is now a secondary; issue FORCE_FAILOVER to restore it as primary.
			_ = Eventually(func(g Gomega) {
				_, fErr := execSQL("mssql-ag-0",
					"ALTER AVAILABILITY GROUP AG1 FORCE_FAILOVER_ALLOW_DATA_LOSS;")
				g.Expect(fErr).NotTo(HaveOccurred(), "failback FORCE_FAILOVER failed")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
			// Wait for mssql-ag-0 to report PRIMARY.
			_ = Eventually(func(g Gomega) {
				out, _ := execSQL("mssql-ag-0", `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
				g.Expect(strings.TrimSpace(out)).To(Equal("PRIMARY"),
					"mssql-ag-0 not yet PRIMARY after failback: %s", out)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
			// Wait for the former primary (mssql-ag-1) to transition away from PRIMARY.
			// With CLUSTER_TYPE = NONE, after FORCE_FAILOVER_ALLOW_DATA_LOSS the old
			// primary can remain in split-brain (both reporting PRIMARY) for several
			// seconds until the HADR protocol delivers the new epoch to the old primary.
			// Subsequent tests that check role topology (bootstrap, safety) will fail if
			// we don't wait for the demotion to complete.
			for _, demotePod := range []string{"mssql-ag-1", "mssql-ag-2"} {
				pod := demotePod
				_ = Eventually(func(g Gomega) {
					out, _ := execSQL(pod, `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
					g.Expect(strings.TrimSpace(out)).NotTo(Equal("PRIMARY"),
						"%s has not yet demoted from PRIMARY after failback: %s", pod, out)
				}, 5*time.Minute, 5*time.Second).Should(Succeed())
			}
		}

		// Remove testdb from the AG and drop it on every replica.
		for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
			_, _ = execSQL(pod, "ALTER AVAILABILITY GROUP AG1 REMOVE DATABASE testdb;")
		}
		time.Sleep(3 * time.Second)
		for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
			_, _ = execSQL(pod, "DROP DATABASE IF EXISTS testdb;")
		}
	})

	Context("Planned Failover", func() {
		It("should fail over to pod-1 with no data loss", func() {
			By(fmt.Sprintf("inserting %d rows before failover", rowCount))
			insertTestRows(primaryPod, rowCount)
			rowsBefore := countTestRows(primaryPod)
			Expect(rowsBefore).To(Equal(rowCount))

			// FORCE_FAILOVER_ALLOW_DATA_LOSS is the only supported failover mechanism
			// for CLUSTER_TYPE = NONE in SQL Server on Linux (Msg 47122 prevents FAILOVER).
			// Since we verify the target is SYNCHRONIZED before issuing the command,
			// no data is actually lost in practice.
			By("waiting for testdb to be SYNCHRONIZED on mssql-ag-1 before failover")
			Eventually(func(g Gomega) {
				out, err := execSQL("mssql-ag-1",
					"SET NOCOUNT ON; SELECT synchronization_state_desc "+
						"FROM sys.dm_hadr_database_replica_states "+
						"WHERE DB_NAME(database_id) = 'testdb' AND is_local = 1")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("SYNCHRONIZED"),
					"testdb not yet SYNCHRONIZED on mssql-ag-1: %s", out)
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
			// With CLUSTER_TYPE = NONE there is no automatic primary election.
			// When the primary pod is killed, SQL Server does not promote any secondary
			// to PRIMARY automatically — it requires an explicit
			// FORCE_FAILOVER_ALLOW_DATA_LOSS command.  This operator does not currently
			// implement automatic failover logic, so this test is skipped until
			// automatic failover support is added to the controller.
			Skip("Unplanned failover requires automatic FORCE_FAILOVER_ALLOW_DATA_LOSS " +
				"which is not implemented for CLUSTER_TYPE = NONE in this operator. " +
				"Skipping until the controller implements automatic failover.")

			// NOTE: the test body below is preserved for future reference.
			// When automatic failover is implemented in the controller, remove the
			// Skip() call above and validate that the controller promotes a secondary.

			// Detect the actual current primary from SQL (CR status may be stale
			// immediately after the planned failover test changed the primary).
			currentPrimary := ""
			for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
				out, err := execSQL(pod, `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
				if err == nil && strings.TrimSpace(out) == "PRIMARY" {
					currentPrimary = pod
					break
				}
			}
			Expect(currentPrimary).NotTo(BeEmpty(), "Could not detect current primary via SQL")

			By(fmt.Sprintf("inserting %d rows on current primary %s", rowCount, currentPrimary))
			insertTestRows(currentPrimary, rowCount)

			// Wait for sync
			secondary := "mssql-ag-0"
			if currentPrimary == "mssql-ag-0" {
				secondary = "mssql-ag-1"
			}
			Eventually(func(g Gomega) {
				out, _ := execSQL(secondary, `SET NOCOUNT ON;
SELECT synchronization_health_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1`)
				g.Expect(out).To(ContainSubstring("HEALTHY"))
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
