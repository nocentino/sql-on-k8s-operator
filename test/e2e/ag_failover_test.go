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

// failoverToReplica performs a clean planned failover to the target secondary.
// Must be run on the target (future primary) secondary replica.
//
// With CLUSTER_TYPE = EXTERNAL the operator acts as the external cluster manager.
// sp_set_session_context authorizes the DDL (without it SQL Server returns Msg 47104).
// ALTER AVAILABILITY GROUP ... FAILOVER is the correct planned-failover command for
// EXTERNAL mode; it requires the target to be SYNCHRONIZED and causes zero data loss.
func failoverToReplica(targetPod, _ string) error {
	sql := "EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';" +
		" ALTER AVAILABILITY GROUP AG1 FAILOVER;"
	out, err := execSQL(targetPod, sql)
	if err != nil {
		return fmt.Errorf("%w — sqlcmd output: %s", err, out)
	}
	return nil
}

// forceFailoverToReplica promotes a secondary using FORCE_FAILOVER_ALLOW_DATA_LOSS.
// Use this only when the current primary is unavailable (unplanned failover or failback
// after a forced failover already changed the primary).
// sp_set_session_context is required to authorize the DDL in EXTERNAL mode (Msg 47104).
func forceFailoverToReplica(targetPod string) error {
	sql := "EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';" +
		" ALTER AVAILABILITY GROUP AG1 FORCE_FAILOVER_ALLOW_DATA_LOSS;"
	out, err := execSQL(targetPod, sql)
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
		// Try REMOVE DATABASE on every pod because the current primary may have changed
		// after a previous failover test (ordered containers share an AG lifecycle).
		for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
			_, _ = execSQL(pod, "ALTER AVAILABILITY GROUP AG1 REMOVE DATABASE testdb;")
		}
		// Allow SQL Server to complete HADR session teardown before issuing the DROP.
		// Without this pause the DROP of a RESTORING secondary database can race with
		// internal HADR cleanup and leave residual state that blocks the next VDI seed.
		time.Sleep(5 * time.Second)
		for _, pod := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
			_, _ = execSQL(pod, "DROP DATABASE IF EXISTS testdb;")
		}
		// Wait for testdb to be completely absent from sys.databases on all pods
		// before creating a fresh copy.  If a DROP fails silently (e.g. due to a
		// lock held by the HADR redo thread), VDI seeding will never start.
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
		// forceFailoverToReplica uses sp_set_session_context + FORCE_FAILOVER_ALLOW_DATA_LOSS,
		// which is correct for a failback when the target is not yet synchronized.
		// We detect the actual primary from SQL, not from the CR status (which may
		// lag behind by a reconcile cycle).
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
			// mssql-ag-0 is now a secondary. The planned failover test moved primary to mssql-ag-1.
			// Use FORCE_FAILOVER_ALLOW_DATA_LOSS to failback — this is the emergency path
			// appropriate here since we are in AfterAll cleanup (not a coordinated planned failover).
			_ = Eventually(func(g Gomega) {
				g.Expect(forceFailoverToReplica("mssql-ag-0")).To(Succeed(), "failback FORCE_FAILOVER failed")
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
			// After FORCE_FAILOVER_ALLOW_DATA_LOSS the old primary can remain in split-brain
			// (both reporting PRIMARY) for several seconds until the HADR protocol delivers
			// the new epoch. Subsequent tests that check role topology (bootstrap, safety)
			// will fail if we don't wait for the demotion to complete.
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

			// failoverToReplica issues: sp_set_session_context + ALTER AG FAILOVER.
			// The session context authorizes the operator as the external cluster manager
			// (CLUSTER_TYPE = EXTERNAL); without it SQL Server returns Msg 47104.
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

			// After the replica role transitions to PRIMARY, the individual databases go
			// through SECONDARY → RESOLVING → PRIMARY at the database level.  Wait for
			// testdb to be ONLINE in sys.databases AND for the table to be accessible
			// before querying it.  sys.databases.state_desc transitions to ONLINE as soon
			// as the replica becomes PRIMARY, but SQL Server still needs a brief moment to
			// fully re-open the database for DML — running SELECT COUNT(*) immediately
			// after seeing ONLINE can still return exit status 1 (database not yet ready).
			By("waiting for testdb to be ONLINE and accessible on new primary mssql-ag-1")
			Eventually(func(g Gomega) {
				out, err := execSQL("mssql-ag-1",
					"SET NOCOUNT ON; SELECT state_desc FROM sys.databases WHERE name = 'testdb'")
				g.Expect(err).NotTo(HaveOccurred(),
					"sys.databases query failed on mssql-ag-1: %s", out)
				g.Expect(strings.TrimSpace(out)).To(Equal("ONLINE"),
					"testdb not yet ONLINE on mssql-ag-1: %s", out)
				// Also verify the table is accessible — ONLINE in sys.databases is necessary
				// but not sufficient; the database may still be recovering internally.
				_, tableErr := execSQL("mssql-ag-1",
					"SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.t")
				g.Expect(tableErr).NotTo(HaveOccurred(),
					"testdb.dbo.t not yet accessible on mssql-ag-1 (database may still be recovering)")
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			By("validating no data loss after planned failover")
			rowsAfter := countTestRows("mssql-ag-1")
			Expect(rowsAfter).To(Equal(rowsBefore), "Data loss detected after planned failover!")
		})
	})

	Context("Unplanned Failover (pod kill)", func() {
		It("should elect a new primary and lose no committed data", func() {
			// Detect the actual current primary from SQL rather than relying on CR status,
			// which may still reflect the old primary immediately after the planned-failover
			// test's AfterAll failback completed.
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

			// Wait for the synchronous secondary (mssql-ag-1, replica index 1) to be HEALTHY
			// before killing the primary so we have a SYNCHRONIZED target for automatic failover.
			syncSecondary := "mssql-ag-0"
			if currentPrimary == "mssql-ag-0" {
				syncSecondary = "mssql-ag-1"
			}
			By(fmt.Sprintf("waiting for synchronous secondary %s to be HEALTHY before killing primary", syncSecondary))
			Eventually(func(g Gomega) {
				out, _ := execSQL(syncSecondary, `SET NOCOUNT ON;
SELECT synchronization_health_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1`)
				g.Expect(strings.TrimSpace(out)).To(Equal("HEALTHY"),
					"synchronous secondary not yet HEALTHY: %s", out)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			rowsBefore := countTestRows(currentPrimary)

			By("killing the primary pod to simulate unplanned failure")
			killErr := exec.Command("kubectl", "delete", "pod", currentPrimary, "-n", agNamespace, "--grace-period=0", "--force").Run()
			Expect(killErr).NotTo(HaveOccurred())

			// The operator detects the primary pod as NotReady and waits for
			// automaticFailover.failoverThresholdSeconds (30s in the fixture) before
			// issuing FORCE_FAILOVER_ALLOW_DATA_LOSS on the best synchronous secondary.
			// Allow up to 3 minutes for detection + failover + SQL Server role transition.
			By("waiting for the operator to promote a new primary (automatic unplanned failover)")
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
				g.Expect(newPrimary).NotTo(BeEmpty(), "No new primary promoted yet")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			// After FORCE_FAILOVER_ALLOW_DATA_LOSS the database transitions from a
			// synchronized secondary state to PRIMARY. The role DMV reports PRIMARY
			// before the database is fully online for DML, so wait for testdb to
			// become queryable before counting rows.
			By(fmt.Sprintf("waiting for testdb to be queryable on new primary %s", newPrimary))
			Eventually(func(g Gomega) {
				_, tableErr := execSQL(newPrimary,
					"SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.t")
				g.Expect(tableErr).NotTo(HaveOccurred(),
					"testdb.dbo.t not yet accessible on %s (database may still be recovering)", newPrimary)
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			By(fmt.Sprintf("validating committed rows are present on new primary %s", newPrimary))
			rowsAfter := countTestRows(newPrimary)
			Expect(rowsAfter).To(BeNumerically(">=", rowsBefore),
				"Data loss detected after unplanned failover!")

			By(fmt.Sprintf("waiting for killed pod %s to restart and rejoin as secondary", currentPrimary))
			waitForPodReady(currentPrimary, 5*time.Minute)

			By(fmt.Sprintf("verifying %s rejoined as SECONDARY after restart", currentPrimary))
			Eventually(func(g Gomega) {
				out, err := execSQL(currentPrimary, `SET NOCOUNT ON;
SELECT rs.role_desc FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = 'AG1' AND rs.is_local = 1`)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("SECONDARY"),
					"former primary did not rejoin as SECONDARY: %s", out)
			}, 5*time.Minute, 10*time.Second).Should(Succeed())
		})
	})
})
