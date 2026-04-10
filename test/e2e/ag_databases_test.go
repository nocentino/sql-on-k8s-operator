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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AG Database Membership", Ordered, Label("ag", "databases"), func() {
	var primary string

	AfterEach(func() { captureArtifacts("AG Databases " + CurrentSpecReport().FullText()) })

	BeforeAll(func() {
		By("waiting for AG bootstrap")
		waitForAGBootstrap("mssql-ag", 10*time.Minute)
		primary = getAGPrimary("mssql-ag")
		Expect(primary).NotTo(BeEmpty())
	})

	Context("Add database to AG", func() {
		const dbName = "agdb"

		It("should add a database to the AG and synchronize it across replicas", func() {
			By("creating database " + dbName + " on primary")
			_, err := execSQL(primary, fmt.Sprintf(
				"IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '%s') CREATE DATABASE %s", dbName, dbName))
			Expect(err).NotTo(HaveOccurred())

			By("creating a test table in " + dbName)
			_, err = execSQL(primary, fmt.Sprintf(
				"IF NOT EXISTS (SELECT 1 FROM %s.sys.tables WHERE name = 'records') "+
					"CREATE TABLE %s.dbo.records (id INT IDENTITY PRIMARY KEY, val NVARCHAR(64))", dbName, dbName))
			Expect(err).NotTo(HaveOccurred())

			By("inserting 10 rows before adding to AG")
			for i := 0; i < 10; i++ {
				_, err = execSQL(primary, fmt.Sprintf("INSERT INTO %s.dbo.records (val) VALUES ('pre-ag-%d')", dbName, i))
				Expect(err).NotTo(HaveOccurred())
			}

			By("taking full backup and adding " + dbName + " to AG1")
			_, err = execSQL(primary, fmt.Sprintf(
				"BACKUP DATABASE %s TO DISK = '/var/opt/mssql/data/%s.bak' WITH INIT, FORMAT;"+
					"ALTER AVAILABILITY GROUP AG1 ADD DATABASE %s;", dbName, dbName, dbName))
			Expect(err).NotTo(HaveOccurred())

			By("restoring and joining " + dbName + " on each secondary")
			for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
				_, err = execSQL(pod, fmt.Sprintf(
					"RESTORE DATABASE %s FROM DISK = '/var/opt/mssql/data/%s.bak' WITH NORECOVERY, REPLACE;"+
						"ALTER DATABASE %s SET HADR AVAILABILITY GROUP = AG1;", dbName, dbName, dbName))
				Expect(err).NotTo(HaveOccurred())
			}

			By("verifying " + dbName + " is in AG on the primary")
			Eventually(func(g Gomega) {
				out, err := execSQL(primary, fmt.Sprintf(
					"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.availability_databases_cluster WHERE database_name = '%s'", dbName))
				g.Expect(err).NotTo(HaveOccurred())
				var count int
				for _, line := range strings.Split(out, "\n") {
					line = strings.TrimSpace(line)
					if _, scanErr := fmt.Sscanf(line, "%d", &count); scanErr == nil {
						break
					}
				}
				g.Expect(count).To(Equal(1), "Database %s not in AG", dbName)
			}, 3*time.Minute, 10*time.Second).Should(Succeed())

			By("verifying row count consistency across replicas")
			Eventually(func(g Gomega) {
				for _, pod := range []string{"mssql-ag-1", "mssql-ag-2"} {
					out, err := execSQL(pod, fmt.Sprintf(
						"SET NOCOUNT ON; SELECT COUNT(*) FROM %s.dbo.records", dbName))
					g.Expect(err).NotTo(HaveOccurred(), "query failed on %s", pod)
					var count int
					for _, line := range strings.Split(out, "\n") {
						line = strings.TrimSpace(line)
						if _, scanErr := fmt.Sscanf(line, "%d", &count); scanErr == nil {
							break
						}
					}
					g.Expect(count).To(Equal(10), "replica %s has wrong row count for %s", pod, dbName)
				}
			}, 5*time.Minute, 10*time.Second).Should(Succeed())
		})
	})

	Context("Remove database from AG", func() {
		const dbName = "agdb"

		It("should remove a database from the AG without affecting data on the primary", func() {
			By("verifying " + dbName + " is in the AG before removal")
			out, err := execSQL(primary, fmt.Sprintf(
				"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.availability_databases_cluster WHERE database_name = '%s'", dbName))
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring("1"), dbName+" must be in the AG before removal test")

			By("counting rows before removal to confirm data is preserved")
			rowsBefore := 0
			for _, line := range strings.Split(out, "\n") {
				line = strings.TrimSpace(line)
				if _, scanErr := fmt.Sscanf(line, "%d", &rowsBefore); scanErr == nil {
					break
				}
			}
			// Re-count rows directly
			rowOut, _ := execSQL(primary, fmt.Sprintf(
				"SET NOCOUNT ON; SELECT COUNT(*) FROM %s.dbo.records", dbName))
			for _, line := range strings.Split(rowOut, "\n") {
				line = strings.TrimSpace(line)
				if _, scanErr := fmt.Sscanf(line, "%d", &rowsBefore); scanErr == nil {
					break
				}
			}

			By("removing " + dbName + " from AG1")
			_, err = execSQL(primary, fmt.Sprintf("ALTER AVAILABILITY GROUP AG1 REMOVE DATABASE %s", dbName))
			Expect(err).NotTo(HaveOccurred())

			By("verifying " + dbName + " is no longer in the AG")
			Eventually(func(g Gomega) {
				out, err := execSQL(primary, fmt.Sprintf(
					"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.availability_databases_cluster WHERE database_name = '%s'", dbName))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("0"), dbName+" should not be in the AG after removal")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying data is still intact on primary after removal")
			rowOut2, err := execSQL(primary, fmt.Sprintf(
				"SET NOCOUNT ON; SELECT COUNT(*) FROM %s.dbo.records", dbName))
			Expect(err).NotTo(HaveOccurred())
			var rowsAfter int
			for _, line := range strings.Split(rowOut2, "\n") {
				line = strings.TrimSpace(line)
				if _, scanErr := fmt.Sscanf(line, "%d", &rowsAfter); scanErr == nil {
					break
				}
			}
			Expect(rowsAfter).To(Equal(rowsBefore), "Data loss on primary after removing from AG!")
		})
	})
})
