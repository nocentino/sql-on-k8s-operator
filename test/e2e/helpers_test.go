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
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/anocentino/sql-on-k8s-operator/test/utils"
)

const (
	agNamespace        = "default"
	operatorNamespace  = "sql-on-k8s-operator-system"
	sqlPassword        = "YourStrong!Passw0rd"
	agSecretName       = "mssql-ag-secret"
	instanceSecretName = "mssql-secret"
	sqlImage           = "mcr.microsoft.com/mssql/server:2022-latest"
	artifactDir        = "/tmp/e2e-artifacts"
)

// execSQL runs a T-SQL query in the named pod (container: mssql) and returns the output.
func execSQL(pod, query string) (string, error) {
	cmd := exec.Command("kubectl", "exec", pod, "-n", agNamespace, "--", // nolint:gosec
		"/opt/mssql-tools18/bin/sqlcmd",
		"-S", "localhost", "-U", "sa", "-P", sqlPassword,
		"-Q", query, "-C", "-b", "-h", "-1",
	)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// waitForAGBootstrap polls until InitializationComplete is true.
// The controller sets this flag only after all replicas have reached CONNECTED state.
func waitForAGBootstrap(agName string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "sqlag", agName, "-n", agNamespace,
			"-o", "jsonpath={.status.initializationComplete}")
		out, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("true"), "AG bootstrap not complete yet")
	}, timeout, 10*time.Second).Should(Succeed())
}

// waitForAllReplicasSQLReady polls until every listed pod accepts a SQL connection.
// SQL Server on a secondary can take several minutes after the HADR transport connects
// before it processes regular client logins, so tests must not start until this passes.
func waitForAllReplicasSQLReady(pods []string, timeout time.Duration) {
	GinkgoHelper()
	for _, pod := range pods {
		p := pod
		Eventually(func(g Gomega) {
			out, err := execSQL(p, "SET NOCOUNT ON; SELECT 1")
			g.Expect(err).NotTo(HaveOccurred(), "SQL not ready on pod %s", p)
			g.Expect(out).To(ContainSubstring("1"), "SELECT 1 returned unexpected output on %s: %s", p, out)
		}, timeout, 10*time.Second).Should(Succeed(), "pod %s never accepted SQL connections", p)
	}
}

// waitForReplicasConnected polls the primary until all secondaries report
// connected_state_desc = 'CONNECTED'.  This is the same signal the controller
// uses when it sets initializationComplete=true, so it is meaningful even for
// empty AGs where synchronization_health_desc may remain NOT_HEALTHY until at
// least one database is added.
// Expects at least wantConnected CONNECTED secondaries.
func waitForReplicasConnected(primaryPod, agName string, wantConnected int, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := execSQL(primaryPod, fmt.Sprintf(`
SET NOCOUNT ON;
SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states rs
JOIN sys.availability_groups ag ON rs.group_id = ag.group_id
WHERE ag.name = '%s'
  AND rs.role_desc = 'SECONDARY'
  AND rs.connected_state_desc = 'CONNECTED'`, agName))
		g.Expect(err).NotTo(HaveOccurred(), "Failed to query replica connection state on %s", primaryPod)
		var count int
		for _, line := range strings.Split(out, "\n") {
			line = strings.TrimSpace(line)
			if _, scanErr := fmt.Sscanf(line, "%d", &count); scanErr == nil {
				break
			}
		}
		g.Expect(count).To(BeNumerically(">=", wantConnected),
			"Expected >=%d CONNECTED secondaries, got %d", wantConnected, count)
	}, timeout, 10*time.Second).Should(Succeed())
}

// waitForPodReady polls until the named pod is Ready.
func waitForPodReady(pod string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pod", pod, "-n", agNamespace,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
		out, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("True"), fmt.Sprintf("pod %s not ready", pod))
	}, timeout, 5*time.Second).Should(Succeed())
}

// getAGPrimary returns the current primary replica pod name.
func getAGPrimary(agName string) string {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "get", "sqlag", agName, "-n", agNamespace,
		"-o", "jsonpath={.status.primaryReplica}")
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(out)
}

// insertTestRows inserts n rows into testdb.dbo.t with identity key and value.
func insertTestRows(pod string, n int) {
	GinkgoHelper()
	for i := 0; i < n; i++ {
		q := fmt.Sprintf("INSERT INTO testdb.dbo.t (val) VALUES ('row-%d')", i)
		_, err := execSQL(pod, q)
		Expect(err).NotTo(HaveOccurred(), "Failed to insert row %d", i)
	}
}

// countTestRows returns the number of rows in testdb.dbo.t on the named pod.
func countTestRows(pod string) int {
	GinkgoHelper()
	out, err := execSQL(pod, "SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.t")
	Expect(err).NotTo(HaveOccurred(), "Failed to count rows on %s", pod)
	var count int
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "-") {
			continue
		}
		if _, err := fmt.Sscanf(line, "%d", &count); err == nil {
			return count
		}
	}
	return -1
}

// copyBackupBetweenPods copies a SQL Server backup file from sourcePod to targetPod
// via a local temp file. This is required because each pod has its own PVC and the
// backup file on the primary is not accessible from secondary pods directly.
func copyBackupBetweenPods(sourcePod, targetPod, remotePath string) error {
	f, err := os.CreateTemp("", "backup-*.bak")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	f.Close()
	defer os.Remove(f.Name())

	copyFrom := exec.Command("kubectl", "cp",
		fmt.Sprintf("%s/%s:%s", agNamespace, sourcePod, remotePath),
		f.Name())
	if out, err := copyFrom.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl cp from %s: %w\n%s", sourcePod, err, out)
	}

	copyTo := exec.Command("kubectl", "cp",
		f.Name(),
		fmt.Sprintf("%s/%s:%s", agNamespace, targetPod, remotePath))
	if out, err := copyTo.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl cp to %s: %w\n%s", targetPod, err, out)
	}
	return nil
}

// captureArtifacts saves logs, events, and AG CR state to the artifact directory on failure.
func captureArtifacts(testName string) {
	if !CurrentSpecReport().Failed() {
		return
	}
	dir := filepath.Join(artifactDir, strings.ReplaceAll(testName, " ", "_"))
	_ = os.MkdirAll(dir, 0o755)

	// Operator logs
	if out, err := exec.Command("kubectl", "logs", "-n", operatorNamespace,
		"deployment/sql-on-k8s-operator-controller-manager").CombinedOutput(); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "operator.log"), out, 0o644)
	}
	// AG pod logs
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("mssql-ag-%d", i)
		if out, err := exec.Command("kubectl", "logs", pod, "-n", agNamespace).CombinedOutput(); err == nil {
			_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.log", pod)), out, 0o644)
		}
	}
	// K8s events
	if out, err := exec.Command("kubectl", "get", "events", "-n", agNamespace,
		"--sort-by=.lastTimestamp").CombinedOutput(); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "events.txt"), out, 0o644)
	}
	// AG CR state
	if out, err := exec.Command("kubectl", "get", "sqlag", "-n", agNamespace,
		"-o", "yaml").CombinedOutput(); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "sqlag.yaml"), out, 0o644)
	}
	// AG SQL state (best-effort).
	// Uses synchronization_health_desc (replica-level aggregate: HEALTHY/PARTIALLY_HEALTHY/NOT_HEALTHY)
	// from sys.dm_hadr_availability_replica_states.  synchronization_state_desc is only available
	// in the database-level DMV sys.dm_hadr_database_replica_states.
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("mssql-ag-%d", i)
		q := "SET NOCOUNT ON; SELECT replica_id, role_desc, synchronization_health_desc, connected_state_desc " +
			"FROM sys.dm_hadr_availability_replica_states;"
		if out, err := execSQL(pod, q); err == nil {
			_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s-ag-state.txt", pod)), []byte(out), 0o644)
		}
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "Artifacts saved to %s\n", dir)
}
