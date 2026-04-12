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

// Package sqlutil provides helpers for executing T-SQL queries against SQL Server
// from within the Kubernetes operator via kubectl exec into the pod.
package sqlutil

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Executor runs T-SQL statements inside a running SQL Server pod using kubectl exec.
type Executor struct {
	Client     kubernetes.Interface
	RestConfig *rest.Config
}

// ExecResult holds the stdout/stderr from a kubectl exec operation.
type ExecResult struct {
	Stdout string
	Stderr string
}

// ExecSQL executes a T-SQL query inside the named pod/container using sqlcmd.
// The saPassword is used for the -P flag; callers must ensure it is retrieved
// from the relevant Kubernetes Secret prior to calling this function.
func (e *Executor) ExecSQL(ctx context.Context, namespace, podName, containerName, saPassword, query string) (ExecResult, error) {
	cmd := []string{
		"/opt/mssql-tools18/bin/sqlcmd",
		"-S", "localhost",
		"-U", "sa",
		"-P", saPassword,
		"-Q", query,
		"-C", // trust server certificate (self-signed in new installs)
		"-b", // exit on error
	}

	req := e.Client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.RestConfig, "POST", req.URL())
	if err != nil {
		return ExecResult{}, fmt.Errorf("could not create SPDY executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	result := ExecResult{
		Stdout: strings.TrimSpace(stdout.String()),
		Stderr: strings.TrimSpace(stderr.String()),
	}

	if err != nil {
		// sqlcmd sends T-SQL error messages to stdout, not stderr, so include both.
		return result, fmt.Errorf("sqlcmd exec failed (stdout=%q stderr=%q): %w",
			result.Stdout, result.Stderr, err)
	}
	return result, nil
}

// ServerDiagnostics holds the per-component health reported by sp_server_diagnostics.
// A true value means the component is healthy (state != 3/error).
// Matches the Diagnostics struct in Microsoft's mssql-server-ha mssqlcommon package.
type ServerDiagnostics struct {
	System          bool // OS schedulers, CPU, memory allocator
	Resource        bool // Buffer pool, out-of-memory conditions
	QueryProcessing bool // Deadlocked workers, long-running queries
}

// IsHealthyAt returns true when the diagnostics pass the given threshold, where threshold
// is one of "system", "resource", or "query_processing". The semantics match Microsoft's
// health level constants (ServerCriticalError, ServerModerateError, ServerAnyQualifiedError):
//
//   - "system"           → only system errors cause unhealthy (default, least sensitive)
//   - "resource"         → system or resource errors cause unhealthy
//   - "query_processing" → system, resource, or query-processing errors cause unhealthy
func (d ServerDiagnostics) IsHealthyAt(threshold string) bool {
	switch threshold {
	case "query_processing":
		return d.System && d.Resource && d.QueryProcessing
	case "resource":
		return d.System && d.Resource
	default: // "system"
		return d.System
	}
}

// CheckServerDiagnostics runs EXEC sp_server_diagnostics on the named pod and returns
// the per-component health state. This is the Kubernetes-native equivalent of the
// OpenDBWithHealthCheck + QueryDiagnostics pattern used in Microsoft's mssql-server-ha
// ag-helper binary.
//
// The query captures the stored procedure output into a table variable and extracts
// just the component_name and state columns to keep sqlcmd output easy to parse.
// state = 3 means error for a component; other values (1=clean, 2=warning) are healthy.
//
// If the stored procedure cannot be reached (SQL Server down, exec failure), an error
// is returned. Callers should treat exec errors conservatively (assume healthy) so that
// the Kubernetes pod readiness probe — which also connects to SQL — remains the primary
// liveness signal.
func (e *Executor) CheckServerDiagnostics(ctx context.Context, namespace, podName, containerName, saPassword string) (ServerDiagnostics, error) {
	const query = `SET NOCOUNT ON;
DECLARE @diag TABLE (
    creation_time  NVARCHAR(50),
    component_type NVARCHAR(50),
    component_name NVARCHAR(100),
    state          INT,
    state_desc     NVARCHAR(50),
    data           XML
);
INSERT INTO @diag EXEC sp_server_diagnostics;
SELECT component_name + '|' + CAST(state AS NVARCHAR(5))
FROM   @diag
WHERE  component_name IN ('system', 'resource', 'query_processing');`

	res, err := e.ExecSQL(ctx, namespace, podName, containerName, saPassword, query)
	if err != nil {
		return ServerDiagnostics{}, err
	}

	// sp_server_diagnostics state values: 1=clean, 2=warning, 3=error.
	const stateError = 3

	// All components default to healthy; only flip to false when state = 3.
	diag := ServerDiagnostics{System: true, Resource: true, QueryProcessing: true}
	for line := range strings.SplitSeq(res.Stdout, "\n") {
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		state, parseErr := strconv.Atoi(strings.TrimSpace(parts[1]))
		if parseErr != nil {
			continue
		}
		switch name {
		case "system":
			diag.System = state != stateError
		case "resource":
			diag.Resource = state != stateError
		case "query_processing":
			diag.QueryProcessing = state != stateError
		}
	}
	return diag, nil
}

// IsReady returns true when a SELECT 1 query succeeds, indicating SQL Server is up.
func (e *Executor) IsReady(ctx context.Context, namespace, podName, containerName, saPassword string) bool {
	res, err := e.ExecSQL(ctx, namespace, podName, containerName, saPassword, "SELECT 1")
	if err != nil {
		return false
	}
	return strings.Contains(res.Stdout, "1")
}

// GetAGRole returns the current AG role of the named instance (PRIMARY, SECONDARY, or RESOLVING).
func (e *Executor) GetAGRole(ctx context.Context, namespace, podName, containerName, saPassword, agName string) (string, error) {
	query := fmt.Sprintf(`
SET NOCOUNT ON;
SELECT rs.role_desc
FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = '%s' AND rs.is_local = 1;`, agName)

	res, err := e.ExecSQL(ctx, namespace, podName, containerName, saPassword, query)
	if err != nil {
		return "", err
	}

	for line := range strings.SplitSeq(res.Stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "PRIMARY" || line == "SECONDARY" || line == "RESOLVING" {
			return line, nil
		}
	}
	return "", nil
}

// ReadFileFromPod reads a file from inside a pod and returns its raw bytes.
// Uses `cat` over SPDY streaming to transfer binary-safe content.
func (e *Executor) ReadFileFromPod(ctx context.Context, namespace, podName, containerName, remotePath string) ([]byte, error) {
	cmd := []string{"cat", remotePath}
	req := e.Client.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName, Command: cmd, Stdout: true, Stderr: true,
		}, scheme.ParameterCodec)

	ex, err := remotecommand.NewSPDYExecutor(e.RestConfig, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("could not create SPDY executor for read: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if err := ex.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return nil, fmt.Errorf("read file %s from %s failed (stderr=%q): %w", remotePath, podName, stderr.String(), err)
	}
	return stdout.Bytes(), nil
}

// WriteFileToPod writes raw bytes to a file inside a pod via stdin streaming.
// Uses `cat >` to pipe stdin directly to the target path.  Stdout is explicitly
// disabled in PodExecOptions to avoid a SPDY deadlock that occurs when the server
// opens a stdout channel but the client provides no reader to drain it.
func (e *Executor) WriteFileToPod(ctx context.Context, namespace, podName, containerName, remotePath string, data []byte) error {
	cmd := []string{"/bin/bash", "-c", fmt.Sprintf("cat > '%s'", remotePath)}
	req := e.Client.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName, Command: cmd, Stdin: true, Stdout: false, Stderr: true,
		}, scheme.ParameterCodec)

	ex, err := remotecommand.NewSPDYExecutor(e.RestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("could not create SPDY executor for write: %w", err)
	}
	var stderr bytes.Buffer
	if err := ex.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: bytes.NewReader(data), Stderr: &stderr,
	}); err != nil {
		return fmt.Errorf("write file %s to %s failed (stderr=%q): %w", remotePath, podName, stderr.String(), err)
	}
	return nil
}

// GetAGSyncState returns the synchronization health for the local replica in the named AG.
// It queries synchronization_health_desc from sys.dm_hadr_availability_replica_states,
// which returns HEALTHY, PARTIALLY_HEALTHY, or NOT_HEALTHY at the replica level.
// Note: synchronization_state_desc (SYNCHRONIZED/SYNCHRONIZING) is a database-level column
// found only in sys.dm_hadr_database_replica_states, not in the replica-level DMV.
func (e *Executor) GetAGSyncState(ctx context.Context, namespace, podName, containerName, saPassword, agName string) (string, error) {
	query := fmt.Sprintf(`
SET NOCOUNT ON;
SELECT rs.synchronization_health_desc
FROM sys.availability_groups ag
JOIN sys.dm_hadr_availability_replica_states rs ON ag.group_id = rs.group_id
WHERE ag.name = '%s' AND rs.is_local = 1;`, agName)

	res, err := e.ExecSQL(ctx, namespace, podName, containerName, saPassword, query)
	if err != nil {
		return "", err
	}

	for line := range strings.SplitSeq(res.Stdout, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && line != "---" {
			return line, nil
		}
	}
	return "", nil
}
