/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"fmt"
	"strings"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
)

// Container, endpoint, and label constants used across the AG controller.
// Keeping them here (rather than magic strings sprinkled in the code) makes
// future renames safe and grep-friendly.
const (
	// containerName is the single SQL Server container inside every managed pod.
	containerName = "mssql"
	// agEndpointName is the T-SQL endpoint name created on every replica during
	// bootstrap for HADR mirroring. Must be a valid SQL identifier.
	agEndpointName = "AGEP"
	// agFinalizer blocks deletion of the CR until the controller has had a chance
	// to clean up controller-local state (recovery timers in sync.Map) keyed by
	// this AG. Kubernetes owner references already handle the StatefulSet/Service
	// tear-down, so the finalizer does not issue any SQL (the pods may already
	// be gone).
	agFinalizer = "sql.mssql.microsoft.com/ag-finalizer"
)

// clearAllRecoveryState removes every entry in the shared sync.Map that was
// keyed by this AG's namespace/name prefix. Called from the finalizer path so
// a deleted AG does not leak timer state into the next AG that happens to be
// created with the same name.
func (r *SQLServerAvailabilityGroupReconciler) clearAllRecoveryState(ag *sqlv1alpha1.SQLServerAvailabilityGroup) {
	prefix := ag.Namespace + "/" + ag.Name + "/"
	r.reseatFirstFailureTime.Range(func(key, _ any) bool {
		if s, ok := key.(string); ok && strings.HasPrefix(s, prefix) {
			r.reseatFirstFailureTime.Delete(key)
		}
		return true
	})
}

// podNameForReplica returns the pod name for the replica at index i in the
// StatefulSet. Centralising the naming convention here means a future rename
// (e.g. to support the "replicaset" PodManagementPolicy naming) needs to
// change a single line.
func podNameForReplica(ag *sqlv1alpha1.SQLServerAvailabilityGroup, i int) string {
	return fmt.Sprintf("%s-%d", ag.Name, i)
}

// preStopScript is executed by the container's lifecycle.preStop hook when a
// pod is about to be removed. It attempts a clean planned failover from the
// current primary to a HEALTHY synchronous secondary so draining the pod does
// not leave the AG headless. If the pod is not the primary, or no eligible
// secondary exists, the script exits successfully and the pod terminates.
//
// Extracted to a constant so the shell is reviewable and unit-testable rather
// than being lost in a >1 KB one-liner inside buildAGStatefulSet. Uses the
// SQLCMDPASSWORD environment variable (populated from MSSQL_SA_PASSWORD) so
// the SA password never appears on sqlcmd's command line.
const preStopScript = `set -o pipefail
export SQLCMDPASSWORD="${MSSQL_SA_PASSWORD}"
ROLE=$(/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -h -1 -W \
    -Q "SET NOCOUNT ON; SELECT role FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1" \
    2>/dev/null | head -1 | tr -d '[:space:]')
if [ "$ROLE" != "1" ]; then exit 0; fi
TARGET=$(/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -h -1 -W \
    -Q "SET NOCOUNT ON;
        SELECT TOP 1 ar.replica_server_name
        FROM sys.dm_hadr_availability_replica_states ars
        JOIN sys.availability_replicas ar ON ars.replica_id = ar.replica_id
        WHERE ars.role_desc = 'SECONDARY'
          AND ars.synchronization_health_desc = 'HEALTHY'
          AND ars.connected_state_desc = 'CONNECTED'" \
    2>/dev/null | head -1 | tr -d '[:space:]')
if [ -z "$TARGET" ]; then exit 0; fi
SVC="${HOSTNAME%-*}-headless"
/opt/mssql-tools18/bin/sqlcmd -S "${TARGET}.${SVC},1433" -U sa -C \
    -Q "EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';
        ALTER AVAILABILITY GROUP [$AG_NAME] FAILOVER;" 2>/dev/null
exit 0
`

// probeScript is the shell one-liner used by both liveness and readiness probes.
// It connects to the local SQL Server, runs SELECT 1, and exits non-zero on any
// failure. SQLCMDPASSWORD is sourced from the MSSQL_SA_PASSWORD env var so the
// password never appears on the sqlcmd command line (visible to ps inside the pod).
const probeScript = `SQLCMDPASSWORD="${MSSQL_SA_PASSWORD}" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -Q "SELECT 1" -C -b 2>&1 | grep -q "1"`
