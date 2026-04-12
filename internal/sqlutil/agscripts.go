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

package sqlutil

import (
	"fmt"
	"strings"
	"time"
)

// clusterTypeNone is the SQL Server cluster type for standalone AG (no Pacemaker/WSFC).
const clusterTypeNone = "NONE"

// FailoverSQL returns the T-SQL to promote the current replica to PRIMARY.
//
// Used for both planned and unplanned failover. With CLUSTER_TYPE = EXTERNAL, SQL Server
// persists each replica's synchronization state locally, so ALTER AVAILABILITY GROUP FAILOVER
// succeeds on a synchronized secondary even when the primary is offline — SQL Server verifies
// from its local copy of the AG configuration that the replica had received every committed
// transaction. If the replica is not synchronized, SQL Server rejects the command with
// error 41142; the operator logs the rejection and retries on the next reconcile cycle.
//
// This mirrors the promote action in Microsoft's mssql-server-ha ag-helper, which always
// issues ALTER AVAILABILITY GROUP FAILOVER and surfaces error 41142 back to Pacemaker
// rather than falling back to FORCE_FAILOVER_ALLOW_DATA_LOSS.
//
// The sp_set_session_context call is required when CLUSTER_TYPE = EXTERNAL to authorize
// the operator as the external cluster manager (Msg 47104 is returned if it is absent).
func FailoverSQL(agName string) string {
	return fmt.Sprintf(
		"EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';\nALTER AVAILABILITY GROUP [%s] FAILOVER;",
		agName)
}

// SetRoleToSecondarySQL returns the T-SQL to transition a replica from RESOLVING to SECONDARY role.
//
// When CLUSTER_TYPE = EXTERNAL, secondaries land in RESOLVING state after JOIN and stay there
// until an explicit SET (ROLE = SECONDARY) is issued. This is analogous to Pacemaker's "start"
// action in the mssql-server-ha agent. Without this step, waitForSecondariesReady times out
// because the replica never reports role_desc = 'SECONDARY'.
func SetRoleToSecondarySQL(agName string) string {
	return fmt.Sprintf("ALTER AVAILABILITY GROUP [%s] SET (ROLE = SECONDARY);", agName)
}

// ResolvingReplicasSQL returns a query that lists the replica_server_name of every
// non-local replica that is currently in RESOLVING role, as seen from the primary.
//
// Used by the controller to detect replicas that have restarted and need
// SET (ROLE = SECONDARY) to transition out of RESOLVING state in EXTERNAL mode.
func ResolvingReplicasSQL(agName string) string {
	return fmt.Sprintf(`SET NOCOUNT ON;
SELECT ar.replica_server_name
FROM sys.availability_groups ag
JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
JOIN sys.dm_hadr_availability_replica_states rs ON ar.replica_id = rs.replica_id
WHERE ag.name = '%s'
  AND rs.is_local = 0
  AND rs.role_desc = 'RESOLVING';`, agName)
}

// SyncReplicaStatesSQL returns a query that yields one row per non-local replica with:
//   - replica_name  : the short pod hostname (@@SERVERNAME on that pod)
//   - avail_mode    : SYNCHRONOUS_COMMIT or ASYNCHRONOUS_COMMIT
//   - sync_state    : SYNCHRONIZED, SYNCHRONIZING, NOT_SYNCHRONIZING, etc.
//   - role          : PRIMARY or SECONDARY (from the local replica's perspective)
//
// Used by the controller to select the best automatic failover target.
func SyncReplicaStatesSQL(agName string) string {
	return fmt.Sprintf(`SET NOCOUNT ON;
SELECT
    ar.replica_server_name AS replica_name,
    ar.availability_mode_desc  AS avail_mode,
    drs.synchronization_state_desc AS sync_state,
    rs.role_desc AS role
FROM sys.availability_groups ag
JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
JOIN sys.dm_hadr_availability_replica_states rs ON ar.replica_id = rs.replica_id
LEFT JOIN sys.dm_hadr_database_replica_states drs
    ON rs.replica_id = drs.replica_id AND drs.is_local = 0
WHERE ag.name = '%s'
  AND rs.is_local = 0;`, agName)
}

// escapeSQLString escapes single quotes for use inside T-SQL string literals.
// SQL Server uses doubled single quotes (”) as the escape sequence for a literal
// single quote inside a string delimited by single quotes.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// CreateMasterKeySQL returns the T-SQL to create the database master key.
func CreateMasterKeySQL(password string) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '%s';
END`, escapeSQLString(password))
}

// CreateCertificateSQL returns T-SQL to create the AG endpoint certificate if it doesn't exist.
// The certificate is created with a 5-year expiry from the current date so that replicas
// do not need to rotate their endpoint certificates for the expected lifetime of the AG.
func CreateCertificateSQL(certName, subject, backupPath string) string {
	expiry := time.Now().AddDate(5, 0, 0).Format("20060102")
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '%s')
BEGIN
    CREATE CERTIFICATE %s WITH SUBJECT = '%s', EXPIRY_DATE = '%s';
    BACKUP CERTIFICATE %s TO FILE = '%s';
END`, certName, certName, subject, expiry, certName, backupPath)
}

// RestoreCertificateSQL returns T-SQL to import a peer's public certificate from a file.
// No AUTHORIZATION clause is used — the cert lives at the server (master) scope and is
// later referenced by a certificate-based login for endpoint authentication.
func RestoreCertificateSQL(certName, filePath string) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '%s')
BEGIN
    CREATE CERTIFICATE [%s] FROM FILE = '%s';
END`, certName, certName, filePath)
}

// CreateLoginFromCertSQL creates a server login backed by a (peer) certificate.
// This is required for SQL Server HADR endpoint certificate authentication:
// the connecting replica presents its cert; the local instance validates using
// the stored public cert and grants access via this login.
func CreateLoginFromCertSQL(loginName, certName string) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '%s')
BEGIN
    CREATE LOGIN [%s] FROM CERTIFICATE [%s];
END`, loginName, loginName, certName)
}

// CreateEndpointSQL returns T-SQL to create the DATABASE_MIRRORING endpoint using certificate auth.
func CreateEndpointSQL(endpointName, certName string, port int32) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.endpoints WHERE name = '%s')
BEGIN
    CREATE ENDPOINT %s
        STATE = STARTED
        AS TCP (LISTENER_PORT = %d, LISTENER_IP = ALL)
        FOR DATABASE_MIRRORING (
            AUTHENTICATION = CERTIFICATE %s,
            ROLE = ALL
        );
END`, endpointName, endpointName, port, certName)
}

// GrantEndpointConnectSQL grants CONNECT on the endpoint to a login.
func GrantEndpointConnectSQL(endpointName, loginName string) string {
	return fmt.Sprintf(`GRANT CONNECT ON ENDPOINT::%s TO %s;`, endpointName, loginName)
}

// EnableHADRSQL returns the mssql.conf hadr enable snippet (applied via ConfigMap, not T-SQL).
// The actual T-SQL to enable HADR is executed after instance restart.
func EnableHADRSQL() string {
	return `
IF (SELECT value_in_use FROM sys.configurations WHERE name = 'hadr enabled') = 0
BEGIN
    EXEC sp_configure 'show advanced options', 1;
    RECONFIGURE;
    EXEC sp_configure 'hadr enabled', 1;
    RECONFIGURE;
END`
}

// AGReplicaInput carries the fields needed to build CREATE AVAILABILITY GROUP replica specs.
// PodName must match @@SERVERNAME inside the SQL Server container (the short pod hostname).
// EndpointFQDN is the DNS name used in the ENDPOINT_URL (headless-service FQDN).
type AGReplicaInput struct {
	PodName           string // e.g. mssql-ag-0
	EndpointFQDN      string // e.g. mssql-ag-0.mssql-ag-headless.default.svc.cluster.local
	AvailabilityMode  string
	FailoverMode      string
	ReadableSecondary bool // when true, sets SECONDARY_ROLE(ALLOW_CONNECTIONS = ALL)
}

// AGExistsSQL returns a query whose result is 1 if the named AG exists, 0 otherwise.
// Caller should check existence in Go before calling CreateAGSQL because
// CREATE AVAILABILITY GROUP cannot be wrapped in a BEGIN...END control-flow block.
func AGExistsSQL(agName string) string {
	return fmt.Sprintf(
		"SET NOCOUNT ON; SELECT COUNT(*) FROM sys.availability_groups WHERE name = '%s'",
		agName)
}

// SecondaryCountSQL returns a query that counts the number of replicas that are
// both in SECONDARY role AND actively CONNECTED, as seen from the primary.
//
// Both conditions are required:
//   - role_desc = 'SECONDARY'  — the JOIN was processed at the protocol level.
//   - connected_state_desc = 'CONNECTED' — the HADR TCP transport session is
//     established. This is critical: role_desc is set by the protocol JOIN message
//     and can be SECONDARY even when the transport is DISCONNECTED (e.g. due to a
//     DNS lookup failure on first connection attempt after CREATE AVAILABILITY GROUP).
//     Requiring CONNECTED ensures the transport session is truly active before
//     declaring bootstrap complete.
func SecondaryCountSQL(agName string) string {
	return fmt.Sprintf(
		`SET NOCOUNT ON; SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states rs
JOIN sys.availability_groups ag ON rs.group_id = ag.group_id
WHERE ag.name = '%s' AND rs.role_desc = 'SECONDARY' AND rs.connected_state_desc = 'CONNECTED'`,
		agName)
}

// CreateAGSQL generates the T-SQL CREATE AVAILABILITY GROUP statement for the primary.
// REPLICA ON N'<PodName>' uses @@SERVERNAME (short hostname); ENDPOINT_URL uses the FQDN.
// NOTE: CREATE AVAILABILITY GROUP cannot appear inside a BEGIN...END block in SQL Server;
// callers must guard idempotency using AGExistsSQL before invoking this.
func CreateAGSQL(agName, clusterType string, replicas []AGReplicaInput, endpointPort int32) string {
	if clusterType == "" {
		clusterType = clusterTypeNone
	}

	var replicaDefs strings.Builder
	for i, r := range replicas {
		availMode := "SYNCHRONOUS_COMMIT"
		if r.AvailabilityMode == "AsynchronousCommit" {
			availMode = "ASYNCHRONOUS_COMMIT"
		}
		// With CLUSTER_TYPE = EXTERNAL, SQL Server requires FAILOVER_MODE = EXTERNAL on
		// every replica (Msg 47102 is returned if any replica uses FAILOVER_MODE = MANUAL).
		// With CLUSTER_TYPE = NONE, the only valid value is MANUAL.
		// The spec's FailoverMode (Automatic vs Manual) controls operator-level behaviour
		// (whether the controller will auto-failover to this replica), not the SQL value.
		failoverMode := "MANUAL"
		if clusterType != clusterTypeNone {
			failoverMode = "EXTERNAL"
		}
		sep := ","
		if i == len(replicas)-1 {
			sep = ""
		}
		secondaryRole := "SECONDARY_ROLE (ALLOW_CONNECTIONS = NO)"
		if r.ReadableSecondary {
			secondaryRole = "SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)"
		}
		replicaDefs.WriteString(fmt.Sprintf(`
    N'%s' WITH (
        ENDPOINT_URL = N'TCP://%s:%d',
        FAILOVER_MODE = %s,
        AVAILABILITY_MODE = %s,
        SEEDING_MODE = AUTOMATIC,
        %s
    )%s`, r.PodName, r.EndpointFQDN, endpointPort, failoverMode, availMode, secondaryRole, sep))
	}

	return fmt.Sprintf(`CREATE AVAILABILITY GROUP [%s]
    WITH (CLUSTER_TYPE = %s, DB_FAILOVER = OFF, DTC_SUPPORT = NONE)
    FOR REPLICA ON%s;`, agName, clusterType, replicaDefs.String())
}

// JoinAGSQL generates T-SQL for secondary replicas to join an existing AG.
//
// Idempotency guard: skip the JOIN if the local replica already has an active
// HADR state for this AG (i.e. the replica has previously joined successfully).
// Note: sys.availability_groups on the SECONDARY is EMPTY before the first JOIN
// because the AG only appears in the secondary's catalog after a successful JOIN.
// Therefore the guard must query sys.dm_hadr_availability_replica_states (which
// has a row with is_local=1 once joined) joined back to sys.availability_groups
// so that the AG name can be matched — do NOT guard on sys.availability_groups
// alone, as that will always be false on the first JOIN attempt and silently
// prevent the JOIN from ever running.
func JoinAGSQL(agName, clusterType string) string {
	if clusterType == "" {
		clusterType = clusterTypeNone
	}
	return fmt.Sprintf(`
IF NOT EXISTS (
    SELECT 1 FROM sys.dm_hadr_availability_replica_states rs
    JOIN sys.availability_groups ag ON rs.group_id = ag.group_id
    WHERE rs.is_local = 1 AND ag.name = '%s'
)
BEGIN
    ALTER AVAILABILITY GROUP [%s] JOIN WITH (CLUSTER_TYPE = %s);
    ALTER AVAILABILITY GROUP [%s] GRANT CREATE ANY DATABASE;
END`, agName, agName, clusterType, agName)
}
