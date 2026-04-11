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
)

// clusterTypeNone is the SQL Server cluster type for standalone AG (no Pacemaker/WSFC).
const clusterTypeNone = "NONE"

// CreateMasterKeySQL returns the T-SQL to create the database master key.
func CreateMasterKeySQL(password string) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '%s';
END`, password)
}

// CreateCertificateSQL returns T-SQL to create the AG endpoint certificate if it doesn't exist.
func CreateCertificateSQL(certName, subject, backupPath string) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '%s')
BEGIN
    CREATE CERTIFICATE %s WITH SUBJECT = '%s';
    BACKUP CERTIFICATE %s TO FILE = '%s';
END`, certName, certName, subject, certName, backupPath)
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
		failoverMode := "MANUAL"
		if r.FailoverMode == "Automatic" && clusterType != clusterTypeNone {
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
