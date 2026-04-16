#!/bin/bash
set -uo pipefail
SA="YourStrong!Passw0rd"

run_sql() {
    local pod=$1; local sql=$2
    kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C -Q "$sql" 2>&1
}

echo "=== AG replica states ==="
run_sql mssql-ag-0 "SELECT r.replica_server_name, rs.role_desc, rs.synchronization_health_desc FROM sys.availability_replicas r JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id=rs.replica_id ORDER BY r.replica_server_name;"

echo ""
echo "=== Seeding mode per replica ==="
run_sql mssql-ag-0 "SELECT replica_server_name, seeding_mode_desc FROM sys.availability_replicas ORDER BY replica_server_name;"

echo ""
echo "=== Automatic seeding progress ==="
run_sql mssql-ag-0 "SELECT r.replica_server_name, d.name AS db, s.seeding_state_desc, s.internal_state_desc, CAST(s.transferred_size_bytes/1048576.0 AS DECIMAL(10,1)) AS xferred_mb, CAST(s.database_size_bytes/1048576.0 AS DECIMAL(10,1)) AS total_mb, s.number_of_retries, s.failure_state_desc, s.error_code FROM sys.dm_hadr_automatic_seeding s JOIN sys.availability_replicas r ON s.replica_id=r.replica_id JOIN sys.databases d ON s.database_id=d.database_id ORDER BY s.start_time DESC;"

echo ""
echo "=== Database state on each replica ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    echo "--- $pod ---"
    run_sql "$pod" "SELECT d.name, d.state_desc, drs.synchronization_state_desc, drs.is_seeding FROM sys.databases d LEFT JOIN sys.dm_hadr_database_replica_states drs ON d.database_id=drs.database_id AND drs.is_local=1 WHERE d.name NOT IN ('master','tempdb','model','msdb');"
done

echo ""
echo "=== Errorlog: seeding entries (primary) ==="
run_sql mssql-ag-0 "EXEC sp_readerrorlog 0, 1, 'Seeding';" | tail -20

echo ""
echo "=== Errorlog: GRANT entries (primary) ==="
run_sql mssql-ag-0 "EXEC sp_readerrorlog 0, 1, 'GRANT';" | tail -10

echo ""
echo "=== Operator logs (last 5 min) ==="
kubectl logs -n sql-on-k8s-operator-system \
    deployment/sql-on-k8s-operator-controller-manager \
    -c manager --since=5m 2>&1 | tail -40
