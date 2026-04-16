#!/bin/bash
set -uo pipefail
SA="YourStrong!Passw0rd"

run() {
    kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q "$2" 2>/dev/null | tr -d '\r'
}

echo "=== Pod status ==="
kubectl get pods -l app=mssql-ag --no-headers

echo ""
echo "=== Replica role / health (self-report from each pod) ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    role=$(run "$pod"   "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" \
        | grep -E "PRIMARY|SECONDARY|RESOLVING" | head -1 | tr -d ' ')
    health=$(run "$pod" "SET NOCOUNT ON; SELECT synchronization_health_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" \
        | grep -v "^$" | head -1 | tr -d ' ')
    echo "  $pod: role=${role:-UNKNOWN}  health=${health:-UNKNOWN}"
done

echo ""
echo "=== tpcc state per pod ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    db_state=$(run "$pod" "SET NOCOUNT ON; SELECT state_desc FROM sys.databases WHERE name='tpcc'" \
        | grep -v "^$" | head -1 | tr -d ' ')
    susp=$(run "$pod" "SET NOCOUNT ON; SELECT ISNULL(CAST(is_suspended AS VARCHAR),'N/A') FROM sys.dm_hadr_database_replica_states WHERE is_local=1 AND database_id=DB_ID('tpcc')" \
        | grep -v "^$" | head -1 | tr -d ' ')
    sync=$(run "$pod" "SET NOCOUNT ON; SELECT ISNULL(synchronization_state_desc,'N/A') FROM sys.dm_hadr_database_replica_states WHERE is_local=1 AND database_id=DB_ID('tpcc')" \
        | grep -v "^$" | head -1 | tr -d ' ')
    echo "  $pod: db_state=${db_state:-UNKNOWN}  sync=${sync:-N/A}  is_suspended=${susp:-N/A}"
done

echo ""
echo "=== Full replica + database states from primary perspective ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    result=$(run "$pod" "SET NOCOUNT ON; SELECT r.replica_server_name, rs.role_desc, drs.synchronization_state_desc, drs.synchronization_health_desc FROM sys.availability_replicas r JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id=rs.replica_id LEFT JOIN sys.dm_hadr_database_replica_states drs ON r.replica_id=drs.replica_id AND drs.database_id=DB_ID('tpcc') ORDER BY r.replica_server_name" \
        | grep "mssql-ag" || true)
    if [ -n "$result" ]; then
        echo "  (queried from $pod)"
        echo "$result" | sed 's/^/    /'
        break
    fi
done

echo ""
echo "=== Operator logs (last 30 lines) ==="
kubectl logs -n sql-on-k8s-operator-system \
    deployment/sql-on-k8s-operator-controller-manager \
    -c manager --since=5m 2>&1 | tail -30
