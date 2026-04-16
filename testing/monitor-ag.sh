#!/bin/bash
LOGFILE="${1:-/Users/anocentino/Documents/GitHub/sql-on-k8s-operator/ag-health-monitor.csv}"
SA="YourStrong!Passw0rd"
echo "timestamp,ag0_role,ag0_health,ag1_role,ag1_health,ag2_role,ag2_health,pod0_ready,pod1_ready,pod2_ready" > "$LOGFILE"
while true; do
  TS=$(date -u +%Y-%m-%dT%H:%M:%S)
  # Try querying from ag-0, fall back to ag-1
  AG=$(kubectl exec mssql-ag-0 -- /opt/mssql-tools18/bin/sqlcmd \
    -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT replica_server_name+','+role_desc+','+synchronization_health_desc FROM sys.dm_hadr_availability_replica_states rs JOIN sys.availability_replicas r ON rs.replica_id=r.replica_id ORDER BY r.replica_server_name" \
    2>/dev/null | tr -d '\r' | grep -v '^$')
  if [ -z "$AG" ]; then
    AG=$(kubectl exec mssql-ag-1 -- /opt/mssql-tools18/bin/sqlcmd \
      -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q \
      "SET NOCOUNT ON; SELECT replica_server_name+','+role_desc+','+synchronization_health_desc FROM sys.dm_hadr_availability_replica_states rs JOIN sys.availability_replicas r ON rs.replica_id=r.replica_id ORDER BY r.replica_server_name" \
      2>/dev/null | tr -d '\r' | grep -v '^$')
  fi
  R0=$(echo "$AG" | grep 'mssql-ag-0' | head -1 | sed 's/mssql-ag-0,//')
  R1=$(echo "$AG" | grep 'mssql-ag-1' | head -1 | sed 's/mssql-ag-1,//')
  R2=$(echo "$AG" | grep 'mssql-ag-2' | head -1 | sed 's/mssql-ag-2,//')
  POD0=$(kubectl get pod mssql-ag-0 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "N/A")
  POD1=$(kubectl get pod mssql-ag-1 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "N/A")
  POD2=$(kubectl get pod mssql-ag-2 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "N/A")
  echo "${TS},${R0:-QUERY_FAILED},${R1:-QUERY_FAILED},${R2:-QUERY_FAILED},${POD0},${POD1},${POD2}" >> "$LOGFILE"
  sleep 1
done
