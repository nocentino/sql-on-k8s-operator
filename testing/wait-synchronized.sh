#!/bin/bash
SA="YourStrong!Passw0rd"
echo "Waiting for AG databases SYNCHRONIZED on all 3 replicas..."
for i in $(seq 1 120); do
  result=$(kubectl exec mssql-ag-0 -- /opt/mssql-tools18/bin/sqlcmd \
    -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT r.replica_server_name + ' ' + ISNULL(drs.synchronization_state_desc, 'SEEDING') + ' ' + rs.synchronization_health_desc FROM sys.availability_replicas r JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id=rs.replica_id LEFT JOIN sys.dm_hadr_database_replica_states drs ON rs.replica_id=drs.replica_id ORDER BY r.replica_server_name;" \
    2>/dev/null | grep mssql-ag)

  echo "$((i*5))s:"
  echo "$result" | sed 's/^/  /'

  not_sync=$(echo "$result" | grep -v "SYNCHRONIZED " | grep mssql-ag | grep -v "^$" | wc -l | tr -d ' ')
  total=$(echo "$result" | grep mssql-ag | wc -l | tr -d ' ')
  if [ "$total" -ge 3 ] && [ "$not_sync" -eq 0 ]; then
    echo ""
    echo "ALL 3 replicas SYNCHRONIZED at $((i*5))s"
    exit 0
  fi
  sleep 5
done
echo "TIMEOUT waiting for synchronization"
exit 1
