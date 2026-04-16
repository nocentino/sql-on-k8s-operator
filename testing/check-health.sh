#!/bin/bash
SA="YourStrong!Passw0rd"
echo "Waiting for all replicas to be HEALTHY and SYNCHRONIZED..."
for i in $(seq 1 60); do
  PRIMARY=""
  for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    ROLE=$(kubectl exec $pod -n default -c mssql -- /opt/mssql-tools18/bin/sqlcmd \
      -S localhost -U sa -P "$SA" -C -h -1 -Q \
      "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1;" \
      2>/dev/null | tr -d '[:space:]')
    if [ "$ROLE" = "PRIMARY" ]; then
      PRIMARY=$pod
      break
    fi
  done

  if [ -z "$PRIMARY" ]; then
    echo "  $(date '+%H:%M:%S') No primary found"
    sleep 5
    continue
  fi

  RESULT=$(kubectl exec $PRIMARY -n default -c mssql -- /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA" -C -h -1 -W -Q "
    SET NOCOUNT ON;
    SELECT ar.replica_server_name, rs.role_desc, ISNULL(drs.synchronization_state_desc,'UNKNOWN'), rs.synchronization_health_desc
    FROM sys.availability_groups ag
    JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
    JOIN sys.dm_hadr_availability_replica_states rs ON ar.replica_id = rs.replica_id
    LEFT JOIN sys.dm_hadr_database_replica_states drs ON drs.replica_id = ar.replica_id
    WHERE ag.name = 'AG1'
    ORDER BY ar.replica_server_name;" 2>/dev/null | grep mssql-ag)

  echo "  $(date '+%H:%M:%S') [from $PRIMARY]:"
  echo "$RESULT" | sed 's/^/    /'

  NOT_HEALTHY=$(echo "$RESULT" | grep -v "HEALTHY" | grep mssql-ag | wc -l | tr -d ' ')
  NOT_SYNC=$(echo "$RESULT" | grep -v "SYNCHRONIZED" | grep mssql-ag | wc -l | tr -d ' ')
  TOTAL=$(echo "$RESULT" | grep mssql-ag | wc -l | tr -d ' ')

  if [ "$TOTAL" -ge 3 ] && [ "$NOT_HEALTHY" -eq 0 ] && [ "$NOT_SYNC" -eq 0 ]; then
    echo ""
    echo "ALL REPLICAS HEALTHY + SYNCHRONIZED"
    exit 0
  fi
  sleep 5
done
echo "TIMEOUT"
exit 1
