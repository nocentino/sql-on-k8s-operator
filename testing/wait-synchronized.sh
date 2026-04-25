#!/bin/bash
SA="YourStrong!Passw0rd"

# Discover AG pods dynamically so this works for any replica count.
AG_PODS=()
while IFS= read -r pod; do
  [ -n "$pod" ] && AG_PODS+=("$pod")
done < <(kubectl get pods -l app=mssql-ag \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
REPLICA_COUNT=${#AG_PODS[@]}
if [ "$REPLICA_COUNT" -eq 0 ]; then
  echo "ERROR: no pods with label app=mssql-ag found"; exit 1
fi

# Find the current primary (must query from primary to see all replicas)
PRIMARY=""
for pod in "${AG_PODS[@]}"; do
  role=$(kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
    -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W \
    -Q "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" \
    2>/dev/null | grep "^PRIMARY$" | head -1 | tr -d ' ') || true
  if [ "$role" = "PRIMARY" ]; then PRIMARY="$pod"; break; fi
done
if [ -z "$PRIMARY" ]; then
  echo "WARNING: could not detect primary; falling back to ${AG_PODS[0]}"
  PRIMARY="${AG_PODS[0]}"
fi
echo "Querying from primary: $PRIMARY"

echo "Waiting for AG databases SYNCHRONIZED on all ${REPLICA_COUNT} replicas..."
for i in $(seq 1 120); do
  result=$(kubectl exec "$PRIMARY" -- /opt/mssql-tools18/bin/sqlcmd \
    -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT r.replica_server_name + ' ' + ISNULL(drs.synchronization_state_desc, 'SEEDING') + ' ' + rs.synchronization_health_desc FROM sys.availability_replicas r JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id=rs.replica_id LEFT JOIN sys.dm_hadr_database_replica_states drs ON rs.replica_id=drs.replica_id ORDER BY r.replica_server_name;" \
    2>/dev/null | grep mssql-ag)

  echo "$((i*5))s:"
  echo "$result" | sed 's/^/  /'

  not_sync=$(echo "$result" | grep -v "SYNCHRONIZED " | grep mssql-ag | grep -v "^$" | wc -l | tr -d ' ')
  total=$(echo "$result" | grep mssql-ag | wc -l | tr -d ' ')
  if [ "$total" -ge "$REPLICA_COUNT" ] && [ "$not_sync" -eq 0 ]; then
    echo ""
    echo "ALL ${REPLICA_COUNT} replicas SYNCHRONIZED at $((i*5))s"
    exit 0
  fi
  sleep 5
done
echo "TIMEOUT waiting for synchronization"
exit 1
