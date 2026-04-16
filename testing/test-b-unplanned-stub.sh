#!/bin/bash
# test-b-unplanned-stub.sh — Unplanned failover x3 (no load).
# Kills the primary pod 3 times and waits for operator to promote + resync.
set -uo pipefail
SA="YourStrong!Passw0rd"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-b-stub.txt"
CSV="${LOGDIR}/ag-health-test-b-stub.csv"
READY_CAP=120

sqlcmd_pod() { kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q "$2" 2>/dev/null; }

get_primary() {
    # Prefer the CR status (authoritative after operator reconciliation)
    local p
    p=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
    if [ -n "$p" ]; then echo "$p"; return 0; fi
    # Fallback: local query
    local q="SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1"
    for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
        role=$(sqlcmd_pod "$pod" "$q" 2>/dev/null | head -1 | tr -d ' ') || continue
        if [ "$role" = "PRIMARY" ]; then echo "$pod"; return 0; fi
    done
    return 1
}

wait_ready() {
    local cap=${1:-$READY_CAP} elapsed=0
    local q="SET NOCOUNT ON;
SELECT (SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states rs
        JOIN sys.availability_groups ag ON rs.group_id = ag.group_id
        WHERE ag.name = 'AG1' AND rs.synchronization_health_desc != 'HEALTHY')
     + (SELECT COUNT(*) FROM sys.dm_hadr_database_replica_states drs
        JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id
        JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
        WHERE ag.name = 'AG1' AND drs.is_local = 1 AND drs.synchronization_state_desc != 'SYNCHRONIZED');"
    while [ $elapsed -lt $cap ]; do
        local all_ok=true
        for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
            val=$(sqlcmd_pod "$pod" "$q" 2>/dev/null | head -1 | tr -d ' ') || { all_ok=false; break; }
            if [ "$val" != "0" ]; then all_ok=false; break; fi
        done
        if $all_ok; then
            echo "  HEALTHY+SYNCHRONIZED (all pods agree) after ${elapsed}s"
            return 0
        fi
        sleep 3; elapsed=$((elapsed + 3))
    done
    echo "  FAIL: replicas not HEALTHY+SYNCHRONIZED after ${cap}s"
    return 1
}

# Start monitor
bash "$(dirname "$0")/monitor-ag.sh" "$CSV" &
MONITOR_PID=$!
trap "kill $MONITOR_PID 2>/dev/null" EXIT

echo "Starting unplanned failover test (stub DB, no load)..." | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"

declare -a KILL_TIMES KILL_RESULTS KILL_PODS
ABORTED=false
for round in 1 2 3; do
    # Pre-kill: ensure all healthy
    echo "" | tee -a "$REPORT"
    echo "  Pre-kill: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        KILL_TIMES+=("0"); KILL_RESULTS+=("FAIL"); KILL_PODS+=("unknown")
        ABORTED=true; break
    fi

    PRIMARY=$(get_primary) || { echo "  FAIL: cannot determine primary" | tee -a "$REPORT"; ABORTED=true; break; }
    KILL_PODS+=("$PRIMARY")
    echo "=== KILL $round/3: deleting $PRIMARY at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"

    kill_start=$(date -u +%s)
    kubectl delete pod "$PRIMARY" --grace-period=0 --force 2>&1 | tee -a "$REPORT"

    echo "  Waiting for operator failover + full recovery..." | tee -a "$REPORT"
    # Wait for pod to come back
    kubectl wait --for=condition=Ready "pod/$PRIMARY" --timeout=120s 2>&1 | tee -a "$REPORT" || true

    ok=true
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        ok=false
    fi

    kill_end=$(date -u +%s)
    dur=$((kill_end - kill_start))
    KILL_TIMES+=("$dur")
    $ok && KILL_RESULTS+=("PASS") || KILL_RESULTS+=("FAIL")
    echo "  Result: ${dur}s $($ok && echo PASS || echo FAIL)" | tee -a "$REPORT"

    NEW_PRIMARY=$(get_primary) || NEW_PRIMARY="unknown"
    echo "  New primary: $NEW_PRIMARY" | tee -a "$REPORT"
done

kill $MONITOR_PID 2>/dev/null; wait $MONITOR_PID 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST B SUMMARY (Stub DB) ===" | tee -a "$REPORT"
printf "%-5s %-15s %-10s %-8s\n" "Kill" "Pod" "Time(s)" "Result" | tee -a "$REPORT"
for i in "${!KILL_TIMES[@]}"; do
    printf "%-5s %-15s %-10s %-8s\n" "$((i+1))" "${KILL_PODS[$i]}" "${KILL_TIMES[$i]}" "${KILL_RESULTS[$i]}" | tee -a "$REPORT"
done
$ABORTED && echo "TEST ABORTED" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
