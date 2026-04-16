#!/bin/bash
# test-b-load.sh — Kill primary pod 3 times under TPC-C load
# Per test plan: no ramp-up, restart HammerDB after each failover, consensus wait_ready.
set -uo pipefail
SA='YourStrong!Passw0rd'
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-b-load.txt"
CSV="${LOGDIR}/ag-health-test-b-load.csv"
READY_CAP=120

sqlcmd_pod() { kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q "$2" 2>/dev/null; }

get_primary() {
    local p
    p=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
    if [ -n "$p" ]; then echo "$p"; return 0; fi
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

start_hammerdb() {
    cd /Users/anocentino/Documents/GitHub/hammerdb
    docker compose run --rm --remove-orphans \
        -e RUN_MODE=load -e BENCHMARK=tprocc \
        hammerdb > /tmp/hammerdb-test-b-load.log 2>&1 &
    HAMMERDB_PID=$!
    echo "  HammerDB started (PID $HAMMERDB_PID)"
    cd "$DIR"
}

stop_hammerdb() {
    if [ -n "${HAMMERDB_PID:-}" ]; then
        kill $HAMMERDB_PID 2>/dev/null; wait $HAMMERDB_PID 2>/dev/null || true
        docker compose -f /Users/anocentino/Documents/GitHub/hammerdb/docker-compose.yaml down 2>/dev/null || true
        HAMMERDB_PID=""
    fi
}

# Start monitor and log backup loop
bash "$(dirname "$0")/monitor-ag.sh" "$CSV" &
MONITOR_PID=$!
bash "$(dirname "$0")/logbackup-loop.sh" 30 >> "${LOGDIR}/logbackup-test-b-load.log" 2>&1 &
LOGBACKUP_PID=$!
HAMMERDB_PID=""
trap "kill $MONITOR_PID $LOGBACKUP_PID 2>/dev/null; stop_hammerdb; true" EXIT

echo "Starting unplanned failover test (TPCC-5G load)..." | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"

# Start HammerDB — no ramp-up per test plan
start_hammerdb 2>&1 | tee -a "$REPORT"
echo "Letting HammerDB run 5 minutes before starting kills..." | tee -a "$REPORT"
sleep 300

declare -a KILL_TIMES KILL_RESULTS KILLED_PODS KILL_NOTES
ABORTED=false

for i in 1 2 3; do
    echo "" | tee -a "$REPORT"

    echo "  Pre-kill: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        KILL_TIMES+=("0"); KILL_RESULTS+=("FAIL"); KILLED_PODS+=("unknown"); KILL_NOTES+=("NotReady-pre-kill")
        ABORTED=true; break
    fi

    PRIMARY=$(get_primary) || { echo "  FAIL: cannot determine primary" | tee -a "$REPORT"; ABORTED=true; break; }
    echo "=== KILL $i/3: deleting $PRIMARY at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"
    KILLED_PODS+=("$PRIMARY")

    # Stop HammerDB before kill (it will disconnect anyway)
    stop_hammerdb

    kill_start=$(date -u +%s)
    kubectl delete pod "$PRIMARY" --force --grace-period=0 2>&1 | tee -a "$REPORT"

    echo "  Waiting for operator failover + full recovery..." | tee -a "$REPORT"
    kubectl wait pod/"$PRIMARY" --for=condition=Ready --timeout=120s 2>&1 | tee -a "$REPORT" || true

    ok=true
    note=""
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        ok=false; note="NotHealthy-post-kill"
    fi

    kill_end=$(date -u +%s)
    dur=$((kill_end - kill_start))
    KILL_TIMES+=("$dur")
    $ok && KILL_RESULTS+=("PASS") || KILL_RESULTS+=("FAIL")
    KILL_NOTES+=("${note:-}")

    NEW_PRIMARY=$(get_primary) || NEW_PRIMARY="unknown"
    echo "  Result: ${dur}s $($ok && echo PASS || echo FAIL) ${note:-}" | tee -a "$REPORT"
    echo "  New primary: $NEW_PRIMARY" | tee -a "$REPORT"

    # Restart HammerDB after recovery
    echo "  Restarting HammerDB after failover..." | tee -a "$REPORT"
    sleep 5
    start_hammerdb 2>&1 | tee -a "$REPORT"
    sleep 10  # let load ramp briefly
done

stop_hammerdb
kill $LOGBACKUP_PID $MONITOR_PID 2>/dev/null; wait $LOGBACKUP_PID $MONITOR_PID 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST B SUMMARY (TPCC-5G Load) ===" | tee -a "$REPORT"
printf "%-5s %-15s %-10s %-8s %-15s\n" "Kill" "Pod" "Time(s)" "Result" "Notes" | tee -a "$REPORT"
for i in "${!KILL_TIMES[@]}"; do
    printf "%-5s %-15s %-10s %-8s %-15s\n" "$((i+1))" "${KILLED_PODS[$i]}" "${KILL_TIMES[$i]}" "${KILL_RESULTS[$i]}" "${KILL_NOTES[$i]:-}" | tee -a "$REPORT"
done
$ABORTED && echo "TEST ABORTED" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
