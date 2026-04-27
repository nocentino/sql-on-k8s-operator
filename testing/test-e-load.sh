#!/bin/bash
# test-e-load.sh — SIGSTOP gray-failure 3 times under TPC-C load.
#
# Simulates a frozen primary (kernel keeps TCP sockets open, sqlservr stops
# scheduling threads). The pod stays Running+Ready until the readiness probe
# fails (~45s), then the operator threshold timer fires and promotes a
# synchronous secondary.
#
# Mirrors test-e-sigstop-stub.sh but adds HammerDB load and runs 3 rounds.
# Per test plan: stop HammerDB before SIGSTOP (it disconnects anyway), restart
# after full recovery.
set -uo pipefail
SA='YourStrong!Passw0rd'
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-e-load.txt"
CSV="${LOGDIR}/ag-health-test-e-load.csv"
READY_CAP=300   # SIGSTOP failover is slower than the fast-path kill (~200s typical).

sqlcmd_pod() { kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q "$2" 2>/dev/null; }

get_primary() {
    local p
    p=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
    if [ -n "$p" ]; then echo "$p"; return 0; fi
    local q="SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1"
    while IFS= read -r pod; do
        role=$(sqlcmd_pod "$pod" "$q" 2>/dev/null | head -1 | tr -d ' ') || continue
        if [ "$role" = "PRIMARY" ]; then echo "$pod"; return 0; fi
    done < <(kubectl get pods -l app=mssql-ag -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
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
        while IFS= read -r pod; do
            val=$(sqlcmd_pod "$pod" "$q" 2>/dev/null | head -1 | tr -d ' ') || { all_ok=false; break; }
            if [ "$val" != "0" ]; then all_ok=false; break; fi
        done < <(kubectl get pods -l app=mssql-ag -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
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
        hammerdb > /tmp/hammerdb-test-e-load.log 2>&1 &
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

FROZEN_POD=""
cleanup() {
    # If test bailed while a pod was still SIGSTOP'd, force-delete it.
    # A frozen pod blocks all subsequent AG recovery.
    if [ -n "${FROZEN_POD:-}" ]; then
        echo "Cleanup: force-deleting frozen pod $FROZEN_POD" | tee -a "$REPORT"
        kubectl delete pod "$FROZEN_POD" --grace-period=0 --force 2>&1 | tee -a "$REPORT" || true
        FROZEN_POD=""
    fi
    kill "${MONITOR_PID:-}" "${LOGBACKUP_PID:-}" 2>/dev/null || true
    stop_hammerdb
}

# Start monitor and log backup loop
bash "$(dirname "$0")/monitor-ag.sh" "$CSV" &
MONITOR_PID=$!
bash "$(dirname "$0")/logbackup-loop.sh" 30 >> "${LOGDIR}/logbackup-test-e-load.log" 2>&1 &
LOGBACKUP_PID=$!
HAMMERDB_PID=""
trap cleanup EXIT

echo "Starting SIGSTOP gray-failure test (TPCC-5G load)..." | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"

# Start HammerDB — no ramp-up per test plan
start_hammerdb 2>&1 | tee -a "$REPORT"
echo "Letting HammerDB run 2 minutes before starting SIGSTOP rounds..." | tee -a "$REPORT"
sleep 120

declare -a STOP_TIMES STOP_RESULTS STOPPED_PODS STOP_NOTES
ABORTED=false

for i in 1 2 3; do
    echo "" | tee -a "$REPORT"

    echo "  Pre-stop: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        STOP_TIMES+=("0"); STOP_RESULTS+=("FAIL"); STOPPED_PODS+=("unknown"); STOP_NOTES+=("NotReady-pre-stop")
        ABORTED=true; break
    fi

    PRIMARY=$(get_primary) || { echo "  FAIL: cannot determine primary" | tee -a "$REPORT"; ABORTED=true; break; }
    echo "=== SIGSTOP $i/3: freezing $PRIMARY at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"
    STOPPED_PODS+=("$PRIMARY")

    # Stop HammerDB before freeze — it will disconnect when primary goes NotReady anyway
    stop_hammerdb

    stop_start=$(date -u +%s)

    # Freeze all sqlservr PIDs. SQL Server on Linux always has two sqlservr
    # processes; stopping only one leaves the other answering readiness probes.
    kubectl exec "$PRIMARY" -- bash -c \
        'pids=$(pgrep -x sqlservr); [ -n "$pids" ] && kill -STOP $pids && echo "SIGSTOP sent to pids=$pids" || { echo "FAIL: sqlservr not running"; exit 1; }' \
        2>&1 | tee -a "$REPORT"
    FROZEN_POD="$PRIMARY"

    # Wait for readiness probe to flip pod NotReady
    echo "  Waiting for pod $PRIMARY to go Ready=False..." | tee -a "$REPORT"
    NOT_READY_DETECTED=false
    for j in $(seq 1 120); do
        ready=$(kubectl get pod "$PRIMARY" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
        if [ "$ready" != "True" ]; then
            elapsed=$(($(date -u +%s) - stop_start))
            echo "  Pod $PRIMARY is Ready=$ready after ${elapsed}s" | tee -a "$REPORT"
            NOT_READY_DETECTED=true
            break
        fi
        sleep 1
    done
    if ! $NOT_READY_DETECTED; then
        echo "  FAIL: pod never flipped Ready=False within 120s" | tee -a "$REPORT"
        STOP_TIMES+=("0"); STOP_RESULTS+=("FAIL"); STOP_NOTES+=("NeverNotReady")
        ABORTED=true; break
    fi

    # Watch for operator-driven failover
    echo "  Watching for operator to promote via NotReady threshold timer..." | tee -a "$REPORT"
    FAILOVER_DETECTED=false
    for j in $(seq 1 180); do
        new_primary=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
        if [ -n "$new_primary" ] && [ "$new_primary" != "$PRIMARY" ]; then
            elapsed=$(($(date -u +%s) - stop_start))
            echo "  Failover detected at $(date -u +%H:%M:%S) (${elapsed}s after SIGSTOP)" | tee -a "$REPORT"
            echo "  New primary: $new_primary" | tee -a "$REPORT"
            FAILOVER_DETECTED=true
            break
        fi
        sleep 1
    done
    if ! $FAILOVER_DETECTED; then
        echo "  FAIL: no failover detected within 180s of NotReady" | tee -a "$REPORT"
        STOP_TIMES+=("0"); STOP_RESULTS+=("FAIL"); STOP_NOTES+=("NoFailoverDetected")
        ABORTED=true; break
    fi

    # Force-delete the frozen pod so it restarts clean and rejoins as SECONDARY.
    # SIGCONT is unreliable after a long freeze; clean restart is more predictable.
    echo "  Force-deleting frozen pod $PRIMARY so it restarts clean..." | tee -a "$REPORT"
    kubectl delete pod "$PRIMARY" --grace-period=0 --force 2>&1 | tee -a "$REPORT"
    FROZEN_POD=""

    # Wait for replacement pod
    echo "  Waiting for pod $PRIMARY to become Ready..." | tee -a "$REPORT"
    kubectl wait --for=condition=Ready "pod/$PRIMARY" --timeout=180s 2>&1 | tee -a "$REPORT" || true

    ok=true
    note=""
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        ok=false; note="NotHealthy-post-stop"
    fi

    stop_end=$(date -u +%s)
    dur=$((stop_end - stop_start))
    STOP_TIMES+=("$dur")
    $ok && STOP_RESULTS+=("PASS") || STOP_RESULTS+=("FAIL")
    STOP_NOTES+=("${note:-}")

    NEW_PRIMARY=$(get_primary) || NEW_PRIMARY="unknown"
    echo "  Result: ${dur}s $($ok && echo PASS || echo FAIL) ${note:-}" | tee -a "$REPORT"
    echo "  New primary: $NEW_PRIMARY" | tee -a "$REPORT"

    # Restart HammerDB after recovery
    echo "  Restarting HammerDB after failover..." | tee -a "$REPORT"
    sleep 5
    start_hammerdb 2>&1 | tee -a "$REPORT"
    sleep 10
done

stop_hammerdb
kill "${LOGBACKUP_PID:-}" "${MONITOR_PID:-}" 2>/dev/null; wait "${LOGBACKUP_PID:-}" "${MONITOR_PID:-}" 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST E SUMMARY (SIGSTOP / Gray Failure, TPCC-5G Load) ===" | tee -a "$REPORT"
printf "%-5s %-15s %-10s %-8s %-15s\n" "Stop" "Pod" "Time(s)" "Result" "Notes" | tee -a "$REPORT"
for i in "${!STOP_TIMES[@]}"; do
    printf "%-5s %-15s %-10s %-8s %-15s\n" "$((i+1))" "${STOPPED_PODS[$i]}" "${STOP_TIMES[$i]}" "${STOP_RESULTS[$i]}" "${STOP_NOTES[$i]:-}" | tee -a "$REPORT"
done
echo "NOT_HEALTHY samples: $(grep -c NOT_HEALTHY "$CSV" 2>/dev/null || echo 0)" | tee -a "$REPORT"
echo "Total samples: $(wc -l < "$CSV")" | tee -a "$REPORT"
$ABORTED && echo "TEST ABORTED" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
