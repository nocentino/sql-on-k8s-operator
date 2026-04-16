#!/bin/bash
# test-a-load.sh — Planned failover rotation 0->1->2->0 under TPC-C load
# Per test plan: no ramp-up, restart HammerDB after each failover, consensus wait_ready.
set -uo pipefail
SA='YourStrong!Passw0rd'
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-a-load.txt"
CSV="${LOGDIR}/ag-health-test-a-load.csv"
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

wait_target_synced() {
    local tgt=$1 cap=${2:-60} elapsed=0
    local q="SET NOCOUNT ON;
SELECT CASE WHEN EXISTS(
    SELECT 1 FROM sys.dm_hadr_database_replica_states
    WHERE is_local=1 AND synchronization_state_desc='SYNCHRONIZED'
) THEN 0 ELSE 1 END;"
    while [ $elapsed -lt $cap ]; do
        val=$(sqlcmd_pod "$tgt" "$q" 2>/dev/null | head -1 | tr -d ' ') || { sleep 2; elapsed=$((elapsed+2)); continue; }
        if [ "$val" = "0" ]; then
            echo "  Target $tgt database SYNCHRONIZED locally after ${elapsed}s"
            return 0
        fi
        sleep 2; elapsed=$((elapsed + 2))
    done
    echo "  FAIL: target $tgt database not SYNCHRONIZED locally after ${cap}s"
    return 1
}

wait_primary_on() {
    local target=$1 max=${2:-30} elapsed=0
    local q="SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1"
    while [ $elapsed -lt $max ]; do
        role=$(sqlcmd_pod "$target" "$q" | grep -E "^PRIMARY$" | head -1 | tr -d ' ') || true
        [ "$role" = "PRIMARY" ] && return 0
        sleep 2; elapsed=$((elapsed + 2))
    done
    echo "  WARNING: $target did not confirm PRIMARY within ${max}s"; return 1
}

start_hammerdb() {
    cd /Users/anocentino/Documents/GitHub/hammerdb
    docker compose run --rm --remove-orphans \
        -e RUN_MODE=load -e BENCHMARK=tprocc \
        hammerdb > /tmp/hammerdb-test-a-load.log 2>&1 &
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
bash "$(dirname "$0")/logbackup-loop.sh" 30 >> "${LOGDIR}/logbackup-test-a-load.log" 2>&1 &
LOGBACKUP_PID=$!
HAMMERDB_PID=""
trap "kill $MONITOR_PID $LOGBACKUP_PID 2>/dev/null; stop_hammerdb; true" EXIT

echo "Starting planned failover test (TPCC-5G load)..." | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"

# Start HammerDB — no ramp-up per test plan
start_hammerdb 2>&1 | tee -a "$REPORT"
echo "Letting HammerDB run 5 minutes before starting failovers..." | tee -a "$REPORT"
sleep 300

TARGETS=("mssql-ag-1" "mssql-ag-2" "mssql-ag-0")
declare -a FO_TIMES FO_RESULTS FO_NOTES FO_DIRS
ABORTED=false
for i in 0 1 2; do
    TGT="${TARGETS[$i]}"

    echo "" | tee -a "$REPORT"
    echo "  Pre-failover: waiting for HEALTHY+SYNCHRONIZED (cap ${READY_CAP}s)..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        FO_TIMES+=("0"); FO_RESULTS+=("FAIL"); FO_NOTES+=("NotReady-aborted"); FO_DIRS+=("?->${TGT}")
        ABORTED=true; break
    fi
    sleep 3

    SRC=$(get_primary) || { echo "  FAIL: cannot determine primary" | tee -a "$REPORT"; ABORTED=true; break; }
    echo "=== FAILOVER $((i+1))/3: $SRC -> $TGT at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"
    FO_DIRS+=("${SRC}->${TGT}")

    if [ "$SRC" = "$TGT" ]; then
        echo "  SKIP: $TGT is already PRIMARY" | tee -a "$REPORT"
        FO_TIMES+=("0"); FO_RESULTS+=("SKIP"); FO_NOTES+=("already-primary")
        continue
    fi

    echo "  Pre-failover: verifying target $TGT SYNCHRONIZED locally..." | tee -a "$REPORT"
    if ! wait_target_synced "$TGT" 60 2>&1 | tee -a "$REPORT"; then
        FO_TIMES+=("0"); FO_RESULTS+=("FAIL"); FO_NOTES+=("TargetNotSynced-aborted")
        ABORTED=true; break
    fi

    # Stop HammerDB before failover to avoid connection errors during transition
    stop_hammerdb

    fo_start=$(date -u +%s)
    note=""
    fo_out=$(kubectl exec "$TGT" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C -Q \
        "EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes'; ALTER AVAILABILITY GROUP [AG1] FAILOVER;" 2>&1)
    echo "$fo_out" | tee -a "$REPORT"
    if echo "$fo_out" | grep -q "41142"; then
        note="Msg41142-rejected"
    elif echo "$fo_out" | grep -q "41122"; then
        note="Msg41122-already-primary"
    else
        wait_primary_on "$TGT" 30 2>&1 | tee -a "$REPORT"
    fi

    echo "  Post-failover: waiting for HEALTHY+SYNCHRONIZED (cap ${READY_CAP}s)..." | tee -a "$REPORT"
    ok=true
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        ok=false; note="${note:+${note},}NotHealthy-post-failover"
    fi

    fo_end=$(date -u +%s)
    dur=$((fo_end - fo_start))
    FO_TIMES+=("$dur"); FO_NOTES+=("$note")
    $ok && FO_RESULTS+=("PASS") || FO_RESULTS+=("FAIL")
    echo "  Result: ${dur}s $($ok && echo PASS || echo FAIL) ${note:-}" | tee -a "$REPORT"

    # Restart HammerDB after failover
    echo "  Restarting HammerDB after failover..." | tee -a "$REPORT"
    sleep 5
    start_hammerdb 2>&1 | tee -a "$REPORT"
    sleep 10  # let load ramp briefly
done

stop_hammerdb
kill $LOGBACKUP_PID $MONITOR_PID 2>/dev/null; wait $LOGBACKUP_PID $MONITOR_PID 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST A SUMMARY (TPCC-5G Load) ===" | tee -a "$REPORT"
printf "%-5s %-25s %-10s %-8s %-15s\n" "Hop" "Direction" "Time(s)" "Result" "Notes" | tee -a "$REPORT"
for i in "${!FO_TIMES[@]}"; do
    printf "%-5s %-25s %-10s %-8s %-15s\n" "$((i+1))" "${FO_DIRS[$i]}" "${FO_TIMES[$i]}" "${FO_RESULTS[$i]}" "${FO_NOTES[$i]:-}" | tee -a "$REPORT"
done
$ABORTED && echo "TEST ABORTED" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
#!/bin/bash
# test-a-load.sh — Planned failover rotation 0->1->2->0 under TPC-C load
# Usage: bash test-a-load.sh <listener-ip>
set -uo pipefail
SA="YourStrong!Passw0rd"
LISTENER_IP="${1:?Usage: $0 <listener-ip>}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPORT="${DIR}/report-test-a-load.txt"
CSV="${DIR}/ag-health-test-a-load.csv"
ROTATIONS=("mssql-ag-0:mssql-ag-1" "mssql-ag-1:mssql-ag-2" "mssql-ag-2:mssql-ag-0")

sqlcmd_pod() { kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q "$2" 2>/dev/null; }

wait_healthy() {
    local max=${1:-180} elapsed=0
    local q="SET NOCOUNT ON; SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states WHERE synchronization_health_desc != 'HEALTHY'"
    while [ $elapsed -lt $max ]; do
        for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
            val=$(sqlcmd_pod "$pod" "$q" | head -1 | tr -d ' ') || continue
            if [ "$val" = "0" ]; then echo "  HEALTHY after ${elapsed}s"; return 0; fi
            break
        done
        sleep 3; elapsed=$((elapsed + 3))
    done
    echo "  WARNING: not all HEALTHY after ${max}s"; return 1
}

wait_primary_on() {
    local target=$1 max=${2:-60} elapsed=0
    local q="SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1"
    while [ $elapsed -lt $max ]; do
        role=$(sqlcmd_pod "$target" "$q" | grep -E "^PRIMARY$" | head -1 | tr -d ' ') || true
        [ "$role" = "PRIMARY" ] && return 0
        sleep 2; elapsed=$((elapsed + 2))
    done
    echo "  WARNING: $target did not confirm PRIMARY within ${max}s"; return 1
}

# Start health monitor and log backup loop
bash "$(dirname "$0")/monitor-ag.sh" "$CSV" &
MONITOR_PID=$!
bash "$(dirname "$0")/logbackup-loop.sh" 30 >> "${DIR}/logbackup-test-a-load.log" 2>&1 &
LOGBACKUP_PID=$!
trap "kill $MONITOR_PID $LOGBACKUP_PID $HAMMERDB_PID 2>/dev/null; true" EXIT

# Start HammerDB against the listener IP
cd /Users/anocentino/Documents/GitHub/hammerdb
HAMMERDB_PID=""
docker compose run --remove-orphans \
    -e RUN_MODE=load -e BENCHMARK=tprocc \
    -e SERVER="$LISTENER_IP" -e PORT=1433 \
    hammerdb > /tmp/hammerdb-test-a-load.log 2>&1 &
HAMMERDB_PID=$!
echo "HammerDB started (PID $HAMMERDB_PID, target=$LISTENER_IP). Waiting 90s ramp-up..." | tee "$REPORT"
sleep 90

declare -a FO_TIMES FO_RESULTS FO_NOTES
for i in 0 1 2; do
    SRC=$(echo "${ROTATIONS[$i]}" | cut -d: -f1)
    TGT=$(echo "${ROTATIONS[$i]}" | cut -d: -f2)
    echo "" | tee -a "$REPORT"
    echo "=== FAILOVER $((i+1))/3: $SRC -> $TGT at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"
    fo_start=$(date -u +%s)
    note=""
    fo_out=$(kubectl exec "$TGT" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C -Q \
        "EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes'; ALTER AVAILABILITY GROUP [AG1] FAILOVER;" 2>&1)
    echo "$fo_out" | tee -a "$REPORT"
    if echo "$fo_out" | grep -q "41142"; then
        note="Msg41142-rejected"
        echo "  NOTE: Msg 41142 — failover rejected (not synchronized)" | tee -a "$REPORT"
    else
        wait_primary_on "$TGT" 60 | tee -a "$REPORT"
    fi
    echo "  Waiting for HEALTHY..." | tee -a "$REPORT"
    ok=true; wait_healthy 180 2>&1 | tee -a "$REPORT" || ok=false
    fo_end=$(date -u +%s)
    dur=$((fo_end - fo_start))
    FO_TIMES+=("$dur"); FO_NOTES+=("$note")
    $ok && FO_RESULTS+=("PASS") || FO_RESULTS+=("FAIL")
    echo "  Result: ${dur}s $($ok && echo PASS || echo FAIL) $note" | tee -a "$REPORT"
    sleep 15
done

kill $HAMMERDB_PID 2>/dev/null; wait $HAMMERDB_PID 2>/dev/null || true
kill $LOGBACKUP_PID $MONITOR_PID 2>/dev/null; wait $LOGBACKUP_PID $MONITOR_PID 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST A (LOAD) SUMMARY ===" | tee -a "$REPORT"
printf "%-5s %-25s %-10s %-8s %-15s\n" "Hop" "Direction" "Time(s)" "Result" "Notes" | tee -a "$REPORT"
for i in 0 1 2; do
    SRC=$(echo "${ROTATIONS[$i]}" | cut -d: -f1)
    TGT=$(echo "${ROTATIONS[$i]}" | cut -d: -f2)
    printf "%-5s %-25s %-10s %-8s %-15s\n" "$((i+1))" "${SRC}->${TGT}" "${FO_TIMES[$i]}" "${FO_RESULTS[$i]}" "${FO_NOTES[$i]:-}" | tee -a "$REPORT"
done
echo "NOT_HEALTHY samples: $(grep -c NOT_HEALTHY "$CSV" 2>/dev/null || echo 0)" | tee -a "$REPORT"
echo "Total samples: $(wc -l < "$CSV")" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
