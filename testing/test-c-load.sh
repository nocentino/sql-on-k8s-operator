#!/bin/bash
# test-c-load.sh — PreStop hook validation under TPC-C load
# Validates that deleting primary with grace-period=30 triggers preStop failover,
# NOT an operator-driven unplanned failover.
set -uo pipefail
SA='YourStrong!Passw0rd'
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-c-load.txt"
CSV="${LOGDIR}/ag-health-test-c-load.csv"
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
        hammerdb > /tmp/hammerdb-test-c-load.log 2>&1 &
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
bash "$(dirname "$0")/logbackup-loop.sh" 30 >> "${LOGDIR}/logbackup-test-c-load.log" 2>&1 &
LOGBACKUP_PID=$!
HAMMERDB_PID=""
trap "kill $MONITOR_PID $LOGBACKUP_PID 2>/dev/null; stop_hammerdb; true" EXIT

echo "=== TEST C: PreStop Hook Validation (TPCC-5G Load) ===" | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"

echo "" | tee -a "$REPORT"
echo "Pre-test: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
    echo "ABORTED: AG not healthy before test start" | tee -a "$REPORT"
    exit 1
fi

# Start HammerDB — no ramp-up per test plan
start_hammerdb 2>&1 | tee -a "$REPORT"
echo "Letting HammerDB run 5 minutes before starting deletes..." | tee -a "$REPORT"
sleep 300

PASS_COUNT=0
FAIL_COUNT=0

for round in 1 2 3; do
    echo "" | tee -a "$REPORT"
    echo "=== ROUND $round/3 ===" | tee -a "$REPORT"

    echo "  Pre-round: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        echo "  FAIL: AG not healthy before round $round" | tee -a "$REPORT"
        FAIL_COUNT=$((FAIL_COUNT+1))
        continue
    fi

    OLD_PRIMARY=$(get_primary) || { echo "  FAIL: cannot determine primary" | tee -a "$REPORT"; FAIL_COUNT=$((FAIL_COUNT+1)); continue; }
    echo "  Current primary: $OLD_PRIMARY" | tee -a "$REPORT"

    # Stop HammerDB before delete (it will disconnect anyway)
    stop_hammerdb

    delete_start=$(date -u +%s)
    echo "  Deleting $OLD_PRIMARY with grace-period=30 at $(date -u +%H:%M:%S)..." | tee -a "$REPORT"
    kubectl delete pod "$OLD_PRIMARY" --grace-period=30 2>&1 | tee -a "$REPORT"

    # Immediately check who is PRIMARY on the surviving pods
    sleep 3
    echo "  Checking primary on surviving pods (3s after delete)..." | tee -a "$REPORT"
    NEW_PRIMARY=""
    for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
        [ "$pod" = "$OLD_PRIMARY" ] && continue
        role=$(sqlcmd_pod "$pod" "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" 2>/dev/null | head -1 | tr -d ' ') || continue
        if [ "$role" = "PRIMARY" ]; then
            NEW_PRIMARY="$pod"
            break
        fi
    done

    if [ -n "$NEW_PRIMARY" ]; then
        echo "  New primary detected: $NEW_PRIMARY (preStop hook likely succeeded)" | tee -a "$REPORT"
    else
        echo "  WARNING: No primary found on surviving pods yet" | tee -a "$REPORT"
    fi

    # Wait for old pod to come back
    echo "  Waiting for $OLD_PRIMARY to restart..." | tee -a "$REPORT"
    kubectl wait --for=condition=Ready "pod/$OLD_PRIMARY" --timeout=120s 2>&1 | tee -a "$REPORT" || true

    # Wait for full recovery
    echo "  Post-delete: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        echo "  FAIL: not recovered within ${READY_CAP}s" | tee -a "$REPORT"
        FAIL_COUNT=$((FAIL_COUNT+1))
        delete_end=$(date -u +%s)
        dur=$((delete_end - delete_start))
        echo "  Duration: ${dur}s FAIL" | tee -a "$REPORT"
        # Try restarting hammerdb anyway
        start_hammerdb 2>&1 | tee -a "$REPORT"
        sleep 10
        continue
    fi

    delete_end=$(date -u +%s)
    dur=$((delete_end - delete_start))

    # Verify the old primary came back as SECONDARY
    old_role=$(sqlcmd_pod "$OLD_PRIMARY" "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" 2>/dev/null | head -1 | tr -d ' ') || old_role="UNKNOWN"
    FINAL_PRIMARY=$(get_primary) || FINAL_PRIMARY="unknown"

    echo "  Duration: ${dur}s" | tee -a "$REPORT"
    echo "  Old primary ($OLD_PRIMARY) role after recovery: $old_role" | tee -a "$REPORT"
    echo "  Current primary: $FINAL_PRIMARY" | tee -a "$REPORT"

    # Validation checks
    round_pass=true

    # Check 1: New primary is not the killed pod
    if [ "$FINAL_PRIMARY" = "$OLD_PRIMARY" ]; then
        echo "  CHECK 1 FAIL: primary is still $OLD_PRIMARY — preStop may not have fired" | tee -a "$REPORT"
        round_pass=false
    else
        echo "  CHECK 1 PASS: primary moved from $OLD_PRIMARY to $FINAL_PRIMARY" | tee -a "$REPORT"
    fi

    # Check 2: Old primary is now SECONDARY
    if [ "$old_role" = "SECONDARY" ]; then
        echo "  CHECK 2 PASS: $OLD_PRIMARY is now SECONDARY" | tee -a "$REPORT"
    else
        echo "  CHECK 2 FAIL: $OLD_PRIMARY role is $old_role (expected SECONDARY)" | tee -a "$REPORT"
        round_pass=false
    fi

    # Check 3: New primary detected within 3s
    if [ -n "$NEW_PRIMARY" ]; then
        echo "  CHECK 3 PASS: new primary ($NEW_PRIMARY) detected within 3s of delete" | tee -a "$REPORT"
    else
        echo "  CHECK 3 FAIL: no primary found on surviving pods within 3s" | tee -a "$REPORT"
        round_pass=false
    fi

    # Check 4: Operator logs show no "unplanned failover"
    UNPLANNED_COUNT=0
    for opod in $(kubectl get pods -n sql-on-k8s-operator-system -l control-plane=controller-manager -o name 2>/dev/null); do
        uc=$(kubectl logs -n sql-on-k8s-operator-system "$opod" -c manager --since=120s 2>/dev/null | grep -c "unplanned failover" || true)
        UNPLANNED_COUNT=$((UNPLANNED_COUNT + uc))
    done
    if [ "$UNPLANNED_COUNT" -eq 0 ]; then
        echo "  CHECK 4 PASS: no 'unplanned failover' in operator logs — preStop handled it" | tee -a "$REPORT"
    else
        echo "  CHECK 4 FAIL: operator logged 'unplanned failover' $UNPLANNED_COUNT time(s)" | tee -a "$REPORT"
        round_pass=false
    fi

    echo "  Total recovery (including pod restart + re-sync): ${dur}s" | tee -a "$REPORT"

    if $round_pass; then
        echo "  ROUND $round: PASS (${dur}s)" | tee -a "$REPORT"
        PASS_COUNT=$((PASS_COUNT+1))
    else
        echo "  ROUND $round: FAIL (${dur}s)" | tee -a "$REPORT"
        FAIL_COUNT=$((FAIL_COUNT+1))
    fi

    # Restart HammerDB after recovery
    echo "  Restarting HammerDB..." | tee -a "$REPORT"
    sleep 5
    start_hammerdb 2>&1 | tee -a "$REPORT"
    sleep 10
done

stop_hammerdb
kill $LOGBACKUP_PID $MONITOR_PID 2>/dev/null; wait $LOGBACKUP_PID $MONITOR_PID 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST C SUMMARY (PreStop Hook, TPCC-5G Load) ===" | tee -a "$REPORT"
echo "PASS: $PASS_COUNT / 3" | tee -a "$REPORT"
echo "FAIL: $FAIL_COUNT / 3" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
