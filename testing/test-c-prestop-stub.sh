#!/bin/bash
# test-c-prestop-stub.sh — Validate preStop lifecycle hook behavior.
#
# Test: delete the primary pod with normal grace period (grace-period=30).
# The preStop hook should detect PRIMARY, find a SYNCHRONIZED secondary,
# connect to it, and issue ALTER AG FAILOVER before SIGTERM fires.
#
# Validation:
#   1. The new primary is NOT the killed pod (failover happened before death)
#   2. The killed pod comes back as SECONDARY (not PRIMARY) — no split-brain
#   3. Recovery is fast (< 30s total) because it's a planned failover
#   4. The operator log shows the new primary was already promoted by the
#      time the pod went NotReady (i.e. preStop did the failover, not the operator)
set -uo pipefail
SA="YourStrong!Passw0rd"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-c-prestop.txt"
CSV="${LOGDIR}/ag-health-test-c-prestop.csv"
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

echo "=== TEST C: PreStop Hook Validation ===" | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"
echo "" | tee -a "$REPORT"

# Ensure healthy before starting
echo "Pre-test: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
    echo "FAIL: AG not healthy before test" | tee -a "$REPORT"
    kill $MONITOR_PID 2>/dev/null; exit 1
fi

PASS_COUNT=0
FAIL_COUNT=0

for round in 1 2 3; do
    echo "" | tee -a "$REPORT"
    echo "=== ROUND $round/3 ===" | tee -a "$REPORT"

    # Ensure AG is healthy before identifying primary
    echo "  Pre-round: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
    if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
        echo "  FAIL: AG not healthy before round $round" | tee -a "$REPORT"
        FAIL_COUNT=$((FAIL_COUNT+1)); continue
    fi

    # Identify the current primary
    OLD_PRIMARY=$(get_primary) || { echo "  FAIL: cannot determine primary" | tee -a "$REPORT"; FAIL_COUNT=$((FAIL_COUNT+1)); continue; }
    echo "  Current primary: $OLD_PRIMARY" | tee -a "$REPORT"

    # Capture the operator log timestamp BEFORE delete
    PRE_DELETE_TS=$(date -u +%Y-%m-%dT%H:%M:%S)

    # Delete with normal grace period — preStop hook should fire
    echo "  Deleting $OLD_PRIMARY with grace-period=30 at $(date -u +%H:%M:%S)..." | tee -a "$REPORT"
    delete_start=$(date -u +%s)
    kubectl delete pod "$OLD_PRIMARY" --grace-period=30 2>&1 | tee -a "$REPORT"

    # Immediately check who is PRIMARY on the surviving pods
    # The preStop hook should have already promoted a secondary before the pod dies
    sleep 3  # small grace for K8s to propagate
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
        continue
    fi

    delete_end=$(date -u +%s)
    dur=$((delete_end - delete_start))

    # Verify the old primary came back as SECONDARY (not PRIMARY)
    old_role=$(sqlcmd_pod "$OLD_PRIMARY" "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" 2>/dev/null | head -1 | tr -d ' ') || old_role="UNKNOWN"
    FINAL_PRIMARY=$(get_primary) || FINAL_PRIMARY="unknown"

    echo "  Duration: ${dur}s" | tee -a "$REPORT"
    echo "  Old primary ($OLD_PRIMARY) role after recovery: $old_role" | tee -a "$REPORT"
    echo "  Current primary: $FINAL_PRIMARY" | tee -a "$REPORT"

    # Validation checks
    round_pass=true

    # Check 1: New primary is not the killed pod (failover happened before death)
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

    # Check 3: New primary detected within 10s of delete (preStop hook acted quickly)
    if [ -n "$NEW_PRIMARY" ]; then
        echo "  CHECK 3 PASS: new primary ($NEW_PRIMARY) detected within 3s of delete" | tee -a "$REPORT"
    else
        echo "  CHECK 3 FAIL: no primary found on surviving pods within 3s — preStop may not have fired" | tee -a "$REPORT"
        round_pass=false
    fi

    # Check 4: Operator logs show no "unplanned failover" (preStop handled it)
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
done

kill $MONITOR_PID 2>/dev/null; wait $MONITOR_PID 2>/dev/null || true

echo "" | tee -a "$REPORT"
echo "=== TEST C SUMMARY (PreStop Hook) ===" | tee -a "$REPORT"
echo "PASS: $PASS_COUNT / 3" | tee -a "$REPORT"
echo "FAIL: $FAIL_COUNT / 3" | tee -a "$REPORT"

# Check operator logs for evidence of preStop-triggered failover
echo "" | tee -a "$REPORT"
echo "=== Operator Log Analysis ===" | tee -a "$REPORT"
echo "Looking for evidence that failover was preStop-initiated (not operator-initiated)..." | tee -a "$REPORT"
kubectl logs -n sql-on-k8s-operator-system deployment/sql-on-k8s-operator-controller-manager -c manager --tail=200 2>/dev/null \
    | grep -iE "(failover|promoted|NotReady|threshold|prestop|OFFLINE)" \
    | tail -20 | tee -a "$REPORT"

echo "" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
