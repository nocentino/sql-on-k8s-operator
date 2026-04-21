#!/bin/bash
# test-d-slow-failover.sh — Unplanned failover via the NotReady threshold timer (slow path).
#
# The normal unplanned failover tests (test-b) trigger the fast path: the
# StatefulSet recreates the pod so quickly that the operator detects a headless
# AG before the failoverThresholdSeconds timer fires.
#
# This test forces the SLOW path by cordoning the node before killing the
# primary pod. The StatefulSet creates the replacement, but it stays Pending
# (unschedulable) so the operator sees a NotReady/missing primary for the full
# threshold duration and promotes via the timer.
#
# Flow:
#   1. Identify the primary pod and its node.
#   2. Cordon the node (mark unschedulable).
#   3. Force-delete the primary pod.
#   4. Wait for the operator to promote via the NotReady threshold timer.
#   5. Uncordon the node so the replacement pod can be scheduled.
#   6. Wait for full HEALTHY + SYNCHRONIZED.
#
# Expected operator log messages:
#   - "Primary pod NotReady; starting failover threshold timer"
#   - "Primary pod NotReady; waiting for threshold"
#   - "Primary NotReady threshold exceeded; selecting failover target"
#   - "Automatic unplanned failover succeeded"

set -uo pipefail
SA="YourStrong!Passw0rd"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-d-slow-failover.txt"
CSV="${LOGDIR}/ag-health-test-d-slow-failover.csv"
READY_CAP=300   # Recovery takes longer because the pod stays Pending during the threshold window.

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

cleanup() {
    # Always uncordon to leave the cluster in a clean state.
    if [ -n "${CORDONED_NODE:-}" ]; then
        echo "Cleanup: uncordoning node $CORDONED_NODE" | tee -a "$REPORT"
        kubectl uncordon "$CORDONED_NODE" 2>&1 | tee -a "$REPORT"
    fi
    kill $MONITOR_PID 2>/dev/null
}

# Start monitor
bash "$(dirname "$0")/monitor-ag.sh" "$CSV" &
MONITOR_PID=$!
trap cleanup EXIT

echo "Starting slow-path failover test (NotReady threshold timer)..." | tee "$REPORT"
echo "Ready cap: ${READY_CAP}s" | tee -a "$REPORT"
echo "" | tee -a "$REPORT"

# --- Pre-check ---
echo "Pre-check: waiting for HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
    echo "FAIL: AG not healthy before test start" | tee -a "$REPORT"
    exit 1
fi

PRIMARY=$(get_primary) || { echo "FAIL: cannot determine primary" | tee -a "$REPORT"; exit 1; }
echo "Primary pod: $PRIMARY" | tee -a "$REPORT"

# --- Get the node and cordon it ---
NODE=$(kubectl get pod "$PRIMARY" -o jsonpath='{.spec.nodeName}')
echo "Primary is on node: $NODE" | tee -a "$REPORT"
echo "Cordoning node $NODE to prevent pod rescheduling..." | tee -a "$REPORT"
kubectl cordon "$NODE" 2>&1 | tee -a "$REPORT"
CORDONED_NODE="$NODE"

# --- Kill the primary ---
echo "" | tee -a "$REPORT"
echo "=== KILL: deleting $PRIMARY at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"
kill_start=$(date -u +%s)
kubectl delete pod "$PRIMARY" --grace-period=0 --force 2>&1 | tee -a "$REPORT"

# --- Wait for the replacement pod to be Pending (unschedulable) ---
echo "  Waiting for replacement pod to be Pending (unschedulable)..." | tee -a "$REPORT"
for i in $(seq 1 30); do
    phase=$(kubectl get pod "$PRIMARY" -o jsonpath='{.status.phase}' 2>/dev/null)
    if [ "$phase" = "Pending" ]; then
        echo "  Pod $PRIMARY is Pending after ${i}s" | tee -a "$REPORT"
        break
    fi
    sleep 1
done

# --- Wait for the operator to detect NotReady and fire the threshold timer ---
# The threshold is 30s. The operator should start the timer immediately and
# promote after ~30s. We watch the CR status for a primary change.
echo "  Watching for operator to promote via NotReady threshold timer..." | tee -a "$REPORT"
FAILOVER_DETECTED=false
for i in $(seq 1 90); do
    new_primary=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
    if [ -n "$new_primary" ] && [ "$new_primary" != "$PRIMARY" ]; then
        failover_time=$(date -u +%H:%M:%S)
        elapsed=$(($(date -u +%s) - kill_start))
        echo "  Failover detected at $failover_time (${elapsed}s after kill)" | tee -a "$REPORT"
        echo "  New primary: $new_primary" | tee -a "$REPORT"
        FAILOVER_DETECTED=true
        break
    fi
    sleep 1
done

if ! $FAILOVER_DETECTED; then
    echo "  FAIL: no failover detected within 90s" | tee -a "$REPORT"
    # Still uncordon to clean up
    exit 1
fi

# --- Uncordon the node so the killed pod's replacement can be scheduled ---
echo "" | tee -a "$REPORT"
echo "Uncordoning node $NODE to allow pod scheduling..." | tee -a "$REPORT"
kubectl uncordon "$NODE" 2>&1 | tee -a "$REPORT"
CORDONED_NODE=""   # Prevent double-uncordon in cleanup

# --- Wait for replacement pod to become Ready ---
echo "  Waiting for pod $PRIMARY to become Ready..." | tee -a "$REPORT"
kubectl wait --for=condition=Ready "pod/$PRIMARY" --timeout=120s 2>&1 | tee -a "$REPORT" || true

# --- Wait for full sync ---
echo "  Waiting for full HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
ok=true
if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
    ok=false
fi

kill_end=$(date -u +%s)
dur=$((kill_end - kill_start))

echo "" | tee -a "$REPORT"
echo "=== TEST D SUMMARY (Slow-Path Failover) ===" | tee -a "$REPORT"
echo "Pod killed:       $PRIMARY" | tee -a "$REPORT"
echo "New primary:      $(get_primary)" | tee -a "$REPORT"
echo "Node cordoned:    $NODE" | tee -a "$REPORT"
echo "Total time:       ${dur}s" | tee -a "$REPORT"
echo "Result:           $($ok && echo PASS || echo FAIL)" | tee -a "$REPORT"
echo "" | tee -a "$REPORT"
echo "Check operator log for these messages:" | tee -a "$REPORT"
echo '  - "Primary pod NotReady; starting failover threshold timer"' | tee -a "$REPORT"
echo '  - "Primary NotReady threshold exceeded; selecting failover target"' | tee -a "$REPORT"
echo '  - "Automatic unplanned failover succeeded"' | tee -a "$REPORT"
echo "" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"
