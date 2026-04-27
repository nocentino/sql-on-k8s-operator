#!/bin/bash
# test-e-sigstop-stub.sh — Frozen-primary / gray failure via SIGSTOP (no load).
#
# Normal unplanned tests (test-b) kill the pod outright. That surfaces as the
# StatefulSet recreating the pod and a brief NotReady window. SIGSTOP is a
# different failure mode: the container keeps running, the kernel still holds
# the TCP socket open, but sqlservr stops scheduling threads. From the outside
# the pod looks like it's alive; only a real query against the server reveals
# the freeze. Before the v0.45 probe rewrite + STS probe sync this failure
# mode never triggered failover (the old readiness probe grep'd "1" against
# error text and silently "succeeded").
#
# Flow:
#   1. Identify the primary pod.
#   2. SIGSTOP the sqlservr process inside the container.
#   3. Watch the kubelet readiness probe flip the pod to NotReady.
#   4. Watch the operator start the failover threshold timer and promote.
#   5. Force-delete the frozen pod so its container restarts clean and the
#      replica rejoins as SECONDARY.
#   6. Wait for full HEALTHY + SYNCHRONIZED.
#
# Expected operator log messages:
#   - "Primary pod NotReady; starting failover threshold timer"
#   - "Primary NotReady threshold exceeded; selecting failover target"
#   - "Automatic unplanned failover succeeded"
#
# Reference verified run (v0.45): pod Ready=False at T+45s,
# primaryNotReadySince set at T+63s, failover completed at T+113s.

set -uo pipefail
SA="YourStrong!Passw0rd"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGDIR="$(dirname "$0")/logs"
mkdir -p "$LOGDIR"
REPORT="${LOGDIR}/report-test-e-sigstop-stub.txt"
CSV="${LOGDIR}/ag-health-test-e-sigstop-stub.csv"
READY_CAP=300   # SIGSTOP failover is slower than the fast-path kill (no headless AG shortcut).

sqlcmd_pod() { kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q "$2" 2>/dev/null; }

get_primary() {
    local p
    p=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
    if [ -n "$p" ]; then echo "$p"; return 0; fi
    local q="SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1"
    # Enumerate AG pods dynamically; works for any replica count.
    while IFS= read -r pod; do
        role=$(sqlcmd_pod "$pod" "$q" 2>/dev/null | head -1 | tr -d ' ') || continue
        if [ "$role" = "PRIMARY" ]; then echo "$pod"; return 0; fi
    done < <(kubectl get pods -l app=mssql-ag -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
    return 1
}

# wait_ready matches test-b/test-d: all AG replicas HEALTHY and local databases SYNCHRONIZED.
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

FROZEN_POD=""
cleanup() {
    # If the test bailed while a pod was still SIGSTOP'd we must force-delete
    # it — SIGCONT might not land (node load / stuck kubelet), and a frozen
    # pod blocks all subsequent AG recovery. Force-delete always works because
    # it tears down the container regardless of in-process signal state.
    if [ -n "${FROZEN_POD:-}" ]; then
        echo "Cleanup: force-deleting frozen pod $FROZEN_POD" | tee -a "$REPORT"
        kubectl delete pod "$FROZEN_POD" --grace-period=0 --force 2>&1 | tee -a "$REPORT" || true
    fi
    kill $MONITOR_PID 2>/dev/null || true
}

# Start monitor
bash "$(dirname "$0")/monitor-ag.sh" "$CSV" &
MONITOR_PID=$!
trap cleanup EXIT

echo "Starting SIGSTOP (gray-failure) test..." | tee "$REPORT"
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

# --- SIGSTOP the sqlservr process ---
#
# Find the sqlservr PID and deliver SIGSTOP. pgrep is available in the mssql
# image. We intentionally target sqlservr (not PID 1) in case the entrypoint
# is a wrapper script: freezing the wrapper wouldn't freeze the actual server.
echo "" | tee -a "$REPORT"
echo "=== SIGSTOP sqlservr on $PRIMARY at $(date -u +%H:%M:%S) ===" | tee -a "$REPORT"
sigstop_start=$(date -u +%s)

kubectl exec "$PRIMARY" -- bash -c 'pids=$(pgrep -x sqlservr); [ -n "$pids" ] && kill -STOP $pids && echo "SIGSTOP sent to pids=$pids" || { echo "FAIL: sqlservr not running"; exit 1; }' 2>&1 | tee -a "$REPORT"
# Mark the pod as frozen so the trap cleans it up on any early exit.
FROZEN_POD="$PRIMARY"

# --- Watch the kubelet flip the pod to NotReady ---
echo "  Waiting for pod $PRIMARY to go Ready=False..." | tee -a "$REPORT"
NOT_READY_DETECTED=false
for i in $(seq 1 120); do
    ready=$(kubectl get pod "$PRIMARY" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
    if [ "$ready" != "True" ]; then
        elapsed=$(($(date -u +%s) - sigstop_start))
        echo "  Pod $PRIMARY is Ready=$ready after ${elapsed}s" | tee -a "$REPORT"
        NOT_READY_DETECTED=true
        break
    fi
    sleep 1
done
if ! $NOT_READY_DETECTED; then
    echo "  FAIL: pod never flipped Ready=False within 120s" | tee -a "$REPORT"
    exit 1
fi

# --- Watch for operator-driven failover ---
# The operator starts the threshold timer the first time it observes
# NotReady, then promotes after failoverThresholdSeconds. We cap at 180s
# to allow for a conservative threshold (default 30s + reconcile jitter).
echo "  Watching for operator to promote via NotReady threshold timer..." | tee -a "$REPORT"
FAILOVER_DETECTED=false
for i in $(seq 1 180); do
    new_primary=$(kubectl get sqlag mssql-ag -o jsonpath='{.status.primaryReplica}' 2>/dev/null | tr -d ' ')
    if [ -n "$new_primary" ] && [ "$new_primary" != "$PRIMARY" ]; then
        failover_time=$(date -u +%H:%M:%S)
        elapsed=$(($(date -u +%s) - sigstop_start))
        echo "  Failover detected at $failover_time (${elapsed}s after SIGSTOP)" | tee -a "$REPORT"
        echo "  New primary: $new_primary" | tee -a "$REPORT"
        FAILOVER_DETECTED=true
        break
    fi
    sleep 1
done

if ! $FAILOVER_DETECTED; then
    echo "  FAIL: no failover detected within 180s" | tee -a "$REPORT"
    exit 1
fi

# --- Force-delete the frozen pod so its replacement joins as SECONDARY ---
# We can't SIGCONT reliably: a stuck sqlservr after a long freeze may leave
# HADR in a wedged state locally. Clean restart is more predictable and
# matches what a kubelet restart or node reboot would do in production.
echo "" | tee -a "$REPORT"
echo "Force-deleting frozen pod $PRIMARY so it restarts clean..." | tee -a "$REPORT"
kubectl delete pod "$PRIMARY" --grace-period=0 --force 2>&1 | tee -a "$REPORT"
FROZEN_POD=""   # Prevent double-delete in cleanup trap.

# --- Wait for replacement pod to become Ready ---
echo "  Waiting for pod $PRIMARY to become Ready..." | tee -a "$REPORT"
kubectl wait --for=condition=Ready "pod/$PRIMARY" --timeout=180s 2>&1 | tee -a "$REPORT" || true

# --- Wait for full sync ---
echo "  Waiting for full HEALTHY+SYNCHRONIZED..." | tee -a "$REPORT"
ok=true
if ! wait_ready "$READY_CAP" 2>&1 | tee -a "$REPORT"; then
    ok=false
fi

sigstop_end=$(date -u +%s)
dur=$((sigstop_end - sigstop_start))

echo "" | tee -a "$REPORT"
echo "=== TEST E SUMMARY (SIGSTOP / Gray Failure) ===" | tee -a "$REPORT"
echo "Frozen pod:       $PRIMARY" | tee -a "$REPORT"
echo "New primary:      $(get_primary)" | tee -a "$REPORT"
echo "Total time:       ${dur}s" | tee -a "$REPORT"
echo "Result:           $($ok && echo PASS || echo FAIL)" | tee -a "$REPORT"
echo "" | tee -a "$REPORT"
echo "Check operator log for:" | tee -a "$REPORT"
echo "  \"Primary pod NotReady; starting failover threshold timer\"" | tee -a "$REPORT"
echo "  \"Primary NotReady threshold exceeded; selecting failover target\"" | tee -a "$REPORT"
echo "  \"Automatic unplanned failover succeeded\"" | tee -a "$REPORT"
echo "Completed: $(date -u)" | tee -a "$REPORT"

$ok || exit 1
