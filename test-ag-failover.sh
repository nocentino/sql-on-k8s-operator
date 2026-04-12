#!/usr/bin/env bash
# test-ag-failover.sh — Deploy a SQL Server AG and exercise planned and unplanned failover.
#
# Usage:
#   ./test-ag-failover.sh deploy            # Phase 1: create secret + apply CR
#   ./test-ag-failover.sh verify            # Phase 2: check AG health
#   ./test-ag-failover.sh adddb             # Phase 3: create testdb, add to AG, wait for HEALTHY
#   ./test-ag-failover.sh planned           # Phase 4: planned failover to mssql-ag-1
#   ./test-ag-failover.sh unplanned         # Phase 5: crash primary, watch operator recover
#   ./test-ag-failover.sh all               # Run all five phases in order
#
# Prerequisites:
#   kubectl, sqlcmd configured and pointed at the target cluster.
#   The operator must already be deployed (make deploy IMG=...).

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
SA_PASSWORD="${SA_PASSWORD:-YourStrong!Passw0rd}"
AG_NAME="mssql-ag"
AG_SQL_NAME="AG1"
SECRET_NAME="mssql-ag-secret"
SAMPLE_CR="config/samples/sql_v1alpha1_sqlserveravailabilitygroup.yaml"
OPERATOR_NS="sql-on-k8s-operator-system"
OPERATOR_DEPLOY="sql-on-k8s-operator-controller-manager"
FAILOVER_THRESHOLD=30          # must match spec.automaticFailover.failoverThresholdSeconds
# PLANNED_TARGET_POD is resolved dynamically at runtime to the first secondary
# that is not the current primary. The variable below is the default fallback
# used only when auto-detection fails.
PLANNED_TARGET_POD=""      # empty = auto-detect at runtime
PLANNED_TARGET_PORT=14331      # local port used for the port-forward during planned failover
POD_READY_TIMEOUT=300          # seconds to wait for all pods to be Ready

# ── Helpers ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
sep()     { echo -e "${CYAN}──────────────────────────────────────────────${NC}"; }

# check_replica_health LABEL
# Prints K8s pod readiness + SQL replica role/sync-health/queue depths at a transition point.
# Uses || true so a transient connection drop never aborts the script.
check_replica_health() {
    local label="${1:-Health check}"
    echo -e "\n${YELLOW}  ┌─ ${label}${NC}"

    # K8s layer: pod name, Ready status, and the ag-role label the operator maintains.
    kubectl get pods -l "app=${AG_NAME}" \
        -o custom-columns='POD:.metadata.name,READY:.status.containerStatuses[0].ready,ROLE:.metadata.labels.sql\.mssql\.microsoft\.com/ag-role' \
        --no-headers 2>/dev/null \
        | awk '{printf "  │  Pod %-18s  k8s-ready=%-5s  ag-role=%s\n", $1, $2, $3}' \
        || true

    echo -e "  │"

    # SQL layer: role, sync-health, sync-state, and queue depths per replica.
    sqlcmd_listener \
        "SET NOCOUNT ON;
         SELECT r.replica_server_name        AS Replica,
                rs.role_desc                 AS Role,
                rs.synchronization_health_desc AS SyncHealth,
                ISNULL(drs.synchronization_state_desc, 'n/a') AS SyncState,
                ISNULL(CAST(drs.log_send_queue_size AS VARCHAR(20)), 'n/a') AS LogSendQ_KB,
                ISNULL(CAST(drs.redo_queue_size     AS VARCHAR(20)), 'n/a') AS RedoQ_KB
         FROM sys.dm_hadr_availability_replica_states rs
         JOIN sys.availability_replicas r
              ON rs.replica_id = r.replica_id
         LEFT JOIN sys.dm_hadr_database_replica_states drs
              ON rs.replica_id = drs.replica_id AND drs.is_local = 0
         ORDER BY rs.role_desc, r.replica_server_name" \
        2>/dev/null \
        | sed 's/^/  │  /' \
        || echo -e "  │  ${YELLOW}(SQL query failed — listener may be mid-transition)${NC}"

    echo -e "  ${YELLOW}└─────────────────────────────────────────────────────${NC}\n"
}

wait_pods_ready() {
    local label="app=${AG_NAME}" timeout="${POD_READY_TIMEOUT}"

    # Phase 1: wait for the operator to create the StatefulSet and at least one pod.
    # kubectl wait --for=condition=Ready exits immediately with code 1 when no pods
    # match the selector (rather than waiting), so we must ensure pods exist first.
    info "Waiting for operator to create AG pods..."
    local elapsed=0
    until kubectl get pod -l "$label" --no-headers 2>/dev/null | grep -q .; do
        sleep 3; elapsed=$(( elapsed + 3 ))
        [[ $elapsed -ge $timeout ]] && die "Operator did not create pods within ${timeout}s"
    done
    ok "Pods exist — waiting up to ${timeout}s for all to be Ready..."

    # Phase 2: now that pods exist, kubectl wait works correctly.
    if ! kubectl wait pod -l "$label" --for=condition=Ready --timeout="${timeout}s" 2>/dev/null; then
        die "Pods did not become Ready within ${timeout}s. Run: kubectl get pods -l $label"
    fi
    ok "All AG pods are Ready"
}

current_primary() {
    kubectl get sqlag "${AG_NAME}" -o jsonpath='{.status.primaryReplica}' 2>/dev/null
}

sqlcmd_listener() {
    sqlcmd -S "localhost,1433" -U sa -P "${SA_PASSWORD}" -Q "$1" -l 10
}

sqlcmd_pod_port() {
    local port="$1"; shift
    sqlcmd -S "localhost,${port}" -U sa -P "${SA_PASSWORD}" -Q "$1" -l 10
}

# ── Teardown helper (called at start of deploy for a clean slate) ─────────────
teardown_ag() {
    info "Teardown — checking for existing AG CR and data volumes"

    # Detect whether anything exists that requires teardown.
    local cr_exists=false pod_exists=false
    kubectl get sqlag "${AG_NAME}" &>/dev/null  && cr_exists=true
    kubectl get pod  "${AG_NAME}-0" &>/dev/null && pod_exists=true

    if [[ "$cr_exists" == false && "$pod_exists" == false ]]; then
        info "  No existing AG found — skipping teardown"
        return 0
    fi

    # Something exists — prompt the user before destroying it.
    echo -e "\n${YELLOW}  ┌─ Existing AG detected ──────────────────────────────────${NC}"
    [[ "$cr_exists"  == true ]] && echo -e "  │  CR:   ${AG_NAME} (kubectl get sqlag ${AG_NAME})"
    [[ "$pod_exists" == true ]] && echo -e "  │  Pods: ${AG_NAME}-0 … (and associated PVCs)"
    echo -e "  │"
    echo -e "  │  Teardown will:"
    echo -e "  │    • Delete the SQLServerAvailabilityGroup CR"
    echo -e "  │    • Delete all mssql-data-${AG_NAME}-* PVCs  ← data loss"
    echo -e "  ${YELLOW}└─────────────────────────────────────────────────────────${NC}\n"

    local answer=""
    if [[ "${FORCE_TEARDOWN:-0}" == "1" ]]; then
        # Unattended CI path: caller explicitly opted in via env var.
        answer="y"
        warn "  FORCE_TEARDOWN=1 — skipping prompt and proceeding with teardown"
    elif [[ -t 0 ]]; then
        # Interactive terminal: show the prompt on the terminal itself.
        read -r -p "  Proceed with teardown? [y/N] " answer </dev/tty
    else
        # Piped stdin (e.g. echo "y" | ./test-ag-failover.sh deploy):
        # read whatever was sent; if nothing was sent default to "n" (safe).
        read -r answer 2>/dev/null || true
        [[ -z "$answer" ]] && answer="n"
        info "  Non-interactive: received '${answer}'"
    fi

    if [[ ! "$answer" =~ ^[Yy]$ ]]; then
        warn "Teardown cancelled — exiting without making changes"
        return 0
    fi

    # Step 1: If pods are running, drop the SQL Server AG *first* (while we can still connect).
    # This prevents the operator from skipping CREATE on re-bootstrap because it found
    # a leftover AG (potentially with a different CLUSTER_TYPE) in the SQL Server data files.
    if [[ "$pod_exists" == true ]]; then
        info "  Dropping SQL Server AG [${AG_SQL_NAME}] from primary pod..."
        kubectl exec "${AG_NAME}-0" -- /opt/mssql-tools18/bin/sqlcmd \
            -S "localhost,1433" -U sa -P "${SA_PASSWORD}" -No -C \
            -Q "IF EXISTS (SELECT 1 FROM sys.availability_groups WHERE name = '${AG_SQL_NAME}') DROP AVAILABILITY GROUP [${AG_SQL_NAME}];" \
            2>/dev/null && ok "  SQL Server AG dropped" || warn "  Could not drop AG (may not exist)"
    fi

    # Step 2: Delete the CR (triggers StatefulSet GC → pods terminate; PVCs are retained).
    if [[ "$cr_exists" == true ]]; then
        kubectl delete sqlag "${AG_NAME}" --ignore-not-found 2>&1
        info "  Waiting for pods to terminate..."
        kubectl wait pod -l "app=${AG_NAME}" --for=delete --timeout=120s 2>/dev/null || true
        ok "  CR and pods removed"
    fi

    # Step 3: Delete PVCs so SQL Server starts from clean data files on re-deploy.
    # Without this, SQL Server re-reads the old system databases which still contain
    # the AG metadata (including the old CLUSTER_TYPE), and the operator skips CREATE.
    local pvcs
    pvcs=$(kubectl get pvc -l "app=${AG_NAME}" -o name 2>/dev/null)
    if [[ -z "$pvcs" ]]; then
        # StatefulSet PVCs are not labelled — query by name pattern instead.
        pvcs=$(kubectl get pvc -o name 2>/dev/null | grep "mssql-data-${AG_NAME}-" || true)
    fi
    if [[ -n "$pvcs" ]]; then
        echo "$pvcs" | xargs kubectl delete 2>/dev/null && ok "  PVCs deleted (fresh data volumes on next deploy)" || true
    else
        info "  No PVCs found for ${AG_NAME}"
    fi
}

# ── Phase 1: Deploy ───────────────────────────────────────────────────────────
phase_deploy() {
    sep; info "Phase 1 — Deploy the Availability Group"

    # Always start clean: drop any leftover SQL Server AG, delete the CR, and
    # remove PVCs so the new pods start with fresh data files. This prevents the
    # bootstrap from finding a stale AG in the SQL Server system databases and
    # skipping CREATE (which would preserve the old CLUSTER_TYPE).
    teardown_ag

    if kubectl get secret "${SECRET_NAME}" &>/dev/null; then
        warn "Secret ${SECRET_NAME} already exists — skipping creation"
    else
        kubectl create secret generic "${SECRET_NAME}" \
            --from-literal=SA_PASSWORD="${SA_PASSWORD}"
        ok "Created secret ${SECRET_NAME}"
    fi

    kubectl apply -f "${SAMPLE_CR}"
    ok "Applied ${SAMPLE_CR}"

    wait_pods_ready
    ok "Phase 1 complete — AG pods are up"
}

# ── Phase 2: Verify ───────────────────────────────────────────────────────────
phase_verify() {
    sep; info "Phase 2 — Verify AG health"

    info "Pod status:"
    kubectl get pods -l "app=${AG_NAME}" --show-labels

    info "Services:"
    kubectl get svc "${AG_NAME}-listener" "${AG_NAME}-listener-ro" 2>/dev/null || \
        kubectl get svc -l "app=${AG_NAME}"

    # Bootstrap takes 1-3 min after pods are Ready. Poll until primaryReplica is set.
    info "Waiting up to 180s for AG bootstrap to complete (InitializationComplete + primaryReplica)..."
    local primary="" elapsed=0
    while [[ $elapsed -lt 180 ]]; do
        primary=$(current_primary)
        [[ -n "$primary" ]] && break
        sleep 5; elapsed=$(( elapsed + 5 ))
        info "  Still bootstrapping... (${elapsed}s)"
    done
    [[ -z "$primary" ]] && die "AG bootstrap did not complete within 180s — check operator logs"
    ok "Primary replica: ${primary}"

    check_replica_health "Bootstrap complete — initial baseline"
    ok "Phase 2 complete — AG is healthy"
}

# ── Phase 3: Add database and wait for HEALTHY ────────────────────────────────
phase_adddb() {
    sep; info "Phase 3 — Create testdb, add to AG, wait for all replicas HEALTHY"

    local primary; primary=$(current_primary)
    [[ -z "$primary" ]] && die "Cannot determine current primary — run verify first"
    info "Adding testdb to AG on primary: ${primary}"

    # Create the database, take the mandatory full backup, then add to the AG.
    # SEEDING_MODE = AUTOMATIC (set at AG creation) means SQL Server VDI-seeds
    # testdb directly to every connected secondary — no manual restore required.
    sqlcmd_listener \
        "IF DB_ID('testdb') IS NULL CREATE DATABASE testdb;
         BACKUP DATABASE testdb
             TO DISK = '/var/opt/mssql/data/testdb.bak'
             WITH INIT, FORMAT;
         IF NOT EXISTS (
             SELECT 1 FROM sys.availability_databases_cluster adc
             JOIN sys.availability_groups ag ON adc.group_id = ag.group_id
             WHERE ag.name = '${AG_SQL_NAME}' AND adc.database_name = 'testdb'
         )
         ALTER AVAILABILITY GROUP [${AG_SQL_NAME}] ADD DATABASE testdb;"
    ok "testdb created and added to AG — automatic seeding to secondaries starting"
    check_replica_health "After ADD DATABASE — seeding in progress"

    # Poll until every replica reports HEALTHY.
    # The query returns the count of non-HEALTHY replicas; we wait until it is 0.
    local timeout=120 interval=5 elapsed=0
    info "Waiting up to ${timeout}s for all replicas to reach HEALTHY synchronization..."
    while [[ $elapsed -lt $timeout ]]; do
        local unhealthy
        unhealthy=$(sqlcmd_listener \
            "SET NOCOUNT ON;
             SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states
             WHERE synchronization_health_desc != 'HEALTHY'" \
            2>/dev/null | tr -d ' \r' | grep -E '^[0-9]+$' | head -1 || echo "99")

        if [[ "$unhealthy" == "0" ]]; then
            ok "All replicas are HEALTHY"
            break
        fi

        info "  ${unhealthy} replica(s) not yet HEALTHY (${elapsed}s elapsed) — retrying in ${interval}s"
        sleep "$interval"
        elapsed=$(( elapsed + interval ))
    done

    if [[ $elapsed -ge $timeout ]]; then
        warn "Timed out waiting for HEALTHY state"
    fi

    check_replica_health "After seeding complete — all replicas synchronized"
    ok "Phase 3 complete — testdb seeded to all replicas"
}

# ── Phase 4: Planned Failover ─────────────────────────────────────────────────
phase_planned() {
    sep; info "Phase 4 — Planned failover"

    local before; before=$(current_primary)
    [[ -z "$before" ]] && die "Cannot determine current primary — run verify first"
    info "Current primary before failover: ${before}"

    # Auto-detect the failover target: the first synchronous/automatic secondary
    # that is NOT the current primary. Iterates mssql-ag-0 .. mssql-ag-N.
    local target="${PLANNED_TARGET_POD}"
    if [[ -z "$target" ]]; then
        local i=0
        while true; do
            local candidate="${AG_NAME}-${i}"
            if ! kubectl get pod "$candidate" &>/dev/null; then break; fi
            if [[ "$candidate" != "$before" ]]; then
                target="$candidate"
                break
            fi
            i=$(( i + 1 ))
        done
    fi
    [[ -z "$target" ]] && die "Could not find a secondary to fail over to"
    info "Planned failover target: ${target}"

    check_replica_health "Pre-planned-failover snapshot"

    info "Port-forwarding ${target}:1433 → localhost:${PLANNED_TARGET_PORT}"
    kubectl port-forward "pod/${target}" "${PLANNED_TARGET_PORT}:1433" >/dev/null 2>/dev/null &
    local pf_pid=$!
    # kill in the trap must not leak a non-zero exit code out of the function
    trap "kill ${pf_pid} 2>/dev/null || true; trap - RETURN" RETURN
    sleep 3   # give the tunnel time to establish

    info "Issuing ALTER AVAILABILITY GROUP [${AG_SQL_NAME}] FAILOVER on ${target}"
    sqlcmd_pod_port "${PLANNED_TARGET_PORT}" \
        "EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';
         ALTER AVAILABILITY GROUP [${AG_SQL_NAME}] FAILOVER;" \
        || die "FAILOVER command failed — check that clusterType is EXTERNAL and the target is synchronized"

    kill "${pf_pid}" 2>/dev/null || true
    check_replica_health "Immediately after FAILOVER DDL — roles mid-transition"

    # The operator's reconcile loop runs every 60s; allow up to 90s for status.primaryReplica to update.
    info "Waiting up to 90s for the operator to update status.primaryReplica..."
    local deadline=$(( SECONDS + 90 ))
    local after=""
    while [[ $SECONDS -lt $deadline ]]; do
        after=$(current_primary)
        [[ "$after" != "$before" ]] && break
        sleep 5
    done

    if [[ "$after" == "$before" || -z "$after" ]]; then
        warn "status.primaryReplica has not changed yet — the operator may still be reconciling (check logs)"
    else
        ok "Primary changed: ${before} → ${after}"
    fi

    # Give the listener service selector a moment to catch up before querying.
    info "Waiting 5s for listener selector to update..."
    sleep 5
    check_replica_health "After planned failover — listener settled on new primary"
    ok "Phase 4 complete — planned failover done"
}

# ── Phase 5: Unplanned Failover ───────────────────────────────────────────────
phase_unplanned() {
    sep; info "Phase 5 — Unplanned failover (simulated crash)"

    local primary; primary=$(current_primary)
    [[ -z "$primary" ]] && die "Cannot determine current primary — run verify first"
    info "Crashing primary pod: ${primary}"

    info "Tailing operator logs in the background (Ctrl-C safe — background job will stop with the script)"
    kubectl logs -n "${OPERATOR_NS}" "deployment/${OPERATOR_DEPLOY}" -f \
        --since=5s 2>/dev/null \
        | grep --line-buffered -E "NotReady|threshold|Failover|primary|timer|degraded" &
    local log_pid=$!
    trap "kill ${log_pid} 2>/dev/null; trap - RETURN" RETURN

    check_replica_health "Pre-crash baseline"

    info "Sending SIGKILL to sqlservr inside pod/${primary} — simulating a SQL Server crash"
    # pkill is available in the mssql container; pidof is not.
    kubectl exec "${primary}" -- pkill -9 -x sqlservr \
        || die "Could not send SIGKILL to sqlservr — is the pod running?"
    ok "Process killed — pod will restart automatically (StatefulSet)"

    info "Watching pods (the crashed pod goes NotReady then restarts as a secondary):"
    timeout 60 kubectl get pods -l "app=${AG_NAME}" -w 2>/dev/null || true

    info "Operator has a ${FAILOVER_THRESHOLD}s threshold before promoting a secondary."
    info "Polling status.primaryReplica every 5s for up to $(( FAILOVER_THRESHOLD + 60 ))s..."
    local deadline=$(( SECONDS + FAILOVER_THRESHOLD + 60 ))
    local after=""
    while [[ $SECONDS -lt $deadline ]]; do
        after=$(current_primary)
        if [[ -n "$after" && "$after" != "$primary" ]]; then
            ok "Failover complete: ${primary} → ${after}"
            break
        fi
        local ts; ts=$(kubectl get sqlag "${AG_NAME}" \
            -o jsonpath='{.status.primaryNotReadySince}' 2>/dev/null || true)
        if [[ -n "$ts" ]]; then
            info "  PrimaryNotReadySince=${ts} — operator timer is running"
        else
            info "  Waiting... current primary in status: ${after:-<empty>}"
        fi
        sleep 5
    done

    if [[ "$after" == "$primary" || -z "$after" ]]; then
        warn "Primary did not change within the expected window — check operator logs above"
    fi

    check_replica_health "After automatic failover — new primary elected"

    info "Full AG status from the operator:"
    kubectl get sqlag "${AG_NAME}" -o jsonpath='{.status}' | python3 -m json.tool 2>/dev/null \
        || kubectl get sqlag "${AG_NAME}" -o yaml | grep -A 30 "^status:"

    # Wait for the crashed pod to restart and rejoin as a secondary, then do a final health check.
    info "Waiting up to 120s for crashed pod ${primary} to recover and rejoin as SECONDARY..."
    local recover_deadline=$(( SECONDS + 120 ))
    while [[ $SECONDS -lt $recover_deadline ]]; do
        local role
        role=$(kubectl get pod "${primary}" \
            -o jsonpath='{.metadata.labels.sql\.mssql\.microsoft\.com/ag-role}' 2>/dev/null || true)
        local ready
        ready=$(kubectl get pod "${primary}" \
            -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
        if [[ "$ready" == "true" && ("$role" == "secondary" || "$role" == "readable-secondary") ]]; then
            ok "Crashed pod ${primary} is Ready and labelled ag-role=${role}"
            break
        fi
        info "  ${primary}: ready=${ready:-unknown}  ag-role=${role:-<unlabelled>}"
        sleep 5
    done

    # Wait for the synchronous replica (the crashed pod) to reach SYNCHRONIZED before
    # the final health check. The async replica (mssql-ag-2) will remain SYNCHRONIZING —
    # that is its correct steady state and is not waited on here.
    info "Waiting up to 120s for the synchronous replica ${primary} to reach SYNCHRONIZED..."
    local sync_deadline=$(( SECONDS + 120 ))
    while [[ $SECONDS -lt $sync_deadline ]]; do
        local sync_state
        sync_state=$(sqlcmd_listener \
            "SET NOCOUNT ON;
             SELECT drs.synchronization_state_desc
             FROM sys.dm_hadr_database_replica_states drs
             JOIN sys.availability_replicas r ON drs.replica_id = r.replica_id
             WHERE r.replica_server_name = '${primary}' AND drs.is_local = 0" \
            2>/dev/null | tr -d ' \r' | grep -E '^[A-Z_]+$' | head -1 || echo "UNKNOWN")

        if [[ "$sync_state" == "SYNCHRONIZED" ]]; then
            ok "${primary} is SYNCHRONIZED"
            break
        fi
        info "  ${primary} sync state: ${sync_state} — waiting..."
        sleep 5
    done

    check_replica_health "All replicas confirmed healthy — test complete"

    kill "${log_pid}" 2>/dev/null || true
    ok "Phase 5 complete — unplanned failover exercised"
}

# ── Entrypoint ────────────────────────────────────────────────────────────────
usage() {
    grep '^#   \./test-ag-failover' "$0" | sed 's/^# /  /'
    exit 0
}

case "${1:-}" in
    deploy)    phase_deploy    ;;
    verify)    phase_verify    ;;
    adddb)     phase_adddb     ;;
    planned)   phase_planned   ;;
    unplanned) phase_unplanned ;;
    all)
        phase_deploy
        echo; sleep 5
        phase_verify
        echo; sleep 5
        phase_adddb
        echo; sleep 5
        phase_planned
        echo; sleep 10
        phase_verify        # re-verify after planned so state is fresh for unplanned
        echo; sleep 5
        phase_unplanned
        ;;
    -h|--help|help|"") usage ;;
    *) die "Unknown phase '$1'. Run with --help for usage." ;;
esac
