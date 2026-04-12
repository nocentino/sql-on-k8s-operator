#!/usr/bin/env bash
# test-ag2-failover-round.sh
# Deploy AG2 (all-sync replicas) and run planned + unplanned failover round-trips.
#
# Usage:
#   ./test-ag2-failover-round.sh deploy      # Deploy AG2 + add testag2db
#   ./test-ag2-failover-round.sh planned     # Planned failover 0->1->2->0
#   ./test-ag2-failover-round.sh unplanned   # Unplanned failover 0->1->2->0
#   ./test-ag2-failover-round.sh all         # Full run
set -euo pipefail
SA_PASSWORD="${SA_PASSWORD:-YourStrong!Passw0rd}"
AG=mssql-ag2
AGSQL=AG2
SECRET=mssql-ag2-secret
CR=config/samples/sql_v1alpha1_sqlserveravailabilitygroup_ag2.yaml
THRESHOLD=30
TIMEOUT=300
DB=testag2db
PF_PORT=14332
C='\033[0;36m'; G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; N='\033[0m'
info() { echo -e "${C}[INFO]${N}  $*"; }
ok()   { echo -e "${G}[OK]${N}    $*"; }
warn() { echo -e "${Y}[WARN]${N}  $*"; }
die()  { echo -e "${R}[ERR]${N}  $*" >&2; exit 1; }
sep()  { echo -e "${C}----------------------------------------------${N}"; }
get_primary() { kubectl get sqlag $AG -o jsonpath='{.status.primaryReplica}' 2>/dev/null; }
sql() { kubectl exec "$1" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA_PASSWORD" -No -C -Q "$2" 2>&1; }
status() {
    local l=${1:-status} p; p=$(get_primary); echo; info "$l"
    kubectl get pods -l "app=$AG" -o custom-columns='POD:.metadata.name,READY:.status.containerStatuses[0].ready,ROLE:.metadata.labels.sql\.mssql\.microsoft\.com/ag-role' --no-headers 2>/dev/null || true
    [[ -n "$p" ]] && sql "$p" "SET NOCOUNT ON; SELECT r.replica_server_name AS Replica, rs.role_desc AS Role, rs.synchronization_health_desc AS Health, ISNULL(drs.synchronization_state_desc,'n/a') AS SyncState FROM sys.dm_hadr_availability_replica_states rs JOIN sys.availability_replicas r ON rs.replica_id=r.replica_id LEFT JOIN sys.dm_hadr_database_replica_states drs ON rs.replica_id=drs.replica_id AND drs.is_local=0 ORDER BY rs.role_desc, r.replica_server_name" 2>/dev/null || true
    echo
}
wait_healthy() {
    local p=$1
    local t=${2:-180}
    local i=10 e=0 resumed=0
    info "Waiting ${t}s for HEALTHY..."
    while [[ $e -lt $t ]]; do
        local u; u=$(sql "$p" "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states WHERE synchronization_health_desc<>'HEALTHY'" 2>/dev/null | tr -d ' \r' | grep -E '^[0-9]+$' | head -1 || echo 99)
        [[ "$u" == "0" ]] && { ok "All HEALTHY"; return 0; }
        # After 30s, try resuming data movement on all replicas from the primary
        if [[ $e -ge 30 && $resumed -eq 0 ]]; then
            info "Attempting HADR RESUME on $DB from primary..."
            sql "$p" "ALTER DATABASE [$DB] SET HADR RESUME;" >/dev/null 2>&1 || true
            # Also resume on each secondary
            for idx in 0 1 2; do
                local pod="${AG}-${idx}"
                [[ "$pod" == "$p" ]] && continue
                sql "$pod" "ALTER DATABASE [$DB] SET HADR RESUME;" >/dev/null 2>&1 || true
            done
            resumed=1
        fi
        sleep $i; e=$((e+i))
    done; warn "Timed out waiting for HEALTHY"; return 1
}
wait_change() {
    local old=$1
    local t=${2:-90}
    local dl=$((SECONDS+t))
    while [[ $SECONDS -lt $dl ]]; do
        local a; a=$(get_primary)
        [[ -n "$a" && "$a" != "$old" ]] && { ok "Primary: $old -> $a"; return 0; }
        sleep 5
    done; warn "Primary unchanged"; return 1
}
phase_deploy() {
    sep; info "DEPLOY $AG + $DB"
    if kubectl get sqlag $AG &>/dev/null; then
        info "Teardown..."; kubectl delete sqlag $AG --ignore-not-found
        kubectl wait pod -l "app=$AG" --for=delete --timeout=120s 2>/dev/null || true
        kubectl get pvc -o name 2>/dev/null | grep "mssql-data-${AG}-" | xargs kubectl delete 2>/dev/null || true
    fi
    kubectl create secret generic $SECRET --from-literal=SA_PASSWORD="$SA_PASSWORD" 2>/dev/null || true
    kubectl apply -f $CR; ok "CR applied"
    local e=0; until kubectl get pod -l "app=$AG" --no-headers 2>/dev/null | grep -q .; do sleep 3; e=$((e+3)); [[ $e -ge $TIMEOUT ]] && die "No pods"; done
    kubectl wait pod -l "app=$AG" --for=condition=Ready --timeout=${TIMEOUT}s || die "Not ready"; ok "Pods ready"
    e=0; while [[ $e -lt 300 ]]; do [[ "$(kubectl get sqlag $AG -o jsonpath='{.status.initializationComplete}' 2>/dev/null)" == "true" ]] && break; sleep 5; e=$((e+5)); done
    [[ $e -ge 300 ]] && die "Bootstrap timeout"; ok "Bootstrap done"
    sql "${AG}-0" "IF DB_ID('$DB') IS NULL CREATE DATABASE $DB; BACKUP DATABASE $DB TO DISK='/var/opt/mssql/data/${DB}.bak' WITH INIT, FORMAT; IF NOT EXISTS (SELECT 1 FROM sys.availability_databases_cluster adc JOIN sys.availability_groups ag ON adc.group_id=ag.group_id WHERE ag.name='$AGSQL' AND adc.database_name='$DB') ALTER AVAILABILITY GROUP [$AGSQL] ADD DATABASE $DB;"
    ok "$DB added"; wait_healthy "${AG}-0" 120; status "Baseline"; ok "Deploy done"
}
do_planned() {
    local from=$1 to=$2; sep; info "PLANNED: $from -> $to"; status "Pre-failover"
    kubectl port-forward "pod/$to" ${PF_PORT}:1433 >/dev/null 2>&1 & local pf=$!; sleep 3
    sqlcmd -S "localhost,$PF_PORT" -U sa -P "$SA_PASSWORD" -Q "EXEC sp_set_session_context @key=N'external_cluster',@value=N'yes'; ALTER AVAILABILITY GROUP [$AGSQL] FAILOVER;" -l 10 2>&1 || die "FAILOVER failed"
    kill $pf 2>/dev/null || true; wait $pf 2>/dev/null || true
    wait_change "$from" 90; sleep 5; local np; np=$(get_primary)
    # CLUSTER_TYPE=EXTERNAL: secondaries need SET(ROLE=SECONDARY) after planned failover
    # to re-establish database sync with the new primary.
    info "Re-seating secondaries with SET(ROLE=SECONDARY)..."
    for idx in 0 1 2; do
        local pod="${AG}-${idx}"
        [[ "$pod" == "$np" ]] && continue
        sql "$pod" "ALTER AVAILABILITY GROUP [$AGSQL] SET (ROLE = SECONDARY);" >/dev/null 2>&1 || true
    done
    wait_healthy "$np" 180; status "Post ($np primary)"; ok "Planned $from -> $to done"
}
do_unplanned() {
    local crash=$1; sep; info "UNPLANNED: crash $crash"; status "Pre-crash"
    kubectl exec "$crash" -- pkill -9 -x sqlservr || die "kill failed"; ok "Killed"
    info "Waiting for operator failover..."
    wait_change "$crash" $((THRESHOLD+60))
    local np; np=$(get_primary); [[ -z "$np" || "$np" == "$crash" ]] && die "No failover"
    info "Waiting up to 180s for $crash to recover..."
    local dl=$((SECONDS+180)); while [[ $SECONDS -lt $dl ]]; do
        local r; r=$(kubectl get pod "$crash" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
        [[ "$r" == "true" ]] && { ok "$crash Ready"; break; }; sleep 5; done
    # After crash recovery, re-seat all secondaries (RESOLVING->SECONDARY + db sync)
    info "Re-seating secondaries with SET(ROLE=SECONDARY)..."
    for idx in 0 1 2; do
        local pod="${AG}-${idx}"
        [[ "$pod" == "$np" ]] && continue
        sql "$pod" "ALTER AVAILABILITY GROUP [$AGSQL] SET (ROLE = SECONDARY);" >/dev/null 2>&1 || true
    done
    wait_healthy "$np" 180; status "Post ($np primary)"; ok "Unplanned from $crash done"
}
# next_pod CURRENT — return the next pod in the 0->1->2->0 rotation
next_pod() {
    local cur=$1
    local idx=${cur##*-}  # extract trailing number
    local nxt=$(( (idx + 1) % 3 ))
    echo "${AG}-${nxt}"
}

phase_planned() {
    sep; info "=== PLANNED ROUND: 0->1->2->0 ==="
    local p; p=$(get_primary)
    [[ -z "$p" ]] && die "No primary found"
    local t1; t1=$(next_pod "$p")
    local t2; t2=$(next_pod "$t1")
    do_planned "$p"  "$t1"; sleep 5
    do_planned "$t1" "$t2"; sleep 5
    do_planned "$t2" "$p"
    ok "=== PLANNED ROUND COMPLETE ==="
}
phase_unplanned() {
    sep; info "=== UNPLANNED ROUND: 0->1->2->0 ==="
    do_unplanned "$(get_primary)"; sleep 10
    do_unplanned "$(get_primary)"; sleep 10
    do_unplanned "$(get_primary)"
    ok "=== UNPLANNED ROUND COMPLETE ==="
}
case "${1:-}" in
    deploy)    phase_deploy;;
    planned)   phase_planned;;
    unplanned) phase_unplanned;;
    all) phase_deploy; echo; sleep 10; phase_planned; echo; sleep 10; phase_unplanned; sep; ok "ALL PASSED";;
    *) echo "Usage: $0 {deploy|planned|unplanned|all}"; exit 1;;
esac
