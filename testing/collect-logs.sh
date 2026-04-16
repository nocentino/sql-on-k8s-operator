#!/bin/bash
# collect-logs.sh — Pull operator, pod container, and SQL errorlogs; run analysis
set -uo pipefail
SA="YourStrong!Passw0rd"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${DIR}/logs"
mkdir -p "$LOG_DIR"

echo "=== Collecting operator logs ==="
kubectl logs -n sql-on-k8s-operator-system \
    deployment/sql-on-k8s-operator-controller-manager -c manager \
    > "${LOG_DIR}/operator.log" 2>&1
echo "  operator.log: $(wc -l < "${LOG_DIR}/operator.log") lines"

echo ""
echo "=== Collecting pod container logs ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    kubectl logs "$pod" -c mssql > "${LOG_DIR}/${pod}-container.log" 2>&1
    echo "  ${pod}-container.log: $(wc -l < "${LOG_DIR}/${pod}-container.log") lines"
done

echo ""
echo "=== Collecting SQL Server errorlogs (current + previous) ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C \
        -Q "EXEC sp_readerrorlog 0, 1;" \
        > "${LOG_DIR}/${pod}-errorlog-current.log" 2>&1
    kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C \
        -Q "EXEC sp_readerrorlog 1, 1;" \
        > "${LOG_DIR}/${pod}-errorlog-previous.log" 2>&1
    echo "  ${pod}-errorlog-current.log:  $(wc -l < "${LOG_DIR}/${pod}-errorlog-current.log") lines"
    echo "  ${pod}-errorlog-previous.log: $(wc -l < "${LOG_DIR}/${pod}-errorlog-previous.log") lines"
done

echo ""
echo "=== Analysis: operator ==="
echo "Reconciles:             $(grep -c 'Reconciling SQLServerAvailabilityGroup' "${LOG_DIR}/operator.log" || echo 0)"
echo "Label updates:          $(grep -c 'Updated pod AG role label' "${LOG_DIR}/operator.log" || echo 0)"
echo "NOT SYNCHRONIZING det.: $(grep -c 'Detected NOT SYNCHRONIZING' "${LOG_DIR}/operator.log" || echo 0)"
echo "Re-seat verified:       $(grep -c 'Re-seat verified' "${LOG_DIR}/operator.log" || echo 0)"
echo "Auto failovers:         $(grep -c 'Automatic unplanned failover' "${LOG_DIR}/operator.log" || echo 0)"
echo "Errors:                 $(grep -c 'ERROR' "${LOG_DIR}/operator.log" || echo 0)"
grep -E 'Automatic unplanned failover|threshold exceeded|No HEALTHY replica' "${LOG_DIR}/operator.log" | sed 's/.*INFO\t/  /' | sed 's/{"controller.*//' || true

echo ""
echo "=== Analysis: SQL Server errors per replica ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    f="${LOG_DIR}/${pod}-errorlog-current.log"
    echo "--- $pod ---"
    printf "  %-10s %s\n" "41142:" "$(grep -c '41142' "$f" 2>/dev/null || echo 0) (failover rejected - not synchronized)"
    printf "  %-10s %s\n" "41145:" "$(grep -c '41145' "$f" 2>/dev/null || echo 0) (database already joined)"
    printf "  %-10s %s\n" "35278:" "$(grep -c '35278' "$f" 2>/dev/null || echo 0) (database resync restart)"
    printf "  %-10s %s\n" "35250:" "$(grep -c '35250' "$f" 2>/dev/null || echo 0) (connection to primary lost)"
    printf "  %-10s %s\n" "Harden:" "$(grep -c 'Remote harden' "$f" 2>/dev/null || echo 0) (remote harden failures)"
    printf "  %-10s %s\n" "Killed:" "$(grep -c 'killed batches' "$f" 2>/dev/null || echo 0) (killed batches)"
done

echo ""
echo "=== Analysis: state transitions ==="
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    echo "--- $pod ---"
    grep -E 'has changed from|is changing roles|previous state was' \
        "${LOG_DIR}/${pod}-errorlog-current.log" 2>/dev/null | sed 's/^/  /' || echo "  (none)"
done

echo ""
echo "=== Kubernetes events ==="
kubectl get events --sort-by='.lastTimestamp' | grep -v Normal | head -20
echo "Logs saved to: $LOG_DIR"
