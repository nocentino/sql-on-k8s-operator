#!/bin/bash
# logbackup-loop.sh — Run BACKUP LOG [tpcc] TO DISK='NUL' every 30s on the current primary.
# Keeps the log chain from growing while load tests run.  Runs until killed.
set -uo pipefail
SA="YourStrong!Passw0rd"
INTERVAL=${1:-30}

run_backup() {
    for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
        out=$(kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
            -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W -Q \
            "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" \
            2>/dev/null | tr -d '\r' | grep -E "^PRIMARY$" | head -1) || continue
        if [ "$out" = "PRIMARY" ]; then
            kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
                -S localhost,1433 -U sa -P "$SA" -No -C -Q \
                "BACKUP LOG [tpcc] TO DISK = 'NUL' WITH NO_COMPRESSION;" 2>/dev/null \
                && echo "$(date -u +%H:%M:%S) log backup OK on $pod" \
                || echo "$(date -u +%H:%M:%S) log backup failed on $pod"
            return
        fi
    done
    echo "$(date -u +%H:%M:%S) could not find primary for log backup"
}

echo "Log backup loop started (interval=${INTERVAL}s, PID=$$)"
while true; do
    run_backup
    sleep "$INTERVAL"
done
