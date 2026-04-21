#!/bin/bash
# setup-tpcc.sh — Restore TPCC-5G.bak from Azure Blob, add to AG, wait for sync
set -uo pipefail
SA="YourStrong!Passw0rd"
DB="tpcc"
STORAGE_URL="https://testdbaen.blob.core.windows.net/testdb"
BAK_FILE="TPCC-5G.bak"
SAS_TOKEN="sv=2025-11-05&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2027-06-03T21:33:46Z&st=2026-04-21T13:18:46Z&spr=https&sig=AoLaWqNC4J1a%2BTOvB1K08ez5xJJB12l3sVED8FTpmjE%3D"

echo "=== Detecting primary replica ==="
PRIMARY=""
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    role=$(kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W \
        -Q "SET NOCOUNT ON; SELECT role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local=1" \
        2>/dev/null | grep "^PRIMARY$" | head -1 | tr -d ' ') || true
    if [ "$role" = "PRIMARY" ]; then
        PRIMARY="$pod"
        break
    fi
done
if [ -z "$PRIMARY" ]; then
    echo "ERROR: No primary found."
    exit 1
fi
echo "Primary: $PRIMARY"

sqlcmd() { kubectl exec "$PRIMARY" -- /opt/mssql-tools18/bin/sqlcmd -S localhost,1433 -U sa -P "$SA" -No -C "$@"; }

echo ""
echo "=== Step 0: Remove tpcc/testdb from AG and drop on all replicas (if present) ==="
for db in tpcc testdb; do
    sqlcmd -Q "
IF EXISTS (
    SELECT 1 FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
    JOIN sys.databases d ON drs.database_id = d.database_id
    WHERE ag.name = 'AG1' AND d.name = '${db}' AND drs.is_local = 1
)
BEGIN
    ALTER AVAILABILITY GROUP [AG1] REMOVE DATABASE [${db}];
    PRINT '${db} removed from AG1';
END
ELSE PRINT '${db} not in AG1';
" 2>/dev/null || true
done
sleep 5
for db in tpcc testdb; do
    for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
        kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
            -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W \
            -Q "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '${db}') BEGIN ALTER DATABASE [${db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [${db}]; PRINT 'Dropped ${db} on ${pod}'; END ELSE PRINT '${db} not on ${pod}'" \
            2>/dev/null || true
    done
done

echo ""
echo "=== Step 1: Create Azure Blob credential on primary ==="
sqlcmd -Q "
IF EXISTS (SELECT 1 FROM sys.credentials WHERE name = '${STORAGE_URL}')
    DROP CREDENTIAL [${STORAGE_URL}];
CREATE CREDENTIAL [${STORAGE_URL}]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '${SAS_TOKEN}';
PRINT 'Credential created';
"

echo ""
echo "=== Step 2: Restore ${DB} from Azure Blob (this may take several minutes) ==="
sqlcmd -Q "
RESTORE DATABASE [${DB}]
FROM URL = N'${STORAGE_URL}/${BAK_FILE}'
WITH MOVE N'TPCC' TO N'/var/opt/mssql/data/tpcc.mdf',
     MOVE N'TPCC_log' TO N'/var/opt/mssql/data/tpcc_log.ldf',
     STATS = 10;
PRINT 'Restore complete';
"

echo ""
echo "=== Step 3: Set RECOVERY FULL and DELAYED_DURABILITY ==="
sqlcmd -Q "
ALTER DATABASE [${DB}] SET RECOVERY FULL;
ALTER DATABASE [${DB}] SET DELAYED_DURABILITY = DISABLED;
PRINT 'Database settings applied';
"

echo ""
echo "=== Step 4: Log backup to NUL (initialize LSN chain) ==="
sqlcmd -Q "BACKUP LOG [${DB}] TO DISK = 'NUL' WITH NO_COMPRESSION;"

echo ""
echo "=== Step 5: Add ${DB} to AG ==="
sqlcmd -Q "
IF NOT EXISTS (
    SELECT 1 FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
    JOIN sys.databases d ON drs.database_id = d.database_id
    WHERE ag.name = 'AG1' AND d.name = '${DB}' AND drs.is_local = 1
)
BEGIN
    ALTER AVAILABILITY GROUP [AG1] ADD DATABASE [${DB}];
    PRINT 'Added ${DB} to AG1';
END
ELSE PRINT '${DB} already in AG1';
"

echo ""
echo "=== Step 6: Wait for SYNCHRONIZED on all 3 replicas ==="
bash "$(dirname "$0")/wait-synchronized.sh"
