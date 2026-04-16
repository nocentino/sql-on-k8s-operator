#!/bin/bash
# setup-db.sh — Create a lightweight test database and add it to the AG
set -euo pipefail
SA="YourStrong!Passw0rd"
DB="testdb"

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
echo "=== Step 0: Remove $DB from AG and drop on all replicas (if present) ==="
sqlcmd -Q "
IF EXISTS (
    SELECT 1 FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
    JOIN sys.databases d ON drs.database_id = d.database_id
    WHERE ag.name = 'AG1' AND d.name = '$DB' AND drs.is_local = 1
)
BEGIN
    ALTER AVAILABILITY GROUP [AG1] REMOVE DATABASE [$DB];
    PRINT '$DB removed from AG1';
END
ELSE PRINT '$DB not in AG1';
"
for pod in mssql-ag-0 mssql-ag-1 mssql-ag-2; do
    kubectl exec "$pod" -- /opt/mssql-tools18/bin/sqlcmd \
        -S localhost,1433 -U sa -P "$SA" -No -C -h -1 -W \
        -Q "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '$DB') BEGIN DROP DATABASE [$DB]; PRINT 'Dropped $DB on $pod'; END ELSE PRINT '$DB not on $pod'" \
        2>/dev/null || true
done

echo ""
echo "=== Step 1: Create $DB ==="
sqlcmd -Q "
CREATE DATABASE [$DB];
ALTER DATABASE [$DB] SET RECOVERY FULL;
"

echo ""
echo "=== Step 2: Seed with a small table ==="
sqlcmd -Q "
USE [$DB];
CREATE TABLE dbo.SeedData (id INT IDENTITY PRIMARY KEY, val NVARCHAR(100));
INSERT INTO dbo.SeedData (val) VALUES ('hello'), ('world'), ('test');
"

echo ""
echo "=== Step 3: Full backup + log backup to NUL (initialize LSN chain) ==="
sqlcmd -Q "BACKUP DATABASE [$DB] TO DISK = 'NUL';"
sqlcmd -Q "BACKUP LOG [$DB] TO DISK = 'NUL' WITH NO_COMPRESSION;"

echo ""
echo "=== Step 4: Add $DB to AG ==="
sqlcmd -Q "
IF NOT EXISTS (
    SELECT 1 FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
    JOIN sys.databases d ON drs.database_id = d.database_id
    WHERE ag.name = 'AG1' AND d.name = '$DB' AND drs.is_local = 1
)
BEGIN
    ALTER AVAILABILITY GROUP [AG1] ADD DATABASE [$DB];
    PRINT 'Added $DB to AG1';
END
ELSE PRINT '$DB already in AG1';
"

echo ""
echo "=== Step 5: Wait for SYNCHRONIZED on all 3 replicas ==="
bash "$(dirname "$0")/wait-synchronized.sh"
