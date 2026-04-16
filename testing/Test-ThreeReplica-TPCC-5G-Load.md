Switch the interactive shell to `bash` to avoid the quoting and heredoc issues encountered previously in `zsh`. 

Change the deployment to delete rather than retain the existing `sqlserveravailabilitygroup` resources to ensure a clean slate for testing.

You can remove any existing `sqlserveravailabilitygroup` resources, associated Pods, PVs,and PVCs using the following commands.

Perform a clean deployment and validation of the 3-sync-replica Availability Group using the following sequence:

Store all logs and outputs in a structured directory for post-test analysis in ./mytesting/logs

1. **Environment Cleanup & Deployment**:
   - Delete any existing `sqlserveravailabilitygroup` resources, associated Pods, and PVCs.
   - Deploy the 3-sync AG configuration using `config/samples/sql_v1alpha1_sqlserveravailabilitygroup_3sync.yaml`.
   - Wait until `status.initializationComplete` is `true` for the `mssql-ag` resource.

2. **Database Restore from Azure Blob**:
   - On the primary replica (`mssql-ag-0`), create a SQL Server Credential for `https://testdbaen.blob.core.windows.net/testdb` using your SAS token.
   - Restore the `tpcc` database from `https://testdbaen.blob.core.windows.net/testdb/TPCC-5G.bak`. Use `MOVE` clauses to map logical files `TPCC` and `TPCC_log` to `/var/opt/mssql/data/tpcc.mdf` and `/var/opt/mssql/data/tpcc_log.ldf`.
   - Post-restore, execute:
     ```sql
     ALTER DATABASE [tpcc] SET RECOVERY FULL;
     ALTER DATABASE [tpcc] SET DELAYED_DURABILITY = DISABLED;
     ```

3. **AG Integration & Synchronization**:
   - Add the database to the AG.
   - Use the `wait-synchronized.sh` script to poll until `tpcc` reports `SYNCHRONIZED` and `HEALTHY` on all three replicas (`mssql-ag-0`, `mssql-ag-1`, and `mssql-ag-2`).

4. **Load Testing & Failover Execution**:
    - Start a HammerDB TPC-C workload (50 warehouses) and initiate the 1-second health monitor. Run a log backup loop every 30 seconds to NUL. 
    - For the hammerdb test, use https://github.com/nocentino/hammerdb and set up the hammerdb.env file properly.
    - On a failover you'll need to restart the hammerdb test since it will disconnect.
    - Also, no rampup time is needed, just start counting right away.
    - Run the tests for 5 minutes, then perform the failovers. 
    - A real DBA wouldn't attempt this failover until the databaes are heathly and synchronized, so we should do the same here. Don't block the tests on time, but replication state, but cap out the test at 60 seconds. if a replica is lost and permanently not healthy, fail the test.
    - **Test A (Planned Rotation)**: Execute a coordinated failover sequence: `0 -> 1`, then `1 -> 2`, then `2 -> 0`. Ensure all replicas return to a `HEALTHY` and `SYNCHRONIZED` synchronization state between each hop. Monitor for any errors.
    - **Test B (Unplanned Failover)**: Simulate a hard failure by `SIGKILL`ing the SQL Server process or deleting the primary Pod 3 consecutive times. Allow the operator to detect the failure and perform an automatic promotion. Wait for the cluster to reach a steady `HEALTHY` state before the next kill.

5. **Log Collection & Forensic Analysis**:
    - Export the operator controller logs, store them in `./mytesting/logs/
    - Capture the container logs (`stdout/stderr`) for all three `mssql` containers.
    - Execute `sp_readerrorlog` on all three replicas to capture internal SQL Server state transitions and errors.
    - Provide a summary analysis of the failover timings and any data hardening failures.

