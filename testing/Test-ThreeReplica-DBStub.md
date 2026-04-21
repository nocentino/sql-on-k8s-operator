ensure youre running the latest operator image.

Switch the interactive shell to `bash` to avoid the quoting and heredoc issues encountered previously in `zsh`. 

Change the deployment to delete rather than retain the existing `sqlserveravailabilitygroup` resources to ensure a clean slate for testing.

You can remove any existing `sqlserveravailabilitygroup` resources, associated Pods, PVs,and PVCs using the following commands.

Perform a clean deployment and validation of the 3-sync-replica Availability Group using the following sequence:

Store all logs and outputs in a structured directory for post-test analysis in ./testing/logs

1. **Environment Cleanup & Deployment**:
   - Delete any existing `sqlserveravailabilitygroup` resources, associated Pods, and PVCs.
   - Deploy the 3-sync AG configuration using `config/samples/sql_v1alpha1_sqlserveravailabilitygroup_3sync.yaml`.
   - Wait until `status.initializationComplete` is `true` for the `mssql-ag` resource.

2. **Create a Test Database and Add it to the AG**:
    - Create a `tpcc` database on the primary replica (`mssql-ag-0`) with the following command:
      ```sql
      CREATE DATABASE tpcc;
      ```
    - Ensure the database is created successfully and is in an `ONLINE` state. In full recovery mode, take a full and a log backup to 'null' to initialize the log chain:
      ```sql
      BACKUP DATABASE [tpcc] TO DISK = 'NUL' WITH NO_COMPRESSION, STATS = 50;
      BACKUP LOG [tpcc] TO DISK = 'NUL' WITH NO_COMPRESSION, STATS = 50;
      ```

3. **AG Integration & Synchronization**:
   - Add the database to the AG.
   - Use the `wait-synchronized.sh` script to poll until `tpcc` reports `SYNCHRONIZED` and `HEALTHY` on all three replicas (`mssql-ag-0`, `mssql-ag-1`, and `mssql-ag-2`).

4. **Failover Execution**:
   - Without running a load test, execute the following failover scenarios:
   - A real DBA wouldn't attempt a failover until the databaes are heathly and synchronized, so we should do the same here. Don't block the tests on time, but replication state, but cap out the test at 60 seconds. if a replica is lost and permanently not healthy, fail the test.
   - **Test A (Planned Rotation)**: Execute a coordinated failover sequence: `0 -> 1`, then `1 -> 2`, then `2 -> 0`. Ensure all replicas return to a `HEALTHY` and `SYNCHRONIZED` synchronization state between each hop. Monitor for any errors. If an error is encountered, fail the test and start troubleshooting.
   - **Test B (Unplanned Failover)**: Simulate a hard failure by `SIGKILL`ing the SQL Server process or deleting the primary Pod 3 consecutive times. Allow the operator to detect the failure and perform an automatic promotion. Wait for all replicas to reach a steady `HEALTHY` state before the next kill. If an error is encountered, fail the test and start troubleshooting.

5. **Log Collection & Forensic Analysis**:
    - Export the operator controller logs, store them in `./testing/logs/
    - Capture the container logs (`stdout/stderr`) for all three `mssql` containers.
    - Execute `sp_readerrorlog` on all three replicas to capture internal SQL Server state transitions and errors.
    - Provide a summary analysis of the failover timings and any data hardening failures.



