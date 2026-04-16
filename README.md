# sql-on-k8s-operator

A Kubernetes operator for **SQL Server on Linux** that automates the full lifecycle of standalone instances and Always On Availability Groups (AG).

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Automatic Failover](#automatic-failover)
- [API Reference](#api-reference)
- [Testing Failover](#testing-failover)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Uninstall](#uninstall)



## Overview

The operator provides two custom resources:

| Kind | API | Purpose |
|------|-----|---------|
| `SQLServerInstance` | `sql.mssql.microsoft.com/v1alpha1` | Standalone SQL Server — one pod, one PVC, one Service |
| `SQLServerAvailabilityGroup` | `sql.mssql.microsoft.com/v1alpha1` | Multi-replica AG with automatic T-SQL bootstrap, certificate-based endpoint auth, and read-write / read-only listener Services |

### What the operator manages:

**SQLServerInstance** — for each CR the operator creates and reconciles:
- A `ConfigMap` containing `mssql.conf` (memory limits, SQL Agent, arbitrary settings)
- A `StatefulSet` with a single SQL Server pod and a persistent data volume
- A headless `Service` (pod DNS) and a ClusterIP `Service` (client access)
- Status conditions including `Available` and the current pod phase

**SQLServerAvailabilityGroup** — for each CR the operator creates and reconciles:
- A `StatefulSet` with one pod per replica, each with its own PVC
- A headless `Service` for intra-cluster DNS (`<pod>.<ag>.svc.cluster.local`)
- A `ConfigMap` with `mssql.conf` applied to every replica
- Certificate-based AG endpoint authentication (self-signed, managed by the operator)
- T-SQL bootstrap: `CREATE AVAILABILITY GROUP … WITH (CLUSTER_TYPE = NONE|EXTERNAL)` and `SEEDING_MODE = AUTOMATIC`
- A read-write **listener** `Service` whose selector tracks the current PRIMARY replica
- An optional read-only **listener** `Service` that targets readable SECONDARY replicas with `ClientIP` session affinity
- Per-replica status (role, synchronization state, connected)
- **Automatic unplanned failover** (when `clusterType: EXTERNAL`) — the operator promotes the best synchronous secondary when the primary pod is continuously unhealthy beyond a configurable threshold

## Quick Start

### Prerequisites

- Go **1.25+**
- Docker **17.03+** (or compatible container runtime)
- kubectl **v1.11.3+**
- A Kubernetes **v1.11.3+** cluster
- [cert-manager](https://cert-manager.io/) is **not** required — the operator manages its own certificates

### 1. Deploy the operator

**Option A — from Docker Hub (recommended):**

```sh
# Install CRDs and deploy the controller directly from the published image
kubectl apply -f https://raw.githubusercontent.com/nocentino/sql-on-k8s-operator/main/dist/install.yaml
```

The controller image (`nocentino/sql-on-k8s-operator:latest`) is available on Docker Hub for both `linux/amd64`.

**Option B — build from source:**

```sh
# Build and push the controller image
make docker-build docker-push IMG=<registry>/sql-on-k8s-operator:latest

# Install CRDs
make install

# Deploy the controller
make deploy IMG=<registry>/sql-on-k8s-operator:latest
```

### 2. Create a standalone SQL Server instance

```sh
# Create the SA password secret
kubectl create secret generic mssql-secret \
  --from-literal=SA_PASSWORD='YourStrong!Passw0rd'

# Apply the sample CR
kubectl apply -f config/samples/sql_v1alpha1_sqlserverinstance.yaml

# Watch until Available
kubectl get sqli mssql-standalone -w

# Connect (Docker Desktop exposes LoadBalancer services on localhost)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd'
```

The full annotated sample CR is at [`config/samples/sql_v1alpha1_sqlserverinstance.yaml`](config/samples/sql_v1alpha1_sqlserverinstance.yaml). Key fields are documented in the [API Reference](#api-reference).

### 3. Create an Availability Group

```sh
# Create the SA password secret
kubectl create secret generic mssql-ag-secret \
  --from-literal=SA_PASSWORD='YourStrong!Passw0rd'

# Apply the sample CR
kubectl apply -f config/samples/sql_v1alpha1_sqlserveravailabilitygroup.yaml

# Watch bootstrap — initializationComplete flips to true when the AG is ready
kubectl get sqlag mssql-ag -w

# Connect via the read-write listener
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT @@SERVERNAME, role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1"
```

The full annotated sample CR is at [`config/samples/sql_v1alpha1_sqlserveravailabilitygroup.yaml`](config/samples/sql_v1alpha1_sqlserveravailabilitygroup.yaml). The sample configures a three-replica AG (`clusterType: EXTERNAL`) with two synchronous replicas and one asynchronous replica, LoadBalancer listeners, and SQL Agent enabled. Key fields are documented in the [API Reference](#api-reference).

## Automatic Failover

Automatic failover is active when `clusterType: EXTERNAL` and `automaticFailover.enabled: true`. The operator acts as the external cluster manager, replacing Pacemaker in a Kubernetes-native way.

### Two-layer health model

The operator uses two independent signals to decide whether the primary is healthy. Together they cover every real-world failure mode.

**Layer 1 — Kubernetes pod readiness (process-level)**

The kubelet continuously evaluates the readiness probe on each pod. When the primary pod transitions to `Ready=False` (crash, OOMKill, failed probe), the operator starts the `failoverThresholdSeconds` countdown timer. This is the primary liveness signal and carries zero SQL overhead — the kubelet owns it.

**Layer 2 — `sp_server_diagnostics` (SQL-internal health)**

On every reconcile the operator runs `EXEC sp_server_diagnostics` directly inside the primary pod via `kubectl exec`. This mirrors the `OpenDBWithHealthCheck` + `QueryDiagnostics` pattern in Microsoft's [mssql-server-ha](https://github.com/microsoft/mssql-server-ha) Pacemaker resource agent. It catches SQL Server internal failures that don't immediately crash the process — and therefore don't flip the pod to `NotReady`:

| `sp_server_diagnostics` component | What it detects |
|---|---|
| `system` | Non-yielding schedulers, OS-level errors |
| `resource` | Buffer pool exhaustion, out-of-memory pressure |
| `query_processing` | Deadlocked worker threads, runaway queries |

When the primary pod **is** `Ready` but `sp_server_diagnostics` reports a failure at or beyond the configured `healthThreshold`, the operator treats the pod as degraded and starts the same failover timer — exactly as if the pod had become `NotReady`.

**Why both layers are needed:**

| Failure scenario | K8s Ready | `sp_server_diagnostics` | Detected by |
|---|---|---|---|
| Pod crash / OOMKill | → NotReady | exec fails | Layer 1 |
| SQL process killed | → NotReady | exec fails | Layer 1 |
| Non-yielding scheduler | → Ready | `system` = error | Layer 2 |
| Memory pressure | → Ready | `resource` = error | Layer 2 |
| Worker deadlock | → Ready | `query_processing` = error | Layer 2 |

If `sp_server_diagnostics` cannot be reached (exec error, SQL temporarily slow), the operator treats the result conservatively as healthy. Layer 1 remains the authority for true liveness failures.

### `healthThreshold` configuration

The `healthThreshold` field maps directly to the component hierarchy checked by `sp_server_diagnostics`. It controls how deep the health check descends before declaring the primary degraded:

| Value | Trigger condition | Equivalent Microsoft constant |
|---|---|---|
| `system` (default) | `system` component in error state | `ServerCriticalError` |
| `resource` | `system` **or** `resource` in error state | `ServerModerateError` |
| `query_processing` | `system`, `resource`, **or** `query_processing` in error state | `ServerAnyQualifiedError` |

Example — increase sensitivity to memory pressure:

```yaml
spec:
  clusterType: EXTERNAL
  automaticFailover:
    enabled: true
    failoverThresholdSeconds: 30
    healthThreshold: resource
```

### Replica selection

When a failover is triggered the operator selects the best available synchronous secondary:

1. Only replicas configured with `availabilityMode: SynchronousCommit` and `failoverMode: Automatic` are candidates (asynchronous replicas are never auto-promoted).
2. The controller queries `synchronization_health_desc` on each candidate. A replica reporting `HEALTHY` is preferred.
3. If no candidate reports `HEALTHY` — which is normal when the primary crashed suddenly, because `sys.dm_hadr_database_replica_states` reports `NOT SYNCHRONIZING` even for replicas that were fully synchronized at the moment of the crash — the operator falls back to the first `SynchronousCommit`/`Automatic` replica it can reach.

This matches the behaviour documented in Microsoft's source:
> *"If the PRIMARY is down, all DB replicas report themselves as NOT SYNCHRONIZING in `sys.dm_hadr_database_replica_states` even if their copy of the AG configuration indicates they were synchronized before the PRIMARY went down."*

### Failover DDL and external cluster authorization

With `CLUSTER_TYPE = EXTERNAL`, SQL Server requires the cluster manager to identify itself before issuing any failover DDL. The operator sets the session context before every failover command:

```sql
EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';
ALTER AVAILABILITY GROUP [AG1] FAILOVER;
```

This is the same DDL for both planned and unplanned failover. It directly mirrors the promote action in Microsoft's [mssql-server-ha](https://github.com/microsoft/mssql-server-ha) ag-helper, which always issues `ALTER AVAILABILITY GROUP FAILOVER` — never `FORCE_FAILOVER_ALLOW_DATA_LOSS`.

With `CLUSTER_TYPE = EXTERNAL`, SQL Server persists each replica's synchronization state locally. When the primary is offline, a secondary can issue `FAILOVER` and SQL Server verifies from its local copy of the AG configuration that the replica had received every committed transaction. If the replica was not synchronized, SQL Server rejects the command with **error 41142** rather than risking data loss. The operator logs the rejection and retries on the next reconcile cycle, trying a different candidate if available.

`FORCE_FAILOVER_ALLOW_DATA_LOSS` is never used by this operator.

### RESOLVING → SECONDARY after failover

After an unplanned failover, the evicted primary pod restarts. With `CLUSTER_TYPE = EXTERNAL`, SQL Server leaves a restarted replica in `RESOLVING` state indefinitely — it waits for the external cluster manager to explicitly assign its role. On every reconcile the operator queries the current primary for any `RESOLVING` replicas and promotes them back to `SECONDARY`:

```sql
ALTER AVAILABILITY GROUP [AG1]
  MODIFY REPLICA ON N'<pod-fqdn>' WITH (ROLE = SECONDARY);
```

### Typical unplanned failover timeline

| Time | Event |
|---|---|
| T+0 s | Primary pod is killed / crashes |
| T+0 s | Kubelet marks pod `NotReady` (Layer 1 fires) |
| T+~2 s | Next reconcile detects `NotReady`; `PrimaryNotReadySince` is set; requeue after threshold |
| T+30 s | Threshold exceeded; best `SynchronousCommit`/`Automatic` replica selected |
| T+~31 s | `ALTER AVAILABILITY GROUP FAILOVER` issued on target pod |
| T+~32 s | New primary promoted; `status.primaryReplica` updated; listener Service re-pointed |
| T+~60 s | Killed pod restarts; re-joins as `RESOLVING` |
| T+~90 s | Operator issues `SET (ROLE = SECONDARY)`; full synchronization resumes |

### Graceful shutdown — preStop lifecycle hook

Without intervention, every rolling update (`kubectl rollout restart`) or node drain on the primary pod triggers an **unplanned** failover — identical to a pod crash. Kubernetes sends SIGTERM, SQL Server shuts down, connections drop, and the surviving replicas spend 30–60 seconds electing a new primary.

The operator sets a `preStop` lifecycle hook on every SQL Server container to convert this into a **planned** failover:

```
preStop → detect PRIMARY (role=1) → find SYNCHRONIZED secondary → connect to secondary
       → ALTER AG FAILOVER on secondary → secondary is now PRIMARY → SIGTERM on now-demoted pod
```

**How it works:**

1. Kubernetes runs the preStop hook **before** sending SIGTERM — the pod is still fully alive.
2. The hook queries `sys.dm_hadr_availability_replica_states WHERE is_local = 1` to check whether this pod is the current PRIMARY (role = 1). If not, it exits immediately (no-op for secondaries).
3. The hook queries for a SYNCHRONIZED, CONNECTED secondary by joining `sys.dm_hadr_availability_replica_states` with `sys.availability_replicas` to get the target's `replica_server_name` (headless service FQDN).
4. The hook connects to the target secondary via its FQDN and port 1433, sets `sp_set_session_context @key = N'external_cluster', @value = N'yes'`, and issues `ALTER AVAILABILITY GROUP [AG] FAILOVER`.
5. The secondary atomically becomes the new PRIMARY — client connections redirect via the listener, zero dropped transactions.
6. SIGTERM then fires on the now-demoted pod — shutting down a former primary that has already handed off leadership is completely benign.

If no SYNCHRONIZED/CONNECTED secondary is available, the hook bails out immediately (`exit 0`) and Kubernetes proceeds with SIGTERM — falling back to unplanned failover. The hook always exits 0 to avoid blocking shutdown.

**Timing constraint:** The hook must complete within `terminationGracePeriodSeconds` (currently 30 s in the operator deployment). The planned failover itself takes ~6 s, well within the budget.

**Net effect:** Rolling updates and node drains behave like planned failovers (~6 s recovery under load) instead of hard failures (~30–60 s with connection drops).

## API Reference

### SQLServerInstance spec fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `image` | string | `mcr.microsoft.com/mssql/server:2025-latest` | SQL Server container image |
| `edition` | string | `Developer` | `Developer`, `Express`, `Standard`, `Enterprise`, `EnterpriseCore` |
| `acceptEula` | string | `Y` | Must be `Y` to accept the SQL Server EULA |
| `saPasswordSecretRef` | SecretKeySelector | — | Secret containing the `SA_PASSWORD` key |
| `port` | int32 | `1433` | TCP port SQL Server listens on |
| `storage.dataVolumeSize` | Quantity | `10Gi` | PVC size for data and log files |
| `storage.storageClassName` | string | — | StorageClass name (cluster default if omitted) |
| `storage.reclaimPolicy` | string | `Retain` | PersistentVolume reclaim policy: `Retain` (data preserved after PVC deletion) or `Delete` (PV removed with PVC). The operator patches the bound PV directly so the StorageClass default is overridden. |
| `mssqlConf` | map[string]string | — | Key-value pairs written to `mssql.conf` |
| `resources` | ResourceRequirements | — | CPU and memory requests/limits |
| `timezone` | string | — | TZ environment variable for the container |
| `additionalEnvVars` | []EnvVar | — | Extra environment variables injected into the pod |

### SQLServerAvailabilityGroup spec fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `agName` | string | — | T-SQL name of the Availability Group |
| `image` | string | `mcr.microsoft.com/mssql/server:2025-latest` | SQL Server container image (all replicas) |
| `edition` | string | `Developer` | SQL Server edition |
| `acceptEula` | string | `Y` | Must be `Y` |
| `saPasswordSecretRef` | SecretKeySelector | — | Secret containing the `SA_PASSWORD` key |
| `endpointPort` | int32 | `5022` | TCP port for the AG mirroring endpoint |
| `replicas` | []AGReplicaSpec | — | 1–9 replica definitions (see below) |
| `storage.dataVolumeSize` | Quantity | `10Gi` | PVC size per replica |
| `storage.storageClassName` | string | — | StorageClass name |
| `storage.reclaimPolicy` | string | `Retain` | PersistentVolume reclaim policy: `Retain` or `Delete`. Applied to each replica's bound PV. Defaults to `Retain` to protect AG data. |
| `mssqlConf` | map[string]string | — | Key-value pairs written to `mssql.conf` on every replica; `hadr.hadrenabled=1` is always set automatically |
| `clusterType` | string | `NONE` | AG cluster type: `NONE` (read-scale, manual failover) or `EXTERNAL` (operator-managed, enables automatic failover) |
| `automaticFailover.enabled` | bool | `true` | Promote a synchronous secondary automatically when the primary is unhealthy (requires `clusterType: EXTERNAL`) |
| `automaticFailover.failoverThresholdSeconds` | int32 | `30` | Seconds the primary must be continuously unhealthy before an automatic failover is triggered (minimum 10) |
| `automaticFailover.healthThreshold` | string | `system` | SQL Server internal health sensitivity: `system` (default), `resource`, or `query_processing` — see [Automatic Failover](#automatic-failover) |
| `listener` | ListenerSpec | — | Read-write Service pointing at the current PRIMARY |
| `readOnlyListener` | ListenerSpec | — | Read-only Service pointing at readable SECONDARY replicas |
| `resources` | ResourceRequirements | — | CPU and memory requests/limits per replica |
| `nodeSelector` | map[string]string | — | Node label constraints for all replica pods |
| `tolerations` | []Toleration | — | Pod tolerations for all replica pods |

**AGReplicaSpec fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | — | Replica name (used as the pod name suffix) |
| `availabilityMode` | string | `SynchronousCommit` | `SynchronousCommit` or `AsynchronousCommit` |
| `failoverMode` | string | `Automatic` | `Automatic` or `Manual` |
| `readableSecondary` | bool | `false` | Allow read queries on this secondary (`SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)`) |

### SQLServerAvailabilityGroup status fields

| Field | Type | Description |
|-------|------|-------------|
| `initializationComplete` | bool | `true` once the AG T-SQL bootstrap has completed |
| `primaryReplica` | string | Pod name of the current PRIMARY replica |
| `phase` | string | Overall lifecycle phase |
| `replicaStatuses` | []AGReplicaStatus | Per-replica role, synchronization state, and connectivity |

## Testing Failover

A self-contained test script, `test-ag-failover.sh`, ships with the repository. It deploys a three-replica AG, seeds a test database, and exercises both planned and unplanned automatic failover end-to-end with health checks at every state transition.

### Prerequisites

- `kubectl` pointed at a cluster with the operator already deployed (see [Quick Start](#quick-start))
- `sqlcmd` on your PATH — install via [mssql-tools18](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools)
- `python3` for JSON pretty-printing of operator status

### Phases

The script is composed of five independent phases that can be run individually or all at once:

| Phase | Command | What it does |
|---|---|---|
| 1 — Deploy | `deploy` | Tears down any existing AG (drops the SQL Server AG, deletes the CR and PVCs for a clean slate), creates the SA password secret, applies the sample CR, and waits for all pods to be Ready |
| 2 — Verify | `verify` | Waits for bootstrap to complete (`InitializationComplete=true`), shows pod labels, listener services, and an initial replica health snapshot |
| 3 — Add database | `adddb` | Creates `testdb`, backs it up, adds it to the AG, waits for all synchronous replicas to reach `HEALTHY`; shows health snapshots before and after seeding |
| 4 — Planned failover | `planned` | Auto-detects the first synchronous secondary, port-forwards to it, issues `ALTER AVAILABILITY GROUP FAILOVER` with the required external-cluster session context, waits for the operator to update `status.primaryReplica`, and shows three health snapshots (pre-DDL, mid-transition, settled) |
| 5 — Unplanned failover | `unplanned` | Sends `SIGKILL` to `sqlservr` inside the primary pod to simulate a crash, watches the operator timer count down, confirms promotion of the new primary, waits for the crashed pod to rejoin as a synchronous secondary and reach `SYNCHRONIZED`, then shows a final all-clear health snapshot |

### Running the full test

```bash
# Make the script executable (first time only)
chmod +x test-ag-failover.sh

# Run all five phases in order
./test-ag-failover.sh all

# Or run phases individually
./test-ag-failover.sh deploy
./test-ag-failover.sh verify
./test-ag-failover.sh adddb
./test-ag-failover.sh planned
./test-ag-failover.sh unplanned
```

Override the SA password if yours differs from the default:

```bash
SA_PASSWORD='MyPassword!' ./test-ag-failover.sh all
```

### Health check output

At each state transition the script prints a labelled box showing both layers of the health model side by side:

```
  ┌─ Pre-planned-failover snapshot
  │  Pod mssql-ag-0   k8s-ready=true   ag-role=primary
  │  Pod mssql-ag-1   k8s-ready=true   ag-role=readable-secondary
  │  Pod mssql-ag-2   k8s-ready=true   ag-role=readable-secondary
  │
  │  Replica     Role       SyncHealth  SyncState    LogSendQ_KB  RedoQ_KB
  │  mssql-ag-0  PRIMARY    HEALTHY     n/a          n/a          n/a
  │  mssql-ag-1  SECONDARY  HEALTHY     SYNCHRONIZED 0            0
  │  mssql-ag-2  SECONDARY  HEALTHY     SYNCHRONIZING 0           0
  └─────────────────────────────────────────────────────────────────────
```

The K8s layer (pod readiness and `ag-role` label) and the SQL layer (role, sync health, sync state, queue depths) are shown together so you can see at a glance whether any lag exists between what Kubernetes reports and what SQL Server reports.

### What to look for

| Transition | Expected `SyncHealth` | Expected `SyncState` |
|---|---|---|
| Initial baseline (empty AG) | `NOT_HEALTHY` | `n/a` — no databases yet |
| After `ADD DATABASE`, seeding in progress | Primary `HEALTHY`; secondaries `NOT_HEALTHY` | `NOT SYNCHRONIZING` → `SYNCHRONIZING` |
| After seeding complete | All `HEALTHY` | Sync replicas: `SYNCHRONIZED`; async replica: `SYNCHRONIZING` |
| Immediately after planned `FAILOVER` DDL | Old primary `NOT_HEALTHY` | `n/a` — mid-transition, listener re-pointing |
| Planned failover settled | All `HEALTHY` | New primary; old primary `SYNCHRONIZED` |
| After unplanned failover — new primary elected | Crashed pod `NOT_HEALTHY` | `RESOLVING` while SQL Server rejoins |
| After crashed pod recovered | All `HEALTHY` | Sync replicas: `SYNCHRONIZED`; async replica: `SYNCHRONIZING` |

> **Note on the async replica (`mssql-ag-2`):** its steady-state `SyncState` is always `SYNCHRONIZING`, never `SYNCHRONIZED`. This is correct behavior for `AsynchronousCommit` — the replica continuously applies log as it arrives rather than holding transactions until the secondary acknowledges. The script only waits for the **synchronous** replica to reach `SYNCHRONIZED` before declaring the final health check.

## Troubleshooting

### Operator

```sh
# Stream operator logs
kubectl logs -n sql-on-k8s-operator-system \
  deployment/sql-on-k8s-operator-controller-manager -f

# Filter for errors and reconcile decisions
kubectl logs -n sql-on-k8s-operator-system \
  deployment/sql-on-k8s-operator-controller-manager --since=10m \
  | grep -E "ERROR|error|Failover|NotReady|threshold|bootstrap|InitializationComplete"

# Check the operator pod itself (ImagePullBackOff, CrashLoopBackOff, etc.)
kubectl describe pod -n sql-on-k8s-operator-system \
  -l control-plane=controller-manager
```

### Standalone instance

```sh
# Check CR status conditions
kubectl get sqli mssql-standalone -o yaml | grep -A 20 "^status:"

# Check the StatefulSet and pod
kubectl describe statefulset mssql-standalone
kubectl describe pod mssql-standalone-0

# Check PVC binding (pod stuck Pending = unbound PVC)
kubectl get pvc -l app=mssql-standalone

# Verify the bound PV has the expected reclaimPolicy (should be Retain by default)
kubectl get pv $(kubectl get pvc mssql-data-mssql-standalone-0 -o jsonpath='{.spec.volumeName}') \
  -o jsonpath='{.spec.persistentVolumeReclaimPolicy}'

# Check mssql.conf that was applied
kubectl get configmap mssql-standalone-mssql-conf -o yaml
```

### Availability Group — bootstrap

```sh
# Watch initializationComplete and primaryReplica
kubectl get sqlag mssql-ag -o jsonpath='{.status}' | python3 -m json.tool

# Check pod readiness and ag-role labels (operator sets these during bootstrap)
kubectl get pods -l app=mssql-ag \
  -o custom-columns='POD:.metadata.name,READY:.status.containerStatuses[0].ready,ROLE:.metadata.labels.sql\.mssql\.microsoft\.com/ag-role'

# Check events for a specific pod (image pull failures, probe failures, etc.)
kubectl describe pod mssql-ag-0 | grep -A 20 "^Events:"

# Check which SQL Server AG cluster type is actually configured
# (NONE vs EXTERNAL — mismatch causes FAILOVER DDL to be rejected)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT name, cluster_type_desc FROM sys.availability_groups"
```

### Availability Group — synchronization health

```sh
# Full replica health: role, sync state, and queue depths
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, rs.role_desc,
          rs.synchronization_health_desc,
          drs.synchronization_state_desc,
          drs.log_send_queue_size, drs.redo_queue_size
   FROM sys.dm_hadr_availability_replica_states rs
   JOIN sys.availability_replicas r ON rs.replica_id = r.replica_id
   LEFT JOIN sys.dm_hadr_database_replica_states drs
       ON rs.replica_id = drs.replica_id AND drs.is_local = 0
   ORDER BY rs.role_desc, r.replica_server_name"

# Check automatic seeding progress (rows disappear when seeding completes)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, das.current_state,
          das.failure_state_desc, das.transferred_size_bytes
   FROM sys.dm_hadr_automatic_seeding das
   JOIN sys.availability_replicas r ON das.remote_id = r.replica_id"

# Check per-database sync state and whether data movement is suspended
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, adc.database_name,
          drs.synchronization_state_desc, drs.is_suspended,
          drs.suspend_reason_desc
   FROM sys.dm_hadr_database_replica_states drs
   JOIN sys.availability_replicas r ON drs.replica_id = r.replica_id
   JOIN sys.availability_databases_cluster adc
       ON drs.group_database_id = adc.group_database_id"

# Connect directly to a specific pod (bypass the listener)
kubectl port-forward pod/mssql-ag-1 14331:1433 &
sqlcmd -S localhost,14331 -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT @@SERVERNAME, role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1"
kill %1
```

### Availability Group — failover

```sh
# Failover rejected with error 41142 = target was not synchronized at the time of the crash.
# The operator retries on the next reconcile. Check which replicas are candidates:
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, r.availability_mode_desc,
          r.failover_mode_desc, rs.synchronization_health_desc
   FROM sys.availability_replicas r
   JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id = rs.replica_id
   WHERE r.availability_mode_desc = 'SYNCHRONOUS_COMMIT'
     AND r.failover_mode_desc     = 'AUTOMATIC'"

# RESOLVING replicas after unplanned failover = operator has not yet run SET ROLE = SECONDARY.
# This clears automatically on the next reconcile. Check current status:
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, rs.role_desc, rs.operational_state_desc
   FROM sys.dm_hadr_availability_replica_states rs
   JOIN sys.availability_replicas r ON rs.replica_id = r.replica_id
   WHERE rs.role_desc = 'RESOLVING'"
```

### Services and connectivity

```sh
# Check listener services and their selectors
kubectl get svc -l app=mssql-ag -o wide

# Check which pod the listener is pointing at (selector must match the PRIMARY pod's labels)
kubectl get svc mssql-ag-listener -o jsonpath='{.spec.selector}' | python3 -m json.tool

# LoadBalancer EXTERNAL-IP stuck as <pending> — normal on bare-metal without MetalLB;
# on Docker Desktop it appears as 'localhost' automatically.
kubectl get svc mssql-ag-listener

# Force a reconcile to correct drifted service types or selector mismatches
kubectl annotate sqlag mssql-ag reconcile-trigger="$(date +%s)" --overwrite
```

### CRD schema errors

```sh
# "unknown field" on kubectl apply = cluster is running the old CRD schema.
# Re-apply the generated CRD after any change to *_types.go:
kubectl apply -f config/crd/bases/sql.mssql.microsoft.com_sqlserveravailabilitygroups.yaml
kubectl apply -f config/crd/bases/sql.mssql.microsoft.com_sqlserverinstances.yaml

# Verify the CRD version the cluster is running
kubectl get crd sqlserveravailabilitygroups.sql.mssql.microsoft.com \
  -o jsonpath='{.metadata.resourceVersion}'
```

## Development

```sh
# Run unit tests
make test

# Run end-to-end tests against the current kubeconfig context
make test-e2e

# Regenerate CRDs and RBAC after editing *_types.go
make manifests generate

# Auto-fix lint issues
make lint-fix

# Run locally (no in-cluster deployment required)
make run
```

Run `make help` for the full list of available targets.

## Uninstall

```sh
# Remove CRs
kubectl delete -k config/samples/

# Remove the controller and CRDs
make undeploy
make uninstall
```

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
