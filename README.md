# sql-on-k8s-operator

A Kubernetes operator for **SQL Server on Linux** that automates the full lifecycle of standalone instances and Always On Availability Groups (AG).

## About

For a detailed introduction to see [Introducing sql-on-k8s-operator](https://www.nocentino.com/posts/2026-04-12-introducing-sql-on-k8s-operator/).

## Table of Contents

- [About](#about)
- [Overview](#overview)
- [Quick Start](#quick-start)
- [Automatic Failover](#automatic-failover)
- [Admission Webhooks](#admission-webhooks)
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
- **Automatic unplanned failover** (when `clusterType: EXTERNAL`) — the operator promotes the best synchronous secondary when the primary pod is continuously unhealthy beyond a configurable threshold, then recovers the restarted replica via bilateral HADR endpoint restarts
- **Graceful planned failover via preStop hook** — rolling updates and node drains trigger `ALTER AVAILABILITY GROUP FAILOVER` on a synchronized secondary before SIGTERM, converting maintenance into zero-downtime events
- **Two-tier endpoint restart escalation** — persistent NOT SYNCHRONIZING replicas are recovered with secondary-only restart at 15 s, escalating to bilateral restart (secondary + primary) at 30 s
- **Bootstrap circuit breaker** — if secondaries repeatedly fail to converge during initial `CREATE AVAILABILITY GROUP`, the operator caps DROP-and-recreate retries at 3 and surfaces a `BootstrapFailed` condition rather than looping indefinitely
- **Validating admission webhooks** — admission-time rejection of unsupported mutations (rename, `clusterType` switch, edition/storage changes, replica removal) and cross-field invariants the CRD schema can't express (see [Admission Webhooks](#admission-webhooks))

## Quick Start

### Prerequisites

- Go **1.25+**
- Docker **17.03+** (or compatible container runtime)
- kubectl **v1.11.3+**
- A Kubernetes **v1.11.3+** cluster
- The operator manages its own AG endpoint certificates — no cert-manager required for the core install (`dist/install.yaml`). [cert-manager](https://cert-manager.io/) **is** required only if you deploy via `make deploy` with the validating admission webhooks enabled.

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

The full annotated sample CR is at [`config/samples/sql_v1alpha1_sqlserveravailabilitygroup.yaml`](config/samples/sql_v1alpha1_sqlserveravailabilitygroup.yaml). The sample configures a three-replica AG (`clusterType: EXTERNAL`) with two synchronous replicas and one asynchronous replica, LoadBalancer listeners, and SQL Agent enabled. A three-synchronous-replica variant for failover testing is available at [`config/samples/sql_v1alpha1_sqlserveravailabilitygroup_3sync.yaml`](config/samples/sql_v1alpha1_sqlserveravailabilitygroup_3sync.yaml). Key fields are documented in the [API Reference](#api-reference).

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

### Planned failover (preStop hook)

The operator sets a `preStop` lifecycle hook on every SQL Server container. When Kubernetes terminates the primary pod (rolling update, node drain, manual delete with grace period), the hook runs **before** SIGTERM while the pod is still fully alive:

1. Query `sys.dm_hadr_availability_replica_states WHERE is_local = 1` to check if this pod is PRIMARY (`role = 1`). Exit immediately if not.
2. Find a SYNCHRONIZED, CONNECTED secondary by joining replica states with `sys.availability_replicas`.
3. Connect to the target secondary via its headless service FQDN and issue `sp_set_session_context @key = N'external_cluster', @value = N'yes'; ALTER AVAILABILITY GROUP [AG] FAILOVER`.
4. The secondary atomically becomes PRIMARY — client connections redirect via the listener with zero dropped transactions.
5. SIGTERM then fires on the now-demoted pod. Shutting down a secondary is completely benign.

If no eligible secondary exists, the hook exits 0 immediately and Kubernetes proceeds with SIGTERM — falling back to unplanned failover. The hook always exits 0 to avoid blocking shutdown.

**Net effect:** Rolling updates and node drains behave like planned failovers (~6 s recovery) instead of hard failures (~90 s with connection drops).

### Unplanned failover (automatic promotion)

When the primary pod crashes or is force-deleted (`--grace-period=0`), the preStop hook cannot run. The operator detects the failure through Kubernetes pod readiness and promotes a secondary:

1. The kubelet marks the primary pod `NotReady`. The operator starts the `failoverThresholdSeconds` countdown.
2. After the threshold expires (default 30 s), the operator selects the best synchronous secondary and issues `ALTER AVAILABILITY GROUP FAILOVER` on it.
3. The new primary begins serving requests. The listener Service selector is updated.
4. The killed pod restarts and re-joins the AG in `RESOLVING` state. The operator issues `SET (ROLE = SECONDARY)` to transition it. A **primary guard** ensures the operator never re-seats the pod recorded as the current primary — after a planned failover, the outgoing primary's DMVs can briefly report the new primary as a RESOLVING secondary, and re-seating it would wreck the AG.
5. If `SET (ROLE = SECONDARY)` fails with Msg 41104 (stale AG resource state), the operator escalates:
   - **If the pod still reports PRIMARY locally** (split-brain after planned failover): issue `ALTER AG OFFLINE` to reset from PRIMARY → RESOLVING.
   - **If the pod is in RESOLVING** (unplanned failover): after 15 s, issue `ALTER AG OFFLINE` + bilateral HADR endpoint restart (restart endpoints on both the stuck replica and the current primary). The mirroring transport caches connection state on both sides — restarting only one side is insufficient.
6. If the first bilateral restart doesn't resolve it, retry every 90 s.

### Headless AG detection

After an unplanned kill + pod restart, the recorded primary may come back K8s-Ready but serving as SECONDARY or RESOLVING — leaving the AG with no active PRIMARY. On every reconcile the operator verifies the recorded primary's actual SQL role:

1. If another pod is already PRIMARY (e.g. a preStop-triggered failover completed before the operator noticed), the operator corrects `status.primaryReplica` and requeues.
2. If **no** pod is PRIMARY, the AG is headless. The operator triggers an immediate failover to the best synchronous secondary — bypassing the `failoverThresholdSeconds` timer since the AG is already degraded.

This catches edge cases where the NotReady timer never fires because the pod is technically Ready but not serving as PRIMARY.

### Post-failover NOT SYNCHRONIZING recovery

After any failover, secondaries may lose database synchronization with the new primary. Their replica role is SECONDARY but databases are stuck in NOT SYNCHRONIZING state. The operator handles this with two-tier escalation:

1. Re-issue `SET (ROLE = SECONDARY)` to force re-establishment of the database mirroring session. The **primary guard** ensures the operator never re-seats the pod recorded as the current primary — stale DMV data after a planned failover can briefly report the new primary as a NOT SYNCHRONIZING secondary.
2. If the replica is actively seeding (automatic seeding has not yet delivered all databases), the operator skips re-seating to avoid destabilising the data transfer.
3. If the replica remains NOT SYNCHRONIZING for 15 s despite successful re-seats, restart its HADR endpoint (clears collateral damage from bilateral restarts affecting bystander replicas).
4. If still NOT SYNCHRONIZING at 30 s, escalate to bilateral restart (secondary + primary endpoints). Reset the timer and repeat the cycle.

This layered approach handles direct connectivity failures, collateral damage from primary endpoint restarts, and transient DMV inconsistencies after role transitions.

### Replica selection

When a failover is triggered the operator selects the best available synchronous secondary:

1. Only replicas configured with `availabilityMode: SynchronousCommit` and `failoverMode: Automatic` are candidates (asynchronous replicas are never auto-promoted).
2. A candidate reporting `synchronization_health_desc = HEALTHY` is preferred.
3. If no candidate reports `HEALTHY` — normal after a primary crash, since `sys.dm_hadr_database_replica_states` reports `NOT SYNCHRONIZING` even for replicas that were synchronized — the operator falls back to the first reachable `SynchronousCommit`/`Automatic` replica.

### Failover DDL and external cluster authorization

With `CLUSTER_TYPE = EXTERNAL`, SQL Server requires the cluster manager to identify itself before issuing failover DDL:

```sql
EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';
ALTER AVAILABILITY GROUP [AG1] FAILOVER;
```

This mirrors the promote action in Microsoft's [mssql-server-ha](https://github.com/microsoft/mssql-server-ha) ag-helper. SQL Server verifies from its local copy of the AG configuration that the target replica had received every committed transaction. If not, it rejects the command with **error 41142** and the operator retries on the next reconcile. `FORCE_FAILOVER_ALLOW_DATA_LOSS` is never used.

### Typical planned failover timeline

| Time | Event |
|---|---|
| T+0 s | `ALTER AVAILABILITY GROUP FAILOVER` issued on a SYNCHRONIZED secondary |
| T+~1 s | Target secondary atomically becomes PRIMARY; old primary demotes to SECONDARY in-place |
| T+~2 s | Operator detects role change; updates `status.primaryReplica`; listener Service re-points via `ag-role=primary` label |
| T+~10 s | Demoted replica re-establishes database mirroring session with the new primary |
| T+~60 s | All replicas HEALTHY + SYNCHRONIZED |

No pod is killed — the old primary stays running and transitions to SECONDARY. Recovery time is dominated by the demoted replica re-synchronizing its databases with the new primary.

### Typical unplanned failover timeline

| Time | Event |
|---|---|
| T+0 s | Primary pod is killed / crashes |
| T+0 s | Kubelet marks pod `NotReady` (Layer 1 fires) |
| T+~2 s | Next reconcile detects `NotReady`; `PrimaryNotReadySince` is set; requeue after threshold |
| T+30 s | Threshold exceeded; best `SynchronousCommit`/`Automatic` replica selected |
| T+~31 s | `ALTER AVAILABILITY GROUP FAILOVER` issued on target pod |
| T+~32 s | New primary promoted; `status.primaryReplica` updated; listener Service re-pointed |
| T+~55 s | Killed pod restarts; re-joins as `RESOLVING` |
| T+~60 s | Operator issues `SET (ROLE = SECONDARY)` — may fail with Msg 41104 (stale transport state) |
| T+~75 s | `ALTER AG OFFLINE` + bilateral endpoint restart fires (secondary + primary endpoints cycled) |
| T+~90 s | Transport reconnects; `SET (ROLE = SECONDARY)` succeeds on next reconcile |
| T+~120 s | All replicas HEALTHY + SYNCHRONIZED |

### Measured failover results (v0.41, 3-node AKS, 3 synchronous replicas)

Average wall-clock time from initiation to all replicas HEALTHY + SYNCHRONIZED (3 rounds each):

| Scenario | Stub DB (no load) | TPC-C 5 GB (under load) |
|---|---|---|
| **Planned failover** | ~60 s | ~88 s |
| **Unplanned failover** | ~131 s | ~110 s |

Planned failover completes the role switch in seconds; the bulk of the time is the demoted replica restarting and re-synchronizing. Unplanned failover adds the 30 s `failoverThresholdSeconds` wait plus the 41104 recovery path (ALTER AG OFFLINE + bilateral endpoint restart).

## Admission Webhooks

When deployed via `make deploy` (or any kustomize overlay that includes `config/webhook` and `config/certmanager`), the operator runs a **validating admission webhook** for both CRDs. The webhook rejects mutations that the reconciler cannot safely apply to a live resource and enforces cross-field invariants that the CRD OpenAPI schema cannot express.

**Immutable fields (`SQLServerAvailabilityGroup`):**

| Field | Why it's immutable |
|---|---|
| `spec.agName` | Renaming a live AG requires `ALTER AVAILABILITY GROUP MODIFY NAME`, which is not supported by the reconciler |
| `spec.clusterType` | Switching `NONE`\u2194`EXTERNAL` on a running AG silently breaks failover and listener behaviour |
| `spec.edition` | A SQL Server edition change requires a full reinstall of the binary |
| `spec.storage.storageClassName` / `dataVolumeSize` / `accessModes` | PVC recreation is not supported in v1alpha1 |

**Other checks:**

- `spec.agName` must match `^[A-Za-z_][A-Za-z0-9_]*$` (valid SQL identifier).
- At least one replica is required; **removing** an existing replica is rejected (drop from AG, endpoint grant removal, and PVC cleanup are not automated).
- `automaticFailover.failoverThresholdSeconds` \u2265 10 when set (prevents spurious failovers from transient probe flaps).
- `listener.port` must be in 1\u201365535 when specified.
- `saPasswordSecretRef.name` and `.key` are both required.

`SQLServerInstance` is validated with the same pattern: `edition`, `storage.storageClassName`, `storage.dataVolumeSize`, and `storage.accessModes` are immutable after creation.

Webhook certificates are provisioned by cert-manager (see `config/certmanager/`) and injected into the `ValidatingWebhookConfiguration` via the standard `cert-manager.io/inject-ca-from` annotation. The webhook is not enabled by the bundled `dist/install.yaml`; use the kustomize overlay if you want admission-time validation.

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
| `bootstrapAttempts` | int32 | Number of DROP-and-recreate bootstrap retries; resets to `0` on success and caps at 3 before surfacing `BootstrapFailed` |
| `replicaStatuses` | []AGReplicaStatus | Per-replica role, synchronization state, and connectivity |
| `conditions` | []Condition | Includes `BootstrapFailed` when the circuit breaker trips after repeated bootstrap failures |

## Testing Failover

See [testing.md](testing.md) for full details on test plans, scripts, and load testing under TPC-C.

A quick smoke test using `test-ag-failover.sh`:

```bash
./test-ag-failover.sh all
```

This deploys a three-replica AG, seeds a test database, and exercises both planned and unplanned failover with health checks at every state transition.

Additional helpers under `testing/`:

- `setup-tpcc.sh` — restores the 5 GB `TPCC-5G.bak` from Azure Blob into the current primary, switches the database to FULL recovery, adds it to the AG, and waits for all three replicas to report `SYNCHRONIZED`.
- `test-d-slow-failover.sh` — drives the slow-failover scenario used to validate the `failoverThresholdSeconds` timer and the NOT SYNCHRONIZING recovery ladder under a non-crash primary degradation.
- `wait-synchronized.sh` — dynamically discovers the current primary (via `status.primaryReplica`) before polling `sys.dm_hadr_availability_replica_states`, so it keeps working after unplanned failovers rotate the role off `mssql-ag-0`.

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
kubectl get sqli mssql-standalone -o yaml | grep -A 20 "^status:"
kubectl describe statefulset mssql-standalone
kubectl describe pod mssql-standalone-0
kubectl get pvc -l app=mssql-standalone
kubectl get configmap mssql-standalone-mssql-conf -o yaml
```

### Availability Group — bootstrap

```sh
# Status and primary replica
kubectl get sqlag mssql-ag -o jsonpath='{.status}' | python3 -m json.tool

# Pod readiness and ag-role labels
kubectl get pods -l app=mssql-ag \
  -o custom-columns='POD:.metadata.name,READY:.status.containerStatuses[0].ready,ROLE:.metadata.labels.sql\.mssql\.microsoft\.com/ag-role'

# Pod events
kubectl describe pod mssql-ag-0 | grep -A 20 "^Events:"

# Verify AG cluster type (NONE vs EXTERNAL)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT name, cluster_type_desc FROM sys.availability_groups"
```

### Availability Group — synchronization health

```sh
# Replica health: role, sync state, queue depths
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

# Automatic seeding progress (rows disappear when complete)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, das.current_state,
          das.transferred_size_bytes
   FROM sys.dm_hadr_automatic_seeding das
   JOIN sys.availability_replicas r ON das.remote_id = r.replica_id"

# Connect directly to a specific pod (bypass listener)
kubectl port-forward pod/mssql-ag-1 14331:1433 &
sqlcmd -S localhost,14331 -U sa -P 'YourStrong!Passw0rd' \
  -Q "SELECT @@SERVERNAME, role_desc FROM sys.dm_hadr_availability_replica_states WHERE is_local = 1"
kill %1
```

### Availability Group — failover

```sh
# Check which replicas are failover candidates
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, r.availability_mode_desc,
          r.failover_mode_desc, rs.synchronization_health_desc
   FROM sys.availability_replicas r
   JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id = rs.replica_id
   WHERE r.availability_mode_desc = 'SYNCHRONOUS_COMMIT'
     AND r.failover_mode_desc     = 'AUTOMATIC'"

# Check RESOLVING replicas (operator will transition these on next reconcile)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong!Passw0rd' -Q \
  "SELECT r.replica_server_name, rs.role_desc, rs.operational_state_desc
   FROM sys.dm_hadr_availability_replica_states rs
   JOIN sys.availability_replicas r ON rs.replica_id = r.replica_id
   WHERE rs.role_desc = 'RESOLVING'"
```

### Services and connectivity

```sh
kubectl get svc -l app=mssql-ag -o wide
kubectl get svc mssql-ag-listener -o jsonpath='{.spec.selector}' | python3 -m json.tool
kubectl get svc mssql-ag-listener

# Force a reconcile
kubectl annotate sqlag mssql-ag reconcile-trigger="$(date +%s)" --overwrite
```

### CRD schema errors

```sh
# Re-apply CRDs after editing *_types.go
kubectl apply -f config/crd/bases/sql.mssql.microsoft.com_sqlserveravailabilitygroups.yaml
kubectl apply -f config/crd/bases/sql.mssql.microsoft.com_sqlserverinstances.yaml
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
