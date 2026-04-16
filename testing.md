# Testing

This document covers the failover testing infrastructure for the sql-on-k8s-operator. All scripts live in the `testing/` directory.

## Test plans

Three test plans define the failover validation strategy, each building on the previous:

### 1. Stub database — no load ([Test-ThreeReplica-DBStub.md](testing/Test-ThreeReplica-DBStub.md))

Validates failover mechanics in isolation. Deploys a 3-sync-replica AG, creates a lightweight `tpcc` database (empty, no restore), adds it to the AG, and runs failover scenarios with no transactional load. This is the fastest way to verify operator logic after code changes.

### 2. TPCC-5G database — no load ([Test-ThreeReplica-TPCC-5G.md](testing/Test-ThreeReplica-TPCC-5G.md))

Same failover scenarios but with a real 6.3 GB TPC-C database (50 warehouses) restored from Azure Blob Storage. Tests that the operator handles the seeding, synchronization, and failover of a production-sized database correctly — without concurrent write pressure.

### 3. TPCC-5G database — under TPC-C load ([Test-ThreeReplica-TPCC-5G-Load.md](testing/Test-ThreeReplica-TPCC-5G-Load.md))

The full validation. Restores the same 6.3 GB database, starts HammerDB TPC-C workload (50 warehouses, 8 virtual users), waits 5 minutes for steady state, then runs failover scenarios under continuous transactional pressure. HammerDB is stopped before each failover and restarted after recovery.

## Failover scenarios

All three test plans exercise the same core scenarios:

| Scenario | Script (stub) | Script (load) | What it tests |
|----------|--------------|---------------|---------------|
| **Test A — Planned rotation** | `test-a-planned-stub.sh` | `test-a-load.sh` | Coordinated failover: 0→1→2→0. Each hop uses `ALTER AG FAILOVER` with external cluster session context. Validates the preStop hook path. |
| **Test B — Unplanned failover** | `test-b-unplanned-stub.sh` | `test-b-load.sh` | Force-delete the primary pod 3 times. The operator detects the failure, promotes a sync secondary, and recovers the restarted replica via bilateral HADR endpoint restart. |
| **Test C — PreStop hook** | `test-c-prestop-stub.sh` | `test-c-load.sh` | Graceful delete (with grace period) 3 times. The preStop lifecycle hook runs `ALTER AG FAILOVER` on a synchronized secondary before SIGTERM, so the operator never triggers unplanned failover. |

### Pass criteria

Each round must satisfy:
- All 3 replicas reach `HEALTHY` + `SYNCHRONIZED` within 120 seconds
- The operator updates `status.primaryReplica` to match the actual SQL Server primary
- No data loss (planned failover uses `ALTER AG FAILOVER`, never `FORCE_FAILOVER_ALLOW_DATA_LOSS`)

## End-to-end failover test

`test-ag-failover.sh` is a self-contained script that deploys an AG from scratch and exercises both planned and unplanned failover. It does not require HammerDB or a pre-existing database.

```bash
./testing/test-ag-failover.sh all        # Run all 5 phases
./testing/test-ag-failover.sh deploy     # Phase 1: clean deploy
./testing/test-ag-failover.sh verify     # Phase 2: wait for bootstrap
./testing/test-ag-failover.sh adddb      # Phase 3: create + seed testdb
./testing/test-ag-failover.sh planned    # Phase 4: planned failover
./testing/test-ag-failover.sh unplanned  # Phase 5: unplanned failover
```

## Scripts

### Test scripts

| Script | Description |
|--------|-------------|
| `test-ag-failover.sh` | End-to-end: deploy AG + planned + unplanned failover (no load) |
| `test-a-planned-stub.sh` | Planned failover rotation with stub DB |
| `test-a-load.sh` | Planned failover rotation under TPC-C load |
| `test-b-unplanned-stub.sh` | Unplanned failover ×3 with stub DB |
| `test-b-load.sh` | Unplanned failover ×3 under TPC-C load |
| `test-c-prestop-stub.sh` | PreStop hook failover ×3 with stub DB |
| `test-c-load.sh` | PreStop hook failover ×3 under TPC-C load |

### Helper scripts (called by test scripts)

| Script | Description | Used by |
|--------|-------------|---------|
| `monitor-ag.sh` | Writes 1-second CSV of AG role/health/pod readiness | All test scripts |
| `logbackup-loop.sh` | `BACKUP LOG [tpcc] TO DISK='NUL'` every 30 s | Load test scripts |
| `wait-synchronized.sh` | Polls until all replicas are SYNCHRONIZED + HEALTHY | `setup-db.sh` |

### Setup scripts

| Script | Description |
|--------|-------------|
| `phase1-deploy.sh` | Clean-slate: tear down existing AG, deploy operator + 3-sync CR |
| `setup-db.sh` | Create test DB, add to AG, wait for synchronization |
| `wait-ag-init.sh` | Block until `initializationComplete=true` |

### Diagnostic scripts

| Script | Description |
|--------|-------------|
| `check-health.sh` | Check if all replicas are healthy and synchronized |
| `check-seeding.sh` | Debug automatic seeding progress and failures |
| `diagnose-ag.sh` | Per-pod role/health snapshot (useful when primary is unknown) |
| `collect-logs.sh` | Gather operator + SQL Server logs into `logs/` |

## Load testing prerequisites

The load test scripts (`test-a-load.sh`, `test-b-load.sh`, `test-c-load.sh`) require:

1. A [HammerDB](https://github.com/nocentino/hammerdb) Docker setup
2. A `hammerdb.env` file pointing at the AG listener IP
3. TPCC-5G database (50 warehouses, ~6.3 GB) restored and added to the AG

## Output

All test scripts save results and health CSVs to `testing/logs/`. The CSV from `monitor-ag.sh` captures 1-second snapshots of replica role, sync state, and pod readiness for post-test analysis.
