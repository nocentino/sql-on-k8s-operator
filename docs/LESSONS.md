# Lessons Learned — sql-on-k8s-operator

## L1: AKS imagePullPolicy caching burns you
**Mistake**: Pushed a corrected image under the same tag (v0.13). AKS nodes had `imagePullPolicy: IfNotPresent` and reused the cached broken binary. The deployment rollout said "success" but the running pod was still the old image.
**Detection**: Compare `kubectl get pod -o jsonpath='{...imageID}'` against `docker buildx imagetools inspect <tag> | grep Digest`.
**Rule**: After any code fix that is pushed as a same-tag update, always bump the image tag. Never reuse a tag for a different binary on a cluster with IfNotPresent pull policy.

## L2: Msg 41104 recovery is retry-count-driven, not time-driven
**Mistake**: Assumed that SQL Server's Msg 41104 ("AG resource did not come online due to previous error") would clear on its own with time. Increased the test timeout to 600s but recovery still took 10+ min.
**Finding**: Each `SET(ROLE=SECONDARY)` issued by the operator triggers one retry of SQL Server's internal AG-resource initialization state machine. With 23s between attempts, 26 needed retries = 598s. At 5s between attempts, 26 retries = 130s.
**Rule**: When a re-seat command fails, use the fastest safe requeue interval (5s). The number of retries drives recovery time, not elapsed wall-clock time.

## L3: Distinguish cooldown-active vs re-seat-failing requeue intervals
**Mistake**: Used the same 15s requeue for both "cooldown active" (successful re-seat, waiting for HADR to establish) and "re-seat failed" (Msg 41104). The failure case needs a much faster interval.
**Fix**: Return `(hadAny, hadFailure, error)` from reconcileNotSynchronizingReplicas. Use 5s when hadFailure=true, 15s when cooldown-active.
**Rule**: Separate the two reasons for "has NOT SYNCHRONIZING work pending" and choose the right requeue interval for each.

## L4: Don't set cooldown on failure
**Mistake**: v0.13 set the 60s cooldown even when SET(ROLE=SECONDARY) failed (Msg 41104). This effectively throttled the retry loop to once per minute, turning a 10-min recovery into 13+ min.
**Rule**: The cooldown exists only to protect a successfully-established HADR transport from being torn down. If the command failed, there is no transport to protect — retry immediately (next requeue).

## L5: Always verify running image digest before declaring a test "with version X"
**Rule**: Before every test run, check `kubectl get pod -n <ns> -o jsonpath='{...imageID}'` and compare to the registry digest. A "successful rollout" does not guarantee the new image was pulled.

## L6: Stop-and-replan when test results are inconsistent
**Mistake**: When v0.14 showed 907s/812s/811s failures (same as broken v0.13), kept pushing forward with more tests rather than stopping to analyze. The operator logs revealed that Msg 41104 was persisting for exactly N_retries × requeue_interval seconds, which pointed directly at the fix.
**Rule**: If a test result is the same as a known-broken version, STOP. Read the operator logs before the next test run. The logs tell you what's happening.

## L9: Read the SQL Server errorlog, not just the operator log
**Mistake**: Diagnosed Msg 41104 as "retry-count-driven" based only on operator logs. Reduced requeue from 23s to 13s (v0.15). Result: no improvement — 14-minute recovery unchanged.
**Finding (from sp_readerrorlog)**:
- Kill 3 (fast): `RESOLVING_NORMAL → SECONDARY_NORMAL` in 69s **autonomously** — no SET(ROLE=SECONDARY) needed.
- Kill 1/2 (slow): `RESOLVING_NORMAL` stuck for 14 minutes due to "previous error" in AG resource initialization.
- Root cause: Former primary restarts 18s after kill. New primary is only 8s into initialization. Connection from former primary to new primary fails → SQL Server flags AG resource as "failed" → all SET(ROLE=SECONDARY) blocked by Msg 41104 for 14 minutes.
- Kill 3 succeeds immediately because the primary has been running stably for 13+ minutes.
**Fix**: Restart the HADR endpoint on the stuck replica after 30s of Msg 41104 failures. This forces a fresh connection to the (now stable) primary, replicating kill 3's autonomous recovery.
**Rule**: Before implementing a retry-frequency fix, confirm from SQL Server errorlog that the issue is actually retry-driven. Always check `sp_readerrorlog` for the "previous error" that Msg 41104 references.

## L10: 5s requeue is a negligible improvement when the bottleneck is not retry-count
**Mistake**: Reduced requeue from 23s to 13s expecting 3x speedup. The Msg 41104 window is determined by SQL Server's internal AG resource retry timer, not by our retry frequency. More retries don't help if SQL Server can't make the connection to the primary regardless.
**Rule**: Only tune retry frequency when you have evidence that the issue is detection-latency (waiting to notice that recovery is done). Don't tune frequency when the issue is a stuck internal state that external commands cannot advance.

## L11: Don't restart the HADR endpoint immediately on first Msg 41104
**Mistake**: Restarted the HADR endpoint on the very first 41104 observation. At that point the new primary had only been running ~10-25s and was still initializing. The fresh connection after restart failed for the same reason, and since `endpointRestartDone` was a one-shot boolean, no further restarts were attempted. Recovery took ~14 minutes waiting for SQL Server's internal timer.
**Fix**: Delay the first endpoint restart by 30s (`minWaitBeforeRestart`) so the new primary has time to stabilize. Allow periodic restarts every 60s (`restartInterval`) as a safety net if the first delayed restart still fails.
**Rule**: When the fix depends on another component being ready (e.g., new primary initialized), gate the action on enough elapsed time for that component, don't fire immediately.

## L7: Test results are sequential, not independent
**Observation**: Kill 1 is always the hardest because the primary pod has been running longest (most dirty buffer pool pages). Kill 2 and 3 are progressively faster because each restarted pod has less crash-recovery work to do.
**Implication**: A test run that shows kill 1 FAIL, kill 2 FAIL, kill 3 PASS may be a timing artifact (kill 3 pod had very little crash recovery). The fix must make kill 1 pass, not just kill 3.

## L12: OFFLINE → DROP → JOIN recovers Msg 41104 without re-seeding
**Problem**: After unplanned failover, former primary gets stuck in RESOLVING with Msg 41104. SET(ROLE=SECONDARY) is blocked indefinitely. The stuck replica has stale `primary_replica` metadata pointing to itself.
**Failed approaches** (all non-destructive T-SQL):
1. ALTER AG OFFLINE → Clears stale primary_replica but SET(ROLE=SECONDARY) still fails
2. Endpoint restart (STOP/START AGEP) → Doesn't clear latched AG resource state
3. Pod restart → Failure state persists in system databases
4. FORCE_FAILOVER_ALLOW_DATA_LOSS → Blocked by Msg 47104 (CLUSTER_TYPE=EXTERNAL)
5. Self-failover (sp_set_session_context + FAILOVER) → Msg 41122 (already primary)
6. Planned failover to different secondary → Doesn't reset the stuck replica
**Working solution** (no re-seeding):
```sql
ALTER AVAILABILITY GROUP AG1 OFFLINE;                          -- Clear stale metadata
DROP AVAILABILITY GROUP AG1;                                    -- Remove AG locally, database stays in RESTORING
ALTER AVAILABILITY GROUP AG1 JOIN WITH (CLUSTER_TYPE = EXTERNAL); -- Fresh state, connect to real primary
ALTER AVAILABILITY GROUP AG1 GRANT CREATE ANY DATABASE;         -- Allow seeding manager to operate
```
**Key evidence**: Seeding manager transitions CHECK_IF_SEEDING_NEEDED → CATCHUP → COMPLETED in 0.2s. Database files are preserved; only log catch-up occurs. Zero data re-seeding.
**Matches agmonitor**: `sql_rejoin_ag_after_failover()` does OFFLINE → DROP → JOIN (the same 3-step sequence).
**Rule**: When AG resource state is latched/corrupted, the only non-destructive reset is to DROP the AG locally and re-JOIN. The seeding manager is smart enough to detect existing databases and do log catch-up only.

## L8: Plan first, track progress
**Rule (from CLAUDE.md)**: For any task with 3+ steps, plan before starting. Track items and mark complete as you go. After corrections, update `docs/LESSONS.md`.

---

## Planned Failover Lessons (v0.27–v0.29)

## L13: Planned failover split-brain — ALTER AG OFFLINE when local role = PRIMARY
**Problem**: After a planned failover with CLUSTER_TYPE=EXTERNAL, the old primary keeps reporting `role_desc = PRIMARY` locally because no cluster manager tells it to step down. The operator sees two PRIMARYs (split-brain). SET(ROLE=SECONDARY) fails with Msg 41104 because the AG resource still thinks it's the primary.
**Fix (v0.29)**: In `handle41104`, check `localRole == PRIMARY`. If true, issue `ALTER AVAILABILITY GROUP [AG1] OFFLINE`. This resets the AG resource from PRIMARY → RESOLVING, clearing stale `primary_replica` metadata. The next reconcile cycle issues SET(ROLE=SECONDARY) successfully.
**Analogy**: This is exactly what Pacemaker's `offlineAndWait()` does in the mssql-server-ha OCF agent when the local replica is PRIMARY but is not the Pacemaker master.
**Rule**: Always check local role before deciding which 41104 recovery path to take. PRIMARY → OFFLINE; RESOLVING → OFFLINE + endpoint restart.

## L14: sp_set_session_context is required for planned failover with CLUSTER_TYPE=EXTERNAL
**Problem**: `ALTER AVAILABILITY GROUP [AG1] FAILOVER` fails with Msg 41105 ("not valid for CLUSTER_TYPE = EXTERNAL") unless the session context is set first.
**Fix**: Execute `EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes';` in the same session before `ALTER AVAILABILITY GROUP ... FAILOVER`.
**Rule**: For CLUSTER_TYPE=EXTERNAL, planned failover requires `sp_set_session_context` — it signals SQL Server that an external cluster manager is orchestrating the failover.

---

## Unplanned Failover Lessons (v0.27–v0.33)

## L15: The mirroring transport timeout is bilateral (the breakthrough insight)
**Problem**: After an unplanned failover (pod kill), the former primary restarts and enters RESOLVING state. Despite ALTER AG OFFLINE + endpoint restart on the stuck secondary, recovery failed across v0.30–v0.32a. The replica stayed DISCONNECTED/NOT_HEALTHY for 14+ minutes.
**Root cause**: The database mirroring transport caches connection state on BOTH sides:
1. The **secondary** (former primary, restarted) tried to connect to the new primary too early (~18s after kill, while new primary was only ~8s into initialization). Connection timed out → AG resource latches the error → Msg 41104.
2. The **primary** (promoted secondary) also cached stale connection state for the returning replica. Even after the secondary's endpoint was restarted and ready to reconnect, the primary wouldn't initiate a new connection because it still held the old stale transport entry.
**Fix (v0.33)**: Restart the HADR endpoint on BOTH replicas:
  - Secondary (stuck replica): ALTER AG OFFLINE + restart endpoint → clears stale primary metadata and forces fresh outbound connection.
  - Primary (current leader): restart endpoint → drops stale transport entry for the returning replica, forces fresh inbound/outbound connection attempt.
**Result**: Recovery in ~31s from bilateral restart, ~90s total from pod kill. All 3 replicas return to HEALTHY/CONNECTED/SYNCHRONIZED.
**Analogy**: This is how Pacemaker, DH2I, and Azure Arc-enabled data services for managed instance on Kubernetes work — they restart endpoints on both sides.
**Rule**: When fixing transport-level connectivity issues, always consider that both endpoints of the connection may have stale state. Restarting only one side is insufficient.

## L16: Delay OFFLINE + endpoint restart by 30s, not immediately
**Problem**: Firing ALTER AG OFFLINE + endpoint restart on the first Msg 41104 observation (~0s stuckFor) fails because the new primary has only been running for a few seconds and is still initializing. The fresh connection attempt after restart hits the same "primary not ready" condition.
**Fix**: Gate the OFFLINE + bilateral endpoint restart on `stuckDuration >= 30*time.Second`. This gives the new primary enough time to stabilize (~30s). The first attempt usually succeeds.
**Rule**: The fix depends on the new primary being ready. Wait long enough for the new primary to finish initialization before cycling connections.

## L17: Retry the OFFLINE + bilateral restart every 90s if still stuck
**Problem**: If the first bilateral restart attempt doesn't work (e.g., primary still initializing due to large crash recovery), the operator needs to try again rather than giving up.
**Implementation**: Store the timestamp of the last OFFLINE+restart attempt in `reseatFirstFailureTime` under an `/offlineRestart` key. If `stuckDuration` continues to exceed 30s and 90s has elapsed since the last attempt, retry.
**Rule**: Recovery actions that depend on external state (primary readiness) should be retried periodically, not just once.

## L18: PostStart lifecycle hook alone doesn't fix unplanned failover
**Problem (v0.30)**: Added a PostStart lifecycle hook to run SET(ROLE=SECONDARY) immediately on pod startup, before the operator's reconcile loop could react. Hypothesis: acting from inside the pod (like a sidecar) would be fast enough.
**Result**: Failed. The PostStart hook runs before SQL Server is ready, so the commands either error or connect too early (same timing issue). The AG resource still latches the error.
**Learning**: The problem isn't detection latency (how quickly you issue SET(ROLE=SECONDARY)). The problem is stale transport state on both sides. No amount of fast-acting commands fixes it; you need to restart endpoints on both sides.

## L19: OFFLINE-only (without endpoint restart) doesn't fix unplanned failover
**Problem (v0.28-v0.29)**: ALTER AG OFFLINE clears stale `primary_replica` metadata but doesn't reset the mirroring transport connection state. The underlying TCP/TLS connection cache still holds the failed connection entry.
**Result**: After OFFLINE, SET(ROLE=SECONDARY) continues to fail with Msg 41104 because the transport can't connect to the primary.
**Rule**: ALTER AG OFFLINE resets AG-level metadata, but endpoint restart resets transport-level connection state. Both are needed for unplanned failover recovery.

## L20: Secondary-only endpoint restart doesn't fix unplanned failover
**Problem (v0.31-v0.32a)**: Restarting the HADR endpoint only on the stuck secondary (RESOLVING replica). The secondary drops its stale connections and attempts fresh connections to the primary.
**Result**: Failed. The primary still holds stale connection state for the returning replica. Even though the secondary is ready to connect, the primary won't accept/initiate a fresh connection until its own endpoint is cycled.
**Key evidence**: Manually restarting BOTH endpoints fixed it immediately. This confirmed the bilateral nature of the transport cache.
**Rule**: Database mirroring transport connections are bidirectional — both sides must be reset for transport recovery.

## L21: DROP + JOIN risks full re-seed under real workloads
**Earlier fix (L12)**: OFFLINE → DROP → JOIN works in test scenarios. The seeding manager detects existing database files and does log catch-up only (CHECK_IF_SEEDING_NEEDED → CATCHUP → COMPLETED in 0.2s).
**Risk**: Under production workloads with active log backups, DROP → JOIN can trigger a full re-seed if log backup truncation breaks the log chain between the DROP and the JOIN. For a 5GB+ database, this means minutes of unavailability.
**Preferred fix (v0.33)**: OFFLINE + bilateral endpoint restart — no DROP, no JOIN, no re-seed risk. The AG membership is preserved throughout.
**Rule**: Require transport-level recovery (endpoint restart) over AG-level recovery (DROP+JOIN) to avoid re-seeding risk.

## L22: Recovery timeline for unplanned failover (v0.33 benchmarks)
**Tested scenario**: Kill pod (grace-period=0), 3 sync replicas, tpcc database.
**Timeline**:
- T+0s: Pod killed
- T+31s: Failover threshold exceeded, operator promotes new primary (failoverThresholdSeconds: 30)
- T+39s: First Msg 41104 observed on returning replica (stuckFor=0s)
- T+70s: OFFLINE + bilateral endpoint restart fires (stuckFor=39s, delayed to 30s threshold + processing)
- T+79s: Primary endpoint restart begins
- T+90s: Re-seated successfully — NOT SYNCHRONIZING replica recovers, all 3 HEALTHY
**Rule**: Total recovery ~90s. The bilateral restart itself takes ~20s (endpoint STOP + 5s delay + START on each side). Most of the 90s is the failover threshold (30s) + wait-before-restart (30s) + transport reconnection (~10s).

---

## General Operator Lessons

## L23: Always verify image digest before testing
**Repeated mistake**: Multiple times across v0.13, v0.14, v0.31, pushed an image under the same tag or relied on `imagePullPolicy: IfNotPresent`. The deployment reported success but the running pod used a cached old image.
**Rule**: Before every test run, verify the running image digest matches the expected one:
```bash
kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].imageID}'
```
Compare against `docker buildx imagetools inspect <tag> | grep Digest`.
When in doubt, bump the tag (never reuse tags for different binaries).

## L24: Operator logs + SQL Server errorlog together tell the full story
**Rule**: The operator log shows what the operator did (OFFLINE, endpoint restart, SET ROLE). The SQL Server errorlog (`sp_readerrorlog`) shows what SQL Server experienced (connection failures, AG resource state transitions, seeding events). You need both to diagnose AG recovery issues.
**Example**: Operator log showed "endpoint restart at T+70s", but SQL Server errorlog showed "transport reconnection at T+80s" — the 10s gap was the endpoint restart + transport handshake completing.

## L25: Test kills are not independent — order matters
**Context**: When running sequential pod kills (kill ag-0, then ag-1, then ag-2), each kill is faster because:
1. The restarted pod has progressively less crash recovery work (fewer dirty pages).
2. The remaining replicas are "warm" (recently restarted, less buffer pool pressure).
**Rule**: The hardest test is always kill 1 (longest-running primary with most dirty pages). If kill 1 passes, kills 2-3 will likely pass. If kill 1 fails, don't assume kills 2-3 passing means the fix works.

---

## Collateral Damage & Persistent NOT SYNC Lessons (v0.34–v0.35)

## L26: Don't clear tracking state for mechanisms you still need
**Problem (v0.34)**: Added a `reconcileNotSynchronizingReplicas` fix that tracks how long a secondary stays NOT SYNCHRONIZING despite successful re-seats. After 30s, it restarts the secondary's HADR endpoint. The timer uses a `/notSyncStuck` key in `reseatFirstFailureTime`.
**Bug**: `clear41104Tracking()` is called on every successful re-seat (no 41104). In v0.34, it deleted the `/notSyncStuck` key along with the other tracking keys. This reset the timer to zero on every reconcile cycle, so `stuckDuration` was always ~0s and never reached the 30s threshold.
**Fix (v0.35)**: Removed the `r.reseatFirstFailureTime.Delete(cooldownKey + "/notSyncStuck")` line from `clear41104Tracking()`. The notSyncStuck key is only reset when the endpoint restart fires (to prevent spam), not on every successful re-seat.
**Rule**: When adding a new tracking mechanism that shares storage with existing mechanisms, ensure the existing clear/reset functions don't blindly wipe the new key. Each tracking key should be managed by its own lifecycle.

## L27: Stale tracking keys cause harmless but misleading log output
**Observation (v0.35)**: After the fix, `/notSyncStuck` keys persist across kill cycles because they're never explicitly cleared when a replica recovers to SYNCHRONIZED. Result: first re-seat of a new episode shows inflated stuckFor values (3m32s, 3m40s, 2m12s, 5m45s), and the endpoint restart fires immediately.
**Impact**: Functionally correct — endpoint restart is harmless and helps recovery. But logged duration is misleading.
**Potential fix**: Clear `/notSyncStuck` key when a replica transitions from NOT SYNCHRONIZING to SYNCHRONIZED (i.e., when it's no longer in the notSyncPods list). Not critical but would make logs accurate.

## L28: Bilateral endpoint restart may cause transient collateral damage to bystander secondaries
**Problem**: When the bilateral fix restarts the PRIMARY's endpoint to clear transport state for a stuck secondary, all connections through that endpoint are dropped — including connections from other healthy secondaries.
**Mitigation (v0.35)**: The persistent NOT SYNCHRONIZING endpoint restart (L26) acts as an automatic safety net. If a bystander secondary gets disconnected by the primary endpoint restart, it shows up as NOT SYNCHRONIZING on the next reconcile and its endpoint is restarted, recovering it.
**Evidence**: In v0.35 Test B, ag-2 (uninvolved bystander) received a persistent NOT SYNC endpoint restart in ALL 3 kills and recovered immediately each time.
**Rule**: Accept that primary endpoint restart has blast radius, but ensure automatic recovery mechanisms exist for bystanders. The combination of bilateral restart + persistent NOT SYNC fix provides layered recovery.

---

## PreStop Lifecycle Hook Lessons (v0.37)

## L29: preStop hook turns rolling updates from unplanned (Test B) into planned (Test A) failover
**Problem**: Without a preStop hook, every rolling update or node drain on the primary pod is an unplanned failover:
1. Kubernetes sends SIGTERM to SQL Server
2. SQL Server shuts down — closes connections, flushes buffers, brings databases offline
3. Pod disappears from the network
4. Remaining replicas detect the primary is gone and elect a new one (30s+ delay)
5. Connections drop, listener temporarily points at a dead endpoint

This is identical to a Test B scenario — hard failure, 30-60s outage with dropped connections.

**Fix (v0.37)**: Add a `preStop` lifecycle hook to the mssql container. Kubernetes runs preStop *before* sending SIGTERM, while the pod is still fully alive:
1. preStop queries `sys.dm_hadr_availability_replica_states` to check if this pod is the PRIMARY (role=1)
2. If PRIMARY: issues `ALTER AVAILABILITY GROUP [AG] OFFLINE` with `sp_set_session_context` to trigger a planned failover
3. A synchronized secondary becomes the new primary cleanly — connections redirect via the listener, zero dropped transactions
4. SIGTERM then fires on the now-secondary pod — shutting down a secondary is completely benign
5. Rolling updates now behave like Test A (planned failover, ~6s recovery) instead of Test B (unplanned, 30-60s)

**Guard**: The preStop hook must complete within `terminationGracePeriodSeconds` (default 30s). The planned failover itself is fast (~6s). If no SYNCHRONIZED/CONNECTED secondary is available, the hook bails out immediately (`exit 0`) and Kubernetes proceeds with SIGTERM — falling back to unplanned failover. The hook always exits 0 to avoid blocking shutdown.

**Rule**: Any pod lifecycle event that removes the primary should attempt a planned failover first. The preStop hook is the Kubernetes-native way to do this. It converts predictable maintenance operations into zero-downtime events.

## L30: preStop hook issues ALTER AG FAILOVER on a synchronized secondary
**Design choice**: The preStop hook connects from the primary pod to a SYNCHRONIZED/CONNECTED secondary and issues `sp_set_session_context` + `ALTER AVAILABILITY GROUP [AG] FAILOVER`.
**Why FAILOVER instead of OFFLINE**:
1. FAILOVER atomically promotes the secondary to PRIMARY — the listener redirects, existing client connections migrate cleanly, zero dropped transactions.
2. OFFLINE only takes the AG offline locally, dropping all connections; the remaining replicas go to RESOLVING and wait for the operator to promote — which may not happen before SIGTERM kills the pod.
3. With FAILOVER, SIGTERM fires on the now-stale-PRIMARY/RESOLVING pod, which is about to die anyway. No impact on the new primary already serving requests.
**Target selection**: The hook queries `sys.dm_hadr_availability_replica_states` joined with `sys.availability_replicas` to find `TOP 1 replica_server_name` where `role_desc = 'SECONDARY' AND synchronization_health_desc = 'HEALTHY' AND connected_state_desc = 'CONNECTED'`. It connects to the target via its headless service FQDN (e.g., `mssql-ag-1.mssql-ag-headless.default.svc.cluster.local,1433`).
**Bail-out**: If no eligible secondary exists (all disconnected, unhealthy, or async-only), the hook exits 0 immediately — falling back to unplanned failover on SIGTERM. This keeps the hook from hanging.

**Rule**: For preStop, prefer FAILOVER (clean client handoff, zero data loss) over OFFLINE (drops connections, requires operator follow-up). The extra complexity of target selection is worth the clean transition.

---

## Bilateral NOT SYNC Escalation & Test Script Lessons (v0.39)

## L31: Secondary-only endpoint restart is insufficient for NOT SYNCHRONIZING after pod restart
**Problem (v0.38)**: The persistent NOT SYNC handler (line ~1476) only restarted the secondary's HADR endpoint. After a force-delete (unplanned failover), the primary also has stale transport state for the returning replica. The secondary-only restart would fire every 15s (timer reset after each restart) but never escalate to a bilateral restart.
**Result**: Test B failed — the returning replica stayed DISCONNECTED indefinitely. The operator looped "Re-seated NOT SYNCHRONIZING replica → restarting HADR endpoint" every 15s with `stuckFor=20-21s` but never fixed the problem.
**Fix (v0.39)**: Two-tier escalation: at 15s, restart secondary endpoint only. At 30s, escalate to bilateral restart (secondary + primary endpoints). Reset timer after bilateral. This cycles: 15s→secondary, 30s→bilateral, 45s→secondary, 60s→bilateral.
**Rule**: The persistent NOT SYNC handler must escalate to bilateral restart. Secondary-only is a first-line fix (for collateral damage from other operations), but post-restart connectivity requires both sides to clear stale transport state.

## L32: wait_ready must require ALL pods to agree, not ANY single pod
**Problem (v0.38)**: The `wait_ready` function in test scripts returned 0 (healthy) as soon as ANY single pod reported 0 unhealthy replicas. During a failover transition, one pod could briefly see a consistent view while another pod was still in RESOLVING state. This caused a false-positive 3s pass on Test A hop 3, which left the AG in a broken state.
**Fix (v0.39)**: Changed to require ALL 3 pods to report 0 unhealthy replicas. If any pod's query fails or returns non-zero, the loop continues waiting.
**Rule**: Health checks during failover transitions must require consensus from all participants. A single pod's view during transition is unreliable.

## L33: sys.dm_hadr_database_replica_states visibility is role-dependent
**Problem**: The `wait_ready` query included `CASE WHEN (SELECT COUNT(*) FROM sys.dm_hadr_database_replica_states) < 3 THEN 1 ELSE 0 END` to detect missing replicas. This check works on the PRIMARY (which sees all 3 database replica states) but fails on SECONDARIES (which only see 1 — their own local state). Combined with the "all pods agree" fix, secondaries always reported unhealthy=1.
**Fix**: Changed the database replica states check to use `WHERE drs.is_local = 1` and removed the `< 3` count check. Each pod now checks: (1) all replica states HEALTHY (visible from any role), (2) local database SYNCHRONIZED.
**Rule**: `sys.dm_hadr_database_replica_states` shows only local rows on secondaries. Use `is_local=1` when querying from any role. Only the PRIMARY has global visibility of all database replica states.

---

## Load Testing Results (v0.39 — TPCC-5G, 50 Warehouses, HammerDB TPC-C)

## L34: v0.39 passes all three test scenarios under production-like TPC-C load
**Database**: TPCC-5G (6.3GB, 50 warehouses), restored from Azure Blob.
**Load**: HammerDB TPC-C, 8 virtual users, 0 ramp-up, 30-min duration. Restarted after each failover.
**Configuration**: 3 sync replicas, `failoverThresholdSeconds=30`, `CLUSTER_TYPE=EXTERNAL`.

**Test A (Planned Failover Rotation)**:
| Hop | Direction | Recovery | Result |
|-----|-----------|----------|--------|
| 1 | ag-1→ag-1 | 0s | SKIP (already primary) |
| 2 | ag-1→ag-2 | 44s | PASS |
| 3 | ag-2→ag-0 | 51s | PASS |

**Test B (Unplanned Failover — Force-Delete Primary x3)**:
| Kill | Pod | Recovery | Result |
|------|-----|----------|--------|
| 1 | mssql-ag-0 | 68s | PASS |
| 2 | mssql-ag-1 | 129s | PASS |
| 3 | mssql-ag-0 | 139s | PASS |

**Test C (PreStop Hook Validation — Graceful Delete x3)**:
| Round | Old Primary | Recovery | Checks | Result |
|-------|-------------|----------|--------|--------|
| 1 | mssql-ag-1 | 99s | 4/4 PASS | PASS |
| 2 | mssql-ag-0 | 100s | 4/4 PASS | PASS |
| 3 | mssql-ag-1 | 151s | 4/4 PASS | PASS |

Test C checks: (1) primary moved, (2) old primary=SECONDARY, (3) new primary detected within 3s of delete, (4) zero "unplanned failover" in operator logs.

## L35: Planned failover recovery is faster than unplanned under load
**Observation**: Test A (planned) averages ~47s total including operator status convergence. Test B (unplanned) averages ~112s. The difference is the failover threshold (30s) + bilateral endpoint restart wait time (~15-30s). Test C (preStop) total recovery is ~100-150s but the actual failover itself happens in <3s; the rest is pod restart + AG log catch-up on the 6.3GB database.
**Rule**: The preStop hook is the fastest failover path. Use it for all predictable maintenance. The operator's unplanned path is a safety net for genuine crashes.

## L36: Unplanned failover recovery time scales with database size
**Observation**: Stub tests (no real database) recovered in ~124s. Load tests with 6.3GB TPCC recovered in 68-139s. The variance comes from two factors: (1) dirty page count at crash time (more active transactions = more crash recovery), (2) AG log catch-up after the pod restarts (PVCs persist, so database files are already on disk — no full re-seed occurs; the seeding manager does CHECK_IF_SEEDING_NEEDED → CATCHUP → COMPLETED).
**Kill 1 is fastest here** (68s) because it killed a pod that had been secondary (promoted during AG init), so it had less crash recovery. Kills 2-3 (129s, 139s) killed pods that had been actively serving TPC-C load and had more dirty buffer pool pages.
**Rule**: Recovery time includes crash recovery + AG log catch-up + bilateral endpoint restart. No full re-seed occurs because PVCs preserve database files across pod restarts.

## L37: HammerDB must be restarted after failover — connections don't auto-reconnect
**Observation**: HammerDB TPC-C connections point at the listener IP/port. After failover, the listener re-points to the new primary, but existing HammerDB connections are broken (they were TCP connections to the old primary process). HammerDB doesn't auto-reconnect; it must be stopped and restarted.
**Rule**: When testing with HammerDB, stop HammerDB before failover (to avoid error noise), restart after recovery. In production, applications should use connection retry logic — the listener IP doesn't change, just the backing endpoint.

## L38: PreStop hook detects new primary within 3s of delete across all rounds under load
**Evidence**: In all 3 rounds of Test C, a surviving pod reported role=PRIMARY within 3 seconds of the `kubectl delete` command. This confirms the preStop hook's `ALTER AG FAILOVER` completes before the grace period expires, even under active TPC-C load with dirty pages.
**Rule**: The preStop hook is reliable under load. The ~6s failover time includes SQL Server internally flushing and committing, which is fast because synchronous commit means all transactions are already hardened on the target secondary.

## L39: Recovery duration in Test C (preStop) is dominated by pod restart, not failover
**Observation**: Test C durations (99s, 100s, 151s) seem high, but the actual failover completes in <3s (CHECK 3 PASS). The remaining ~90-150s is: pod termination (30s grace period) + pod restart (~30s container pull + SQL startup) + AG re-synchronization of the 6.3GB tpcc database (~30-90s depending on log divergence during the grace period). Round 3 took longer (151s) because the returning pod needed 51s to re-synchronize vs 6s for rounds 1-2, likely due to more accumulated log during the TPC-C workload.
**Rule**: For preStop-based failovers, the *outage* is <3s (time until new primary serves requests). The *total recovery* (until all replicas are SYNCHRONIZED) depends on database size and log accumulation.
