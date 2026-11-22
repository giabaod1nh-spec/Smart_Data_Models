# Silver V1 operational runbook

Current runtime is `python -m de.silver.main`, existing service `de-silver-processor`, port8095. Approved architecture, Python reader and acceptance contracts are in `docs/DEv1/phase-3/`.

## Start and observe

```bash
docker compose --profile v1-raw --profile v1-bronze --profile v1-silver up -d de-kafka-raw-v1 de-bronze-v1 de-silver-processor
docker compose ps de-silver-processor
docker compose logs --tail 100 de-silver-processor
curl -sS http://localhost:8095/health
curl -sS http://localhost:8095/ready
```

Dependencies: healthy ClickHouse and `de-bronze-v1`, completed `de-migrate-v1-silver` applying008 after Bronze006/007. Silver opens Bronze/archive read-only and owns `de/artifacts/silver_v1/checkpoint.sqlite3`. Preserve that checkpoint, its WAL companions and lock path. Retain the bound Bronze authority and complete journals from1; never prune, replace or rewind beneath bound Silver/replay state.

## Dependency failure and recovery

ClickHouse/source/archive outages keep readiness false or degraded and retry with bounded backoff. Inspect `/health`, persisted obligations and lifecycle poll state; restore the same authority, then confirm cursor progress and `/ready`. Missing archive is retryable; malformed archive/schema/source-binding or same-key content conflict becomes FAULTED. Preserve evidence and resolve the mismatch before restart. Never clear frozen state to make readiness green. Canonical readers return typed AUTHORITY_UNAVAILABLE/SOURCE_LAG/INVALIDATION_LAG and no partial page.

## Restart and replay

`docker compose restart de-silver-processor` resumes frozen obligations and both journal cursors. Offline diagnostics: `python -m de.scripts.inspect_silver_v1` and `python -m de.scripts.inspect_silver_retention_v1` in the configured environment. Freeze replay with `python -m de.scripts.freeze_silver_replay_v1 --run-id RUN --replay-id ID`; see `python -m de.silver.replay --help` for execution/resume. Replay uses isolated state/locks/CH targets; changed semantic boundary aborts the ID. Whole-run gate: `python -m de.scripts.certify_silver_run_v1 --run-id RUN --cycles 120`.

## Forbidden operations

Do not start Gold V2 against Silver V1, clear/rewrite live state, bypass canonical validity through fact-table scans, or destructively modify historical V2 database objects. Phase4 owns Gold V1 implementation. The historical text below documents the retired V2 configuration and is not a current launch/recovery procedure.

---

# Archived historical V2 runbook
# Silver Processor Runbook

**Path:** `docs/shared/SILVER_RUNBOOK.md`  
**Canonical launch:** root `docker compose` only  
**Service:** `de-silver-processor` (port **8095**)

---

## Start

```bash
docker compose up -d --build de-silver-processor
docker compose ps de-silver-processor
curl -sS http://localhost:8095/health
curl -sS http://localhost:8095/ready
```

Dependencies (Compose-enforced): ClickHouse healthy, `de-migrate` completed (`--historical-v2`), `de-bronze-processor` healthy.

## Stop

```bash
docker compose stop de-silver-processor
```

`/ready` becomes unavailable; instance lock is released on clean shutdown.

## Restart

```bash
docker compose restart de-silver-processor
```

Checkpoint file `/app/de/artifacts/silver/checkpoint.sqlite3` (host: `de/artifacts/silver/checkpoint.sqlite3`) must retain namespace `live` offsets (never regress).

## Logs

```bash
docker compose logs de-silver-processor --tail 200 --no-color
```

## Health / readiness

| Endpoint | Meaning |
|---|---|
| `GET /health` | Cached processor snapshot (state, lag, metrics) |
| `GET /ready` | 200 only when READY, deps OK, lock held, snapshot fresh (`SILVER_HEALTH_SNAPSHOT_MAX_AGE_SEC`, Compose=60s) |

Lag > 0 with recent progress is allowed for READY. Lag > 0 without progress beyond `SILVER_READINESS_STALE_SEC` (120s) is DEGRADED → `/ready` 503.

## Checkpoint inspection

```bash
sqlite3 de/artifacts/silver/checkpoint.sqlite3 "SELECT checkpoint_namespace,source_table,topic,partition_id,last_completed_offset FROM silver_checkpoint;"
```

## Replay (isolated)

```bash
PYTHONPATH=. python -m de.silver.replay --manifest path/to/manifest.json --run-id <id>
# resume only with identical manifest hash:
PYTHONPATH=. python -m de.silver.replay --manifest path/to/manifest.json --run-id <id> --resume
```

Replay uses namespace `replay:<id>`, writes only replay tables, must not mutate live checkpoint bytes or main Silver multisets. Approach/Scenario dimension mirrors do not exist — candidates are suppressed.

## Lock contention

Only one live writer per checkpoint namespace. If start fails on lock: ensure no second Silver process; stop stale container; do not delete the SQLite file without incident review.

## FAULTED escalation

1. Capture `/health` JSON (`fault_code`, `fault_message`).  
2. Capture container logs.  
3. Capture checkpoint + ledger sample for the failing source id.  
4. Fix root cause (schema/conflict/env).  
5. `docker compose restart de-silver-processor`.  
6. Do **not** reset Kafka, truncate Silver/Bronze, or invent a new live namespace.

## Forbidden operations

- Resetting Kafka consumer groups as a Silver fix  
- Deleting `checkpoint.sqlite3` to “unblock”  
- Truncating Silver/Bronze tables for evidence  
- Pointing Compose live service at `destination_mode=replay`  
- Enabling `de-webhook` outside profile `rollback`

## Evidence retention

Store S4 evidence under `de/artifacts/silver/evidence/` (compose ps, health JSON, smoke JSON, restart checkpoints, replay isolation JSON).
