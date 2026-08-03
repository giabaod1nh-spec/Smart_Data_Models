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
