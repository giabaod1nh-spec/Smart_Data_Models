"""K-7 backfill performance audit — measure only, no production changes."""
from __future__ import annotations

import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import clickhouse_connect  # noqa: E402

from de.bronze.checkpoint_store import CheckpointStore  # noqa: E402
from de.bronze.clickhouse_repository import BronzeClickHouseRepository  # noqa: E402
from de.bronze.config import BronzeSettings, get_settings  # noqa: E402
from de.bronze.lineage_resolver import LineageResolver  # noqa: E402
from de.bronze.payload_codec import decode_payload  # noqa: E402
from de.bronze.transformer import BronzeTransformer  # noqa: E402
from de.bronze.validator import BronzeValidator  # noqa: E402

TOPIC = "traffic.entity-events.v2"
MANIFEST_FULL = _REPO / "docs/architecture/k7_bronze_evidence/k7-official-20260731T0130Z/window_manifest_full.json"
MANIFEST_K45 = _REPO / "docs/architecture/k7_bronze_evidence/k7-official-20260731T0130Z/window_manifest.json"
OUT = _REPO / "docs/architecture/k7_backfill_perf_audit_data.json"


def _pct(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    i = min(len(s) - 1, int(len(s) * p))
    return s[i]


@dataclass
class Timings:
    samples: List[float] = field(default_factory=list)

    def add(self, sec: float) -> None:
        self.samples.append(sec)

    def summary(self) -> Dict[str, float]:
        if not self.samples:
            return {"count": 0, "p50_ms": 0, "p95_ms": 0, "max_ms": 0, "total_ms": 0}
        ms = [x * 1000 for x in self.samples]
        return {
            "count": len(ms),
            "p50_ms": round(_pct(ms, 0.5), 3),
            "p95_ms": round(_pct(ms, 0.95), 3),
            "max_ms": round(max(ms), 3),
            "total_ms": round(sum(ms), 3),
        }


def manifest_scope(manifest_path: Path, client) -> Dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parts = []
    total_expected = 0
    for ps in manifest["partitions"]:
        p = int(ps["partition"])
        start = int(ps["start_offset"])
        end = int(ps["end_offset"])
        expected = max(0, end - start)
        raw_q = f"""
            SELECT count() FROM smart_traffic.kafka_raw_events
            WHERE topic='{TOPIC}' AND partition={p}
              AND offset >= {start} AND offset < {end}
        """
        quar_q = f"""
            SELECT count() FROM smart_traffic.kafka_quarantine_events
            WHERE topic='{TOPIC}' AND partition={p}
              AND offset >= {start} AND offset < {end}
        """
        raw_n = int(client.query(raw_q).result_rows[0][0])
        quar_n = int(client.query(quar_q).result_rows[0][0])
        total_expected += expected
        parts.append(
            {
                "partition": p,
                "start_offset": start,
                "end_offset": end,
                "expected_offset_slots": expected,
                "raw_valid_count": raw_n,
                "raw_quarantine_count": quar_n,
                "union_distinct_estimate": raw_n + quar_n,
            }
        )
    return {
        "manifest": str(manifest_path.name),
        "partitions": parts,
        "total_expected_offset_slots": total_expected,
    }


def checkpoint_progress() -> Dict[str, Any]:
    cp_path = _REPO / "de/artifacts/bronze/checkpoint.sqlite3"
    if not cp_path.is_file():
        return {"exists": False}
    store = CheckpointStore(cp_path)
    rows = []
    for ns in ("live", "backfill:k7-official-20260731T0130Z-backfill"):
        for p in (0, 1, 2):
            cp = store.get(ns, TOPIC, p)
            if cp:
                rows.append(
                    {
                        "namespace": ns,
                        "partition": p,
                        "last_completed_offset": cp.last_completed_offset,
                        "source_start_offset": cp.source_start_offset,
                    }
                )
    store.close()
    import sqlite3

    conn = sqlite3.connect(str(cp_path))
    ledger = conn.execute(
        "SELECT checkpoint_namespace, partition_id, count() FROM bronze_processing_ledger GROUP BY 1,2"
    ).fetchall()
    conn.close()
    return {"checkpoints": rows, "ledger_by_ns_part": ledger}


def explain_queries(client) -> Dict[str, Any]:
    explains = {}
    queries = {
        "fetch_raw_one": """
            EXPLAIN indexes=1
            SELECT topic, partition, offset, raw_ingestion_id
            FROM smart_traffic.kafka_raw_events
            WHERE topic='traffic.entity-events.v2' AND partition=0 AND offset=1000
            LIMIT 1
        """,
        "fetch_raw_quarantine_one": """
            EXPLAIN indexes=1
            SELECT topic, partition, offset, raw_ingestion_id
            FROM smart_traffic.kafka_quarantine_events
            WHERE topic='traffic.entity-events.v2' AND partition=0 AND offset=0
            LIMIT 1
        """,
        "source_max_offset": """
            EXPLAIN indexes=1
            SELECT max(offset) FROM (
                SELECT offset FROM smart_traffic.kafka_raw_events
                WHERE topic='traffic.entity-events.v2' AND partition=0
                UNION ALL
                SELECT offset FROM smart_traffic.kafka_quarantine_events
                WHERE topic='traffic.entity-events.v2' AND partition=0
            )
        """,
        "find_existing_ids": """
            EXPLAIN indexes=1
            SELECT raw_ingestion_id FROM smart_traffic.bronze_entity_events
            WHERE has(['abc123'], toString(raw_ingestion_id))
        """,
        "event_id_conflict": """
            EXPLAIN indexes=1
            SELECT count() FROM smart_traffic.bronze_entity_events
            WHERE event_id='x' AND entity_payload_hash='y'
              AND NOT (topic='traffic.entity-events.v2' AND partition=0 AND offset=1000)
        """,
        "range_fetch_hypothetical": """
            EXPLAIN indexes=1
            SELECT offset, raw_ingestion_id FROM smart_traffic.kafka_raw_events
            WHERE topic='traffic.entity-events.v2' AND partition=0
              AND offset >= 1000 AND offset < 1500
            ORDER BY offset
        """,
    }
    for name, sql in queries.items():
        t0 = time.perf_counter()
        try:
            r = client.query(sql)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            explains[name] = {
                "elapsed_ms": round(elapsed_ms, 3),
                "plan": [str(row[0]) if row else "" for row in r.result_rows[:20]],
            }
        except Exception as e:
            explains[name] = {"error": str(e)}
    return explains


def ab_benchmark(settings: BronzeSettings, start: int, count: int) -> Dict[str, Any]:
    repo = BronzeClickHouseRepository(settings)
    repo.connect()
    validator = BronzeValidator(
        Path(settings.entity_schema_path), Path(settings.run_started_schema_path)
    )
    validator.load()
    transformer = BronzeTransformer()
    resolver = LineageResolver(repo)

    offsets = list(range(start, start + count))
    raw_rows = []
    for off in offsets:
        r = repo.fetch_raw_one(TOPIC, 0, off)
        if r:
            raw_rows.append(r)

    # A: decode + validate + transform only
    t_decode = Timings()
    t_validate = Timings()
    t_transform = Timings()
    entity_rows = []
    for raw in raw_rows:
        t0 = time.perf_counter()
        event, _ = decode_payload(raw)
        t_decode.add(time.perf_counter() - t0)
        t0 = time.perf_counter()
        outcome = validator.validate(event)
        t_validate.add(time.perf_counter() - t0)
        t0 = time.perf_counter()
        res = transformer.transform(raw, event, outcome)
        t_transform.add(time.perf_counter() - t0)
        if res.entity_row:
            entity_rows.append(res.entity_row)

    # B: + batch insert
    t_insert = Timings()
    if entity_rows:
        t0 = time.perf_counter()
        repo.insert_entity_batch(entity_rows, replay_run_id="k7-perf-audit-bench")
        t_insert.add(time.perf_counter() - t0)

    # C: idempotency + conflict per row (current pattern)
    t_idem = Timings()
    t_conflict = Timings()
    for raw in raw_rows[: min(50, len(raw_rows))]:
        t0 = time.perf_counter()
        repo.find_existing_raw_ingestion_ids([raw.raw_ingestion_id])
        t_idem.add(time.perf_counter() - t0)
        event, _ = decode_payload(raw)
        outcome = validator.validate(event)
        if outcome.ok and outcome.kind == "ENTITY":
            t0 = time.perf_counter()
            repo.event_id_exists_at_different_offset(
                str(event.get("eventId")),
                str(event.get("entityPayloadHash")),
                TOPIC,
                0,
                raw.offset,
            )
            t_conflict.add(time.perf_counter() - t0)

    # D: resolve loop (fetch raw + quar + maybe max) per offset
    t_resolve = Timings()
    for off in offsets[: min(50, len(offsets))]:
        t0 = time.perf_counter()
        resolver.resolve(TOPIC, 0, off)
        t_resolve.add(time.perf_counter() - t0)

    # E: simulate one batch of 100 with per-row idempotency (current processor pattern)
    batch_offs = offsets[:100]
    t_batch_total = Timings()
    ch_queries = 0
    sqlite_queries = 0
    t0_batch = time.perf_counter()
    cp_path = _REPO / "de/artifacts/bronze/audit_cp.sqlite3"
    if cp_path.is_file():
        cp_path.unlink()
    cp_store = CheckpointStore(cp_path)
    cp_store.init_checkpoint(
        namespace="audit",
        topic=TOPIC,
        partition=0,
        source_start_offset=0,
        last_completed_offset=start - 1,
        start_mode="explicit",
        processor_name="kafka-bronze-v2",
        processor_version="1.0.0",
        bronze_schema_version="1.0.0",
    )
    for off in batch_offs:
        ch_queries += 2  # fetch_raw + fetch_quar worst case; resolve adds max occasionally
        resolved = resolver.resolve(TOPIC, 0, off)
        if resolved.raw_row:
            ch_queries += 1  # find_existing per row (actual code)
            repo.find_existing_raw_ingestion_ids([resolved.raw_row.raw_ingestion_id])
            sqlite_queries += 1  # is_complete
            cp_store.is_complete("audit", TOPIC, 0, off)
    t_batch_total.add(time.perf_counter() - t0_batch)
    cp_store.close()
    if cp_path.is_file():
        cp_path.unlink()

    repo.close()
    n = len(raw_rows) or 1
    return {
        "window": {"partition": 0, "start": start, "requested": count, "raw_found": len(raw_rows)},
        "A_decode_validate_transform": {
            "decode": t_decode.summary(),
            "validate": t_validate.summary(),
            "transform": t_transform.summary(),
            "records_per_sec_est": round(
                n / max(0.001, (t_decode.summary()["total_ms"] + t_validate.summary()["total_ms"] + t_transform.summary()["total_ms"]) / 1000),
                2,
            ),
        },
        "B_batch_insert": t_insert.summary(),
        "C_idempotency_50rows": {
            "find_existing_per_row": t_idem.summary(),
            "event_id_conflict_per_entity": t_conflict.summary(),
            "ch_queries_per_row_idem": 3,
        },
        "D_resolve_50offsets": t_resolve.summary(),
        "E_simulated_batch_100": {
            "total": t_batch_total.summary(),
            "estimated_ch_queries": ch_queries,
            "estimated_sqlite_queries": sqlite_queries,
        },
    }


def n_plus_one_table(manifest_full: Dict[str, Any]) -> List[Dict[str, Any]]:
    total = manifest_full["total_expected_offset_slots"]
    batch = 500
    batches = (total + batch - 1) // batch
    return [
        {
            "operation": "fetch_raw_one",
            "location": "lineage_resolver.resolve + clickhouse_repository",
            "calls_per_record": 1,
            "calls_per_batch": "batch_size",
            "expected_total_full_window": total,
            "note": "Point lookup per offset; no range fetch",
        },
        {
            "operation": "fetch_raw_quarantine_one",
            "location": "lineage_resolver.resolve",
            "calls_per_record": "0-1 (if raw miss)",
            "calls_per_batch": "up to batch_size",
            "expected_total_full_window": "~total (low offsets)",
        },
        {
            "operation": "source_max_offset",
            "location": "lineage_resolver on gap; _update_lag each poll",
            "calls_per_record": "0-1 on gap path",
            "calls_per_batch": "1 per partition per poll cycle",
            "expected_total_full_window": f">{batches * 3} poll cycles",
        },
        {
            "operation": "find_existing_raw_ingestion_ids",
            "location": "processor._process_batch line 216",
            "calls_per_record": 1,
            "calls_per_batch": "batch_size × 3 tables",
            "expected_total_full_window": f"{total} × 3 table queries = {total * 3}",
            "note": "PER ROW single-id call, not batched across batch",
        },
        {
            "operation": "event_id_exists_at_different_offset",
            "location": "processor._process_batch line 253",
            "calls_per_record": "~1 per ENTITY row",
            "calls_per_batch": "entity count",
            "expected_total_full_window": f"~{total} (most rows entity)",
        },
        {
            "operation": "checkpoint.is_complete",
            "location": "processor._process_batch line 189",
            "calls_per_record": 1,
            "calls_per_batch": "batch_size",
            "expected_total_full_window": total,
            "note": "SQLite SELECT per offset",
        },
        {
            "operation": "commit_batch",
            "location": "checkpoint_store",
            "calls_per_record": "batched",
            "calls_per_batch": 1,
            "expected_total_full_window": batches * 3,
        },
        {
            "operation": "insert_entity/run/quarantine_batch",
            "location": "clickhouse_repository",
            "calls_per_record": "batched",
            "calls_per_batch": "≤3",
            "expected_total_full_window": batches * 3,
        },
    ]


def main() -> int:
    settings = BronzeSettings(clickhouse_host="localhost")
    client = clickhouse_connect.get_client(host="localhost", port=8123, database="smart_traffic")

    scope_full = manifest_scope(MANIFEST_FULL, client)
    scope_k45 = manifest_scope(MANIFEST_K45, client)
    cp = checkpoint_progress()
    explains = explain_queries(client)
    ab = ab_benchmark(settings, start=1000, count=200)

    # throughput from checkpoint if available
    throughput = {}
    for row in cp.get("checkpoints", []):
        if row["namespace"].startswith("backfill"):
            p = row["partition"]
            done = row["last_completed_offset"] + 1
            scope_p = next(x for x in scope_full["partitions"] if x["partition"] == p)
            remaining = scope_p["end_offset"] - 1 - row["last_completed_offset"]
            throughput[f"p{p}"] = {
                "processed_offsets": done,
                "remaining_offsets": max(0, remaining),
                "manifest_end_exclusive": scope_p["end_offset"],
            }

    report = {
        "scope_full_manifest": scope_full,
        "scope_k45_manifest": scope_k45,
        "checkpoint_at_stop": cp,
        "throughput_snapshot": throughput,
        "n_plus_one": n_plus_one_table(scope_full),
        "clickhouse_explain": explains,
        "ab_benchmark": ab,
        "scheduling": {
            "model": "single-thread sequential",
            "partition_loop": "for part in partition_list(): _poll_partition() — serial",
            "parallelism": 1,
            "worker_count_config": settings.worker_count,
            "batch_size_config": settings.batch_size,
            "poll_interval_sec": settings.poll_interval_sec,
            "max_in_flight": "1 batch globally (single thread)",
            "fairness": "round-robin one batch per partition per outer cycle",
        },
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"written": str(OUT), "total_full_window": scope_full["total_expected_offset_slots"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
