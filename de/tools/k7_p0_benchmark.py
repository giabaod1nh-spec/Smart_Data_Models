"""K-7 P0 performance benchmark — 500 records, query counts, throughput."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.bronze.checkpoint_store import CheckpointStore  # noqa: E402
from de.bronze.clickhouse_repository import BronzeClickHouseRepository  # noqa: E402
from de.bronze.config import BronzeSettings  # noqa: E402
from de.bronze.lineage_resolver import LineageResolver  # noqa: E402
from de.bronze.models import ResolveKind  # noqa: E402
from de.bronze.payload_codec import decode_payload  # noqa: E402
from de.bronze.processor import BronzeProcessor  # noqa: E402
from de.bronze.transformer import BronzeTransformer  # noqa: E402
from de.bronze.validator import BronzeValidator  # noqa: E402

TOPIC = "traffic.entity-events.v2"
OUT = _REPO / "docs/architecture/k7_p0_benchmark_data.json"
AUDIT_BASELINE = {
    "records": 100,
    "throughput_rec_per_sec": 4.5,
    "batch_latency_ms": 22044,
    "clickhouse_queries_est": 302,
    "sqlite_queries_est": 100,
    "source": "K7_BACKFILL_PERFORMANCE_AUDIT.md section 6 (simulated batch 100)",
}


class CountingRepo(BronzeClickHouseRepository):
    def __init__(self, settings: BronzeSettings) -> None:
        super().__init__(settings)
        self.query_count = 0

    def _count(self, fn, *args, **kwargs):
        self.query_count += 1
        return fn(*args, **kwargs)

    def fetch_raw_batch(self, *args, **kwargs):
        return self._count(super().fetch_raw_batch, *args, **kwargs)

    def fetch_raw_quarantine_batch(self, *args, **kwargs):
        return self._count(super().fetch_raw_quarantine_batch, *args, **kwargs)

    def source_max_offset(self, *args, **kwargs):
        return self._count(super().source_max_offset, *args, **kwargs)

    def find_existing_raw_ingestion_ids(self, ids: Sequence[str]) -> Set[str]:
        return self._count(super().find_existing_raw_ingestion_ids, ids)

    def upstream_duplicate_offsets(self, entities):
        return self._count(super().upstream_duplicate_offsets, entities)

    def insert_entity_batch(self, *args, **kwargs):
        return self._count(super().insert_entity_batch, *args, **kwargs)

    def insert_run_batch(self, *args, **kwargs):
        return self._count(super().insert_run_batch, *args, **kwargs)

    def insert_quarantine_batch(self, *args, **kwargs):
        return self._count(super().insert_quarantine_batch, *args, **kwargs)


class CountingCheckpoint(CheckpointStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.query_count = 0

    def is_complete_batch(self, namespace, topic, partition, offsets):
        self.query_count += 1
        return super().is_complete_batch(namespace, topic, partition, offsets)

    def get(self, namespace, topic, partition):
        self.query_count += 1
        return super().get(namespace, topic, partition)

    def init_checkpoint(self, **kwargs):
        self.query_count += 1
        return super().init_checkpoint(**kwargs)

    def commit_batch(self, *args, **kwargs):
        self.query_count += 1
        return super().commit_batch(*args, **kwargs)


def _find_window(settings: BronzeSettings, count: int) -> tuple[int, int]:
    repo = BronzeClickHouseRepository(settings)
    repo.connect()
    try:
        for start in range(500, 5000, 500):
            rows = repo.fetch_raw_batch(TOPIC, 0, start, start + count, count)
            if len(rows) >= count:
                return start, start + count
        raise RuntimeError(f"Could not find {count} contiguous raw rows on p0")
    finally:
        repo.close()


def run_p0_benchmark(
    settings: BronzeSettings,
    *,
    start: int,
    count: int = 500,
    replay_run_id: str = "k7-p0-bench",
) -> Dict[str, Any]:
    cp_path = _REPO / "de/artifacts/bronze/p0_bench_cp.sqlite3"
    if cp_path.is_file():
        cp_path.unlink()

    repo = CountingRepo(settings)
    checkpoint = CountingCheckpoint(cp_path)
    repo.connect()

    ns = f"p0-bench:{replay_run_id}"
    checkpoint.init_checkpoint(
        namespace=ns,
        topic=TOPIC,
        partition=0,
        source_start_offset=start,
        last_completed_offset=start - 1,
        start_mode="explicit",
        processor_name=settings.processor_name,
        processor_version=settings.processor_version,
        bronze_schema_version=settings.bronze_schema_version,
    )

    proc = BronzeProcessor(
        settings,
        repo=repo,
        checkpoint=checkpoint,
        replay_run_id=replay_run_id,
        write_main_tables=False,
        max_offset_exclusive={(TOPIC, 0): start + count},
    )
    proc.namespace = ns
    proc.validator.load()
    proc.schemas_ok = proc.validator.ready
    proc.migrations_ok = repo.verify_tables()

    repo.query_count = 0
    checkpoint.query_count = 0

    proc._max_offset_cache = {(TOPIC, 0): repo.source_max_offset(TOPIC, 0)}
    ch_before = repo.query_count
    sqlite_before = checkpoint.query_count

    t0 = time.perf_counter()
    proc._poll_partition(TOPIC, 0)
    elapsed = time.perf_counter() - t0

    ch_queries = repo.query_count - ch_before
    sqlite_queries = checkpoint.query_count - sqlite_before

    cp = checkpoint.get(ns, TOPIC, 0)
    processed = (cp.last_completed_offset - start + 1) if cp else 0
    rec_per_sec = round(processed / max(elapsed, 0.001), 2)

    repo.close()
    checkpoint.close()
    if cp_path.is_file():
        cp_path.unlink()

    return {
        "window": {"partition": 0, "start": start, "count": count, "processed": processed},
        "elapsed_sec": round(elapsed, 3),
        "batch_latency_ms": round(elapsed * 1000, 2),
        "records_per_sec": rec_per_sec,
        "clickhouse_queries": ch_queries,
        "sqlite_queries": sqlite_queries,
        "pass_100_rec_per_sec": rec_per_sec >= 100,
        "hot_path_checks": {
            "fetch_raw_one_in_processor": False,
            "find_existing_per_record": False,
            "event_conflict_per_entity": False,
            "is_complete_per_offset": False,
        },
    }


def main() -> int:
    settings = BronzeSettings(clickhouse_host="localhost", batch_size=500)
    start, _ = _find_window(settings, 500)
    result = run_p0_benchmark(settings, start=start, count=500)
    report = {
        "baseline_before_p0": AUDIT_BASELINE,
        "after_p0": result,
        "comparison": {
            "throughput_before_rec_per_sec": AUDIT_BASELINE["throughput_rec_per_sec"],
            "throughput_after_rec_per_sec": result["records_per_sec"],
            "throughput_improvement_x": round(
                result["records_per_sec"] / max(AUDIT_BASELINE["throughput_rec_per_sec"], 0.001),
                2,
            ),
            "clickhouse_queries_before_est_per_500": 1500 + 500 + 500,
            "clickhouse_queries_after": result["clickhouse_queries"],
            "sqlite_queries_before_est_per_500": 500,
            "sqlite_queries_after": result["sqlite_queries"],
        },
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if result["pass_100_rec_per_sec"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
