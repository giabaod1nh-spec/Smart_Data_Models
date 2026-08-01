"""K-5R locked durable-outbox benchmark matrix.

The official matrix is intentionally expensive: 100/500/1000/2000 entities and
1000 measured cycles per workload.  Results are machine-readable and a non-zero
exit code means R2/R3 may not advance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parents[2]
VISUALIZE = REPO / "Visualize"
if str(VISUALIZE) not in sys.path:
    sys.path.insert(0, str(VISUALIZE))

from integration.kafka.outbox_store import KafkaOutboxStore, OutboxRow  # noqa: E402

OFFICIAL_ENTITY_MATRIX = (100, 500, 1000, 2000)
OFFICIAL_MEASURED_CYCLES = 1000
OFFICIAL_WARMUP_CYCLES = 20
APPEND_P95_BUDGET_MS = 150.0
APPEND_MAX_BUDGET_MS = 500.0


def _percentile(values: Iterable[float], q: float) -> float | None:
    ordered = sorted(float(v) for v in values)
    if not ordered:
        return None
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[idx]


def _rows(entity_count: int, cycle: int) -> list[OutboxRow]:
    rows: list[OutboxRow] = []
    run_id = "k5r-benchmark"
    for entity_sequence in range(entity_count):
        raw_id = f"{cycle}:{entity_sequence}".encode("utf-8")
        event_id = hashlib.sha256(raw_id).hexdigest()
        payload = json.dumps(
            {"eventId": event_id, "cycleSequence": cycle, "entitySequence": entity_sequence},
            separators=(",", ":"),
        )
        rows.append(
            OutboxRow(
                event_id=event_id,
                simulation_run_id=run_id,
                cycle_sequence=cycle,
                entity_sequence=entity_sequence,
                event_key=f"{run_id}:node-{entity_sequence % 4}",
                topic="traffic.entity-events.v2",
                payload_json=payload,
                payload_hash=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            )
        )
    return rows


def _run_case(root: Path, entity_count: int, warmup: int, cycles: int) -> dict:
    case_dir = root / f"entities-{entity_count}"
    case_dir.mkdir(parents=True, exist_ok=True)
    db = case_dir / "outbox.sqlite3"
    store = KafkaOutboxStore(db)
    sync = int(store._conn.execute("PRAGMA synchronous").fetchone()[0])
    durations: list[float] = []
    started = time.perf_counter()
    try:
        for cycle in range(warmup):
            store.append_cycle(_rows(entity_count, -(cycle + 1)))
        with store._state_lock:
            store._append_durations_ms.clear()
            store._append_gate_wait_ms.clear()
            store._append_insert_ms.clear()
            store._append_commit_ms.clear()
        for cycle in range(cycles):
            durations.append(store.append_cycle(_rows(entity_count, cycle)))
        metrics = store.capacity_metrics()
        row_count = int(store._conn.execute("SELECT count(*) FROM kafka_outbox").fetchone()[0])
    finally:
        store.close()
    p95 = _percentile(durations, 0.95)
    maximum = max(durations) if durations else None
    spikes = sum(1 for value in durations if value > APPEND_MAX_BUDGET_MS)
    passed = bool(
        sync == 2
        and len(durations) == cycles
        and p95 is not None
        and p95 <= APPEND_P95_BUDGET_MS
        and maximum is not None
        and maximum <= APPEND_MAX_BUDGET_MS
        and spikes == 0
        and row_count == entity_count * (warmup + cycles)
    )
    return {
        "entity_count": entity_count,
        "warmup_cycles": warmup,
        "measured_cycles": cycles,
        "sample_count": len(durations),
        "sqlite_synchronous": sync,
        "p50_ms": _percentile(durations, 0.50),
        "p95_ms": p95,
        "p99_ms": _percentile(durations, 0.99),
        "max_ms": maximum,
        "spikes_gt_500ms": spikes,
        "row_count": row_count,
        "elapsed_sec": time.perf_counter() - started,
        "breakdown": {
            "gate_wait_p95_ms": metrics.get("outbox_gate_wait_p95_ms"),
            "insert_p95_ms": metrics.get("outbox_insert_p95_ms"),
            "commit_p95_ms": metrics.get("outbox_commit_p95_ms"),
        },
        "pass": passed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="K-5R locked durable-outbox benchmark")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--entities", type=int, nargs="+", default=list(OFFICIAL_ENTITY_MATRIX))
    parser.add_argument("--cycles", type=int, default=OFFICIAL_MEASURED_CYCLES)
    parser.add_argument("--warmup", type=int, default=OFFICIAL_WARMUP_CYCLES)
    parser.add_argument("--allow-nonofficial", action="store_true")
    args = parser.parse_args()

    official = tuple(args.entities) == OFFICIAL_ENTITY_MATRIX and args.cycles == OFFICIAL_MEASURED_CYCLES
    if not official and not args.allow_nonofficial:
        parser.error("official gate requires entities=100 500 1000 2000 and cycles=1000")
    args.db_root.mkdir(parents=True, exist_ok=True)
    cases = [_run_case(args.db_root, count, args.warmup, args.cycles) for count in args.entities]
    report = {
        "schema": "k5r-latency-benchmark-v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "official_matrix": official,
        "budgets": {
            "append_p95_ms": APPEND_P95_BUDGET_MS,
            "append_max_ms": APPEND_MAX_BUDGET_MS,
            "spikes_gt_500ms": 0,
        },
        "db_root": str(args.db_root.resolve()),
        "disk_free_bytes_after": shutil.disk_usage(args.db_root).free,
        "cases": cases,
        "pass": official and all(case["pass"] for case in cases),
        "next_milestone": "R3_OPEN" if official and all(case["pass"] for case in cases) else "BLOCKED",
        "rollback": {
            "required_on_failure": True,
            "action": "keep previous runtime configuration; benchmark databases are evidence only",
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
