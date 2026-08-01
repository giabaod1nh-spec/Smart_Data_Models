#!/usr/bin/env python3
"""Independent SQLite synchronous=FULL I/O benchmark (K-5 isolation).

No Kafka, SUMO, Projector, or Orion. Measures durable transaction cost only.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS bench_ledger (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    simulation_run_id TEXT NOT NULL,
    cycle_sequence INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE TABLE IF NOT EXISTS bench_entity_state (
    simulation_run_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    last_cycle_sequence INTEGER NOT NULL,
    last_event_id TEXT NOT NULL,
    last_payload_hash TEXT NOT NULL,
    last_applied_at TEXT NOT NULL,
    last_simulation_time REAL,
    PRIMARY KEY (simulation_run_id, entity_id)
);
"""


def _pct(vals: List[float], q: float) -> Optional[float]:
    if not vals:
        return None
    ordered = sorted(vals)
    return ordered[max(0, min(len(ordered) - 1, int(round(q * (len(ordered) - 1)))))]


def _dist(vals: List[float]) -> Dict[str, Any]:
    if not vals:
        return {
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
            "sample_count": 0,
            "spikes_gt_100ms": 0,
            "spikes_gt_500ms": 0,
        }
    return {
        "p50": _pct(vals, 0.50),
        "p95": _pct(vals, 0.95),
        "p99": _pct(vals, 0.99),
        "max": max(vals),
        "sample_count": len(vals),
        "spikes_gt_100ms": sum(1 for v in vals if v > 100.0),
        "spikes_gt_500ms": sum(1 for v in vals if v > 500.0),
    }


def _open(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=FULL")
    sync = int(cur.execute("PRAGMA synchronous").fetchone()[0])
    if sync != 2:
        raise RuntimeError(f"expected synchronous=FULL(2), got {sync}")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.execute("PRAGMA wal_autocheckpoint=0")
    cur.executescript(SCHEMA)
    return conn


def _one_tx(
    conn: sqlite3.Connection,
    *,
    cycle: int,
    entities: int,
    lock: threading.RLock,
) -> Dict[str, float]:
    now = time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
    rows = [
        (
            f"evt-{cycle}-{i}",
            "traffic.entity-events.v2",
            i % 3,
            cycle * entities + i,
            "run-bench",
            cycle,
            f"urn:ngsi-ld:Intersection:{chr(65 + (i % 4))}-{i}",
            "APPLIED",
            f"hash-{cycle}-{i}",
            now,
            now,
        )
        for i in range(entities)
    ]
    states = [
        (
            "run-bench",
            r[6],
            cycle,
            r[0],
            r[8],
            now,
            float(cycle),
        )
        for r in rows
    ]
    out: Dict[str, float] = {}
    t_all = time.perf_counter()
    t_gate = time.perf_counter()
    with lock:
        out["gate_wait_ms"] = (time.perf_counter() - t_gate) * 1000.0
        t_begin = time.perf_counter()
        conn.execute("BEGIN IMMEDIATE")
        out["begin_ms"] = (time.perf_counter() - t_begin) * 1000.0
        try:
            t_exec = time.perf_counter()
            conn.executemany(
                """
                INSERT INTO bench_ledger (
                    event_id, topic, partition, offset, simulation_run_id,
                    cycle_sequence, entity_id, status, payload_hash, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    status=excluded.status,
                    completed_at=excluded.completed_at
                """,
                rows,
            )
            conn.executemany(
                """
                INSERT INTO bench_entity_state (
                    simulation_run_id, entity_id, last_cycle_sequence,
                    last_event_id, last_payload_hash, last_applied_at, last_simulation_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(simulation_run_id, entity_id) DO UPDATE SET
                    last_cycle_sequence=excluded.last_cycle_sequence,
                    last_event_id=excluded.last_event_id,
                    last_payload_hash=excluded.last_payload_hash,
                    last_applied_at=excluded.last_applied_at,
                    last_simulation_time=excluded.last_simulation_time
                """,
                states,
            )
            out["exec_ms"] = (time.perf_counter() - t_exec) * 1000.0
            t_commit = time.perf_counter()
            conn.execute("COMMIT")
            out["commit_ms"] = (time.perf_counter() - t_commit) * 1000.0
        except Exception:
            conn.execute("ROLLBACK")
            raise
    out["total_ms"] = (time.perf_counter() - t_all) * 1000.0
    return out


def run_benchmark(
    *,
    db_path: Path,
    entities: int,
    warmup: int,
    measured: int,
    background: bool,
    label: str,
) -> Dict[str, Any]:
    if db_path.exists():
        db_path.unlink()
    for suffix in ("-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()
    conn = _open(db_path)
    lock = threading.RLock()
    stop = threading.Event()

    def _bg() -> None:
        # Simulated status reader contention (SELECT under lock).
        bg = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
        bg.execute("PRAGMA busy_timeout=5000")
        while not stop.is_set():
            try:
                with lock:
                    bg.execute("SELECT COUNT(*) FROM bench_ledger").fetchone()
            except Exception:
                pass
            time.sleep(0.01)
        bg.close()

    bg_thread = None
    if background:
        bg_thread = threading.Thread(target=_bg, daemon=True)
        bg_thread.start()

    for i in range(warmup):
        _one_tx(conn, cycle=i, entities=entities, lock=lock)

    series = {k: [] for k in ("gate_wait_ms", "begin_ms", "exec_ms", "commit_ms", "total_ms")}
    for i in range(measured):
        m = _one_tx(conn, cycle=warmup + i, entities=entities, lock=lock)
        for k, v in m.items():
            series[k].append(v)

    t_ckpt = time.perf_counter()
    with lock:
        mode = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    checkpoint_ms = (time.perf_counter() - t_ckpt) * 1000.0

    stop.set()
    if bg_thread:
        bg_thread.join(timeout=2.0)
    conn.close()

    size = db_path.stat().st_size if db_path.exists() else 0
    return {
        "label": label,
        "db_path": str(db_path.resolve()),
        "entities_per_cycle": entities,
        "warmup": warmup,
        "measured": measured,
        "background_reader": background,
        "pragma": {"journal_mode": "WAL", "synchronous": "FULL", "wal_autocheckpoint": 0},
        "checkpoint": {"ms": checkpoint_ms, "result": list(mode) if mode else None},
        "db_bytes": size,
        "distributions": {k: _dist(v) for k, v in series.items()},
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", required=True)
    p.add_argument("--label", default="sqlite-full")
    p.add_argument("--entities", type=int, default=40)
    p.add_argument("--warmup", type=int, default=100)
    p.add_argument("--measured", type=int, default=1000)
    p.add_argument("--background", action="store_true")
    p.add_argument("--out", required=True)
    args = p.parse_args()
    result = run_benchmark(
        db_path=Path(args.db_path),
        entities=args.entities,
        warmup=args.warmup,
        measured=args.measured,
        background=bool(args.background),
        label=args.label,
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"wrote": str(out), "total_p95": result["distributions"]["total_ms"]["p95"], "commit_p95": result["distributions"]["commit_ms"]["p95"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
