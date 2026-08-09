#!/usr/bin/env python3
"""Projector-core benchmark without Kafka/SUMO (K-5 isolation).

Feeds synthetic complete cycles directly into OrionProjector.
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional


def _pct(vals: List[float], q: float) -> Optional[float]:
    if not vals:
        return None
    ordered = sorted(vals)
    return ordered[max(0, min(len(ordered) - 1, int(round(q * (len(ordered) - 1)))))]


def _dist(vals: List[float]) -> Dict[str, Any]:
    if not vals:
        return {"p50": None, "p95": None, "p99": None, "max": None, "sample_count": 0,
                "spikes_gt_100ms": 0, "spikes_gt_500ms": 0}
    return {
        "p50": _pct(vals, 0.50),
        "p95": _pct(vals, 0.95),
        "p99": _pct(vals, 0.99),
        "max": max(vals),
        "sample_count": len(vals),
        "spikes_gt_100ms": sum(1 for v in vals if v > 100.0),
        "spikes_gt_500ms": sum(1 for v in vals if v > 500.0),
    }


def _entity(run_id: str, cycle: int, seq: int, node: str) -> dict:
    eid = f"urn:ngsi-ld:Entity:{node}:{seq}"
    return {
        "eventType": "TrafficEntityObserved",
        "eventId": f"{run_id}-{cycle}-{node}-{seq}",
        "simulationRunId": run_id,
        "simulationTime": float(cycle),
        "cycleSequence": cycle,
        "entitySequence": seq,
        "cycleEntityCount": 40,
        "nodeId": node,
        "nodeEntityCount": 10,
        "capturedAt": time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + f"{int((time.time()%1)*1e6):06d}Z",
        "producerId": "visualize-traci",
        "producerSessionId": "sess-isolation",
        "entityPayloadHash": f"h-{run_id}-{cycle}-{node}-{seq}",
        "entity": {
            "id": eid,
            "type": "Intersection",
            "simulationTime": {"type": "Property", "value": float(cycle)},
        },
    }


def _cycle_events(run_id: str, cycle: int) -> List[dict]:
    nodes = ["A", "B", "C", "D"]
    out = []
    seq = 0
    for node in nodes:
        for _ in range(10):
            out.append(_entity(run_id, cycle, seq, node))
            seq += 1
    return out


def main() -> int:
    import sys

    repo = Path(__file__).resolve().parents[2]
    vis = repo / "Visualize"
    sys.path.insert(0, str(vis))
    sys.path.insert(0, str(repo))

    from integration.projector.core import OrionProjector, WriteMode
    from integration.projector.store import ProjectorStore

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--warmup", type=int, default=100)
    p.add_argument("--measured", type=int, default=1000)
    p.add_argument("--orion-delay-ms", type=float, default=0.0)
    p.add_argument("--label", default="projector-core")
    args = p.parse_args()

    db_path = Path(args.db_path)
    if db_path.exists():
        db_path.unlink()
    for suffix in ("-wal", "-shm"):
        side = Path(str(db_path) + suffix)
        if side.exists():
            side.unlink()

    delay_s = max(0.0, float(args.orion_delay_ms) / 1000.0)
    orion_calls = {"n": 0}

    def upsert(entities):
        orion_calls["n"] += 1
        if delay_s:
            time.sleep(delay_s)
        return SimpleNamespace(
            http_status=204,
            success_ids=tuple(e["id"] for e in entities),
            permanent_errors=(),
            retryable_error_ids=(),
            ambiguous_ids=(),
        )

    store = ProjectorStore(db_path)
    run_id = str(uuid.uuid4())
    proj = OrionProjector(
        store,
        batch_upsert=upsert,
        write_mode=WriteMode.ACTIVE,
        target_simulation_run_id=run_id,
        fence_offsets={0: 0, 1: 0, 2: 0},
        shadow=False,
        target_namespace="production",
        defer_ready_until_idle=True,
        node_timeout_ms=10_000.0,
    )
    proj.reset_latency_histograms(warmup_cycles=0)
    proj.process_record(
        topic="t",
        partition=0,
        offset=0,
        value={
            "eventType": "TrafficSimulationRunStarted",
            "producerId": "visualize-traci",
            "producerSessionId": "sess-isolation",
            "simulationRunId": run_id,
        },
    )

    cycle_totals: List[float] = []
    offset = 1
    total = args.warmup + args.measured
    for cycle in range(1, total + 1):
        t0 = time.perf_counter()
        events = _cycle_events(run_id, cycle)
        for ev in events:
            part = offset % 3
            proj.process_record(topic="t", partition=part, offset=offset, value=ev)
            offset += 1
        proj.tick(allow_partial=False)
        elapsed = (time.perf_counter() - t0) * 1000.0
        if cycle > args.warmup:
            cycle_totals.append(elapsed)

    health = proj.health()
    store.close()
    result = {
        "label": args.label,
        "db_path": str(db_path.resolve()),
        "run_id": run_id,
        "warmup": args.warmup,
        "measured": args.measured,
        "orion_delay_ms": args.orion_delay_ms,
        "orion_calls": orion_calls["n"],
        "cycle_total_ms": _dist(cycle_totals),
        "stage_latency": health.get("stage_latency"),
        "pipeline_e2e_latency_ms_p95": health.get("pipeline_e2e_latency_ms_p95"),
        "pipeline_e2e_latency_sample_count": health.get("pipeline_e2e_latency_sample_count"),
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({
        "wrote": str(out),
        "cycle_p95_ms": result["cycle_total_ms"]["p95"],
        "sqlite_p95": (health.get("stage_latency") or {}).get("sqlite_tx_ms", {}).get("p95"),
        "apply_p95": (health.get("stage_latency") or {}).get("apply_total_ms", {}).get("p95"),
        "orion_p95": (health.get("stage_latency") or {}).get("orion_http_ms", {}).get("p95"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
