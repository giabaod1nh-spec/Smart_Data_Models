"""
Orion publish performance benchmark — audit harness (no SUMO required for HTTP path).

Usage (from repo root):
  python Visualize/tools/orion_publish_benchmark.py --scenario B --nodes A --cycles 5
  python Visualize/tools/orion_publish_benchmark.py --scenario D --nodes A,B,C,D --cycles 3

Scenarios:
  B = Orion only (ensure no active subscription or use --skip-subscription-check)
  D = Orion + de-webhook subscription (default if subscription exists)
  E = nodes A only
  F = nodes A,B,C,D
  G = vary --publish-interval (simulated wall-clock spacing between cycles)
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
VIS = ROOT / "Visualize"
if str(VIS) not in sys.path:
    sys.path.insert(0, str(VIS))

os.environ.setdefault("ORION_PERF_AUDIT", "1")

import configuration.config as cfg  # noqa: E402
from integration.orion.client import reset_created_cache, upsert_entity  # noqa: E402
from integration.orion.entity_mapper import build_all_entities  # noqa: E402


def _synthetic_snapshot(node: str, sim_t: float, run_id: str) -> dict[str, Any]:
    """Minimal snapshot sufficient for entity_mapper."""
    return {
        "node_id": node,
        "simulation_time_sec": sim_t,
        "simulation_run_id": run_id,
        "scenario": "normal",
        "phase": "NS_GREEN",
        "next_phase": "NS_YELLOW",
        "phase_remaining": 20.0,
        "phase_duration": 42,
        "green_duration": 42,
        "yellow_duration": 3,
        "red_duration": 42,
        "colors": {"North": "green", "South": "green", "East": "red", "West": "red"},
        "incidents": [],
        "directions": {
            d: {
                "vehicle_count": 2,
                "pcu_equivalent": 1.5,
                "left_count": 0,
                "straight_count": 2,
                "right_count": 0,
                "waiting_vehicle_count": 1,
                "queue_length_m": 5.0,
                "occupancy_rate": 0.2,
                "occupancy_pct": 20.0,
                "average_speed_kmh": 35.0,
                "density": "LOW",
                "traffic_state": "LIGHT",
                "queue_by_movement": {"straight": 3.0, "left": 1.0, "right": 0.0},
            }
            for d in cfg.DIRECTIONS
        },
        "observation_seq": int(sim_t),
        "source_observation_seq": int(sim_t),
    }


@dataclass
class RequestStat:
    entity_id: str
    method: str
    status: int
    duration_ms: float
    ok: bool


@dataclass
class CycleStat:
    cycle_index: int
    sim_time: float
    nodes: list[str]
    entity_count: int
    request_count: int
    total_ms: float
    snapshot_ms: float
    mapper_ms: float
    http_ms: float
    requests: list[RequestStat] = field(default_factory=list)


@dataclass
class BenchmarkResult:
    scenario: str
    nodes: list[str]
    cycles: int
    publish_interval_sec: float
    orion_url: str
    cycle_stats: list[CycleStat] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        totals = [c.total_ms for c in self.cycle_stats]
        reqs = [r.duration_ms for c in self.cycle_stats for r in c.requests]
        req_counts = [c.request_count for c in self.cycle_stats]
        entities = [c.entity_count for c in self.cycle_stats]

        def pct(vals: list[float], p: float) -> float:
            if not vals:
                return 0.0
            s = sorted(vals)
            i = min(len(s) - 1, int(len(s) * p))
            return s[i]

        wall_duration = sum(totals)
        producer_rate = len(self.cycle_stats) / wall_duration if wall_duration > 0 else 0.0
        consumer_capacity = 1000.0 / statistics.mean(totals) if totals else 0.0

        return {
            "scenario": self.scenario,
            "nodes": self.nodes,
            "entities_per_cycle": statistics.mean(entities) if entities else 0,
            "requests_per_cycle_avg": statistics.mean(req_counts) if req_counts else 0,
            "publish_total_ms": {
                "min": min(totals) if totals else 0,
                "avg": statistics.mean(totals) if totals else 0,
                "p95": pct(totals, 0.95),
                "max": max(totals) if totals else 0,
            },
            "request_ms": {
                "min": min(reqs) if reqs else 0,
                "avg": statistics.mean(reqs) if reqs else 0,
                "p95": pct(reqs, 0.95),
                "max": max(reqs) if reqs else 0,
            },
            "producer_rate_cycles_per_sec": producer_rate,
            "consumer_capacity_cycles_per_sec": consumer_capacity,
            "orion_keeps_up": consumer_capacity >= (1.0 / self.publish_interval_sec),
        }


def _instrumented_upsert(entity: dict, bucket: list[RequestStat]) -> None:
    from integration.orion import client as oc

    def capture(entity_id: str, method: str, status: int, t0: float, ok: bool) -> None:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        bucket.append(
            RequestStat(
                entity_id=entity_id,
                method=method,
                status=status,
                duration_ms=duration_ms,
                ok=ok,
            )
        )

    original = oc._record_http
    oc._record_http = capture  # type: ignore[assignment]
    try:
        upsert_entity(entity)
    finally:
        oc._record_http = original  # type: ignore[assignment]


def run_benchmark(
    scenario: str,
    nodes: list[str],
    cycles: int,
    publish_interval: float,
) -> BenchmarkResult:
    reset_created_cache()
    run_id = str(uuid.uuid4())
    result = BenchmarkResult(
        scenario=scenario,
        nodes=nodes,
        cycles=cycles,
        publish_interval_sec=publish_interval,
        orion_url=cfg.ORION_URL,
    )

    for i in range(cycles):
        sim_t = float(i + 1)
        cycle_requests: list[RequestStat] = []
        snap_ms = 0.0
        map_ms = 0.0
        t0 = time.perf_counter()

        entity_total = 0
        for node in nodes:
            st = time.perf_counter()
            snapshot = _synthetic_snapshot(node, sim_t, run_id)
            snap_ms += (time.perf_counter() - st) * 1000.0

            mt = time.perf_counter()
            entities = build_all_entities(node, snapshot)
            entity_total += len(entities)
            map_ms += (time.perf_counter() - mt) * 1000.0

            for ent in entities:
                _instrumented_upsert(ent, cycle_requests)

        total_ms = (time.perf_counter() - t0) * 1000.0
        http_ms = sum(r.duration_ms for r in cycle_requests)
        result.cycle_stats.append(
            CycleStat(
                cycle_index=i,
                sim_time=sim_t,
                nodes=list(nodes),
                entity_count=entity_total,
                request_count=len(cycle_requests),
                total_ms=total_ms,
                snapshot_ms=snap_ms,
                mapper_ms=map_ms,
                http_ms=http_ms,
                requests=cycle_requests,
            )
        )
        if publish_interval > 0 and i + 1 < cycles:
            time.sleep(publish_interval)

    return result


def main() -> int:
    p = argparse.ArgumentParser(description="Orion publish benchmark (audit)")
    p.add_argument("--scenario", default="B", choices=["B", "C", "D", "E", "F", "G"])
    p.add_argument("--nodes", default="A,B,C,D")
    p.add_argument("--cycles", type=int, default=5)
    p.add_argument("--publish-interval", type=float, default=1.0)
    p.add_argument("--output", default=None, help="JSON output path")
    args = p.parse_args()

    nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
    if args.scenario == "E":
        nodes = ["A"]
    elif args.scenario == "F":
        nodes = ["A", "B", "C", "D"]

    os.environ["ORION_URL"] = os.getenv("ORION_URL", "http://localhost:1026")
    cfg.ORION_URL = os.environ["ORION_URL"]

    result = run_benchmark(args.scenario, nodes, args.cycles, args.publish_interval)
    summary = result.summary()
    out = {"summary": summary, "cycles": [asdict(c) for c in result.cycle_stats]}
    text = json.dumps(out, indent=2, default=str)
    print(text)

    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
