"""Runtime gate: per-intersection canonical scenarios (SUMO + TraCI)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import configuration.config as cfg
from configuration.model_params import get_registry
from simulation.backend import SumoBackend


def _steps(backend: SumoBackend, n: int) -> None:
    for _ in range(n):
        if not backend.step():
            break


def _delta_for_node(backend: SumoBackend, node: str) -> float:
    tls = cfg.NODE_TO_TLS[node]
    total = 0.0
    for sid, bucket in backend.runtime.demand._buckets.items():
        src = get_registry().boundary_sources()[sid]
        if str(src.get("turn_at_tls")) == tls:
            total += bucket.target_delta_vph
    return total


def _lane_speed(traci, node: str, direction: str, lane_index: int = 1) -> float:
    tls = cfg.NODE_TO_TLS[node]
    edge = cfg.APPROACH_EDGES[tls][direction]
    return float(traci.lane.getMaxSpeed(f"{edge}_{lane_index}"))


def _count_on_approaches(traci, node: str) -> int:
    tls = cfg.NODE_TO_TLS[node]
    ids = set()
    for d in cfg.DIRECTIONS:
        for lid in cfg.approach_lane_ids(tls, d):
            ids.update(traci.lane.getLastStepVehicleIDs(lid))
    return len(ids)


def main() -> int:
    os.environ.setdefault("SUMO_HOME", r"D:\SUMO")
    cfg.ensure_sumo_tools_on_path()

    backend = SumoBackend(use_gui=False, publish_nodes=["A", "B", "C", "D"])
    passes = 0
    total = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal passes, total
        total += 1
        if ok:
            passes += 1
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))

    print("=== Per-intersection scenario runtime gate ===")
    try:
        backend.start()
        traci = backend._traci
        _steps(backend, 50)

        # A — Peak at C only
        print("\nA. C=peak isolation")
        backend.set_scenario("peak", target_intersection="C")
        delta_c = _delta_for_node(backend, "C")
        delta_a = _delta_for_node(backend, "A")
        check("C peak delta > 7000", delta_c > 7000, f"delta_c={delta_c:.0f}")
        check("A stays normal delta", delta_a < 1200, f"delta_a={delta_a:.0f}")
        check("per_node C=peak", backend.per_node_scenario.get("C") == "peak")
        check("per_node A=normal", backend.per_node_scenario.get("A") == "normal")

        # B — D oversaturated
        print("\nB. D=oversaturated")
        backend.set_scenario("oversaturated", target_intersection="D")
        delta_d = _delta_for_node(backend, "D")
        check("D oversaturated delta > 120000", delta_d > 120000, f"delta_d={delta_d:.0f}")
        check("C still peak", backend.per_node_scenario.get("C") == "peak")

        # C — B rain
        print("\nC. B=rain")
        spd_before_a = _lane_speed(traci, "A", "North")
        backend.set_scenario("rain", target_intersection="B")
        spd_b = _lane_speed(traci, "B", "East")
        spd_a = _lane_speed(traci, "A", "North")
        check("B lane speed reduced", spd_b < 12.0, f"spd_b={spd_b:.1f}")
        check("A lane speed unchanged", abs(spd_a - spd_before_a) < 0.5, f"spd_a={spd_a:.1f}")
        backend.set_scenario("normal", target_intersection="B")
        spd_b_rest = _lane_speed(traci, "B", "East")
        check("B rain restored", spd_b_rest > spd_b + 1.0, f"rest={spd_b_rest:.1f}")

        # D — A incident
        print("\nD. A=incident")
        backend.set_scenario("incident", target_intersection="A")
        vids = backend.runtime.incident.vehicle_ids("A")
        check("2 incident vehicles at A", len(vids) == 2, str(vids))
        check("vehicles exist in SUMO", all(v in traci.vehicle.getIDList() for v in vids))
        check("A demand peak", _delta_for_node(backend, "A") > 7000)
        check("B not incident", backend.per_node_scenario.get("B") != "incident")
        _steps(backend, 200)
        queued_a = _count_on_approaches(traci, "A")
        check("queue builds at A after incident", queued_a >= 3, f"veh={queued_a}")

        # E — simultaneous states
        print("\nE. simultaneous per-node states")
        backend.set_scenario("normal", target_intersection="B")
        backend.set_scenario("normal", target_intersection="C")
        backend.set_scenario("peak", target_intersection="C")
        backend.set_scenario("rain", target_intersection="B")
        states = dict(backend.per_node_scenario)
        check(
            "mixed states",
            states.get("A") == "incident"
            and states.get("B") == "rain"
            and states.get("C") == "peak"
            and states.get("D") == "oversaturated",
            str(states),
        )

        # Peak visible ~30s at C
        print("\nF. C peak traffic growth")
        backend.set_scenario("normal", target_intersection="A")
        backend.set_scenario("peak", target_intersection="C")
        before = _count_on_approaches(traci, "C")
        _steps(backend, 300)
        after = _count_on_approaches(traci, "C")
        check("C peak visible after ~30s", after > before + 2, f"before={before} after={after}")

    finally:
        backend.stop()

    verdict = "PASS" if passes == total else "FAIL"
    print(f"\nVERDICT: {verdict} ({passes}/{total})")
    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
