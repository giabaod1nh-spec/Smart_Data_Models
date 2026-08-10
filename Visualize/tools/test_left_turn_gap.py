"""Verify permissive left-turn gap acceptance after vType tuning (runtime gate)."""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    import traci
except ImportError:
    traci = None  # type: ignore

W_LEFT_VEH = "w_left_probe"
EW_GREEN_START = 45.0


@dataclass
class VtypeJm:
    label: str
    attrs: dict[str, str]


@dataclass
class CaseResult:
    label: str
    scenario: str
    enter_merge: float | None
    done: float | None
    collisions: int
    during_ew_phase: bool


def _attrs(d: dict[str, str]) -> str:
    return " ".join(f'{k}="{v}"' for k, v in d.items())


def _write_rou(path: Path, vt: VtypeJm, headway: float, w_route: tuple[str, str], e_route: tuple[str, str]) -> None:
    w_depart = EW_GREEN_START + 1.0
    vehicles: list[tuple[float, str, str]] = []
    for i in range(28):
        vehicles.append((i * headway, f"e_{i:02d}", "e"))
    vehicles.append((w_depart, W_LEFT_VEH, "w"))
    vehicles.sort(key=lambda x: x[0])
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?><routes>',
        f'    <vType id="probe" {_attrs(vt.attrs)}/>',
        f'    <route id="e" edges="{" ".join(e_route)}"/>',
        f'    <route id="w" edges="{" ".join(w_route)}"/>',
    ]
    for depart, vid, rid in vehicles:
        lines.append(f'    <vehicle id="{vid}" type="probe" route="{rid}" depart="{depart:.1f}"/>')
    lines.append("</routes>")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_cfg(path: Path, net: Path, rou: Path) -> None:
    path.write_text(
        f"""<configuration>
  <input><net-file value="{net.as_posix()}"/><route-files value="{rou.as_posix()}"/></input>
  <time><begin value="0"/><end value="250"/><step-length value="0.1"/></time>
  <processing><collision.action value="warn"/><time-to-teleport value="-1"/></processing>
  <random_number><seed value="42"/></random_number>
</configuration>""",
        encoding="utf-8",
    )


def _merge_edge(edge: str) -> bool:
    # Second internal segment of permissive left-turn (e.g. :J1_23)
    return len(edge) >= 6 and edge[0] == ":" and edge[3] == "_" and edge[4:6] == "23"


def run_case(vt: VtypeJm, scenario: str, headway: float, w_route: tuple[str, str], e_route: tuple[str, str], done_edge: str, sumo_bin: Path) -> CaseResult:
    net = _ROOT / "Visualize" / "intersection.net.xml"
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        rou = tmp_path / "probe.rou.xml"
        cfg = tmp_path / "probe.sumocfg"
        _write_rou(rou, vt, headway, w_route, e_route)
        _write_cfg(cfg, net, rou)
        traci.start([str(sumo_bin), "-c", str(cfg), "--no-step-log"], port=8813)
        enter_merge = done = None
        collisions = 0
        try:
            for _ in range(2500):
                traci.simulationStep()
                collisions = max(collisions, traci.simulation.getCollidingVehiclesNumber())
                if W_LEFT_VEH not in traci.vehicle.getIDList():
                    continue
                edge = traci.vehicle.getRoadID(W_LEFT_VEH)
                if enter_merge is None and _merge_edge(edge):
                    enter_merge = traci.simulation.getTime()
                if edge == done_edge:
                    done = traci.simulation.getTime()
                    break
        finally:
            traci.close()
    return CaseResult(
        label=vt.label,
        scenario=scenario,
        enter_merge=enter_merge,
        done=done,
        collisions=collisions,
        during_ew_phase=done is not None and done < EW_GREEN_START + 42.0,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--headway", type=float, default=1.6)
    args = parser.parse_args()

    sumo_home = os.environ.get("SUMO_HOME", r"D:\SUMO")
    sumo_bin = Path(sumo_home) / "bin" / "sumo.exe"
    if not sumo_bin.is_file():
        print(f"SUMO not found: {sumo_bin}", file=sys.stderr)
        return 1

    tools = Path(sumo_home) / "tools"
    if str(tools) not in sys.path:
        sys.path.insert(0, str(tools))
    global traci
    import traci as _traci

    traci = _traci

    baseline_car = VtypeJm(
        "baseline_car",
        {
            "vClass": "passenger",
            "length": "4.5",
            "maxSpeed": "13.9",
            "accel": "2.5",
            "decel": "4.5",
            "minGap": "1.8",
            "tau": "1.0",
            "impatience": "0.65",
            "jmTimegapMinor": "1.0",
        },
    )
    tuned_car = VtypeJm(
        "tuned_car",
        {
            "vClass": "passenger",
            "length": "4.5",
            "maxSpeed": "13.9",
            "accel": "2.5",
            "decel": "4.5",
            "minGap": "1.5",
            "tau": "0.60",
            "impatience": "0.75",
            "jmTimegapMinor": "0.60",
            "jmAdvance": "3.0",
            "jmExtraGap": "0.35",
        },
    )

    scenarios = [
        ("J1 W-left vs E-through", ("W1J1", "J1J3"), ("J2J1", "J1W1"), "J1J3"),
        ("J2 E-left vs W-through", ("E1J2", "J2S2"), ("J1J2", "J2J4"), "J2S2"),
        ("J3 W-left vs E-through", ("W2J3", "J3N1"), ("J4J3", "J3W2"), "J3N1"),
        ("J4 E-left vs W-through", ("E2J4", "J4N2"), ("J3J4", "J4E2"), "J4N2"),
    ]

    print(f"=== Left-turn gap runtime gate (headway={args.headway}s) ===")
    passes = 0
    for name, wr, er, done_edge in scenarios:
        b = run_case(baseline_car, name, args.headway, wr, er, done_edge, sumo_bin)
        t = run_case(tuned_car, name, args.headway, wr, er, done_edge, sumo_bin)
        delta = None
        if b.enter_merge is not None and t.enter_merge is not None:
            delta = b.enter_merge - t.enter_merge
        ok = (
            t.collisions == 0
            and t.during_ew_phase
            and t.enter_merge is not None
            and delta is not None
            and delta >= 1.0
        )
        if ok:
            passes += 1
        print(
            f"{name}: baseline merge={b.enter_merge} done={b.done} | "
            f"tuned merge={t.enter_merge} done={t.done} delta={delta} collisions={t.collisions} "
            f"{'PASS' if ok else 'FAIL'}"
        )

    verdict = "PASS" if passes >= 2 else "FAIL"
    print(f"VERDICT: {verdict} ({passes}/{len(scenarios)} scenarios improved >=1s, zero collisions)")
    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
