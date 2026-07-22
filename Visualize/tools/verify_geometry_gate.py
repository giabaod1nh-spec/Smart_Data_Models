"""Phase 3.8 geometry / ID / free-flow verification gate."""
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_NET = ROOT / "Visualize" / "intersection.net.xml"
DEFAULT_NOD = ROOT / "Visualize" / "intersection.nod.xml"
DEFAULT_EDG = ROOT / "Visualize" / "intersection.edg.xml"
SPEED = 13.9

REQUIRED_NODES = {
    "J1", "J2", "J3", "J4",
    "N1", "N2", "S1", "S2", "W1", "W2", "E1", "E2",
}
REQUIRED_EDGES = {
    "J1J2", "J2J1", "J1J3", "J3J1", "J2J4", "J4J2", "J3J4", "J4J3",
    "N1J3", "J3N1", "N2J4", "J4N2", "S1J1", "J1S1", "S2J2", "J2S2",
    "W1J1", "J1W1", "W2J3", "J3W2", "E1J2", "J2E1", "E2J4", "J4E2",
}


def _is_internal_edge(eid: str) -> bool:
    return len(eid) == 4 and eid[0] == "J" and eid[2] == "J"


def verify(net_path: Path, nod_path: Path, edg_path: Path) -> list[str]:
    errors: list[str] = []
    nod = ET.parse(nod_path)
    nodes = {n.get("id"): n for n in nod.findall("node")}
    missing_n = REQUIRED_NODES - set(nodes)
    if missing_n:
        errors.append(f"missing nodes: {sorted(missing_n)}")

    # Design spacing from nod (not netOffset)
    try:
        j1 = (float(nodes["J1"].get("x")), float(nodes["J1"].get("y")))
        j2 = (float(nodes["J2"].get("x")), float(nodes["J2"].get("y")))
        j3 = (float(nodes["J3"].get("x")), float(nodes["J3"].get("y")))
        spacing_x = abs(j2[0] - j1[0])
        spacing_y = abs(j3[1] - j1[1])
        if abs(spacing_x - 500.0) > 1e-6 or abs(spacing_y - 500.0) > 1e-6:
            errors.append(f"center spacing not 500 m: dx={spacing_x} dy={spacing_y}")
        w1 = (float(nodes["W1"].get("x")), float(nodes["W1"].get("y")))
        if abs(w1[0] - (j1[0] - 300.0)) > 1e-6:
            errors.append(f"W1 not 300 m west of J1: {w1}")
    except KeyError as e:
        errors.append(f"node lookup failed: {e}")

    edg = ET.parse(edg_path)
    edge_ids = {e.get("id") for e in edg.findall("edge")}
    missing_e = REQUIRED_EDGES - edge_ids
    extra = edge_ids - REQUIRED_EDGES
    if missing_e:
        errors.append(f"missing edges: {sorted(missing_e)}")
    if extra:
        errors.append(f"unexpected edges: {sorted(extra)}")

    net = ET.parse(net_path)
    net_edges = {e.get("id") for e in net.findall("edge") if e.get("function") != "internal"}
    if REQUIRED_EDGES - net_edges:
        errors.append(f"net missing edges: {sorted(REQUIRED_EDGES - net_edges)}")

    internal_lens: list[float] = []
    external_lens: list[float] = []
    for edge in net.findall("edge"):
        eid = edge.get("id") or ""
        if edge.get("function") == "internal" or eid.startswith(":"):
            continue
        for lane in edge.findall("lane"):
            length = float(lane.get("length"))
            speed = float(lane.get("speed"))
            if abs(speed - SPEED) > 1e-6:
                errors.append(f"{lane.get('id')}: speed={speed} != {SPEED}")
            if _is_internal_edge(eid):
                internal_lens.append(length)
            else:
                external_lens.append(length)

    if not internal_lens:
        errors.append("no internal lane lengths found")
    else:
        min_int = min(internal_lens)
        if min_int < 450.0:
            errors.append(f"internal usable length {min_int} < 450 m")
        ff = min_int / SPEED
        if ff < 30.0:
            errors.append(f"internal FF {ff:.2f} s < 30 s")

    if external_lens:
        min_ext = min(external_lens)
        ff_ext = min_ext / SPEED
        if min_ext < 250.0:
            errors.append(f"external usable length {min_ext} < 250 m (expected ~285)")
        if ff_ext < 18.0:
            errors.append(f"external FF {ff_ext:.2f} s below ~20 s class")

    return errors


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--net", type=Path, default=DEFAULT_NET)
    ap.add_argument("--nod", type=Path, default=DEFAULT_NOD)
    ap.add_argument("--edg", type=Path, default=DEFAULT_EDG)
    ap.add_argument("--json-out", type=Path, default=None)
    args = ap.parse_args()
    errs = verify(args.net, args.nod, args.edg)
    report = {"pass": not errs, "errors": errs}
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if errs:
        print("GEOMETRY GATE FAIL:")
        for e in errs:
            print(f"  - {e}")
        sys.exit(1)
    print("GEOMETRY GATE PASS: spacing/IDs/length/FF OK")


if __name__ == "__main__":
    main()
