"""Verify canonical OSM traffic-light programs used by the simulation."""
from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_NET = ROOT / "Visualize" / "intersection.net.xml"
DEFAULT_PROGRAMS = ROOT / "Visualize" / "tls" / "programs_phase38.xml"


def verify(net_path: Path, programs_path: Path = DEFAULT_PROGRAMS) -> list[str]:
    if not programs_path.is_file():
        return [f"canonical programs not found: {programs_path}"]
    net = ET.parse(net_path)
    canonical = ET.parse(programs_path)
    errors: list[str] = []

    for expected in canonical.findall(".//tlLogic"):
        tid = expected.get("id") or ""
        tls = [t for t in net.findall(".//tlLogic") if t.get("id") == tid]
        if len(tls) != 1:
            errors.append(f"{tid}: expected exactly 1 tlLogic, found {len(tls)}")
            continue
        tl = tls[0]
        for attr in ("programID", "type", "offset"):
            if tl.get(attr) != expected.get(attr):
                errors.append(f"{tid}: {attr}={tl.get(attr)!r} != {expected.get(attr)!r}")
        phases = tl.findall("phase")
        durs = [int(float(p.get("duration"))) for p in phases]
        states = [p.get("state") for p in phases]
        exp_phases = expected.findall("phase")
        exp_durs = [int(float(p.get("duration"))) for p in exp_phases]
        if durs != exp_durs:
            errors.append(f"{tid}: durations={durs} != {exp_durs}")
        exp_states = [p.get("state") for p in exp_phases]
        if states != exp_states:
            errors.append(f"{tid}: state strings changed\n  got={states}\n  exp={exp_states}")
        controlled = len([c for c in net.findall(".//connection") if c.get("tl") == tid])
        if states and any(len(state or "") != controlled for state in states):
            errors.append(
                f"{tid}: phase state length does not match controlled_link_count={controlled}"
            )
    return errors


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--net", type=Path, default=DEFAULT_NET)
    ap.add_argument("--programs", type=Path, default=DEFAULT_PROGRAMS)
    ap.add_argument("--freeze", type=Path, help=argparse.SUPPRESS)  # legacy no-op
    args = ap.parse_args()
    errs = verify(args.net, args.programs)
    if errs:
        print("TLS GATE FAIL:")
        for e in errs:
            print(f"  - {e}")
        sys.exit(1)
    print("TLS GATE PASS: canonical OSM programs/phases/controlled-links OK")


if __name__ == "__main__":
    main()
