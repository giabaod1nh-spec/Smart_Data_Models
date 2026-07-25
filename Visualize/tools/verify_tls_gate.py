"""Phase 3.8 TLS verification gate (plan §11).

Exit 0 only if every J1–J4 matches freeze snapshot:
  programID==0, type==static, offset==0,
  phases [42,3,42,3] with frozen state strings,
  controlled link count unchanged.
"""
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_NET = ROOT / "Visualize" / "intersection.net.xml"
DEFAULT_FREEZE = ROOT / "artifacts" / "phase3.8" / "phase38_pre_geometry_audit.json"

EXPECTED_DURATIONS = [42, 3, 42, 3]


def verify(net_path: Path, freeze_path: Path) -> list[str]:
    if not freeze_path.is_file():
        return [
            f"freeze snapshot not found: {freeze_path} (pass --freeze to point at a generated snapshot)"
        ]
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    tls_freeze = freeze["tls_freeze"]
    net = ET.parse(net_path)
    errors: list[str] = []

    for tid in ("J1", "J2", "J3", "J4"):
        expected = tls_freeze[tid]
        tls = [t for t in net.findall(".//tlLogic") if t.get("id") == tid]
        if len(tls) != 1:
            errors.append(f"{tid}: expected exactly 1 tlLogic, found {len(tls)}")
            continue
        tl = tls[0]
        if tl.get("programID") != "0":
            errors.append(f"{tid}: programID={tl.get('programID')!r} != '0'")
        if tl.get("type") != "static":
            errors.append(f"{tid}: type={tl.get('type')!r} != 'static'")
        if str(tl.get("offset")) != "0":
            errors.append(f"{tid}: offset={tl.get('offset')!r} != '0'")
        phases = tl.findall("phase")
        durs = [int(float(p.get("duration"))) for p in phases]
        states = [p.get("state") for p in phases]
        if durs != EXPECTED_DURATIONS:
            errors.append(f"{tid}: durations={durs} != {EXPECTED_DURATIONS}")
        exp_states = [p["state"] for p in expected["phases"]]
        if states != exp_states:
            errors.append(f"{tid}: state strings changed\n  got={states}\n  exp={exp_states}")
        controlled = len([c for c in net.findall(".//connection") if c.get("tl") == tid])
        if controlled != expected["controlled_link_count"]:
            errors.append(
                f"{tid}: controlled_link_count={controlled} != {expected['controlled_link_count']}"
            )
    return errors


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--net", type=Path, default=DEFAULT_NET)
    ap.add_argument("--freeze", type=Path, default=DEFAULT_FREEZE)
    args = ap.parse_args()
    errs = verify(args.net, args.freeze)
    if errs:
        print("TLS GATE FAIL:")
        for e in errs:
            print(f"  - {e}")
        sys.exit(1)
    print("TLS GATE PASS: J1–J4 program/phases/offset/states/controlled-links OK")


if __name__ == "__main__":
    main()
