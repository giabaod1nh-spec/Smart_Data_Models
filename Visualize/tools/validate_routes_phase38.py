"""Validate routes load with zero route errors; optional short smoke for teleports.

Usage (from Visualize/):
  python tools/validate_routes_phase38.py
  python tools/validate_routes_phase38.py --smoke-end 60
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import configuration.config as cfg

ASSETS = ROOT / "Visualize"
OUT_DIR = ROOT / "artifacts" / "phase3.8"


def run_sumo(end_s: float, log_path: Path) -> subprocess.CompletedProcess:
    cmd = [
        cfg.resolve_sumo_binary(False),
        "-c",
        str(ASSETS / "intersection.sumocfg"),
        "--begin",
        "0",
        "--end",
        str(end_s),
        "--no-step-log",
        "true",
        "--duration-log.disable",
        "true",
        "--message-log",
        str(log_path),
        "--error-log",
        str(log_path.with_suffix(".err.log")),
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=max(120, int(end_s) + 60))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check-end", type=float, default=5.0, help="Route load check horizon")
    ap.add_argument("--smoke-end", type=float, default=0.0, help="If >0, run smoke and scan teleports")
    args = ap.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    check_log = OUT_DIR / "route_check.log"
    r = run_sumo(args.check_end, check_log)
    err_text = (check_log.with_suffix(".err.log").read_text(encoding="utf-8", errors="replace") if check_log.with_suffix(".err.log").is_file() else "") + "\n" + (r.stderr or "")
    msg = check_log.read_text(encoding="utf-8", errors="replace") if check_log.is_file() else ""
    combined = msg + "\n" + err_text
    route_errors = [
        line
        for line in combined.splitlines()
        if re.search(r"error|Error|ERROR", line) and "route" in line.lower()
    ]
    # Also catch generic SUMO Error lines on load
    generic_errors = [line for line in combined.splitlines() if line.startswith("Error:")]
    report = {
        "returncode": r.returncode,
        "route_error_lines": route_errors,
        "error_lines": generic_errors[:50],
        "teleports": None,
        "smoke_end": args.smoke_end or None,
    }
    if r.returncode != 0 or route_errors or generic_errors:
        (OUT_DIR / "route_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("ROUTE VALIDATION FAIL")
        print(json.dumps(report, indent=2))
        sys.exit(1)

    if args.smoke_end and args.smoke_end > 0:
        smoke_log = OUT_DIR / "smoke_teleport.log"
        rs = run_sumo(args.smoke_end, smoke_log)
        smoke_txt = ""
        for p in (smoke_log, smoke_log.with_suffix(".err.log")):
            if p.is_file():
                smoke_txt += p.read_text(encoding="utf-8", errors="replace") + "\n"
        smoke_txt += (rs.stderr or "") + "\n" + (rs.stdout or "")
        teleports = [
            line
            for line in smoke_txt.splitlines()
            if "teleport" in line.lower() or "Teleport" in line
        ]
        report["smoke_returncode"] = rs.returncode
        report["teleports"] = teleports
        if rs.returncode != 0 or teleports:
            (OUT_DIR / "route_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            print("SMOKE FAIL (teleports or nonzero exit)")
            print(json.dumps(report, indent=2))
            sys.exit(1)

    (OUT_DIR / "route_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("ROUTE VALIDATION PASS (zero route errors" + ("; no teleports" if args.smoke_end else "") + ")")


if __name__ == "__main__":
    main()
