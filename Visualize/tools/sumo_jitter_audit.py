"""Orchestrate SUMO Kafka jitter root-cause audit."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

VIS = Path(__file__).resolve().parents[1]
REPO = VIS.parent


def _run_case(case: str, out_dir: Path, *, max_sim: float, extra_env: dict | None = None) -> int:
    env = os.environ.copy()
    env.setdefault("SUMO_HOME", r"D:\SUMO")
    env["PATH"] = env["SUMO_HOME"] + r"\bin;" + env.get("PATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    if extra_env:
        env.update(extra_env)
    cmd = [
        sys.executable,
        "-m",
        "tools.jitter_audit.run_case",
        "--case",
        case,
        "--out-dir",
        str(out_dir),
        "--max-sim-time",
        str(max_sim),
    ]
    if case in ("A", "B", "C", "D", "E", "PRIMARY"):
        cmd.append("--gui")
    if case == "F":
        cmd.append("--fast")
    print(f"\n=== Running case {case} ===", flush=True)
    proc = subprocess.run(cmd, cwd=str(VIS), env=env)
    return proc.returncode


def main() -> int:
    ap = argparse.ArgumentParser(description="SUMO Kafka jitter audit orchestrator")
    ap.add_argument("--out-dir", default=None, help="Evidence output directory")
    ap.add_argument("--quick", action="store_true", help="Shorter runs for smoke (60s sim)")
    ap.add_argument("--cases", default="A,B,C,D,E,F,PRIMARY", help="Comma cases")
    ap.add_argument("--report-only", action="store_true")
    args = ap.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_dir or (REPO / "docs" / "implementation" / "jitter_audit_evidence" / stamp))
    report_path = REPO / "docs" / "implementation" / "SUMO_KAFKA_JITTER_ROOT_CAUSE_REPORT.md"

    if not args.report_only:
        max_sim = 60.0 if args.quick else 180.0
        primary_sim = 60.0 if args.quick else 300.0
        cases = [c.strip() for c in args.cases.split(",") if c.strip()]
        rc = 0
        for case in cases:
            ms = primary_sim if case == "PRIMARY" else max_sim
            code = _run_case(case, out_dir, max_sim=ms)
            if code != 0:
                rc = code
        # GC A/B short window on case D
        if "D" in cases and not args.quick:
            _run_case(
                "D_GC",
                out_dir,
                max_sim=120.0,
                extra_env={"AUDIT_DISABLE_GC": "1", "AUDIT_CASE": "D_GC"},
            )
        if rc != 0:
            print(f"Warning: some cases exited non-zero (rc={rc})", file=sys.stderr)

    sys.path.insert(0, str(VIS))
    from tools.jitter_audit.report import build_report

    build_report(out_dir, report_path)
    print(f"Report written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
