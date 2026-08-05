"""Orchestration-only clean demo reset (RT-E). Does NOT reset normal production SQLite/group."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

VIS = Path(__file__).resolve().parents[1]
REPO = VIS.parent


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def capture_kafka_tips(topic: str = "traffic.entity-events.v2") -> dict:
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-run-class.sh",
        "kafka.tools.GetOffsetShell",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        topic,
    ]
    result = subprocess.run(cmd, cwd=REPO, text=True, capture_output=True, check=False)
    tips: dict[int, int] = {}
    for line in (result.stdout or "").splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 3 and parts[-1].isdigit():
            tips[int(parts[1])] = int(parts[-1])
    return {"cmd": cmd, "returncode": result.returncode, "tips": tips, "stderr": result.stderr}


def write_demo_fence(
    *,
    target_run_id: str,
    tips: dict[int, int],
    out_path: Path,
    topic: str = "traffic.entity-events.v2",
) -> None:
    manifest = {
        "targetSimulationRunId": target_run_id,
        "topic": topic,
        "partitions": [
            {"partition": p, "nextOffset": off} for p, off in sorted(tips.items())
        ],
        "generatedAt": _utc(),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description="Clean demo orchestration (not normal mode reset)")
    p.add_argument("--target-run-id", required=True)
    p.add_argument(
        "--demo-db",
        default=str(VIS / "artifacts" / "projector" / "demo.sqlite3"),
    )
    p.add_argument(
        "--fence-out",
        default=str(VIS / "artifacts" / "projector" / "fence" / "demo_fence.json"),
    )
    p.add_argument("--evidence-dir", default=str(REPO / "artifacts" / "realtime_demo"))
    args = p.parse_args()

    evidence = {"started_at": _utc(), "target_run_id": args.target_run_id}
    evidence["kafka_tips"] = capture_kafka_tips()
    write_demo_fence(
        target_run_id=args.target_run_id,
        tips=evidence["kafka_tips"]["tips"],
        out_path=Path(args.fence_out),
    )
    evidence["demo_db"] = args.demo_db
    evidence["fence_out"] = args.fence_out
    evidence["instructions"] = [
        "Stop SUMO and projector",
        f"Start projector with PROJECTOR_CONSUMER_MODE=demo PROJECTOR_DB={args.demo_db} "
        f"PROJECTOR_FENCE_MANIFEST={args.fence_out}",
        "Wait /ready idle",
        "Start SUMO with matching run id",
    ]
    out = Path(args.evidence_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = _utc().replace(":", "").replace("-", "")
    path = out / f"demo_reset_{stamp}.json"
    path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
