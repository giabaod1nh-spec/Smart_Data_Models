#!/usr/bin/env python3
"""Write a K-5 fence manifest from broker watermarks + target run id."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--target-run-id", required=True)
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--topic", default="traffic.entity-events.v2")
    p.add_argument("--parts-json", default="", help="Optional pre-captured partitions JSON")
    args = p.parse_args()

    repo = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "Visualize"))

    if args.parts_json:
        parts = json.loads(args.parts_json)
    else:
        from Visualize.tools.k5_realtime_cutover import _broker_watermarks

        parts = _broker_watermarks(args.bootstrap, args.topic)

    payload = {
        "previousSimulationRunId": "isolation-prev",
        "previousFenceCycleSequence": 0,
        "targetSimulationRunId": args.target_run_id,
        "topic": args.topic,
        "outboxCycleCommitted": 0,
        "directOrionCycleDrained": True,
        "recordedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partitions": parts,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"wrote": str(out), "target": args.target_run_id, "parts": parts}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
