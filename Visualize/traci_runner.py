"""Permanent launcher — keeps `python traci_runner.py` working from Visualize/."""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from app.traci_runner import (  # noqa: E402
    build_parser,
    publish_once,
    run,
    setup_logging,
    start_control_api,
)

__all__ = [
    "build_parser",
    "publish_once",
    "run",
    "setup_logging",
    "start_control_api",
]

if __name__ == "__main__":
    args = build_parser().parse_args()
    args.gui = None
    sys.exit(run(args))
