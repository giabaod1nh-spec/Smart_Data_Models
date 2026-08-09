"""Re-run chaos recovery gate only."""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.bronze.config import get_settings
from de.tools.k7_validation_runner import (
    EVIDENCE_ROOT,
    _cleanup_bronze_lock,
    _start_bronze_live,
    _stop_port,
    _wait_ready,
)

RUN_ID = sys.argv[1] if len(sys.argv) > 1 else "k7-official-20260801T0215Z"
PORT = 8094


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    out = EVIDENCE_ROOT / RUN_ID
    settings = get_settings()
    _cleanup_bronze_lock(settings)
    _stop_port(PORT)

    chaos = []
    proc = _start_bronze_live(PORT, settings)
    h0 = _wait_ready(PORT, 90)
    chaos.append({"ts": _utc_iso(), "event": "start", "health": h0})

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    _cleanup_bronze_lock(settings)
    proc = _start_bronze_live(PORT, settings)
    h_a = _wait_ready(PORT, 90)
    chaos.append({"ts": _utc_iso(), "event": "A_restart_recovered", "health": h_a})

    subprocess.run(["docker", "pause", "de-clickhouse"], check=False, capture_output=True)
    time.sleep(15)
    subprocess.run(["docker", "unpause", "de-clickhouse"], check=False, capture_output=True)
    time.sleep(25)
    h_b = _wait_ready(PORT, 90)
    chaos.append({"ts": _utc_iso(), "event": "B_clickhouse_recovered", "health": h_b})

    proc.terminate()
    _cleanup_bronze_lock(settings)

    passed = h_a.get("status") == "ok" and h_b.get("status") == "ok"
    (out / "chaos_timeline.json").write_text(json.dumps(chaos, indent=2), encoding="utf-8")
    print(json.dumps({"chaos_pass": passed, "events": len(chaos)}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
