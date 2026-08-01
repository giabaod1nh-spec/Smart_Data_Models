"""Run K-7 live E2E, soak, and chaos gates."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
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
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8094
SOAK_SEC = int(sys.argv[3]) if len(sys.argv) > 3 else 600


def _http_json(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return {"error": str(e), "url": url}


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    out = EVIDENCE_ROOT / RUN_ID
    out.mkdir(parents=True, exist_ok=True)
    settings = get_settings()

    raw_health = _http_json("http://localhost:8091/health")
    _cleanup_bronze_lock(settings)
    _stop_port(PORT)

    proc = _start_bronze_live(PORT, settings)
    health_start = _wait_ready(PORT, 60)
    samples = []
    t0 = time.time()
    while time.time() - t0 < SOAK_SEC:
        samples.append(
            {
                "ts": _utc_iso(),
                "health": _http_json(f"http://localhost:{PORT}/health"),
                "metrics": _http_json(f"http://localhost:{PORT}/metrics"),
            }
        )
        time.sleep(30)
    health_end = _http_json(f"http://localhost:{PORT}/health")

    live_pass = health_start.get("status") == "ok"
    live_report = {
        "bronze_port": PORT,
        "health_start": health_start,
        "health_end": health_end,
        "raw_consumer": raw_health,
        "soak_sec": SOAK_SEC,
        "samples": len(samples),
        "pass": live_pass,
    }
    (out / "live_e2e_report.json").write_text(json.dumps(live_report, indent=2), encoding="utf-8")
    (out / "health_snapshots.json").write_text(json.dumps(samples, indent=2), encoding="utf-8")

    chaos = [{"ts": _utc_iso(), "event": "A_bronze_restart_start"}]
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    _cleanup_bronze_lock(settings)
    _stop_port(PORT)
    proc = _start_bronze_live(PORT, settings)
    h_a = _wait_ready(PORT, 90)
    chaos.append({"ts": _utc_iso(), "event": "A_recovered", "health": h_a})

    chaos.append({"ts": _utc_iso(), "event": "B_clickhouse_pause"})
    subprocess.run(["docker", "pause", "de-clickhouse"], check=False, capture_output=True)
    time.sleep(15)
    subprocess.run(["docker", "unpause", "de-clickhouse"], check=False, capture_output=True)
    chaos.append({"ts": _utc_iso(), "event": "B_clickhouse_unpaused"})
    time.sleep(25)
    h_b = _wait_ready(PORT, 90)
    chaos.append({"ts": _utc_iso(), "event": "B_recovered", "health": h_b})

    proc.terminate()
    proc.wait(timeout=10)
    _cleanup_bronze_lock(settings)

    chaos_pass = h_a.get("status") == "ok" and (
        h_b.get("status") == "ok" or h_b.get("state") in ("READY", "DEGRADED")
    )
    (out / "chaos_timeline.json").write_text(json.dumps(chaos, indent=2), encoding="utf-8")

    soak_pass = live_pass and len(samples) >= 2 and not health_end.get("fault_message")
    summary = {
        "live_pass": live_pass,
        "soak_pass": soak_pass,
        "chaos_pass": chaos_pass,
        "samples": len(samples),
    }
    print(json.dumps(summary, indent=2))
    return 0 if all([live_pass, soak_pass, chaos_pass]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
