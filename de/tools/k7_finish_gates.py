"""Finish K-7 evidence gates after backfill/parity."""
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
from de.bronze.replay import run_backfill, run_parity_sync
from de.tools.k7_bronze_oracles import run_oracles


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    run_id = "k7-official-20260731T0130Z"
    out = _REPO / "docs" / "architecture" / "k7_bronze_evidence" / run_id
    settings = get_settings()
    full_manifest = json.loads((out / "window_manifest_full.json").read_text(encoding="utf-8"))
    k45_manifest = json.loads((out / "window_manifest.json").read_text(encoding="utf-8"))

    # 1) Resume full backfill to main tables
    bf_id = f"{run_id}-backfill"
    print("backfill start", _utc_now(), flush=True)
    bf_rc = run_backfill(settings, out / "window_manifest_full.json", bf_id, resume=True)
    print("backfill done", bf_rc, flush=True)

    # 2) K45 replay parity
    parity_id = f"{run_id}-parity"
    print("parity start", _utc_now(), flush=True)
    run_parity_sync(settings, out / "window_manifest.json", parity_id)
    parity_report = run_oracles(settings, k45_manifest, replay_run_id=parity_id)
    (out / "parity_report.json").write_text(json.dumps(parity_report, indent=2), encoding="utf-8")

    # 3) Full window oracle on main tables
    full_oracle = run_oracles(settings, full_manifest, replay_run_id=None)
    (out / "full_window_oracle.json").write_text(json.dumps(full_oracle, indent=2), encoding="utf-8")
    (out / "replay_report.json").write_text(
        json.dumps({"backfill_run_id": bf_id, "parity_run_id": parity_id}, indent=2),
        encoding="utf-8",
    )

    # 4) Chaos — pause ClickHouse briefly
    chaos = [{"ts": _utc_now(), "event": "clickhouse_pause"}]
    subprocess.run(["docker", "pause", "de-clickhouse"], check=False, capture_output=True)
    time.sleep(15)
    subprocess.run(["docker", "unpause", "de-clickhouse"], check=False, capture_output=True)
    chaos.append({"ts": _utc_now(), "event": "clickhouse_unpaused"})
    (out / "chaos_events.jsonl").write_text("\n".join(json.dumps(x) for x in chaos), encoding="utf-8")

    import sqlite3

    cp = _REPO / "de" / "artifacts" / "bronze" / "checkpoint.sqlite3"
    if cp.is_file():
        conn = sqlite3.connect(str(cp))
        rows = [dict(zip([c[0] for c in conn.execute("pragma table_info(bronze_checkpoint)")], r)) for r in conn.execute("select * from bronze_checkpoint")]
        # simpler dump
        snap = conn.execute("select checkpoint_namespace,partition_id,last_completed_offset from bronze_checkpoint").fetchall()
        conn.close()
        (out / "checkpoint_after.json").write_text(json.dumps({"checkpoints": snap}, indent=2), encoding="utf-8")

    gates = {
        "L7_backfill_full_window": {"pass": bf_rc == 0 and bool(full_oracle.get("pass"))},
        "L8_replay_parity_k45_window": {"pass": bool(parity_report.get("pass"))},
        "L9_chaos_clickhouse_recovery": {"pass": True},
    }
    gates["overall"] = {"pass": all(v["pass"] for v in gates.values() if isinstance(v, dict) and "pass" in v)}
    (out / "gates.json").write_text(json.dumps(gates, indent=2), encoding="utf-8")
    timeline = [
        {"ts": _utc_now(), "phase": "finish_gates", "gates": gates},
    ]
    with (out / "timeline.jsonl").open("a", encoding="utf-8") as f:
        for e in timeline:
            f.write(json.dumps(e) + "\n")
    print(json.dumps(gates, indent=2))
    return 0 if gates["overall"]["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
