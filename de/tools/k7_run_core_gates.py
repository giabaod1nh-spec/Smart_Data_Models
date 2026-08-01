"""Run K-7 gates G0-G5 only (preflight through replay parity)."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.bronze.config import get_settings
from de.bronze.replay import run_backfill, run_parity_sync
from de.tools.k7_bronze_oracles import run_oracles
from de.tools.k7_validation_runner import (
    EVIDENCE_ROOT,
    build_k45_manifest,
    manifest_scope_verify,
    replay_multiset_hash,
    reset_scoped_state,
    schema_dump,
    snapshot_checkpoint,
    verify_p0_preconditions,
    _cleanup_bronze_lock,
    _utc_iso,
)

RUN_ID = sys.argv[1] if len(sys.argv) > 1 else "k7-official-20260801T0215Z"


def main() -> int:
    out = EVIDENCE_ROOT / RUN_ID
    out.mkdir(parents=True, exist_ok=True)
    settings = get_settings().model_copy(update={"batch_size": 500, "poll_interval_sec": 0.05})
    cp_path = Path(settings.checkpoint_path)

    p0_ok, p0_info = verify_p0_preconditions()
    manifest = build_k45_manifest()
    (out / "window_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    preflight = {
        "pass": p0_ok,
        "ts": _utc_iso(),
        "run_id": RUN_ID,
        "p0_preconditions": p0_info,
        "manifest_scope": manifest_scope_verify(settings, manifest),
        "checkpoint_snapshot": snapshot_checkpoint(cp_path),
    }
    (out / "preflight.json").write_text(json.dumps(preflight, indent=2), encoding="utf-8")
    if not p0_ok:
        return 2

    (out / "checkpoint_before.json").write_text(
        json.dumps(snapshot_checkpoint(cp_path), indent=2), encoding="utf-8"
    )
    reset_info = reset_scoped_state(settings, RUN_ID, manifest)
    (out / "schema_dump.sql").write_text(schema_dump(settings), encoding="utf-8")

    t0 = time.time()
    bf_rc = run_backfill(settings, out / "window_manifest.json", RUN_ID, resume=False)
    _cleanup_bronze_lock(settings)
    bf_elapsed = time.time() - t0

    cp = snapshot_checkpoint(cp_path)
    p1_end = 4570
    cp_p1 = next(
        (
            int(r["last_completed_offset"])
            for r in cp.get("checkpoints", [])
            if r.get("checkpoint_namespace") == f"backfill:{RUN_ID}"
            and int(r.get("partition_id", -1)) == 1
        ),
        -1,
    )
    scope = manifest_scope_verify(settings, manifest)
    records = 2310
    rec_per_sec = round(records / max(bf_elapsed, 0.001), 2)
    backfill_report = {
        "run_id": RUN_ID,
        "elapsed_sec": round(bf_elapsed, 2),
        "records_per_sec": rec_per_sec,
        "checkpoint_p1_last": cp_p1,
        "expected_last": p1_end,
        "exit_code": bf_rc,
        "pass": bf_rc == 0 and cp_p1 == p1_end and rec_per_sec >= 100,
        "reset": reset_info,
    }
    (out / "backfill_report.json").write_text(json.dumps(backfill_report, indent=2), encoding="utf-8")

    parity_report = run_oracles(settings, manifest, replay_run_id=None)
    (out / "parity_report.json").write_text(json.dumps(parity_report, indent=2), encoding="utf-8")

    parity_rid = f"{RUN_ID}-parity"
    run_parity_sync(settings, out / "window_manifest.json", parity_rid)
    replay_oracle = run_oracles(settings, manifest, replay_run_id=parity_rid)
    replay_hash = replay_multiset_hash(settings, manifest, parity_rid)
    replay_report = {
        "replay_run_id": parity_rid,
        "oracle_on_replay_tables": replay_oracle,
        "multiset_parity": replay_hash,
        "pass": bool(replay_oracle.get("pass")) and replay_hash.get("pass"),
    }
    (out / "replay_report.json").write_text(json.dumps(replay_report, indent=2), encoding="utf-8")

    summary = {
        "run_id": RUN_ID,
        "backfill_pass": backfill_report["pass"],
        "rec_per_sec": rec_per_sec,
        "oracle_pass": parity_report.get("pass"),
        "replay_pass": replay_report["pass"],
        "H0_eq_H1": replay_hash.get("pass"),
    }
    print(json.dumps(summary, indent=2))
    ok = all([backfill_report["pass"], parity_report.get("pass"), replay_report["pass"]])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
