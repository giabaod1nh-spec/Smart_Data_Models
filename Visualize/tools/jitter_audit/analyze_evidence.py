"""Offline analysis of jitter audit JSONL → stdout summary."""
from __future__ import annotations

import json
import sys
from pathlib import Path

VIS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(VIS))

from tools.jitter_audit.recorder import JitterRecorder  # noqa: E402

METRICS = [
    "step_gap_ms",
    "backend_step_ms",
    "snapshot_capture_ms",
    "entity_mapping_ms",
    "event_envelope_build_ms",
    "canonical_hash_ms",
    "json_serialize_ms",
    "outbox_lock_wait_ms",
    "outbox_commit_ms",
    "outbox_append_total_ms",
    "explicit_sleep_ms",
    "gc_pause_ms",
]


def load_safe(path: Path):
    rows = []
    if not path.is_file():
        return rows
    t0 = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = float(row.get("ts_wall") or 0)
        if t0 is None:
            t0 = ts
        if ts - t0 < 5.0:
            continue
        rows.append(row)
    return rows


def main() -> None:
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        "docs/implementation/jitter_audit_evidence/20260731T163437Z"
    )
    for case in ["A", "B", "C", "D", "E", "F", "PRIMARY"]:
        path = base / f"case_{case}.jsonl"
        meta_path = base / f"case_{case}_meta.json"
        recs = load_safe(path)
        meta = json.loads(meta_path.read_text()) if meta_path.is_file() else {}
        if not recs:
            print(f"{case}: NO DATA")
            continue
        g = JitterRecorder.summarize(recs, "step_gap_ms")
        b = JitterRecorder.summarize(recs, "backend_step_ms")
        o = JitterRecorder.summarize(recs, "outbox_append_total_ms")
        cap = JitterRecorder.summarize(
            [r for r in recs if float(r.get("snapshot_capture_ms") or 0) > 0],
            "snapshot_capture_ms",
        )
        spikes = JitterRecorder.top_spikes(recs, threshold_ms=50.0, n=5)
        print(
            f"{case}: n={len(recs)} wall={meta.get('wall_sec',0):.0f}s "
            f"gap p50={g['p50']:.1f} p95={g['p95']:.1f} p99={g['p99']:.1f} max={g['max']:.1f} "
            f"backend_p95={b['p95']:.1f} outbox_p95={o['p95']} cap_p95={cap['p95']} spikes50={len([r for r in recs if float(r.get('step_gap_ms',0))>=50])}"
        )
        if spikes:
            s = spikes[0]
            print(
                f"  top spike gap={s['step_gap_ms']:.1f} comp={s.get('max_component')} "
                f"sim_t={s.get('sim_t')} cycle={s.get('cycle_sequence')}"
            )


if __name__ == "__main__":
    main()
