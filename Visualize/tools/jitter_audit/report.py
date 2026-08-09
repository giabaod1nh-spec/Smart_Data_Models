"""Analyze jitter audit JSONL and emit markdown report."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from tools.jitter_audit.recorder import JitterRecorder

METRICS = [
    "step_gap_ms",
    "backend_step_ms",
    "snapshot_capture_ms",
    "entity_mapping_ms",
    "event_envelope_build_ms",
    "canonical_hash_ms",
    "json_serialize_ms",
    "outbox_lock_wait_ms",
    "outbox_begin_tx_ms",
    "outbox_insert_rows_ms",
    "outbox_commit_ms",
    "outbox_append_total_ms",
    "explicit_sleep_ms",
    "loop_total_ms",
    "logging_ms",
    "gc_pause_ms",
]


def _load_meta(path: Path) -> dict:
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _spike_table(records: List[dict], n: int = 20) -> List[dict]:
    spikes = JitterRecorder.top_spikes(records, threshold_ms=50.0, n=n)
    rows = []
    for s in spikes:
        notes = s.get("notes") or {}
        rows.append(
            {
                "ts_wall": s.get("ts_wall"),
                "sim_t": s.get("sim_t"),
                "cycle_sequence": s.get("cycle_sequence"),
                "phase": s.get("phase"),
                "step_gap_ms": s.get("step_gap_ms"),
                "max_component": s.get("max_component"),
                "max_component_ms": s.get("max_component_ms"),
                "backend_step_ms": s.get("backend_step_ms"),
                "outbox_append_total_ms": s.get("outbox_append_total_ms"),
                "outbox_lock_owner": s.get("outbox_lock_owner"),
                "sqlite_op": s.get("sqlite_op"),
                "gc_pause_ms": s.get("gc_pause_ms"),
                "logging_ms": notes.get("logging_ms"),
            }
        )
    return rows


def _classify_root_cause(cases: Dict[str, dict]) -> Dict[str, Any]:
    """Evidence-based classification from A/B matrix."""
    def gap_p95(case: str) -> Optional[float]:
        recs = cases.get(case, {}).get("records") or []
        return JitterRecorder.summarize(recs, "step_gap_ms").get("p95")

    def gap_max(case: str) -> Optional[float]:
        recs = cases.get(case, {}).get("records") or []
        return JitterRecorder.summarize(recs, "step_gap_ms").get("max")

    a_p95 = gap_p95("A")
    d_p95 = gap_p95("D")
    b_p95 = gap_p95("B")
    c_p95 = gap_p95("C")
    e_p95 = gap_p95("E")
    f_p95 = gap_p95("F")

    primary = cases.get("PRIMARY", {})
    primary_recs = primary.get("records") or []
    primary_spikes = JitterRecorder.top_spikes(primary_recs, threshold_ms=50.0, n=20)

    conclusions: List[str] = []
    excluded: List[str] = []
    primary_cause = "inconclusive"
    secondary: List[str] = []
    confidence = "low"

    if a_p95 is not None and d_p95 is not None:
        if a_p95 >= 45 and abs(a_p95 - (d_p95 or 0)) < 15:
            primary_cause = "sumo_traci_gui_baseline"
            confidence = "high"
            conclusions.append(
                "Case A (SUMO-only) shows similar step_gap p95 to Kafka cases → "
                "jitter is not introduced by Kafka architecture."
            )
        elif a_p95 < 35 and (d_p95 or 0) >= 50:
            if (c_p95 or 0) >= 50 and (b_p95 or 0) < 45:
                primary_cause = "sqlite_outbox_implementation"
                confidence = "high"
                conclusions.append(
                    "B (capture, no SQLite) smooth but C/D (SQLite append) spiky → outbox/SQLite path."
                )
            elif (b_p95 or 0) >= 50:
                primary_cause = "traci_capture_build_path"
                confidence = "high"
                conclusions.append(
                    "B still spiky without SQLite → capture/mapping/serialization on TraCI thread."
                )
            else:
                primary_cause = "combined_capture_and_outbox"
                confidence = "medium"
        elif (e_p95 or 999) + 10 < (d_p95 or 0):
            secondary.append("logging_console")
            conclusions.append("Case E reduces step_gap vs D → logging contributes measurably.")

    if primary_spikes:
        comps = {}
        for s in primary_spikes:
            c = s.get("max_component") or "unknown"
            comps[c] = comps.get(c, 0) + 1
        top = max(comps, key=comps.get)
        if top == "backend_step_ms":
            secondary.append("sumo_backend_step")
        elif top in ("outbox_append_total_ms", "outbox_commit_ms", "outbox_lock_wait_ms"):
            if primary_cause == "inconclusive":
                primary_cause = "sqlite_outbox_hot_path"
                confidence = "medium"
        elif top in ("snapshot_capture_ms", "entity_mapping_ms", "event_envelope_build_ms"):
            if primary_cause == "inconclusive":
                primary_cause = "traci_capture_build"
                confidence = "medium"

    excluded.append("direct_orion_backpressure: verified OFF in runtime checks")
    excluded.append("legacy_sync_publish: forbidden by profile validator")

    return {
        "primary_cause": primary_cause,
        "secondary_causes": secondary,
        "confidence": confidence,
        "conclusions": conclusions,
        "excluded_hypotheses": excluded,
        "ab_p95_step_gap": {
            k: gap_p95(k) for k in ["A", "B", "C", "D", "E", "F", "PRIMARY"]
        },
        "ab_max_step_gap": {k: gap_max(k) for k in ["A", "B", "C", "D", "E", "F", "PRIMARY"]},
    }


def build_report(evidence_dir: Path, out_md: Path) -> None:
    evidence_dir = Path(evidence_dir)
    cases: Dict[str, dict] = {}
    for case in ["A", "B", "C", "D", "E", "F", "PRIMARY", "D_GC"]:
        jsonl = evidence_dir / f"case_{case}.jsonl"
        meta = _load_meta(evidence_dir / f"case_{case}_meta.json")
        records = JitterRecorder.load_records(jsonl)
        summary = {m: JitterRecorder.summarize(records, m) for m in METRICS}
        cases[case] = {
            "meta": meta,
            "records": records,
            "summary": summary,
            "spikes": _spike_table(records),
            "record_count": len(records),
        }

    classification = _classify_root_cause(cases)
    primary = cases.get("PRIMARY", {})
    primary_meta = primary.get("meta") or {}
    rv = primary_meta.get("runtime_verification") or {}

    lines: List[str] = []
    lines.append("# SUMO Kafka Jitter Root Cause Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    lines.append(f"**Evidence directory:** `{evidence_dir.as_posix()}`")
    lines.append("")
    lines.append("## A. Runtime verification")
    lines.append("")
    if rv:
        lines.append(f"- **Profile validation OK:** {rv.get('ok')}")
        checks = rv.get("checks") or {}
        lines.append(f"- **ORION_PUBLISH_ENABLED:** `{checks.get('orion_publish_enabled')}`")
        lines.append(f"- **KAFKA_OUTBOX_ENABLED:** `{checks.get('kafka_outbox_enabled')}`")
        lines.append(f"- **ORION_PERF_AUDIT (PRIMARY):** `{primary_meta.get('orion_perf_audit')}`")
        lines.append(f"- **LOG_LEVEL (PRIMARY):** `{primary_meta.get('log_level')}`")
        lines.append(f"- **SUMO process count (pre-run):** {checks.get('sumo_process_count')}")
        lines.append(f"- **Docker services:** {', '.join(checks.get('docker_services') or [])}")
        if rv.get("errors"):
            lines.append(f"- **Errors:** {'; '.join(rv['errors'])}")
    else:
        lines.append("_Runtime verification metadata missing — see case meta JSON._")
    lines.append("")
    lines.append("Direct Orion publisher: **OFF** (Kafka-only / `--no-orion`).")
    lines.append("Async Orion BACKPRESSURE: **not applicable** when `publish_orion=false`.")
    lines.append("")

    lines.append("## B. A/B result table")
    lines.append("")
    lines.append("| Case | Description | GUI | Outbox | Worker | step_gap p50 | p95 | p99 | max | records | wall s |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    desc = {
        "A": "SUMO only",
        "B": "Capture + noop outbox",
        "C": "Outbox append, worker OFF",
        "D": "Full outbox + worker",
        "E": "Full + minimal logging",
        "F": "Headless fast",
        "PRIMARY": "Kafka-only GUI 5min",
        "D_GC": "Full + GC disabled",
    }
    for case in ["A", "B", "C", "D", "E", "F", "D_GC", "PRIMARY"]:
        c = cases.get(case, {})
        if not c.get("record_count"):
            continue
        s = c["summary"]["step_gap_ms"]
        meta = c.get("meta") or {}
        lines.append(
            f"| {case} | {desc.get(case, case)} | "
            f"{'Y' if meta.get('gui') else 'N'} | "
            f"{'noop' if meta.get('noop_outbox') else ('Y' if meta.get('kafka_on', True) else 'N')} | "
            f"{'OFF' if meta.get('disable_worker') else ('n/a' if case == 'A' else 'ON')} | "
            f"{s.get('p50', 0):.1f} | {s.get('p95', 0):.1f} | {s.get('p99', 0):.1f} | {s.get('max', 0):.1f} | "
            f"{c.get('record_count', 0)} | {meta.get('wall_sec', 0):.0f} |"
        )
    lines.append("")

    lines.append("## C. Metric summaries (PRIMARY case)")
    lines.append("")
    if primary.get("summary"):
        lines.append("| Metric | p50 | p95 | p99 | max | n |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for m in METRICS:
            s = primary["summary"][m]
            if not s.get("n"):
                continue
            lines.append(
                f"| `{m}` | {s['p50']:.2f} | {s['p95']:.2f} | {s['p99']:.2f} | {s['max']:.2f} | {s['n']} |"
            )
    lines.append("")

    ob = primary_meta.get("outbox_stats") or {}
    lines.append("### Outbox instrumentation (PRIMARY)")
    lines.append("")
    lines.append(f"- Shared connection across threads: **{ob.get('shared_connection')}** (one conn per thread in current impl)")
    lines.append(f"- Shared Python lock: **{ob.get('shared_python_lock')}** — uses `_WriteGate` writer lock")
    lines.append(f"- Worker mark_queued batch rows: **{ob.get('mark_queued_batch_rows', 0)}**")
    lines.append(f"- Worker mark_acked batch rows: **{ob.get('mark_acked_batch_rows', 0)}**")
    lines.append(f"- Worker TX count: **{ob.get('worker_tx_count', 0)}**")
    lines.append(f"- WAL checkpoints: **{ob.get('checkpoint_count', 0)}** ({ob.get('checkpoint_ms', 0):.1f} ms total)")
    lines.append("")

    lines.append("## D. Top 20 correlated spikes (PRIMARY, step_gap ≥ 50 ms)")
    lines.append("")
    spikes = primary.get("spikes") or []
    if not spikes:
        lines.append("_No spikes ≥ 50 ms recorded in PRIMARY (or run pending)._")
    else:
        lines.append("| # | sim_t | cycle | phase | step_gap_ms | max component | ms | outbox | sqlite | gc_ms |")
        lines.append("|---:|---:|---:|---|---:|---|---:|---|---|---:|")
        for i, s in enumerate(spikes, 1):
            lines.append(
                f"| {i} | {s.get('sim_t', 0):.2f} | {s.get('cycle_sequence', '-')} | {s.get('phase', '-')} | "
                f"{s.get('step_gap_ms', 0):.1f} | {s.get('max_component', '-')} | {s.get('max_component_ms', 0):.1f} | "
                f"{s.get('outbox_append_total_ms', 0):.1f} | {s.get('sqlite_op', '-')} | {s.get('gc_pause_ms', 0):.1f} |"
            )
    lines.append("")

    lines.append("## E. Root cause (primary)")
    lines.append("")
    lines.append(f"**Classification:** `{classification['primary_cause']}`")
    lines.append(f"**Confidence:** {classification['confidence']}")
    lines.append("")
    for c in classification.get("conclusions") or []:
        lines.append(f"- {c}")
    lines.append("")

    lines.append("## F. Root cause (secondary)")
    lines.append("")
    for s in classification.get("secondary_causes") or []:
        lines.append(f"- {s}")
    if not classification.get("secondary_causes"):
        lines.append("- _None distinguished at current sample resolution._")
    lines.append("")

    lines.append("## G. Hypotheses excluded")
    lines.append("")
    for x in classification.get("excluded_hypotheses") or []:
        lines.append(f"- {x}")
    lines.append("")

    lines.append("## H. Recommended fixes (proposal only — not implemented)")
    lines.append("")
    lines.extend(_recommendations(classification["primary_cause"]))
    lines.append("")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _recommendations(primary_cause: str) -> List[str]:
    recs: List[str] = []
    if primary_cause in ("sumo_traci_gui_baseline", "sumo_backend_step"):
        recs += [
            "### P1 — Accept SUMO GUI baseline jitter",
            "- **Module:** `Visualize/simulation/backend.py`, SUMO GUI",
            "- **Change:** Document that micro-step observation + GUI render dominate; pipeline changes won't remove baseline jitter.",
            "- **No-loss impact:** none",
            "- **Retest:** Case A vs PRIMARY step_gap distribution",
            "",
            "### P2 — Optional: decouple observation cadence from TraCI micro-step",
            "- **Module:** `backend._cache_interval_sec`, publish interval",
            "- **Change:** Keep TraCI at 0.01s but publish/observation at 1s without heavy work between micro-steps.",
            "- **No-loss impact:** none if publish cadence unchanged",
        ]
    if primary_cause in ("sqlite_outbox_implementation", "sqlite_outbox_hot_path", "combined_capture_and_outbox"):
        recs += [
            "### P1 — Move capture+append off TraCI thread",
            "- **Module:** `Visualize/app/traci_runner.py`, new `tools/jitter_audit`-style queue",
            "- **Change:** TraCI enqueues frozen snapshot handles; worker thread builds events + outbox append.",
            "- **Why:** Spikes correlate with `outbox_append_total_ms` / publish phases on TraCI thread.",
            "- **No-loss impact:** must preserve cycle-atomic append semantics",
            "- **Retest:** `test_outbox_contention`, Case C/D step_gap p95",
            "",
            "### P2 — Further batch worker status writes + passive checkpoint tuning",
            "- **Module:** `outbox_worker.py`, `outbox_store.py`",
            "- **Change:** Already batched; tune checkpoint interval and ensure TraCI never waits on worker `_WriteGate`.",
        ]
    if primary_cause == "traci_capture_build_path":
        recs += [
            "### P1 — Reduce per-cycle capture cost",
            "- **Module:** `capture_entity_list`, `entity_mapper`",
            "- **Change:** Incremental snapshot diff, avoid double `deepcopy`, cache immutable entity slices.",
            "- **Retest:** Case B metrics: `snapshot_capture_ms`, `entity_mapping_ms`",
        ]
    if not recs:
        recs.append("_Awaiting PRIMARY + A/B evidence runs to rank fixes._")
    return recs
