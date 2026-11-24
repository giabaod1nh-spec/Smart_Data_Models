"""Summarize the matched source-time segment and final sample from each trial."""
import json
from pathlib import Path


def run(path):
    rows = [json.loads(line) for line in (path / "samples.jsonl").read_text().splitlines()]
    active = [
        row for row in rows
        if (row["outbox"] and 30 <= (row["outbox"]["simulation_seconds"] or 0) < 89
            and row["services"]["gold"] and row["services"]["gold"].get("progress")
            and row["services"]["raw"])
    ]
    first, last = active[0], active[-1]
    elapsed = last["wall_utc"] - first["wall_utc"]
    start, end = (sample["outbox"] for sample in (first, last))
    before, after = (sample["services"]["gold"]["progress"] for sample in (first, last))
    rates = {key: (after[key] - before[key]) / elapsed for key in (
        "bronze_change", "silver_change", "gold_change")}
    backlog = [sample["bronze_change"] - sample["silver_change"] for sample in (before, after)]
    final = next(row for row in reversed(rows) if row["services"]["gold"] and row["services"]["silver"])
    metrics = final["services"]["silver"]["metrics"]["journal_operations"]
    joint_reconciliation = [value for key, value in metrics.items()
                            if key.startswith(("reconcile_existing", "reconcile_visibility", "insert_obligation"))]
    final_gold = final["services"]["gold"]
    return dict(
        samples=len(rows), active_samples=len(active), wall_seconds=elapsed,
        source_time=[start["simulation_seconds"], end["simulation_seconds"]],
        simulation_pace=(end["simulation_seconds"] - start["simulation_seconds"]) / elapsed,
        offered_rate=(end["rows"] - start["rows"]) / elapsed,
        offered_bytes_rate=(end["bytes"] - start["bytes"]) / elapsed,
        raw_persisted_rate=(last["services"]["raw"]["raw_deliveries"] - first["services"]["raw"]["raw_deliveries"]) / elapsed,
        rates=rates, silver_backlog=backlog, silver_backlog_growth=(backlog[1] - backlog[0]) / elapsed,
        gold_change_backlog=[before["change_backlog"], after["change_backlog"]],
        final_progress=final_gold["progress"], final_outbox=final["outbox"],
        silver_accepted_apply=metrics["apply_accepted"],
        silver_reconciliation_calls=sum(item["calls"] for item in joint_reconciliation),
        silver_reconciliation_wall_sec=sum(item["wall_ns"] for item in joint_reconciliation) / 1e9,
        silver_poll_wait_sec=metrics["poll_wait"]["wall_ns"] / 1e9,
        gold_metrics=final_gold["metrics"])


root = Path(".pytest_cache")
print(json.dumps({name: run(root / name) for name in (
    "perf-w2-base-01", "perf-w2-group-02", "perf-w2-scheduler-01")}, indent=2))
