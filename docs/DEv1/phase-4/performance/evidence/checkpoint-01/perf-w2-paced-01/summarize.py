"""Derive stage-native rates from the paced fixture's raw time series."""
import json
from pathlib import Path


root = Path(__file__).parent
rows = [json.loads(line) for line in (root / "samples.jsonl").read_text().splitlines()]
shots = [
    row for row in rows if row["outbox"] and row["outbox"]["simulation_seconds"] is not None
    and all(row["services"].get(stage) for stage in ("raw", "bronze", "silver", "gold"))
    and row["services"]["gold"].get("progress")
]


def gauges(row):
    services = row["services"]
    gold = services["gold"]
    p, m = gold["progress"], gold["metrics"]
    return dict(
        offered=row["outbox"]["rows"], acked=row["outbox"]["acked"],
        outbox_pending=row["outbox"]["pending"], outbox_oldest=row["outbox"]["oldest_pending_seconds"],
        source_bytes=row["outbox"]["bytes"], simulation_second=row["outbox"]["simulation_seconds"],
        raw_persisted=services["raw"]["raw_deliveries"],
        raw_buffered=services["raw"]["buffered_records"],
        bronze_change=p["bronze_change"], bronze_pending_intents=services["bronze"]["pending_intents"],
        bronze_pending_deliveries=services["bronze"]["pending_delivery_count"],
        silver_change=p["silver_change"], bronze_to_silver=p["bronze_change"]-p["silver_change"],
        gold_change=p["gold_change"], silver_to_gold=p["silver_change"]-p["gold_change"],
        gold_invalidation_lag=p["invalidation_backlog"], gold_completed=m.get("work_completed", 0),
        gold_member_reads=m.get("materialization_page_reads", 0), gold_dirty=m.get("dirty_work", 0),
        gold_pending_work=m.get("pending_work", 0), gold_running_work=m.get("running_work", 0),
        gold_promotion_runs=m.get("promotion_pending_runs", 0),
        gold_promotion_age=m.get("promotion_queue_oldest_sec", 0),
        gold_pending_age=m.get("pending_work_oldest_sec", 0),
        gold_running_age=m.get("running_work_oldest_sec", 0),
        host_cpu_percent=row["host"]["cpu_pct"], host_available_bytes=row["host"]["available_memory_bytes"],
    )


def rates(points):
    if len(points) < 2:
        return None
    a, b = map(gauges, (points[0], points[-1]))
    t = points[-1]["wall_utc"] - points[0]["wall_utc"]
    return dict(sample_count=len(points), wall_seconds=t, source_range=[a["simulation_second"], b["simulation_second"]],
                head=a, tail=b,
                rates={key: (b[key]-a[key])/t for key in (
                    "simulation_second", "offered", "acked", "source_bytes", "raw_persisted", "bronze_change",
                    "silver_change", "gold_change", "bronze_to_silver", "silver_to_gold", "gold_completed",
                    "gold_member_reads")})


def segment(low, high):
    return rates([row for row in shots if low <= row["outbox"]["simulation_seconds"] <= high])


active = [row for index, row in enumerate(shots) if index == 0
          or row["outbox"]["rows"] > shots[index-1]["outbox"]["rows"]]
last = shots[-1]
summary = dict(
    matched_early=segment(31, 87.5), first_work_period=segment(60, 120), body=segment(120, 298),
    late_source=segment(300, 329), source_active=rates(active), final_sample=gauges(last),
    final_gold_metrics=last["services"]["gold"]["metrics"],
    final_silver_operations=last["services"]["silver"]["metrics"]["journal_operations"],
    host_minimum_bytes=min(row["host"]["available_memory_bytes"] for row in rows),
)
print(json.dumps(summary, indent=2, sort_keys=True))
