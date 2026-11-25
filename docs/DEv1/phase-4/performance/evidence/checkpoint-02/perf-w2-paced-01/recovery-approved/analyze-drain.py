"""Summarize the approved stopped-source run without touching runtime state."""
from datetime import datetime
import json
from pathlib import Path


def when(sample):
    return datetime.fromisoformat(sample["utc"])


def main():
    directory = Path(__file__).resolve().parent
    path = directory.parent / "downstream-drain-approved.samples.jsonl"
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    active = [row for row in samples if row["phase"] == "drain"]
    valid = [row for row in active if row["measurement"]["valid"]]
    if not valid:
        raise ValueError("No valid drain samples")
    last = valid[-1]
    ready = next(row for row in samples if row["phase"] == "worker-startup"
                 and row["measurement"]["valid"] and row["services"]["gold"].get("progress"))
    gold = last["services"]["gold"]
    silver = last["services"]["silver"]["metrics"]["journal_operations"]
    done = {}
    first_full_ack = None
    for row in valid:
        g = row["services"]["gold"]
        if g["progress"]["gold_change"] == 1535 and first_full_ack is None:
            first_full_ack = row
        completed = g["metrics"].get("work_completed", 0)
        if completed and completed not in done:
            done[completed] = row["elapsed"]
    after_ack = {}
    if first_full_ack and last["elapsed"] > first_full_ack["elapsed"]:
        prior = first_full_ack["services"]["gold"]["metrics"]
        metrics = gold["metrics"]
        seconds = last["elapsed"] - first_full_ack["elapsed"]
        reads = metrics["materialization_page_reads"] - prior["materialization_page_reads"]
        lane_keys = ("work_lane_wall_sec", "change_lane_wall_sec", "discovery_lane_wall_sec",
                     "invalidation_lane_wall_sec")
        after_ack = dict(wall_seconds=seconds, materialization_page_reads=reads, reads_per_second=reads / seconds,
                         lane_wall_seconds={key: metrics.get(key, 0) - prior.get(key, 0) for key in lane_keys})
    report = dict(phase=samples[-1]["phase"], startup_wall_seconds=(when(ready)-when(samples[0])).total_seconds(),
                  active_wall_seconds=(when(samples[-1])-when(active[0])).total_seconds(),
                  memory_preflight=samples[0]["available"], memory_ready=ready["available"],
                  active_memory_minimum=min(row["available"] for row in active),
                  valid_samples=len(valid), invalid_samples=len(active)-len(valid),
                  final_progress=gold["progress"], after_full_ack=after_ack,
                  first_gold_full_ack_sample_seconds=first_full_ack["elapsed"] if first_full_ack else None,
                  first_sample_by_added_work_completed=done,
                  gold_final_metrics=gold["metrics"], silver_final_metrics=silver,
                  silver_fault_messages=sorted({row["services"]["silver"]["fault_message"] for row in valid
                                                if row["services"]["silver"].get("fault_message")}))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
