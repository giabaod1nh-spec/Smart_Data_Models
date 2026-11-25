"""Fixture-only SQLite business-value oracle, with no production imports."""
import argparse
from collections import Counter
from contextlib import closing, ExitStack
import hashlib
import json
from pathlib import Path
import sqlite3


TRAFFIC = "silver_traffic_observation_fact_v1"
SIGNAL = "silver_signal_state_fact_v1"
INTERSECTION = "silver_intersection_state_fact_v1"
TRAFFIC_COLUMNS = (
    "vehicle_count", "pcu_equivalent", "left_count", "straight_count",
    "right_count", "average_speed_kmh", "waiting_vehicle_count", "queue_length_m",
    "occupancy_pct",
)
TIMINGS = {
    "phase_remaining_seconds": "phase_remaining",
    "phase_duration_seconds": "phase_duration",
    "green_duration_seconds": "green_duration",
    "yellow_duration_seconds": "yellow_duration",
    "red_duration_seconds": "red_duration",
}
ROOT = Path(__file__).resolve().parent
ARCHIVE = ROOT.parent / "producer/runs/perf-w2-paced-01"


def check(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, observed {actual!r}")


def open_snapshot(folder, name):
    return closing(sqlite3.connect(
        (folder / (name + ".sqlite3")).as_uri() + "?mode=ro&immutable=1", uri=True,
    ))


def event_values(db, event_id, sequence, family, negative):
    records = {}
    sql = """SELECT status,expected_json,expected_hash FROM silver_v1_obligation
             WHERE namespace='live' AND source_kind='changes'
               AND source_sequence=? AND event_id=? AND target_table=?"""
    for state, data, expected_hash in db.execute(sql, (sequence, event_id, family)):
        check(state, "RECONCILED", (sequence, family, "status"))
        check(hashlib.sha256(data.encode()).hexdigest(), expected_hash, (sequence, family, "hash"))
        doc = json.loads(data)
        key = doc.get("source_direction")
        check(key in records, False, (sequence, family, key, "duplicate"))
        if negative and sequence == 1 and family == INTERSECTION:
            doc["total_vehicle_count"] += 1
        records[key] = doc
    return records


def run(negative=False, snapshot_dir=ROOT):
    manifest = json.loads((ARCHIVE / "run_manifest.json").read_text(encoding="utf-8"))
    catalog = json.loads((ARCHIVE / "network_topology_catalog.json").read_text(encoding="utf-8"))
    check(catalog["topology_hash"], manifest["topology_hash"], "archive topology hash")
    topology_directions = {
        node: set(catalog["approach_edges"][tls]) for node, tls in catalog["node_to_tls"].items()
    }
    checked = Counter()
    payload_hash = hashlib.sha256()
    with ExitStack() as stack:
        outbox, bronze, silver = [
            stack.enter_context(open_snapshot(snapshot_dir, name)) for name in ("outbox", "bronze", "silver")
        ]
        cursor, invalidations = silver.execute(
            "SELECT change_cursor,invalidation_cursor FROM silver_v1_source_binding WHERE namespace='live'"
        ).fetchone()
        check(invalidations, 0, "fixture invalidation cursor")
        changes = bronze.execute(
            """SELECT change_sequence,event_id,change_type FROM v1_changes
               WHERE namespace='live' AND change_sequence <= ? ORDER BY change_sequence""", (cursor,)
        )
        for expected_sequence, (sequence, event_id, kind) in enumerate(changes, 1):
            check(sequence, expected_sequence, "sequence")
            check(kind, "ACCEPTED", (sequence, "change type"))
            (data,) = outbox.execute(
                "SELECT payload_json FROM kafka_outbox WHERE status='ACKED' AND event_id=?", (event_id,)
            ).fetchone()
            payload_hash.update(data.encode())
            event = json.loads(data)
            check(event["eventId"], event_id, (sequence, "event identity"))
            check(event["topologyDigest"], manifest["topologyDigest"], (sequence, "source topology"))
            payload = event["payload"]
            directions = topology_directions[event["intersectionId"]]
            check(set(payload["directions"]), directions, (sequence, "source directions"))
            check(set(payload["colors"]), directions, (sequence, "source signal directions"))
            traffic, signals, intersection = [
                event_values(silver, event_id, sequence, family, negative)
                for family in (TRAFFIC, SIGNAL, INTERSECTION)
            ]
            check(set(traffic), directions, (sequence, "traffic keys"))
            check(set(signals), directions, (sequence, "signal keys"))
            check(set(intersection), {None}, (sequence, "intersection cardinality"))
            counts = []
            for direction in sorted(directions):
                source = payload["directions"][direction]
                observed = traffic[direction]
                available = source.get("metric_availability", {})
                check(json.loads(observed["metric_availability_json"]), available,
                      (sequence, direction, "availability"))
                check(json.loads(observed["direction_payload_json"]), source,
                      (sequence, direction, "direction payload"))
                for column in TRAFFIC_COLUMNS:
                    check(observed[column], source[column], (sequence, direction, column))
                flags = sorted(f"METRIC_{status}:{metric}" for metric, status in available.items())
                check(sorted(json.loads(observed["quality_flags_json"])), flags,
                      (sequence, direction, "quality flags"))
                check(observed["quality_status"], "VALID_WITH_UNAVAILABLE" if flags else "VALID",
                      (sequence, direction, "quality status"))
                counts.append(None if "vehicle_count" in available else source["vehicle_count"])
                signal = signals[direction]
                check(signal["signal_color"], payload["colors"][direction], (sequence, direction, "color"))
                for name in ("phase", "next_phase", "timing_mode"):
                    check(signal[name], payload.get(name), (sequence, direction, name))
                for name, source_name in TIMINGS.items():
                    check(signal[name], payload.get(source_name), (sequence, direction, name))
            result = intersection[None]
            complete = all(value is not None for value in counts)
            check(result["total_vehicle_count"], sum(counts) if complete else None,
                  (sequence, "total_vehicle_count"))
            check(result["total_vehicle_count_quality"],
                  "COMPLETE_TOPOLOGY_SUM" if complete else "PARTIAL_OR_UNAVAILABLE",
                  (sequence, "count quality"))
            check(json.loads(result["intersection_payload_json"]), payload, (sequence, "intersection payload"))
            for name in ("phase", "control_mode", "derived_traffic_state", "spillback_pressure"):
                check(result[name], payload.get(name), (sequence, name))
            for name in ("spillback_detected", "intersection_box_blocked", "downstream_edge_full"):
                expected = int(payload[name]) if name in payload else None
                check(result[name], expected, (sequence, name))
            for name in ("incidents", "operational_state"):
                value = result[name + "_json"]
                check(None if value is None else json.loads(value), payload.get(name), (sequence, name))
            checked.update(events=1, traffic=len(traffic), signal=len(signals), intersection=1,
                           partial_counts=int(not complete), nonzero_count_totals=int(complete and sum(counts) > 0))
        check(checked["events"], cursor, "Silver prefix length")
        check(checked["traffic"] + checked["signal"] + checked["intersection"], silver.execute(
            """SELECT count(*) FROM silver_v1_obligation
               WHERE namespace='live' AND source_kind='changes'
                 AND source_sequence <= ? AND target_table IN (?,?,?)""",
            (cursor, TRAFFIC, SIGNAL, INTERSECTION),
        ).fetchone()[0], "fact inventory across the fixture")
    check(sum(checked.values()) > 0, True, "nonempty evidence")
    return dict(checked=checked, input_envelopes_sha256=payload_hash.hexdigest(), status="PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--negative-control", action="store_true")
    parser.add_argument("--snapshot-dir", type=Path, default=ROOT)
    args = parser.parse_args()
    print(json.dumps(run(args.negative_control, args.snapshot_dir.resolve()), sort_keys=True))
