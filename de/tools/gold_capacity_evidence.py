"""Independent offline audit for a stopped, conflict-free V1 performance fixture."""
from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3

import rfc8785

from de.tools.gold_core_capacity import verify_pipeline


def wire(value):
    """Apply the external V1 JSON projection without reader or processor helpers."""
    def project(item, field=""):
        if isinstance(item, dict):
            return {key: project(val, key) for key, val in item.items()}
        if isinstance(item, list):
            return [project(val, field) for val in item]
        if type(item) is int and (field.endswith(("_sequence", "_high_water", "_prefix")) or abs(item) >= 2**53):
            return str(item)
        return item
    return rfc8785.dumps(project(value))


def digest(body):
    return sha256(body).hexdigest()


def open_readonly(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    return db


def text(value):
    return value.decode("ascii") if isinstance(value, bytes) else value


def fact_cells(row):
    """Normalize the physical UTC timestamp precision declared in V1 fact DDL."""
    result = {}
    for column, value in row.items():
        value = text(value)
        if column in ("created_at", "captured_at"):
            if isinstance(value, str):
                value = datetime.fromisoformat(value)
            value = value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)
            precision = "milliseconds" if column == "captured_at" else "microseconds"
            value = value.isoformat(timespec=precision)
        result[column] = value
    return result


def compare_physical_facts(clickhouse, expected_by_table):
    matches = {}
    for table, expected in expected_by_table.items():
        def key(row):
            return row["event_id"], row.get("source_direction")

        inventory = {key(row): fact_cells(row) for row in expected}
        rows = clickhouse.query(f"SELECT * FROM {table}", settings={
            "max_result_rows": 100_000, "max_result_bytes": 64 * 1024 * 1024,
            "result_overflow_mode": "throw",
        })
        accepted_keys = set()
        mismatches = 0
        for values in rows.result_rows:
            actual = fact_cells(dict(zip(rows.column_names, values)))
            identity = key(actual)
            mismatches += int(inventory.get(identity) != actual)
            accepted_keys.add(identity)
        matches[table] = (len(inventory) == len(expected) and accepted_keys == inventory.keys()
                          and mismatches == 0)
    return matches


def audit_pipeline(outbox_path, raw_path, bronze_path, silver_path, gold_path, clickhouse):
    """Compare durable outputs and recompute current completed work independently."""
    with ExitStack() as stack:
        outbox, raw, bronze, silver, gold = [
            stack.enter_context(closing(open_readonly(path))) for path in
            (outbox_path, raw_path, bronze_path, silver_path, gold_path)
        ]
        if outbox.execute("SELECT count(*) FROM kafka_outbox").fetchone()[0] > 10_000:
            raise ValueError("The offline fixture audit is limited to 10,000 source observations")
        source = dict(outbox.execute("SELECT event_id,payload_json FROM kafka_outbox"))
        expected_wire = {
            (topic, part, offset): (event_id, payload.encode("utf-8"))
            for topic, part, offset, event_id, payload in outbox.execute(
                "SELECT topic,kafka_partition,kafka_offset,event_id,payload_json "
                "FROM kafka_outbox WHERE status='ACKED'")
        }
        raw_evidence = {}
        raw_conflicts = 0
        physical_rows = 0
        with clickhouse.query_row_block_stream(
                "SELECT topic,partition,offset,raw_ingestion_id,physical_record_hash,"
                "event_id,payload_bytes_hash,value_bytes_base64 FROM v1_raw_events LIMIT 10001") as stream:
            for block in stream:
                physical_rows += len(block)
                for topic, part, offset, raw_id, record_hash, event, byte_hash, base64_value in block:
                    value = base64.b64decode(base64_value, validate=True)
                    key = (topic, part, offset)
                    row = (text(raw_id), text(record_hash), event, text(byte_hash), value)
                    raw_conflicts += int(key in raw_evidence and raw_evidence[key] != row)
                    raw_evidence[key] = row
        if physical_rows > 10_000:
            raise ValueError("Raw inventory exceeded the fixture audit bound")
        raw_bytes_match = (
            raw_conflicts == 0 and expected_wire.keys() == raw_evidence.keys()
            and all((event, value) == expected_wire[key] and digest(value) == byte_hash
                    for key, (_, _, event, byte_hash, value) in raw_evidence.items())
        )
        ledger = {
            (topic, part, offset): (raw_id, confirmed)
            for topic, part, offset, raw_id, confirmed in raw.execute(
                "SELECT topic,partition_id,offset_value,raw_ingestion_id,confirmed_hash FROM deliveries "
                "WHERE state='COMMITTED' AND destination='RAW'")
        }
        offsets = defaultdict(set)
        for topic, part, offset in raw_evidence:
            offsets[(topic, part)].add(offset)
        partition_progress = {(topic, part): (committed, safe) for topic, part, committed, safe
                              in raw.execute("SELECT topic,partition_id,committed_next_offset,safe_next_offset "
                                             "FROM partition_progress")}
        raw_committed = (raw_evidence.keys() == ledger.keys()
                         and len(ledger) == raw.execute("SELECT count(*) FROM deliveries").fetchone()[0]
                         and all(ledger[key] == row[:2] for key, row in raw_evidence.items())
                         and all(min(seen) == 0 and len(seen) == max(seen) + 1 and
                                 partition_progress.get(key) == (len(seen), len(seen))
                                 for key, seen in offsets.items()))

        intents = {(topic, part, offset): (raw_id, event, status) for topic, part, offset, raw_id, event, status
                   in bronze.execute("SELECT topic,partition_id,offset_value,raw_id,event_id,processing_status "
                                     "FROM v1_intents WHERE namespace='live'")}
        expected_lineage = {(row[0], *key, row[1], row[2], row[3]) for key, row in raw_evidence.items()}
        lineage_rows = clickhouse.query(
            "SELECT raw_ingestion_id,topic,partition,offset,physical_record_hash,event_id,source_byte_hash "
            "FROM bronze_delivery_dispositions_v1 WHERE namespace='live' LIMIT 10001").result_rows
        if len(lineage_rows) > 10_000:
            raise ValueError("Bronze lineage inventory exceeded the fixture audit bound")
        physical_lineage = {tuple(text(cell) for cell in row) for row in lineage_rows}
        bronze_lineage_match = (expected_lineage == physical_lineage and intents.keys() == raw_evidence.keys()
                                and all(intents[key] == (row[0], row[2], "COMPLETED")
                                        for key, row in raw_evidence.items()))

        transition_fields = ("namespace", "change_sequence", "event_id", "revision", "change_type",
                             "previous_state", "new_state", "content_hash", "invalidation_id")
        transitions = {row[2]: dict(zip(transition_fields, row)) for row in bronze.execute(
            "SELECT " + ",".join(transition_fields) + " FROM v1_changes WHERE namespace='live'")}
        observations = {event: json.loads(payload) for event, payload in source.items()}
        silver_validity = dict(silver.execute("SELECT event_id,change_sequence FROM silver_v1_event_validity "
                                              "WHERE namespace='live' AND state='VALID'"))
        silver_cursor = silver.execute("SELECT change_cursor,invalidation_cursor FROM silver_v1_source_binding "
                                       "WHERE namespace='live'").fetchone()
        silver_applied = (silver_cursor == (len(source), 0) and silver_validity ==
                          {event: row["change_sequence"] for event, row in transitions.items()})
        families = {"silver_intersection_state_fact_v1": "intersection",
                    "silver_traffic_observation_fact_v1": "traffic", "silver_signal_state_fact_v1": "signal"}
        facts = defaultdict(lambda: defaultdict(list))
        expected_physical = {table: [] for table in families}
        bad_obligations = 0
        for event, table, body, expected_hash, status in silver.execute(
                "SELECT event_id,target_table,expected_json,expected_hash,status FROM silver_v1_obligation "
                "WHERE namespace='live' ORDER BY event_id,row_key"):
            bad_obligations += int(status != "RECONCILED" or digest(body.encode()) != expected_hash)
            if table in families:
                row = json.loads(body)
                facts[event][families[table]].append(row)
                expected_physical[table].append(row)
        physical_facts_match = compare_physical_facts(clickhouse, expected_physical)

        projected = {}
        for event, transition in transitions.items():
            if event not in observations:
                continue
            bundle = facts[event]
            directions = set(observations[event]["payload"]["directions"])
            if (len(bundle["intersection"]) != 1 or any(
                    len(bundle[family]) != len(directions)
                    or {row["source_direction"] for row in bundle[family]} != directions
                    for family in ("traffic", "signal"))):
                continue
            intersection = bundle["intersection"][0]
            canonical_bundle = dict(event_id=event, intersection=intersection,
                                    traffic=sorted(bundle["traffic"], key=lambda row: row["source_direction"]),
                                    signal=sorted(bundle["signal"], key=lambda row: row["source_direction"]))
            projected[event] = wire(dict(event_id=event, change_sequence=transition["change_sequence"],
                                         semantic_revision=transition["revision"], scenario_id=intersection["scenario_id"],
                                         transition_hash=digest(wire(transition)), bundle_hash=digest(wire(canonical_bundle))))

        source_windows = defaultdict(list)
        for event, row in observations.items():
            for width in (60, 300):
                lo = int(row["simulationTimeSeconds"]) // width * width
                source_windows[(row["simulationRunId"], width, lo)].append(event)
        verified_work = Counter()
        mismatched_work = 0
        for identity, duration, lo, input_hash, count in gold.execute(
                "SELECT r.identity,w.duration,w.start,g.input_hash,g.input_count "
                "FROM core_run r JOIN core_window w ON r.id=w.run_key JOIN core_work g "
                "ON w.id=g.window_id AND w.generation=g.generation WHERE g.state='COMPLETE'"):
            members = sorted(source_windows[(json.loads(identity)["simulation_run_id"], duration, lo)])
            h = sha256(b"gold-core-input-set-v1\0")
            for member in members:
                body = projected.get(member, b"")
                h.update(len(body).to_bytes(8, "big") + body)
            if count == len(members) and all(member in projected for member in members) and h.hexdigest() == input_hash:
                verified_work[duration] += 1
            else:
                mismatched_work += 1

    prefix = verify_pipeline(outbox_path, bronze_path, gold_path)
    required_work = Counter(item["duration"] for item in prefix["window_progress"] if item["source_crossed"])
    return dict(source_events=len(source), physical_raw_offsets=len(raw_evidence),
                raw_bytes_match=raw_bytes_match, raw_committed=raw_committed,
                bronze_lineage_match=bronze_lineage_match, silver_applied=silver_applied,
                silver_fact_keys_match=projected.keys() == source.keys(),
                silver_physical_facts_match=physical_facts_match,
                bad_silver_obligations=bad_obligations, verified_completed_work=dict(verified_work),
                mismatched_completed_work=mismatched_work, required_completed_work=dict(required_work),
                passed=(len(source) > 0 and raw_bytes_match and raw_committed and bronze_lineage_match
                        and silver_applied and projected.keys() == source.keys()
                        and all(physical_facts_match.values())
                        and bad_obligations == 0 and bool(required_work)
                        and verified_work == required_work and mismatched_work == 0
                        and all(prefix[key] for key in ("full_drain", "exact_sequence_coverage",
                                                       "receipt_coverage", "membership_match", "eligible_work_drained"))),
                prefix=prefix)


def main():
    import clickhouse_connect

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="retained fixture archive root")
    parser.add_argument("--raw-ledger", type=Path, required=True)
    args = parser.parse_args()
    with closing(clickhouse_connect.get_client(host=os.environ["SILVER_CLICKHOUSE_HOST"],
                 username=os.environ["SILVER_CLICKHOUSE_USER"], database="smart_traffic")) as clickhouse:
        evidence = audit_pipeline(args.root / "kafka_outbox/v1-outbox.sqlite3", args.raw_ledger,
                                 os.environ["SILVER_V1_BRONZE_PATH"], os.environ["SILVER_CHECKPOINT_PATH"],
                                 os.environ["GOLD_CHECKPOINT_PATH"], clickhouse)
    print(json.dumps(evidence, sort_keys=True))
    return 0 if evidence["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
