"""Check frozen source bytes, committed Raw coordinates and completed Bronze intents."""
from base64 import b64decode
from collections import Counter, defaultdict
from contextlib import closing, ExitStack
from hashlib import sha256
import json
from pathlib import Path
import sqlite3


ROOT = Path(__file__).resolve().parent


def open_snapshot(folder, name):
    path = (folder / f"{name}.sqlite3").as_uri()
    return closing(sqlite3.connect(path + "?mode=ro&immutable=1", uri=True))


def source_inventory(db):
    return {event: payload.encode() for event, payload in db.execute(
        "SELECT event_id,payload_json FROM kafka_outbox"
    )}


def audit():
    with ExitStack() as stack:
        old = stack.enter_context(open_snapshot(ROOT.parent / "recovery-before", "outbox"))
        outbox, raw, bronze = [
            stack.enter_context(open_snapshot(ROOT, name)) for name in ("outbox", "raw", "bronze")
        ]
        source = source_inventory(outbox)
        unchanged = bool(source) and source_inventory(old) == source
        acknowledgements = {(topic, part, offset): event for topic, part, offset, event in outbox.execute(
            "SELECT topic,kafka_partition,kafka_offset,event_id FROM kafka_outbox WHERE status='ACKED'"
        )}
        deliveries = {}
        offsets = defaultdict(set)
        for topic, part, offset, raw_id, confirmed, body in raw.execute(
            "SELECT topic,partition_id,offset_value,raw_ingestion_id,confirmed_hash,row_json "
            "FROM deliveries WHERE destination='RAW' AND state='COMMITTED'"
        ):
            row = json.loads(body)
            event = row["event_id"]
            value = b64decode(row["value"], validate=True)
            key = (topic, part, offset)
            if (confirmed != row["physical_record_hash"] or
                    sha256(value).hexdigest() != row["payload_bytes_hash"] or source.get(event) != value):
                raise ValueError(f"Raw content mismatch at {key}")
            deliveries[key] = (event, raw_id)
            offsets[(topic, part)].add(offset)
        missing_acks = [key for key, event in acknowledgements.items() if deliveries.get(key, (None,))[0] != event]
        repeats = Counter(event for event, _ in deliveries.values())
        progress = {(topic, part): (committed, safe) for topic, part, committed, safe in raw.execute(
            "SELECT topic,partition_id,committed_next_offset,safe_next_offset FROM partition_progress"
        )}
        contiguous = all(seen == set(range(committed)) and safe == committed
                         for key, (committed, safe) in progress.items() if (seen := offsets[key]))
        intents = {(topic, part, offset): (event, raw_id, status, disposition)
                   for topic, part, offset, event, raw_id, status, disposition in bronze.execute(
            "SELECT topic,partition_id,offset_value,event_id,raw_id,processing_status,disposition "
            "FROM v1_intents WHERE namespace='live'"
        )}
        bad_intents = [key for key, value in intents.items()
                       if key not in deliveries or value[:2] != deliveries[key] or value[2] != "COMPLETED"]
        dispositions = Counter(value[3] for value in intents.values())
        statuses = dict(outbox.execute("SELECT status,count(*) FROM kafka_outbox GROUP BY status"))
        raw_other = raw.execute("SELECT count(*) FROM deliveries").fetchone()[0] - len(deliveries)
        return dict(source_unchanged=unchanged, source_rows=len(source), outbox_statuses=statuses,
                    exact_ack_coverage=len(acknowledgements) - len(missing_acks),
                    missing_or_mismatched_ack_coordinates=len(missing_acks), raw_noncommitted_or_quarantined=raw_other,
                    committed_raw_rows=len(deliveries), distinct_raw_events=len(repeats),
                    raw_extra_exact_repeats=len(deliveries) - len(repeats), raw_offsets_contiguous=contiguous,
                    completed_bronze_intents=len(intents) - len(bad_intents), invalid_bronze_intents=len(bad_intents),
                    missing_bronze_intents=len(deliveries.keys() - intents.keys()),
                    bronze_dispositions=dispositions)


if __name__ == "__main__":
    print(json.dumps(audit(), sort_keys=True))
