from __future__ import annotations

import copy
import json
from pathlib import Path

from de.tools.k6_historical_oracles import (
    compare_kafka_to_raw,
    compare_observation_multisets,
    compare_raw_to_bronze,
    normalize_kafka_entity_events,
    normalize_legacy_notifications,
)
from de.tools.k6_historical_cutover import main as cutover_main
from de.scripts.migrate_clickhouse import select_migrations

REPO = Path(__file__).resolve().parents[3]


def _pair():
    event = json.loads(
        (REPO / "contracts/events/examples/intersection-event.json").read_text("utf-8")
    )
    notification = {
        "id": "notification-1",
        "type": "Notification",
        "data": [copy.deepcopy(event["entity"])],
    }
    return notification, event


def test_dual_ingest_normalization_matches_shared_business_payload():
    notification, event = _pair()
    legacy = normalize_legacy_notifications([notification])
    kafka = normalize_kafka_entity_events([event])
    report = compare_observation_multisets(legacy, kafka)
    assert report["pass"] is True
    assert kafka[0].cycle_sequence == 0
    assert legacy[0].cycle_sequence is None


def test_dual_ingest_reports_payload_mismatch_and_duplicate_delta():
    notification, event = _pair()
    notification["data"][0]["overallTrafficStatus"]["value"] = "HEAVY"
    legacy = normalize_legacy_notifications([notification, notification])
    kafka = normalize_kafka_entity_events([event])
    report = compare_observation_multisets(legacy, kafka)
    assert report["pass"] is False
    assert report["payload_hash_mismatches"]
    assert report["unexpected_duplicate_delta"] > 0


def test_kafka_to_raw_requires_exactly_one_hash_matching_classification():
    key = ("traffic.entity-events.v2", 0, 10)
    expected = {key: "abc"}
    row = {"topic": key[0], "partition": key[1], "offset": key[2], "payload_bytes_hash": "abc"}
    assert compare_kafka_to_raw(expected, [row], [])["pass"] is True
    overlap = compare_kafka_to_raw(expected, [row], [row])
    assert overlap["pass"] is False
    assert overlap["overlap_or_duplicate"]


def test_raw_to_bronze_destination_map_is_disjoint_and_complete():
    raw = [
        {"raw_ingestion_id": "entity", "event_type": "TrafficEntityObserved"},
        {"raw_ingestion_id": "run", "event_type": "TrafficSimulationRunStarted"},
    ]
    quarantine = [{"raw_ingestion_id": "bad"}]
    report = compare_raw_to_bronze(
        raw,
        quarantine,
        [{"raw_ingestion_id": "entity"}],
        [{"raw_ingestion_id": "run"}],
        [{"raw_ingestion_id": "bad"}],
    )
    assert report["pass"] is True
    wrong = compare_raw_to_bronze(
        raw,
        quarantine,
        [{"raw_ingestion_id": "run"}],
        [{"raw_ingestion_id": "entity"}],
        [{"raw_ingestion_id": "bad"}],
    )
    assert wrong["pass"] is False
    assert wrong["wrong_destination"]


def test_operator_cli_writes_machine_readable_report_and_nonzero_on_failure(tmp_path):
    notification, event = _pair()
    legacy_path = tmp_path / "legacy.json"
    kafka_path = tmp_path / "kafka.json"
    output_path = tmp_path / "report.json"
    legacy_path.write_text(json.dumps([notification]), encoding="utf-8")
    kafka_path.write_text(json.dumps([event]), encoding="utf-8")
    rc = cutover_main(
        [
            "dual-parity",
            "--legacy-notifications",
            str(legacy_path),
            "--kafka-events",
            str(kafka_path),
            "--output",
            str(output_path),
        ]
    )
    assert rc == 0
    assert json.loads(output_path.read_text("utf-8"))["pass"] is True

    notification["data"][0]["currentPhase"]["value"] = "FAIL"
    legacy_path.write_text(json.dumps([notification]), encoding="utf-8")
    rc = cutover_main(
        [
            "dual-parity",
            "--legacy-notifications",
            str(legacy_path),
            "--kafka-events",
            str(kafka_path),
            "--output",
            str(output_path),
        ]
    )
    assert rc == 1
    assert json.loads(output_path.read_text("utf-8"))["pass"] is False


def test_historical_v2_migration_mode_excludes_legacy_001():
    files = select_migrations(REPO / "de/migrations", historical_v2=True)
    assert [path.name for path in files] == [
        "002_create_kafka_raw_events.sql",
        "003_create_bronze_v2.sql",
        "004_create_silver.sql",
    ]
