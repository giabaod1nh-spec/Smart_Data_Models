"""Bronze row -> exact Plan 2 input dataclass mapping (Plan 3 §6.3)."""
from __future__ import annotations

from datetime import datetime, timezone

from de.silver.config import SilverSettings, SourceStream
from de.silver.input_models import BronzeEntityInputRecord, BronzeRunInputRecord
from de.silver.readers import BronzeReader
from de.tests.silver.conftest import FakeClient, FakeQueryResult

TS = datetime(2026, 1, 1, tzinfo=timezone.utc)

ENTITY_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "event_id", "event_type",
    "contract_version", "simulation_run_id", "simulation_time", "cycle_sequence",
    "captured_at", "entity_id", "entity_type", "entity_payload_hash",
    "entity_payload_json", "bronze_canonical_hash", "processed_at", "scenario_id",
    "bronze_ingestion_id",
]

RUN_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "event_type", "contract_version",
    "source", "producer_id", "producer_session_id", "simulation_run_id", "started_at",
    "scenario_id", "event_payload_json", "bronze_canonical_hash", "processed_at",
    "bronze_ingestion_id",
]


def test_entity_row_maps_all_fields_and_null_scenario_becomes_empty():
    row = (
        "t1", 0, 5, "raw-1", "evt-1", "TrafficEntityObserved", "2.0.0", "run-1",
        120.5, 3, TS, "entity-1", "VehicleSensor", "hash-1", '{"a":1}', "canon-1",
        TS, None, "bing-1",
    )
    client = FakeClient(responses=[FakeQueryResult([row], ENTITY_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_entity_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, BronzeEntityInputRecord)
    assert rec.scenario_id == ""
    assert rec.event_id == "evt-1"
    assert rec.raw_ingestion_id == "raw-1"
    assert rec.simulation_run_id == "run-1"
    assert rec.simulation_time == 120.5
    assert rec.cycle_sequence == 3
    assert rec.entity_type == "VehicleSensor"
    assert rec.entity_id == "entity-1"
    assert rec.entity_payload_hash == "hash-1"
    assert rec.bronze_canonical_hash == "canon-1"
    assert rec.processed_at == TS
    assert rec.captured_at == TS
    assert receipt.logical_count == 1
    assert receipt.physical_count == 1
    assert receipt.duplicate_count == 0
    assert receipt.first_offset == 5
    assert receipt.last_offset == 5


def test_entity_row_preserves_present_scenario_id():
    row = (
        "t1", 0, 6, "raw-2", "evt-2", "TrafficEntityObserved", "2.0.0", "run-1",
        1.0, 1, TS, "entity-2", "Camera", "hash-2", "{}", "canon-2", TS, "wet", "bing-2",
    )
    client = FakeClient(responses=[FakeQueryResult([row], ENTITY_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_entity_events", "t1", 0)

    records, _ = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert records[0].scenario_id == "wet"


def test_run_row_maps_all_fields():
    row = (
        "t2", 1, 9, "raw-run-1", "TrafficSimulationRunStarted", "2.0.0", "sumo",
        "producer-a", "session-1", "run-9", TS, "normal", '{"b":2}', "canon-run-1",
        TS, "bing-run-1",
    )
    client = FakeClient(responses=[FakeQueryResult([row], RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t2", 1)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert len(records) == 1
    rec = records[0]
    assert isinstance(rec, BronzeRunInputRecord)
    assert rec.scenario_id == "normal"
    assert rec.simulation_run_id == "run-9"
    assert rec.producer_id == "producer-a"
    assert rec.producer_session_id == "session-1"
    assert rec.source == "sumo"
    assert rec.bronze_canonical_hash == "canon-run-1"
    assert rec.started_at == TS
    assert receipt.logical_count == 1


def test_run_row_null_scenario_becomes_empty():
    row = (
        "t2", 1, 10, "raw-run-2", "TrafficSimulationRunStarted", "2.0.0", "sumo",
        "producer-a", "session-1", "run-10", TS, None, "{}", "canon-run-2", TS, "bing-run-2",
    )
    client = FakeClient(responses=[FakeQueryResult([row], RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t2", 1)

    records, _ = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert records[0].scenario_id == ""
