"""Unit tests for de.silver.fact_builders."""
from __future__ import annotations

import json

from de.silver.contracts import ENTITY_VEHICLE_SENSOR
from de.silver.fact_builders import build_traffic_observation, format_quality_flags
from de.silver.normalizers import normalize_fields
from de.silver.unwrapper import unwrap_all_fields
from de.tests.silver.conftest import load_payload, make_entity


def test_format_quality_flags_priority_order():
    flags = {
        "MISSING_TRAFFIC_STATUS",
        "MISSING_WAITING_COUNT",
        "MISSING_ARRIVAL_RATE",
        "ZZ_OTHER",
    }
    assert (
        format_quality_flags(flags)
        == "MISSING_WAITING_COUNT|MISSING_ARRIVAL_RATE|MISSING_TRAFFIC_STATUS|ZZ_OTHER"
    )


def test_traffic_observation_defaults_and_flags():
    payload = load_payload("VehicleSensor.example.jsonld")
    del payload["waitingVehicleCount"]
    del payload["arrivalRatePcuPerSec"]
    del payload["trafficStatus"]
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
        entity_id=payload["id"],
    )
    unwrapped = unwrap_all_fields(ENTITY_VEHICLE_SENSOR, payload)
    normalized = normalize_fields(ENTITY_VEHICLE_SENSOR, unwrapped)
    fact = build_traffic_observation(record, normalized)
    assert fact.waiting_vehicle_count == 0
    assert fact.arrival_rate_pcu_per_sec == 0.0
    assert fact.traffic_status == "UNKNOWN"
    assert fact.quality_status == "VALID_WITH_DEFAULT"
    assert fact.quality_flags == (
        "MISSING_WAITING_COUNT|MISSING_ARRIVAL_RATE|MISSING_TRAFFIC_STATUS"
    )
    assert fact.source_bronze_event_id == record.event_id
    assert fact.source_payload_hash == record.entity_payload_hash
    assert fact.processed_at is None
    assert fact.scenario_id == "normal"
    assert fact.direction == "N"
    assert fact.intersection_id == "A"


def test_traffic_observation_full_lineage():
    payload = load_payload("VehicleSensor.example.jsonld")
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
        entity_id=payload["id"],
        topic="t",
        partition=3,
        offset=99,
    )
    unwrapped = unwrap_all_fields(ENTITY_VEHICLE_SENSOR, payload)
    normalized = normalize_fields(ENTITY_VEHICLE_SENSOR, unwrapped)
    fact = build_traffic_observation(record, normalized)
    assert fact.source_topic == "t"
    assert fact.source_partition == 3
    assert fact.source_offset == 99
    assert fact.source_raw_ingestion_id == record.raw_ingestion_id
