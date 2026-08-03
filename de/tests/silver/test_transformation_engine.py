"""End-to-end TransformationEngine unit tests (Plan 2)."""
from __future__ import annotations

import json
from dataclasses import asdict

from de.silver.contracts import (
    DISPOSITION_PROCESSED,
    DISPOSITION_QUARANTINED,
    ENTITY_CAMERA,
    ENTITY_INTERSECTION,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_VEHICLE_SENSOR,
)
from de.silver.models import (
    SilverCameraObservationFact,
    SilverIntersectionStateFact,
    SilverObservationFact,
    SilverRunEventFact,
    SilverSignalStateFact,
)
from de.tests.silver.conftest import make_entity, make_run, payload_json


def test_vehicle_sensor_cardinality(engine):
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_name="VehicleSensor.example.jsonld",
        entity_id="urn:ngsi-ld:VehicleSensor:A:NORTHBOUND",
    )
    result = engine.transform(record)
    assert result.proposed_disposition == DISPOSITION_PROCESSED
    assert len(result.facts) == 1
    assert isinstance(result.facts[0], SilverObservationFact)
    assert len(result.dimensions) == 1
    assert result.dimensions[0].target_table == "silver_dim_approach"
    assert result.quarantine is None


def test_traffic_light_cardinality(engine):
    record = make_entity(
        entity_type=ENTITY_TRAFFIC_LIGHT,
        payload_name="TrafficLight.example.jsonld",
        entity_id="urn:ngsi-ld:TrafficLight:A-North",
    )
    result = engine.transform(record)
    assert result.proposed_disposition == DISPOSITION_PROCESSED
    assert len(result.facts) == 1
    assert isinstance(result.facts[0], SilverSignalStateFact)
    assert result.dimensions == ()


def test_intersection_cardinality(engine):
    record = make_entity(
        entity_type=ENTITY_INTERSECTION,
        payload_name="Intersection.example.jsonld",
        entity_id="urn:ngsi-ld:Intersection:A",
    )
    result = engine.transform(record)
    assert len(result.facts) == 1
    assert isinstance(result.facts[0], SilverIntersectionStateFact)
    assert len(result.dimensions) == 1
    assert result.dimensions[0].target_table == "silver_dim_intersection"


def test_camera_cardinality(engine):
    record = make_entity(
        entity_type=ENTITY_CAMERA,
        payload_name="Camera.example.jsonld",
        entity_id="urn:ngsi-ld:Camera:A",
    )
    result = engine.transform(record)
    assert len(result.facts) == 1
    assert isinstance(result.facts[0], SilverCameraObservationFact)
    assert result.dimensions == ()
    assert result.proposal.primary_target_table == "silver_fact_camera_observation"


def test_run_started_cardinality(engine):
    result = engine.transform(make_run())
    assert len(result.facts) == 1
    assert isinstance(result.facts[0], SilverRunEventFact)
    assert len(result.dimensions) == 2
    assert result.facts[0].source_bronze_event_id == make_run().bronze_canonical_hash
    assert result.facts[0].event_simulation_time == 0.0
    assert result.facts[0].processed_at is None


def test_unknown_entity_quarantine(engine):
    record = make_entity(
        entity_type="UnknownSensor",
        payload_json_str='{"type":"UnknownSensor","id":"x"}',
        entity_id="urn:x",
    )
    result = engine.transform(record)
    assert result.proposed_disposition == DISPOSITION_QUARANTINED
    assert result.facts == ()
    assert result.dimensions == ()
    assert result.quarantine is not None
    assert result.quarantine.error_code == "UNKNOWN_ENTITY_TYPE"
    assert result.quarantine.failure_stage == "CLASSIFY"


def test_invalid_json_quarantine(engine):
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str="{bad",
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "INVALID_JSON_PAYLOAD"
    assert result.quarantine.failure_stage == "PARSE"


def test_missing_required_envelope(engine):
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_name="VehicleSensor.example.jsonld",
        scenario_id="",
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "REQUIRED_ENVELOPE_FIELD_MISSING"
    assert result.facts == ()


def test_missing_required_domain(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    del payload["vehicleCount"]
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "REQUIRED_DOMAIN_FIELD_MISSING"


def test_invalid_direction(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["trafficDirection"] = {"type": "Property", "value": "DIAGONAL"}
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "INVALID_DIRECTION_ENUM"


def test_negative_metric(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["queueLength"] = {"type": "Property", "value": -5}
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "INVALID_RANGE_METRIC"


def test_occupancy_gt_100(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["occupancyRate"] = {"type": "Property", "value": 101}
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "INVALID_RANGE_OCCUPANCY"


def test_optional_missing_defaults(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    del payload["waitingVehicleCount"]
    del payload["arrivalRatePcuPerSec"]
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    fact = result.facts[0]
    assert fact.waiting_vehicle_count == 0
    assert fact.arrival_rate_pcu_per_sec == 0.0
    assert "MISSING_WAITING_COUNT" in fact.quality_flags
    assert "MISSING_ARRIVAL_RATE" in fact.quality_flags
    assert fact.quality_status == "VALID_WITH_DEFAULT"


def test_bool_not_integer_for_count(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["vehicleCount"] = {"type": "Property", "value": True}
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.proposed_disposition == DISPOSITION_QUARANTINED
    assert result.quarantine.error_code == "INVALID_RANGE_METRIC"


def test_nan_rejected(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["averageSpeed"] = {"type": "Property", "value": float("nan")}
    # JSON can't encode nan by default — inject via Python object then dump with allow_nan
    raw = json.dumps(payload, allow_nan=True)
    record = make_entity(entity_type=ENTITY_VEHICLE_SENSOR, payload_json_str=raw)
    result = engine.transform(record)
    assert result.proposed_disposition == DISPOSITION_QUARANTINED


def test_malformed_ngsi_wrapper(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["vehicleCount"] = {"foo": "bar"}
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "MALFORMED_PROPERTY_WRAPPER"
    assert result.facts == ()


def test_builder_not_run_when_invalid(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    del payload["refIntersection"]
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
    )
    result = engine.transform(record)
    assert result.facts == ()
    assert result.dimensions == ()
    assert result.quarantine is not None


def test_full_lineage_preserved(engine):
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_name="VehicleSensor.example.jsonld",
        topic="topic-x",
        partition=7,
        offset=42,
        event_id="e" * 64,
        entity_payload_hash="h" * 64,
        raw_ingestion_id="r" * 64,
    )
    fact = engine.transform(record).facts[0]
    assert fact.source_topic == "topic-x"
    assert fact.source_partition == 7
    assert fact.source_offset == 42
    assert fact.source_bronze_event_id == "e" * 64
    assert fact.source_payload_hash == "h" * 64
    assert fact.source_raw_ingestion_id == "r" * 64


def test_determinism_same_input(engine):
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_name="VehicleSensor.example.jsonld",
    )
    a = engine.transform(record)
    b = engine.transform(record)
    assert asdict(a.facts[0]) == asdict(b.facts[0])
    assert a.dimensions[0].source_hash == b.dimensions[0].source_hash
    assert a.proposed_disposition == b.proposed_disposition
    assert a.quality_flags == b.quality_flags


def test_quarantine_id_deterministic(engine):
    record = make_entity(
        entity_type="Nope",
        payload_json_str='{"type":"Nope"}',
    )
    a = engine.transform(record)
    b = engine.transform(record)
    assert a.quarantine.silver_quarantine_id == b.quarantine.silver_quarantine_id
    assert len(a.quarantine.silver_quarantine_id) == 64


def test_error_precedence_in_engine(engine):
    payload = json.loads(payload_json("VehicleSensor.example.jsonld"))
    payload["trafficDirection"] = {"type": "Property", "value": "DIAGONAL"}
    payload["occupancyRate"] = {"type": "Property", "value": 200}
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
        scenario_id="",
    )
    result = engine.transform(record)
    assert result.quarantine.error_code == "REQUIRED_ENVELOPE_FIELD_MISSING"


def test_no_scenario_type_weather_on_run(engine):
    result = engine.transform(make_run())
    run_dim = result.dimensions[0].row
    assert not hasattr(run_dim, "scenario_type")
    assert not hasattr(run_dim, "weather")
