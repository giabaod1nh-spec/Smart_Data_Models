"""Unit tests for de.silver.validators."""
from __future__ import annotations

import json

from de.silver.contracts import ENTITY_VEHICLE_SENSOR
from de.silver.normalizers import normalize_fields
from de.silver.unwrapper import unwrap_all_fields
from de.silver.validators import validate_fields
from de.tests.silver.conftest import load_payload, make_entity, payload_json


def _validate_payload(mutator=None, **overrides):
    payload = load_payload("VehicleSensor.example.jsonld")
    if mutator:
        mutator(payload)
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
        entity_id=payload["id"],
        **overrides,
    )
    unwrapped = unwrap_all_fields(ENTITY_VEHICLE_SENSOR, payload)
    normalized = normalize_fields(ENTITY_VEHICLE_SENSOR, unwrapped)
    return validate_fields(ENTITY_VEHICLE_SENSOR, record, normalized)


def test_valid_vehicle_sensor():
    result = _validate_payload()
    assert result.is_valid


def test_missing_required_domain_field():
    def drop(p):
        del p["vehicleCount"]

    result = _validate_payload(drop)
    assert not result.is_valid
    assert result.error_code == "REQUIRED_DOMAIN_FIELD_MISSING"


def test_invalid_direction_enum():
    def bad_dir(p):
        p["trafficDirection"] = {"type": "Property", "value": "DIAGONAL"}

    result = _validate_payload(bad_dir)
    assert not result.is_valid
    assert result.error_code == "INVALID_DIRECTION_ENUM"


def test_negative_metric():
    def neg(p):
        p["averageSpeed"] = {"type": "Property", "value": -1}

    result = _validate_payload(neg)
    assert not result.is_valid
    assert result.error_code == "INVALID_RANGE_METRIC"


def test_occupancy_over_100():
    def occ(p):
        p["occupancyRate"] = {"type": "Property", "value": 150}

    result = _validate_payload(occ)
    assert not result.is_valid
    assert result.error_code == "INVALID_RANGE_OCCUPANCY"


def test_error_precedence_direction_before_occupancy():
    def both(p):
        p["trafficDirection"] = {"type": "Property", "value": "DIAGONAL"}
        p["occupancyRate"] = {"type": "Property", "value": 150}

    result = _validate_payload(both)
    assert result.error_code == "INVALID_DIRECTION_ENUM"


def test_error_precedence_envelope_before_domain():
    def drop(p):
        del p["vehicleCount"]

    result = _validate_payload(drop, scenario_id="")
    assert result.error_code == "REQUIRED_ENVELOPE_FIELD_MISSING"


def test_malformed_property_wrapper():
    def bad(p):
        p["vehicleCount"] = {"type": "NotProperty", "foo": 1}

    result = _validate_payload(bad)
    assert not result.is_valid
    assert result.error_code == "MALFORMED_PROPERTY_WRAPPER"


def test_missing_envelope_simulation_run_id():
    result = _validate_payload(simulation_run_id="")
    assert result.error_code == "REQUIRED_ENVELOPE_FIELD_MISSING"
