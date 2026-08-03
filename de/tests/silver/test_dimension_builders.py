"""Unit tests for de.silver.dimension_builders."""
from __future__ import annotations

import json

from de.silver.contracts import ENTITY_INTERSECTION, ENTITY_VEHICLE_SENSOR, EVENT_RUN_STARTED
from de.silver.dimension_builders import build_dimensions
from de.silver.normalizers import normalize_fields
from de.silver.unwrapper import unwrap_all_fields
from de.tests.silver.conftest import load_payload, make_entity, make_run


def test_approach_dimension_candidate():
    payload = load_payload("VehicleSensor.example.jsonld")
    record = make_entity(
        entity_type=ENTITY_VEHICLE_SENSOR,
        payload_json_str=json.dumps(payload),
        entity_id=payload["id"],
    )
    unwrapped = unwrap_all_fields(ENTITY_VEHICLE_SENSOR, payload)
    normalized = normalize_fields(ENTITY_VEHICLE_SENSOR, unwrapped)
    dims = build_dimensions(ENTITY_VEHICLE_SENSOR, record, normalized)
    assert len(dims) == 1
    cand = dims[0]
    assert cand.target_table == "silver_dim_approach"
    assert cand.business_key == ("A", "N")
    assert len(cand.source_hash) == 64
    assert cand.row.intersection_id == "A"
    assert cand.row.direction == "N"
    assert cand.row.created_at is None


def test_intersection_dimension_candidate():
    payload = load_payload("Intersection.example.jsonld")
    record = make_entity(
        entity_type=ENTITY_INTERSECTION,
        payload_json_str=json.dumps(payload),
        entity_id=payload["id"],
    )
    unwrapped = unwrap_all_fields(ENTITY_INTERSECTION, payload)
    normalized = normalize_fields(ENTITY_INTERSECTION, unwrapped)
    dims = build_dimensions(ENTITY_INTERSECTION, record, normalized)
    assert len(dims) == 1
    cand = dims[0]
    assert cand.target_table == "silver_dim_intersection"
    assert cand.row.intersection_name == "Nguyen Hue - Le Loi"
    assert cand.row.longitude == 106.7009
    assert cand.row.latitude == 10.7769
    assert cand.row.source_hash == cand.source_hash


def test_run_dimensions_two_candidates():
    record = make_run()
    dims = build_dimensions(EVENT_RUN_STARTED, record, {})
    assert len(dims) == 2
    assert dims[0].target_table == "silver_dim_run"
    assert dims[1].target_table == "silver_dim_scenario"
    assert dims[0].row.scenario_id == "normal"
    assert dims[1].row.scenario_id == "normal"
    # no scenario_type / weather
    assert not hasattr(dims[0].row, "scenario_type")
    assert not hasattr(dims[0].row, "weather")
