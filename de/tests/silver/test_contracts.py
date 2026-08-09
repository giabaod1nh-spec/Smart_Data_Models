"""Contract helper unit tests (Plan 1)."""
from __future__ import annotations

from de.silver.contracts import (
    DISPOSITION_PROCESSED,
    DISPOSITION_QUARANTINED,
    ENTITY_CAMERA,
    ENTITY_VEHICLE_SENSOR,
    FORBIDDEN_SILVER_DERIVATIONS,
    MAIN_FACT_TABLES,
    ROUTING_MATRIX,
    normalize_direction,
    parse_urn_id,
    route_entity_type,
    unwrap_geoproperty_coordinates,
    unwrap_property_value,
    unwrap_relationship_object,
)


def test_parse_urn_id():
    assert parse_urn_id("urn:ngsi-ld:Intersection:A") == "A"
    assert parse_urn_id("urn:ngsi-ld:Camera:A") == "A"
    assert parse_urn_id("") == ""
    assert parse_urn_id(None) == ""  # type: ignore[arg-type]


def test_normalize_direction():
    assert normalize_direction("NORTHBOUND") == "N"
    assert normalize_direction("south") == "S"
    assert normalize_direction("E") == "E"
    assert normalize_direction("WESTBOUND") == "W"
    assert normalize_direction("ALL_DIRECTIONS") is None
    assert normalize_direction("") is None


def test_unwrap_property_and_relationship():
    assert unwrap_property_value({"type": "Property", "value": 5}) == 5
    assert unwrap_property_value(
        {"type": "Property", "value": {"@type": "DateTime", "@value": "2026-07-24T07:56:09Z"}}
    ) == "2026-07-24T07:56:09Z"
    assert unwrap_relationship_object(
        {"type": "Relationship", "object": "urn:ngsi-ld:Intersection:A"}
    ) == "urn:ngsi-ld:Intersection:A"
    coords = unwrap_geoproperty_coordinates(
        {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [106.7, 10.7]}}
    )
    assert coords == [106.7, 10.7]


def test_routing_matrix_camera_processed():
    fact, dims, disp = route_entity_type(ENTITY_CAMERA)
    assert fact == "silver_fact_camera_observation"
    assert dims == ()
    assert disp == DISPOSITION_PROCESSED


def test_routing_vehicle_sensor():
    fact, dims, disp = route_entity_type(ENTITY_VEHICLE_SENSOR)
    assert fact == "silver_fact_traffic_observation"
    assert "silver_dim_approach" in dims
    assert disp == DISPOSITION_PROCESSED


def test_unknown_entity_quarantined():
    fact, dims, disp = route_entity_type("UnknownThing")
    assert fact is None
    assert dims == ()
    assert disp == DISPOSITION_QUARANTINED


def test_no_scenario_type_weather_in_routing_targets():
    for entity, (fact, dims, _disp) in ROUTING_MATRIX.items():
        assert fact is None or "scenario_type" not in fact
        assert "weather" not in (fact or "")
    assert FORBIDDEN_SILVER_DERIVATIONS == frozenset({"scenario_type", "weather"})


def test_five_main_facts_including_camera():
    assert "silver_fact_camera_observation" in MAIN_FACT_TABLES
    assert len(MAIN_FACT_TABLES) == 5
