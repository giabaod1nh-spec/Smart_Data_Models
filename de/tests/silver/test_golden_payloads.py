"""Golden payload contract-level tests (Plan 1 — no fact_builders/normalizers)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from de.silver.contracts import (
    ENTITY_CAMERA,
    ENTITY_INTERSECTION,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_VEHICLE_SENSOR,
    ENTITY_PROPERTY_MAP,
    normalize_direction,
    parse_urn_id,
    unwrap_geoproperty_coordinates,
    unwrap_property_value,
    unwrap_relationship_object,
)

REPO = Path(__file__).resolve().parents[3]
PAYLOAD_DIR = REPO / "contracts" / "entity" / "payloads"


def _load(name: str) -> dict:
    return json.loads((PAYLOAD_DIR / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "filename,entity_type",
    [
        ("VehicleSensor.example.jsonld", ENTITY_VEHICLE_SENSOR),
        ("TrafficLight.example.jsonld", ENTITY_TRAFFIC_LIGHT),
        ("Intersection.example.jsonld", ENTITY_INTERSECTION),
        ("Camera.example.jsonld", ENTITY_CAMERA),
    ],
)
def test_golden_payload_files_exist_and_typed(filename: str, entity_type: str):
    payload = _load(filename)
    assert payload["type"] == entity_type
    assert entity_type in ENTITY_PROPERTY_MAP


def test_vehicle_sensor_golden_contract_values():
    p = _load("VehicleSensor.example.jsonld")
    assert p["id"].startswith("urn:ngsi-ld:VehicleSensor:")
    direction = unwrap_property_value(p["trafficDirection"])
    assert normalize_direction(direction) == "N"
    inter = unwrap_relationship_object(p["refIntersection"])
    assert parse_urn_id(inter) == "A"
    assert unwrap_property_value(p["vehicleCount"]) == 5
    assert unwrap_property_value(p["averageSpeed"]) == 18.5
    assert unwrap_property_value(p["queueLength"]) == 12.0
    assert unwrap_property_value(p["pcuEquivalent"]) == 3.2


def test_traffic_light_golden_contract_values():
    p = _load("TrafficLight.example.jsonld")
    inter = unwrap_relationship_object(p["refIntersection"])
    assert parse_urn_id(inter) == "A"
    direction = unwrap_property_value(p["trafficDirection"])
    assert normalize_direction(direction) in {"N", "S", "E", "W"}
    status = str(unwrap_property_value(p["currentStatus"])).upper()
    assert status in {"GREEN", "RED", "YELLOW"}
    assert unwrap_property_value(p["currentPhase"]) is not None


def test_intersection_golden_contract_values():
    p = _load("Intersection.example.jsonld")
    assert parse_urn_id(p["id"]) == "A"
    assert unwrap_property_value(p["name"]) == "Nguyen Hue - Le Loi"
    coords = unwrap_geoproperty_coordinates(p["location"])
    assert coords is not None
    lon, lat = coords
    assert lon == pytest.approx(106.7009)
    assert lat == pytest.approx(10.7769)
    assert unwrap_property_value(p["overallTrafficStatus"]) == "LIGHT"
    assert unwrap_property_value(p["totalVehicleCount"]) == 20
    assert unwrap_property_value(p["currentPhase"]) == "NS_GREEN"
    assert unwrap_property_value(p["hasActiveIncident"]) is False


def test_camera_golden_contract_values():
    p = _load("Camera.example.jsonld")
    assert parse_urn_id(p["id"]) == "A"
    inter = unwrap_relationship_object(p["refIntersection"])
    assert parse_urn_id(inter) == "A"
    assert unwrap_property_value(p["vehicleCount"]) == 20
    assert unwrap_property_value(p["averageSpeed"]) == 18.5
    assert unwrap_property_value(p["occupancyRate"]) == 22.0
    assert unwrap_property_value(p["trafficStatus"]) == "LIGHT"
    assert unwrap_property_value(p["incidentDetected"]) is False
    assert unwrap_property_value(p["confidence"]) == 1.0
    assert unwrap_property_value(p["recommendedSignalAction"]) == "KEEP"
    assert unwrap_property_value(p["incidentType"]) == "NONE"
    assert unwrap_property_value(p["incidentSeverity"]) == "NONE"


def test_golden_payloads_do_not_require_scenario_type_weather_fields():
    """Silver stores scenario_id only; golden payloads must not imply Silver derivation."""
    for name in (
        "VehicleSensor.example.jsonld",
        "TrafficLight.example.jsonld",
        "Intersection.example.jsonld",
        "Camera.example.jsonld",
    ):
        p = _load(name)
        # scenarioId may exist as NGSI property; scenario_type/weather must not be required
        assert "scenario_type" not in p
        assert "weather" not in p
        if "scenarioId" in p:
            assert unwrap_property_value(p["scenarioId"]) == "normal"
