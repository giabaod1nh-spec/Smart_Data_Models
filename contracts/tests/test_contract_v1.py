"""Offline RT-DE contract tests (no Orion / SUMO)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"
PAYLOADS = CONTRACTS / "entity" / "payloads"
DELIVERY = CONTRACTS / "delivery"
TOPO_FIX = CONTRACTS / "topology" / "fixtures"

SIM_FIELDS = ("simulationTime", "simulationRunId", "scenarioId")
ENTITY_FILES = {
    "Intersection": "Intersection.example.jsonld",
    "TrafficLight": "TrafficLight.example.jsonld",
    "Camera": "Camera.example.jsonld",
    "VehicleSensor": "VehicleSensor.example.jsonld",
}


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _prop_value(entity: dict, name: str):
    node = entity.get(name)
    assert isinstance(node, dict), f"missing Property {name}"
    assert node.get("type") == "Property"
    return node.get("value")


def test_version_file():
    assert (CONTRACTS / "VERSION").read_text(encoding="utf-8").strip() == "1.0.0"


@pytest.mark.parametrize("etype,fname", list(ENTITY_FILES.items()))
def test_entity_sim_fields(etype: str, fname: str):
    ent = _load(PAYLOADS / fname)
    assert ent["type"] == etype
    for f in SIM_FIELDS:
        val = _prop_value(ent, f)
        assert val is not None and val != ""


def test_current_phase_on_tl_and_intersection():
    ix = _load(PAYLOADS / "Intersection.example.jsonld")
    tl = _load(PAYLOADS / "TrafficLight.example.jsonld")
    assert _prop_value(ix, "currentPhase") in {
        "NS_GREEN",
        "NS_YELLOW",
        "EW_GREEN",
        "EW_YELLOW",
    }
    assert _prop_value(tl, "currentPhase") == _prop_value(ix, "currentPhase")


def test_topology_fixtures_hash_match():
    catalog = _load(TOPO_FIX / "network_topology_catalog.example.json")
    manifest = _load(TOPO_FIX / "run_manifest.example.json")
    assert catalog.get("topology_hash")
    assert manifest.get("topology_hash") == catalog["topology_hash"]
    assert manifest.get("simulation_run_id")


def test_notification_example_matches_schema():
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load(DELIVERY / "notification.schema.json")
    example = _load(DELIVERY / "notification.example.json")
    jsonschema.validate(instance=example, schema=schema)
    assert example["type"] == "Notification"
    assert len(example["data"]) >= 1
    for ent in example["data"]:
        for f in SIM_FIELDS:
            assert f in ent


def test_notification_example_entities_have_phase_where_required():
    example = _load(DELIVERY / "notification.example.json")
    by_type = {e["type"]: e for e in example["data"]}
    assert "currentPhase" in by_type["TrafficLight"]
    assert "currentPhase" in by_type["Intersection"]


def test_subscription_template_watches_four_types():
    sub = _load(DELIVERY / "subscription_template.json")
    types = {e["type"] for e in sub["entities"]}
    assert types == {"Intersection", "TrafficLight", "VehicleSensor", "Camera"}
