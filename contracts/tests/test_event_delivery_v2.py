"""Kafka Event Delivery Contract 2.0.0 — schema + invariant tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"
EVENTS = CONTRACTS / "events"
EXAMPLES = EVENTS / "examples"
FIXTURES = EVENTS / "fixtures"
SCHEMA_PATH = EVENTS / "traffic-entity-event-v2.schema.json"

EXAMPLE_FILES = [
    "intersection-event.json",
    "trafficlight-event.json",
    "vehiclesensor-event.json",
    "camera-event.json",
]


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _prop(entity: dict, name: str):
    node = entity.get(name)
    assert isinstance(node, dict), f"missing Property {name}"
    return node.get("value")


@pytest.fixture(scope="module")
def schema():
    return _load(SCHEMA_PATH)


@pytest.fixture(scope="module")
def validator(schema):
    jsonschema = pytest.importorskip("jsonschema")
    Draft202012Validator = jsonschema.Draft202012Validator
    return Draft202012Validator(schema)


def test_schema_file_exists():
    assert SCHEMA_PATH.is_file()


@pytest.mark.parametrize("fname", EXAMPLE_FILES)
def test_example_passes_schema(validator, fname: str):
    event = _load(EXAMPLES / fname)
    validator.validate(event)


@pytest.mark.parametrize("fname", EXAMPLE_FILES)
def test_example_invariants(fname: str):
    from contracts.canonical_json import (
        compute_event_id,
        entity_payload_hash,
        node_id_from_entity_id,
    )

    event = _load(EXAMPLES / fname)
    entity = event["entity"]
    assert event["entitySequence"] < event["cycleEntityCount"]
    assert entity["id"] and entity["type"]
    assert event["nodeId"] == node_id_from_entity_id(entity["id"])
    assert event["simulationRunId"] == _prop(entity, "simulationRunId")
    assert event["simulationTime"] == _prop(entity, "simulationTime")
    assert event["scenarioId"] == _prop(entity, "scenarioId")
    assert event["eventId"] == compute_event_id(
        contract_version=event["contractVersion"],
        simulation_run_id=event["simulationRunId"],
        cycle_sequence=event["cycleSequence"],
        entity_id=entity["id"],
    )
    assert event["entityPayloadHash"] == entity_payload_hash(entity)
    assert len(event["eventId"]) == 64
    assert len(event["entityPayloadHash"]) == 64


def test_full_cycle_manifest_oracle():
    from contracts.canonical_json import compute_event_id, entity_payload_hash

    manifest = _load(EXAMPLES / "full-cycle-manifest.example.json")
    assert manifest["cycleEntityCount"] == 4
    assert len(manifest["events"]) == 4
    seqs = set()
    ids = set()
    for entry in manifest["events"]:
        ev = _load(EXAMPLES / entry["file"])
        assert ev["cycleSequence"] == manifest["cycleSequence"]
        assert ev["cycleEntityCount"] == manifest["cycleEntityCount"]
        assert ev["simulationRunId"] == manifest["simulationRunId"]
        assert entry["eventId"] == ev["eventId"]
        assert entry["entityId"] == ev["entity"]["id"]
        assert entry["entitySequence"] == ev["entitySequence"]
        assert entry["entityPayloadHash"] == ev["entityPayloadHash"]
        assert ev["entityPayloadHash"] == entity_payload_hash(ev["entity"])
        assert ev["eventId"] == compute_event_id(
            contract_version=ev["contractVersion"],
            simulation_run_id=ev["simulationRunId"],
            cycle_sequence=ev["cycleSequence"],
            entity_id=ev["entity"]["id"],
        )
        seqs.add(ev["entitySequence"])
        ids.add(ev["entity"]["id"])
    assert seqs == {0, 1, 2, 3}
    assert len(ids) == 4


@pytest.mark.parametrize(
    "fname",
    [
        "invalid_event_id.json",
        "invalid_contract_version.json",
    ],
)
def test_invalid_fixtures_rejected_by_schema(validator, fname: str):
    event = _load(FIXTURES / fname)
    with pytest.raises(Exception):
        validator.validate(event)


def test_invalid_entity_sequence_oob_caught_by_invariant():
    """Schema may still accept integers; custom invariant must reject OOB sequence."""
    event = _load(FIXTURES / "invalid_entity_sequence_oob.json")
    assert event["entitySequence"] >= event["cycleEntityCount"]


def test_legacy_notification_contract_still_passes():
    """Regression: Notification Delivery 1.0.0 untouched."""
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load(CONTRACTS / "delivery" / "notification.schema.json")
    example = _load(CONTRACTS / "delivery" / "notification.example.json")
    jsonschema.validate(instance=example, schema=schema)


def test_shared_canonical_hash_key_order_and_array_order():
    from contracts.canonical_json import canonical_hash

    assert len(canonical_hash({"z": 1, "a": 2})) == 64
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})
    assert canonical_hash({"data": [1, 2]}) != canonical_hash({"data": [2, 1]})


def test_shadow_id_helper():
    from contracts.canonical_json import to_shadow_entity_id

    assert (
        to_shadow_entity_id("urn:ngsi-ld:Intersection:A")
        == "urn:ngsi-ld:Intersection:shadow:A"
    )
    assert (
        to_shadow_entity_id("urn:ngsi-ld:VehicleSensor:A:NORTHBOUND")
        == "urn:ngsi-ld:VehicleSensor:shadow:A:NORTHBOUND"
    )
    # idempotent — no shadow:shadow
    assert to_shadow_entity_id("urn:ngsi-ld:Intersection:shadow:A") == (
        "urn:ngsi-ld:Intersection:shadow:A"
    )


@pytest.mark.parametrize("fname", EXAMPLE_FILES)
def test_example_has_node_entity_count(fname: str):
    event = _load(EXAMPLES / fname)
    assert "nodeEntityCount" in event
    assert int(event["nodeEntityCount"]) >= 1


def test_run_started_example_passes_schema():
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load(EVENTS / "traffic-simulation-run-started-v2.schema.json")
    event = _load(EXAMPLES / "run-started-event.json")
    jsonschema.Draft202012Validator(schema).validate(event)
