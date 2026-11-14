"""Tests for the domain event and local durability boundary."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

from contracts.traffic_observation import (
    TOPIC,
    build_observation,
    observation_event_id,
    validate_observation,
)

_VISUALIZE = Path(__file__).resolve().parents[2] / "Visualize"
if str(_VISUALIZE) not in sys.path:
    sys.path.insert(0, str(_VISUALIZE))

from integration.kafka.durable_publisher import DurableKafkaPublisher  # noqa: E402
from integration.projector.observation_mapper import observation_to_entities  # noqa: E402
from simulation.traci_adapter import TraCIAdapter  # noqa: E402


def _snapshot(node: str = "A") -> dict:
    return {
        "node_id": node,
        "simulation_run_id": "run-1",
        "simulation_time_sec": 12.0,
        "scenario": "normal",
        "control_mode": "FIXED",
        "directions": {
            "North": {
                "vehicle_count": 3,
                "waiting_vehicle_count": 1,
                "queue_length_m": 7.5,
                "average_speed_kmh": 12.0,
            }
        },
        "colors": {"North": "green"},
    }


def test_observation_is_stable_and_validates_metrics() -> None:
    schema_path = Path(__file__).resolve().parents[2] / "contracts" / "events" / "intersection-observation-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert schema["properties"]["eventType"]["const"] == "IntersectionObservation"
    one = build_observation(_snapshot(), cycle_sequence=4, simulation_step=1200)
    two = build_observation(_snapshot(), cycle_sequence=4, simulation_step=1200)
    assert one["eventId"] == two["eventId"] == observation_event_id("run-1", "A", 4)
    assert one["payloadHash"] == two["payloadHash"]
    bad = copy.deepcopy(one)
    bad["payload"]["directions"]["North"]["queue_length_m"] = float("nan")
    with pytest.raises(ValueError):
        validate_observation(bad)


def test_observation_cycle_is_durable_without_broker_ack(tmp_path: Path) -> None:
    publisher = DurableKafkaPublisher(db_path=tmp_path / "outbox.sqlite3", topic=TOPIC)
    try:
        assert publisher.append_observation_cycle(
            [_snapshot("A"), _snapshot("B")], cycle_sequence=4, simulation_step=1200
        ) == 2
        assert publisher.store.get(observation_event_id("run-1", "A", 4)) is not None
        assert publisher.store.get(observation_event_id("run-1", "B", 4)) is not None
        assert publisher.store.capacity_metrics()["outbox_pending_rows"] == 3
    finally:
        publisher.store.close()


def test_existing_entity_cycle_no_longer_waits_for_kafka(tmp_path: Path) -> None:
    publisher = DurableKafkaPublisher(db_path=tmp_path / "entity-outbox.sqlite3")
    entity = {
        "id": "urn:ngsi-ld:Intersection:A",
        "type": "Intersection",
        "simulationRunId": {"type": "Property", "value": "run-1"},
        "simulationTime": {"type": "Property", "value": 12.0},
        "scenarioId": {"type": "Property", "value": "normal"},
    }
    try:
        assert publisher.append_cycle([entity], cycle_sequence=4) == 1
        assert publisher.store.capacity_metrics()["outbox_pending_rows"] == 2
        # The broker was never started and the next cycle still persists.
        assert publisher.append_cycle([entity], cycle_sequence=5) == 1
        assert publisher.store.capacity_metrics()["outbox_pending_rows"] == 3
    finally:
        publisher.store.close()


def test_projection_keeps_stable_logical_ids() -> None:
    from contracts.tests.test_mapper_contract_fields import _snap

    snapshot = _snap()
    snapshot["node_id"] = "A"
    snapshot["control_mode"] = "FIXED"
    event = build_observation(snapshot, cycle_sequence=4, simulation_step=1200)
    entities = observation_to_entities(event)
    assert {e["type"] for e in entities} == {
        "Intersection", "Camera", "TrafficLight", "VehicleSensor"
    }
    assert any(e["id"] == "urn:ngsi-ld:Intersection:A" for e in entities)
    assert all(e["simulationRunId"]["value"] == "run-mapper-test" for e in entities)


def test_adapter_owns_connection_lifecycle() -> None:
    class FakeTraCI:
        def __init__(self):
            self.calls = []

        def start(self, args):
            self.calls.append(("start", args))

        def simulationStep(self):
            self.calls.append(("step",))

        def close(self):
            self.calls.append(("close",))

    fake = FakeTraCI()
    adapter = TraCIAdapter(fake)
    with pytest.raises(RuntimeError):
        adapter.simulation_step()
    adapter.connect(["sumo", "-c", "map.sumocfg"])
    adapter.simulation_step()
    adapter.close()
    assert fake.calls == [
        ("start", ["sumo", "-c", "map.sumocfg"]), ("step",), ("close",)
    ]


def test_adapter_cleans_up_failed_connect() -> None:
    class FailingTraCI:
        closed = False

        def start(self, _args):
            raise ConnectionError("start failed after socket open")

        def close(self):
            self.closed = True

    fake = FailingTraCI()
    adapter = TraCIAdapter(fake)
    with pytest.raises(ConnectionError):
        adapter.connect(["sumo"])
    assert fake.closed and not adapter.connected
