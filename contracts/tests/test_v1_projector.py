"""V1 projector recovery and Orion current-state behavior."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "Visualize"))

from contracts.topology.v1_digest import topology_digest
from contracts.traffic_observation import build_observation, canonical_json
from integration.projector.v1_orion import BatchOutcome, RetryableOrionError, V1Orion
from integration.projector.v1_projection import entity_id, project
from integration.projector.v1_runtime import ProjectionFault, RetryLater, V1Projector
from integration.projector.v1_store import V1Store
from runtime.active_run_registry import ActiveRunRegistry

CATALOG = json.loads((Path(__file__).resolve().parents[2] / "Visualize/generated/network_topology_catalog.json").read_text(encoding="utf-8"))


class FakeOrion:
    def __init__(self):
        self.entities: dict[str, dict] = {}
        self.deleted: list[str] = []
        self.fail_once = False
        self.permanent_once = False
        self.timeout_after_write_once = False
        self.upsert_calls = 0

    def inventory(self):
        return copy.deepcopy(self.entities)

    def get(self, eid):
        return copy.deepcopy(self.entities.get(eid))

    def upsert(self, entities):
        self.upsert_calls += 1
        ids = [e["id"] for e in entities]
        if self.timeout_after_write_once:
            self.timeout_after_write_once = False
            for entity in entities:
                self.entities.setdefault(entity["id"], {}).update(copy.deepcopy(entity))
            raise RetryableOrionError("simulated timeout after Orion applied the batch")
        if self.fail_once:
            self.fail_once = False
            first = entities[0]
            self.entities.setdefault(first["id"], {}).update(copy.deepcopy(first))
            return BatchOutcome(frozenset({first["id"]}), frozenset(ids[1:]), frozenset())
        if self.permanent_once:
            self.permanent_once = False
            first = entities[0]
            self.entities.setdefault(first["id"], {}).update(copy.deepcopy(first))
            return BatchOutcome(frozenset({first["id"]}), frozenset(), frozenset(ids[1:]))
        for entity in entities:
            self.entities.setdefault(entity["id"], {}).update(copy.deepcopy(entity))
        return BatchOutcome(frozenset(ids), frozenset(), frozenset())

    def delete_attribute(self, eid, attr):
        self.entities[eid].pop(attr, None)

    def delete_entity(self, eid):
        self.deleted.append(eid)
        self.entities.pop(eid, None)


def event_for(catalog, node="A", run="run-one", cycle=1, occupancy=0.0, optional=True):
    dirs = catalog["approach_edges"][catalog["node_to_tls"][node]]
    data = {
        "vehicle_count": 0, "pcu_equivalent": 0.0,
        "left_count": 0, "straight_count": 0, "right_count": 0,
        "average_speed_kmh": 0.0, "waiting_vehicle_count": 0,
        "queue_length_m": 0.0, "occupancy_pct": occupancy,
        "density": "LOW",
    }
    if optional:
        data["arrival_rate_pcu_per_sec"] = 0.0
    if occupancy is None:
        data["metric_availability"] = {"occupancy_pct": "UNAVAILABLE"}
    snapshot = {
        "node_id": node, "simulation_run_id": run, "simulation_time_sec": float(cycle),
        "scenario": "normal", "control_mode": "FIXED", "phase": "NS_GREEN",
        "captured_at": f"2026-01-01T00:00:{cycle:02d}Z", "topology_digest": topology_digest(catalog),
        "incidents": [], "colors": {d: "green" for d in dirs},
        "directions": {d: copy.deepcopy(data) for d in dirs},
    }
    return build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 100)


def add_run(runs: Path, catalog, run: str, generation: int, participants: list[str], registry: ActiveRunRegistry, status="RUNNING"):
    folder = runs / run
    folder.mkdir(parents=True)
    (folder / "network_topology_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    manifest = {"simulationRunId": run, "topologyDigest": topology_digest(catalog), "participatingIntersectionIds": participants, "runGeneration": generation, "runStatus": "RUNNING"}
    (folder / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    registry.activate(manifest)
    if status != "RUNNING":
        registry.set_status(run, status)
        manifest["runStatus"] = status
        (folder / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def setup_projector(tmp_path, *, status="RUNNING"):
    runs = tmp_path / "runs"
    registry = ActiveRunRegistry(runs / "active_run.sqlite3")
    add_run(runs, CATALOG, "run-one", 1, ["A", "B", "C", "D", "M"], registry, status=status)
    orion = FakeOrion()
    store = V1Store(tmp_path / "ledger.sqlite3")
    return V1Projector(store=store, orion=orion, runs_dir=runs), orion, registry, runs


def apply(projector, event, offset):
    return projector.process(topic="traffic.observations.v1", partition=0, offset=offset, key=f"{event['simulationRunId']}:{event['intersectionId']}".encode(), value=canonical_json(event).encode())


@pytest.mark.parametrize("status", ["RUNNING", "COMPLETE", "INCOMPLETE", "STOPPED"])
def test_selected_terminal_runs_drain_and_duplicate_is_idempotent(tmp_path, status):
    projector, orion, _, _ = setup_projector(tmp_path, status=status)
    event = event_for(CATALOG)
    assert apply(projector, event, 0) == "APPLIED"
    eid = entity_id("VehicleSensor", "A", "East")
    assert orion.entities[eid]["occupancyRate"]["value"] == 0.0
    assert apply(projector, event, 1) == "APPLIED"
    assert projector.store.watermark("run-one", "A") == 1


def test_metric_unavailable_optional_removal_and_recovery(tmp_path):
    projector, orion, _, _ = setup_projector(tmp_path)
    first = event_for(CATALOG, cycle=1, occupancy=65.5)
    second = event_for(CATALOG, cycle=2, occupancy=None, optional=False)
    third = event_for(CATALOG, cycle=3, occupancy=42.0)
    assert apply(projector, first, 0) == "APPLIED"
    orion.fail_once = True
    with pytest.raises(RetryLater):
        apply(projector, second, 1)
    assert apply(projector, second, 1) == "APPLIED"
    eid = entity_id("VehicleSensor", "A", "East")
    assert "occupancyRate" not in orion.entities[eid]
    assert "arrivalRatePcuPerSec" not in orion.entities[eid]
    assert orion.entities[eid]["metricAvailability"]["value"]["occupancyRate"] == "UNAVAILABLE"
    assert apply(projector, third, 2) == "APPLIED"
    assert orion.entities[eid]["occupancyRate"]["value"] == 42.0
    assert "metricAvailability" not in orion.entities[eid]


def test_new_run_fences_old_and_deletes_obsolete_direction(tmp_path):
    projector, orion, registry, runs = setup_projector(tmp_path)
    old = event_for(CATALOG)
    assert apply(projector, old, 0) == "APPLIED"
    changed = copy.deepcopy(CATALOG)
    tls = changed["node_to_tls"]["A"]
    del changed["approach_edges"][tls]["West"]
    add_run(runs, changed, "run-two", 2, ["A", "B"], registry)
    projector.ensure_reconciled()
    assert entity_id("TrafficLight", "A", "West") in orion.deleted
    assert entity_id("VehicleSensor", "A", "West") in orion.deleted
    assert "totalVehicleCount" not in orion.entities[entity_id("Intersection", "C")]
    assert apply(projector, old, 1) == "FENCED"
    assert projector.store.watermark("run-one", "A") == 1
    assert apply(projector, event_for(changed, run="run-two"), 2) == "APPLIED"
    refs = orion.entities[entity_id("Intersection", "A")]["refTrafficLights"]["object"]
    assert entity_id("TrafficLight", "A", "West") not in refs


def test_orion_207_reports_only_confirmed_entities():
    class Response:
        status_code = 207
        text = ""

        def json(self):
            return {"success": ["a"], "errors": [{"entityId": "b", "error": {"status": 503}}]}

    class Session:
        def request(self, *_args, **_kwargs):
            return Response()

    outcome = V1Orion("http://orion", session=Session()).upsert([{"id": "a"}, {"id": "b"}])
    assert outcome.succeeded == {"a"}
    assert outcome.retryable == {"b"}


def test_permanent_partial_write_holds_offset_until_repair(tmp_path):
    projector, orion, _, _ = setup_projector(tmp_path)
    event = event_for(CATALOG)
    projector.ensure_reconciled()
    orion.permanent_once = True
    with pytest.raises(ProjectionFault):
        apply(projector, event, 0)
    assert projector.store.terminal_outcome("traffic.observations.v1", 0, 0) is None
    assert projector.store.completed_ops(event["eventId"])
    assert apply(projector, event, 0) == "APPLIED"
    assert projector.store.watermark("run-one", "A") == 1


def test_orion_success_before_ledger_write_is_replayed(tmp_path, monkeypatch):
    projector, orion, _, _ = setup_projector(tmp_path)
    event = event_for(CATALOG)
    original = projector.store.mark_op
    raised = False

    def crash_once(event_id, operation):
        nonlocal raised
        if not raised:
            raised = True
            raise OSError("simulated crash after Orion success")
        original(event_id, operation)

    monkeypatch.setattr(projector.store, "mark_op", crash_once)
    with pytest.raises(OSError):
        apply(projector, event, 0)
    assert orion.entities[entity_id("Intersection", "A")]
    assert projector.store.terminal_outcome("traffic.observations.v1", 0, 0) is None
    assert apply(projector, event, 0) == "APPLIED"


def test_timeout_after_orion_write_and_crash_before_offset_commit(tmp_path):
    projector, orion, _, _ = setup_projector(tmp_path)
    projector.ensure_reconciled()
    event = event_for(CATALOG)
    orion.timeout_after_write_once = True
    with pytest.raises(RetryableOrionError):
        apply(projector, event, 0)
    assert projector.store.terminal_outcome("traffic.observations.v1", 0, 0) is None
    assert apply(projector, event, 0) == "APPLIED"
    before = orion.upsert_calls
    restarted = V1Projector(store=V1Store(projector.store.path), orion=orion, runs_dir=projector.archives.runs_dir)
    assert apply(restarted, event, 0) == "APPLIED"
    assert orion.upsert_calls == before


def test_corrupt_selected_manifest_holds_message_for_repair(tmp_path):
    projector, _, _, runs = setup_projector(tmp_path)
    manifest_path = runs / "run-one" / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["participatingIntersectionIds"] = ["UNKNOWN"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ProjectionFault):
        apply(projector, event_for(CATALOG), 0)
    assert projector.store.terminal_outcome("traffic.observations.v1", 0, 0) is None


def test_obsolete_entity_with_external_attribute_faults_reconciliation(tmp_path):
    projector, orion, registry, runs = setup_projector(tmp_path)
    assert apply(projector, event_for(CATALOG), 0) == "APPLIED"
    old_id = entity_id("TrafficLight", "A", "West")
    orion.entities[old_id]["externalOwner"] = {"type": "Property", "value": "keep"}
    changed = copy.deepcopy(CATALOG)
    del changed["approach_edges"][changed["node_to_tls"]["A"]]["West"]
    add_run(runs, changed, "run-two", 2, ["A", "B"], registry)
    with pytest.raises(ProjectionFault, match="ownership ambiguous"):
        projector.ensure_reconciled()
    assert old_id in orion.entities
    assert projector.store.reconciliation(2)["status"] == "FAULTED"


def test_reconciliation_resumes_its_persisted_operation_plan(tmp_path):
    projector, orion, _, runs = setup_projector(tmp_path)
    orion.fail_once = True
    with pytest.raises(RetryLater):
        projector.ensure_reconciled()
    journal = projector.store.reconciliation(1)
    assert journal["status"] == "IN_PROGRESS"
    assert json.loads(journal["completed_ops"])
    restarted = V1Projector(store=V1Store(projector.store.path), orion=orion, runs_dir=runs)
    restarted.ensure_reconciled()
    journal = restarted.store.reconciliation(1)
    assert journal["status"] == "READY"
    assert len(json.loads(journal["plan"])) == len(json.loads(journal["completed_ops"]))


def test_unknown_same_namespace_entity_is_not_deleted_during_first_cutover(tmp_path):
    projector, orion, _, runs = setup_projector(tmp_path)
    unknown = "urn:ngsi-ld:TrafficLight:A-Unknown"
    orion.entities[unknown] = {"id": unknown, "type": "TrafficLight"}
    with pytest.raises(ProjectionFault, match="ownership ambiguous"):
        projector.ensure_reconciled()
    assert unknown in orion.entities
    assert projector.store.reconciliation(1)["status"] == "FAULTED"
    del orion.entities[unknown]
    restarted = V1Projector(store=V1Store(projector.store.path), orion=orion, runs_dir=runs)
    restarted.ensure_reconciled()
    assert restarted.store.reconciliation(1)["status"] == "READY"


def test_historical_replay_does_not_change_live_selection(tmp_path):
    live, _, registry, runs = setup_projector(tmp_path)
    add_run(runs, CATALOG, "run-two", 2, ["A", "B"], registry)
    isolated = FakeOrion()
    replay = V1Projector(store=V1Store(tmp_path / "replay.sqlite3"), orion=isolated, runs_dir=runs, replay_run="run-one")
    assert apply(replay, event_for(CATALOG), 0) == "APPLIED"
    assert apply(replay, event_for(CATALOG, run="run-two"), 1) == "FENCED"
    assert registry.selected()["run_id"] == "run-two"
    assert entity_id("Intersection", "A") not in live.orion.entities
