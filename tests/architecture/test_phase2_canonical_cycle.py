"""Phase 2: immutable capture, quality semantics and all-or-nothing V1 output."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from contracts.traffic_observation import validate_observation
from contracts.topology.v1_digest import topology_digest

ROOT = Path(__file__).resolve().parents[2]
VIS = ROOT / "Visualize"
if str(VIS) not in sys.path:
    sys.path.insert(0, str(VIS))

from observation.canonical_cycle import (  # noqa: E402
    CanonicalCycleSnapshot, CanonicalIntersection, build_v1_cycle,
    freeze_run_catalog, generate_v1_cycle,
)
from observation.snapshot_provider import SumoSnapshotProvider  # noqa: E402
from simulation.backend import SumoBackend  # noqa: E402


@pytest.fixture
def setup_cycle(tmp_path):
    catalog = json.loads((VIS / "generated/network_topology_catalog.json").read_text(encoding="utf-8"))
    context = freeze_run_catalog("run-phase2", catalog, tmp_path / "catalog.json")
    assert context.archive_ready
    sources = {}
    for node in ("A", "B", "C", "D", "M"):
        tls = catalog["node_to_tls"][node]
        directions = catalog["approach_edges"][tls]
        metrics = {
            "vehicle_count": 0, "pcu_equivalent": 0.0, "left_count": 0,
            "straight_count": 0, "right_count": 0, "average_speed_kmh": 0.0,
            "waiting_vehicle_count": 0, "queue_length_m": 0.0, "occupancy_pct": 0.0,
        }
        sources[node] = {
            "node_id": node, "simulation_run_id": "run-phase2",
            "simulation_time_sec": 1.23, "scenario": "normal", "control_mode": "FIXED",
            "phase": "NS_GREEN", "incidents": [],
            "directions": {d: copy.deepcopy(metrics) for d in directions},
            "colors": {d: "red" for d in directions},
            "_measurement_quality": {d: {"unavailable_core_metrics": []} for d in directions},
            "internal_only": "not a V1 field",
        }
    return context, sources


def _generate(setup_cycle, **overrides):
    context, sources = setup_cycle
    args = dict(
        context=context, participating_nodes=("A", "B", "C", "D", "M"),
        sources=sources, simulation_time_seconds=1.23456789,
        simulation_step=124, cycle_sequence=7,
    )
    args.update(overrides)
    return generate_v1_cycle(**args)


def test_complete_cycle_is_immutable_and_deterministic(setup_cycle):
    result = _generate(setup_cycle)
    assert result.success and len(result.observations) == 5
    assert {e["intersectionId"] for e in result.observations} == {"A", "B", "C", "D", "M"}
    assert {e["cycleSequence"] for e in result.observations} == {7}
    assert {e["simulationStep"] for e in result.observations} == {124}
    assert {e["simulationTimeSeconds"] for e in result.observations} == {1.23456789}
    assert len({e["capturedAt"] for e in result.observations}) == 1
    assert all("internal_only" not in e["payload"] for e in result.observations)
    for event in result.observations:
        validate_observation(event)
    setup_cycle[1]["A"]["directions"].clear()
    assert build_v1_cycle(result.snapshot) == result.observations


@pytest.mark.parametrize("change,code", [
    ("missing_node", "MISSING_INTERSECTION"),
    ("missing_identity", "MISSING_IDENTITY"),
    ("missing_phase", "MISSING_TLS_STATE"),
    ("missing_metadata", "MISSING_METADATA"),
    ("missing_direction", "MISSING_TOPOLOGY"),
])
def test_structural_failure_emits_no_partial_cycle(setup_cycle, change, code):
    context, sources = setup_cycle
    if change == "missing_node":
        del sources["C"]
    elif change == "missing_identity":
        sources["C"]["node_id"] = "other"
    elif change == "missing_phase":
        sources["C"]["phase"] = ""
    elif change == "missing_metadata":
        sources["C"].pop("scenario")
    else:
        sources["C"]["directions"].clear()
    failed = _generate(setup_cycle)
    assert not failed.success and failed.failure_code == code
    assert failed.observations == () and failed.cycle_sequence == 7
    # The next attempt has a new sequence even when the prior one failed.
    assert _generate(setup_cycle, cycle_sequence=8).cycle_sequence == 8


def test_unavailable_measurement_preserves_v2_source(setup_cycle):
    _context, sources = setup_cycle
    direction = next(iter(sources["B"]["directions"]))
    v2_original = copy.deepcopy(sources["B"]["directions"][direction])
    sources["B"]["_measurement_quality"][direction]["unavailable_core_metrics"] = ["vehicle_count", "occupancy_pct"]
    result = _generate(setup_cycle)
    assert result.success
    event = next(e for e in result.observations if e["intersectionId"] == "B")
    direction_payload = event["payload"]["directions"][direction]
    assert direction_payload["vehicle_count"] is None
    assert direction_payload["occupancy_pct"] is None
    assert direction_payload["metric_availability"] == {
        "vehicle_count": "UNAVAILABLE", "occupancy_pct": "UNAVAILABLE",
    }
    assert sources["B"]["directions"][direction] == v2_original
    other = next(d for d in event["payload"]["directions"] if d != direction)
    assert event["payload"]["directions"][other]["vehicle_count"] == 0


def test_unavailable_optional_sensor_state_is_omitted(setup_cycle):
    _context, sources = setup_cycle
    sources["A"].update({
        "spillback_pressure": 0.0, "downstream_edge_full": False,
        "spillback_detected": False, "intersection_box_blocked": False,
        "vehicles_blocked_at_entry": 0, "yellow_commitment_count": 0,
    })
    sources["A"]["_measurement_quality"]["_top_level"] = {
        "spillback_unavailable": True, "yellow_commitment_unavailable": True,
    }
    result = _generate(setup_cycle)
    assert result.success
    payload = next(e["payload"] for e in result.observations if e["intersectionId"] == "A")
    for field in (
        "spillback_pressure", "downstream_edge_full", "spillback_detected",
        "intersection_box_blocked", "vehicles_blocked_at_entry", "yellow_commitment_count",
    ):
        assert field not in payload
        assert field in sources["A"]  # V2 compatibility source remains numeric.


def test_catalog_archive_and_unsupported_topology(setup_cycle, tmp_path):
    context, sources = setup_cycle
    assert context.topology_digest == topology_digest(context.catalog)
    assert json.loads(context.archive_path.read_bytes()) == context.catalog
    context.archive_path.write_text("{}", encoding="utf-8")
    # A bad archive must be rejected by a new run context, not silently reused.
    denied = context.__class__(context.simulation_run_id, context.catalog_json, context.topology_digest,
                               context.archive_path, False, "archive mismatch")
    assert _generate(setup_cycle, context=denied).failure_code == "CATALOG_NOT_READY"
    catalog = context.catalog
    catalog["node_to_tls_groups"]["A"].append("secondary-tls")
    other = freeze_run_catalog("run-phase2", catalog, tmp_path / "other.json")
    assert _generate(setup_cycle, context=other).failure_code == "UNSUPPORTED_TOPOLOGY"


def test_archive_readback_mismatch_is_not_ready(setup_cycle, tmp_path, monkeypatch):
    catalog = setup_cycle[0].catalog
    archive = tmp_path / "corrupt.json"
    original = Path.read_bytes

    def wrong_read(path):
        return b"{}" if path == archive else original(path)

    monkeypatch.setattr(Path, "read_bytes", wrong_read)
    context = freeze_run_catalog("run-phase2", catalog, archive)
    assert not context.archive_ready
    assert "differs" in context.error
    assert _generate(setup_cycle, context=context).failure_code == "CATALOG_NOT_READY"


def test_missing_archive_is_not_ready(setup_cycle, tmp_path, monkeypatch):
    archive = tmp_path / "missing.json"
    original = Path.read_bytes

    def missing_read(path):
        if path == archive:
            raise FileNotFoundError("archive missing")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", missing_read)
    context = freeze_run_catalog("run-phase2", setup_cycle[0].catalog, archive)
    assert not context.archive_ready
    assert _generate(setup_cycle, context=context).observations == ()


def test_existing_v2_catalog_archive_is_verified_without_rewrite(setup_cycle, tmp_path):
    archive = tmp_path / "existing.json"
    original_bytes = json.dumps(setup_cycle[0].catalog, indent=3, ensure_ascii=False).encode("utf-8")
    archive.write_bytes(original_bytes)
    context = freeze_run_catalog("run-phase2", setup_cycle[0].catalog, archive)
    assert context.archive_ready
    assert archive.read_bytes() == original_bytes


def test_backend_freezes_catalog_for_context_and_capacity():
    backend = SumoBackend(use_gui=False, v1_observations=True)
    frozen = json.loads(backend._v1_runtime_catalog_bytes)
    assert frozen == backend.runtime.capacity._catalog
    links = frozen["inter_node_links"]
    expected_links = ({row.get("id", str(i)): row for i, row in enumerate(links)}
                      if isinstance(links, list) else links)
    assert backend.runtime.coordinator.topology.links == expected_links
    assert backend._v1_participating_nodes == ("A", "B", "C", "D", "M")


def test_backend_post_step_controller_precedes_observation(monkeypatch):
    import configuration.config as cfg

    backend = SumoBackend(use_gui=False, publish_nodes=["A"])
    order = []
    monkeypatch.setattr(cfg, "LIVE_STREAM_ENABLED", False)
    backend._started = True
    backend._traci = SimpleNamespace(
        simulation_step=lambda: order.append("SUMO step"),
        simulation=SimpleNamespace(
            getTime=lambda: 0.01, getArrivedNumber=lambda: 0,
            getMinExpectedNumber=lambda: 1,
        ),
    )
    backend.signals = {"A": SimpleNamespace(
        tick_pending=lambda _traci: None, manual_mode=False,
        update_preemption=lambda _traci: None,
    )}
    backend.runtime.pre_step_actuators = lambda *_a: None
    backend.runtime.context_tick = lambda *_a: None
    backend.trips.on_step = lambda *_a: None
    backend.control_mode = "ADAPTIVE"
    backend.get_snapshot_fresh = lambda _node: order.append("fresh control read") or {}
    backend._rl_controller = SimpleNamespace(
        maybe_tick=lambda b: (order.append("controller"), b.get_snapshot_fresh("A")),
    )
    backend._build_physical_snapshot_dict = lambda _node, **_kw: order.append("publishing capture") or {"node_id": "A"}
    backend._merge_context_into_snapshot = lambda _node, raw: raw
    backend._build_stats_now = lambda: {}
    backend.step()
    assert order == ["SUMO step", "controller", "fresh control read", "publishing capture"]
    assert backend._completed_simulation_steps == 1
    assert backend._observation_seq == 1
    assert backend.last_v1_cycle is None


def test_actual_sumo_fixture_has_valid_complete_cycle():
    fixture = json.loads((ROOT / "phase-2/fixtures/current-sumo-cycle.json").read_text(encoding="utf-8"))
    snapshot = fixture["canonicalSnapshot"]
    assert fixture["observationCount"] == len(fixture["observations"]) == 5
    assert {i["intersectionId"] for i in snapshot["intersections"]} == {"A", "B", "C", "D", "M"}
    assert snapshot["simulationStep"] >= 6000 and snapshot["cycleSequence"] >= 60
    assert snapshot["simulationTimeSeconds"] >= 60
    assert sum(d["vehicle_count"] or 0 for e in fixture["observations"] for d in e["payload"]["directions"].values()) > 0
    assert snapshot["topologyDigest"] == topology_digest(json.loads((VIS / "generated/network_topology_catalog.json").read_text(encoding="utf-8")))
    for event in fixture["observations"]:
        validate_observation(event)
    rebuilt = CanonicalCycleSnapshot(
        snapshot["simulationRunId"], snapshot["simulationTimeSeconds"],
        snapshot["simulationStep"], snapshot["cycleSequence"],
        snapshot["capturedAt"], snapshot["topologyDigest"],
        tuple(CanonicalIntersection(
            item["intersectionId"], json.dumps(item["source"]).encode("utf-8"),
            json.dumps(item["quality"]).encode("utf-8"),
        ) for item in snapshot["intersections"]),
    )
    assert build_v1_cycle(rebuilt) == tuple(fixture["observations"])


def test_provider_quality_sidecar_preserves_legacy_numeric_view(monkeypatch):
    provider = SumoSnapshotProvider(node_id="A")
    direction = provider.directions[0]
    lanes = ["approach_0"]
    monkeypatch.setattr("configuration.config.approach_lane_ids", lambda _tls, _direction: lanes)
    monkeypatch.setattr(provider.detectors, "approach_queue_by_movement", lambda *_a, **_kw: {})
    monkeypatch.setattr(provider.detectors, "approach_occupancy_pct", lambda *_a, **_kw: None)
    monkeypatch.setattr(provider, "_full_link_queue_metrics", lambda *_a, **_kw: {
        "approach_length_m": 100.0, "full_link_halting_count": 0,
        "full_link_jam_length_m": 0.0, "storage_utilization_lane": 0.0,
        "storage_utilization_pcu": 0.0, "nominal_storage_pcu_geometric": 10.0,
    })
    lane = SimpleNamespace(
        getLastStepVehicleIDs=lambda _lane: ["car-1"],
        getLastStepOccupancy=lambda _lane: (_ for _ in ()).throw(RuntimeError("no occupancy")),
    )
    vehicle = SimpleNamespace(getTypeID=lambda _vid: (_ for _ in ()).throw(RuntimeError("vehicle gone")))
    traci = SimpleNamespace(lane=lane, vehicle=vehicle)
    quality = {}
    v1_source = provider._build_direction(traci, direction, "red", quality=quality)
    v2_source = provider._build_direction(traci, direction, "red")
    assert v1_source == v2_source
    assert v2_source["vehicle_count"] == 1 and v2_source["pcu_equivalent"] == 0.0
    assert "vehicle_count" not in quality["unavailable_core_metrics"]
    assert {"pcu_equivalent", "occupancy_pct"} <= set(quality["unavailable_core_metrics"])

    lane.getLastStepVehicleIDs = lambda _lid: ["car-1", "car-2"]
    vehicle.getTypeID = lambda vid: "passenger" if vid == "car-1" else (_ for _ in ()).throw(RuntimeError("car-2 gone"))
    vehicle.getSpeed = lambda _vid: 5.0
    vehicle.getRoute = lambda _vid: []
    vehicle.getRouteIndex = lambda _vid: 0
    partial_quality = {}
    partial = provider._build_direction(traci, direction, "red", quality=partial_quality)
    assert partial["vehicle_count"] == 2 and partial["pcu_equivalent"] > 0
    assert "pcu_equivalent" in partial_quality["unavailable_core_metrics"]


def test_provider_measured_zero_and_failed_lane_are_distinct(monkeypatch):
    provider = SumoSnapshotProvider(node_id="A")
    direction = provider.directions[0]
    monkeypatch.setattr("configuration.config.approach_lane_ids", lambda *_a: ["lane-a"])
    monkeypatch.setattr(provider.detectors, "approach_queue_by_movement", lambda *_a, **_kw: {})
    monkeypatch.setattr(provider.detectors, "approach_occupancy_pct", lambda *_a, **_kw: 0.0)
    monkeypatch.setattr(provider, "_full_link_queue_metrics", lambda *_a, **_kw: {
        "approach_length_m": 100.0, "full_link_halting_count": 0,
        "full_link_jam_length_m": 0.0, "storage_utilization_lane": 0.0,
        "storage_utilization_pcu": 0.0, "nominal_storage_pcu_geometric": 10.0,
    })
    lane = SimpleNamespace(getLastStepVehicleIDs=lambda _lid: [])
    traci = SimpleNamespace(lane=lane, vehicle=SimpleNamespace())
    measured = {}
    result = provider._build_direction(traci, direction, "red", quality=measured)
    assert result["vehicle_count"] == 0 and result["occupancy_pct"] == 0.0
    assert measured["unavailable_core_metrics"] == []
    lane.getLastStepVehicleIDs = lambda _lid: (_ for _ in ()).throw(RuntimeError("lane unavailable"))
    incomplete = {}
    legacy = provider._build_direction(traci, direction, "red", quality=incomplete)
    assert legacy["vehicle_count"] == 0
    assert "vehicle_count" in incomplete["unavailable_core_metrics"]
