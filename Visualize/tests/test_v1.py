"""
Minimal unit tests for SUMO TraCI backend v1 (mock TraCI — no SUMO binary required).
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

VISUALIZE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(VISUALIZE))
sys.path.insert(0, str(VISUALIZE.parent / "simulator"))

import config as cfg
from sumo_signal_controller import SumoSignalController
from sumo_snapshot_provider import (
    SumoSnapshotProvider,
    pcu_for_vtype,
    get_unknown_vtypes,
)
from sumo_backend import SumoBackend


# ── helpers ─────────────────────────────────────────────────────────

TOP_LEVEL_KEYS = {
    "node_id", "directions", "phase", "next_phase", "phase_remaining",
    "phase_duration", "colors", "scenario", "blocked_direction", "incidents",
    "simulation_time_sec", "downstream_edge_full", "spillback_pressure",
    "spillback_detected", "intersection_box_blocked", "vehicles_blocked_at_entry",
    "yellow_commitment_count", "preemption_active",
}

DIRECTION_KEYS = {
    "vehicle_count", "pcu_equivalent", "left_count", "straight_count", "right_count",
    "average_speed_kmh", "waiting_vehicle_count", "queue_length_m", "queue_by_movement",
    "occupancy_pct", "density", "arrival_rate_pcu_per_sec", "waiting_reason_counts",
    "dominant_waiting_reason", "theoretical_speed_kmh", "moto_front_pct",
    "vehicle_class_composition",
}


def make_mock_traci(
    phase_idx: int = 0,
    sim_t: float = 10.0,
    vehicles_by_lane: Dict[str, List[str]] | None = None,
    vehicle_meta: Dict[str, dict] | None = None,
):
    """Build a lightweight TraCI mock for J1 approaches."""
    vehicles_by_lane = vehicles_by_lane or {}
    vehicle_meta = vehicle_meta or {}

    traci = MagicMock()
    traci.simulation.getTime.return_value = sim_t
    traci.simulation.getMinExpectedNumber.return_value = 1
    traci.trafficlight.getPhase.return_value = phase_idx
    traci.trafficlight.getNextSwitch.return_value = sim_t + 20.0

    phase0 = SimpleNamespace(duration=42.0, state="GGGggrrrrrGGGggrrrrr", minDur=42, maxDur=42, next=(), name="")
    phase1 = SimpleNamespace(duration=3.0, state="yyyyyrrrrryyyyyrrrrr", minDur=3, maxDur=3, next=(), name="")
    phase2 = SimpleNamespace(duration=42.0, state="rrrrrGGGggrrrrrGGGgg", minDur=42, maxDur=42, next=(), name="")
    phase3 = SimpleNamespace(duration=3.0, state="rrrrryyyyyrrrrryyyyy", minDur=3, maxDur=3, next=(), name="")
    logic = SimpleNamespace(phases=[phase0, phase1, phase2, phase3], programID="0")
    traci.trafficlight.getAllProgramLogics.return_value = [logic]

    def lane_vehicle_ids(lane_id):
        return list(vehicles_by_lane.get(lane_id, []))

    def lane_occupancy(lane_id):
        return 0.25 if vehicles_by_lane.get(lane_id) else 0.0

    def lane_length(lane_id):
        return 100.0

    traci.lane.getLastStepVehicleIDs.side_effect = lane_vehicle_ids
    traci.lane.getLastStepOccupancy.side_effect = lane_occupancy
    traci.lane.getLength.side_effect = lane_length

    def get_type(vid):
        return vehicle_meta.get(vid, {}).get("type", "car")

    def get_speed(vid):
        return vehicle_meta.get(vid, {}).get("speed", 5.0)

    def get_length(vid):
        return vehicle_meta.get(vid, {}).get("length", 4.5)

    def get_lane(vid):
        return vehicle_meta.get(vid, {}).get("lane", "J3J1_0")

    def get_lane_pos(vid):
        return vehicle_meta.get(vid, {}).get("pos", 90.0)

    def get_route(vid):
        return vehicle_meta.get(vid, {}).get("route", ["J3J1", "J1S1"])

    def get_route_index(vid):
        return vehicle_meta.get(vid, {}).get("route_index", 0)

    traci.vehicle.getTypeID.side_effect = get_type
    traci.vehicle.getSpeed.side_effect = get_speed
    traci.vehicle.getLength.side_effect = get_length
    traci.vehicle.getLaneID.side_effect = get_lane
    traci.vehicle.getLanePosition.side_effect = get_lane_pos
    traci.vehicle.getRoute.side_effect = get_route
    traci.vehicle.getRouteIndex.side_effect = get_route_index
    traci.vehicle.getIDList.return_value = list(vehicle_meta.keys())

    return traci


# ── 1. Phase mapping ────────────────────────────────────────────────

def test_phase_index_mapping():
    assert cfg.PHASE_INDEX_TO_NAME[0] == "NS_GREEN"
    assert cfg.PHASE_INDEX_TO_NAME[1] == "NS_YELLOW"
    assert cfg.PHASE_INDEX_TO_NAME[2] == "EW_GREEN"
    assert cfg.PHASE_INDEX_TO_NAME[3] == "EW_YELLOW"
    assert cfg.PHASE_NAME_TO_INDEX["NS_GREEN"] == 0


def test_signal_controller_reads_phase_and_colors():
    traci = make_mock_traci(phase_idx=0)
    sc = SumoSignalController("J1")
    assert sc.current_phase_name(traci) == "NS_GREEN"
    assert sc.next_phase_name(traci) == "NS_YELLOW"
    colors = sc.colors(traci)
    assert colors["North"] == "green"
    assert colors["East"] == "red"
    assert sc.phase_remaining_seconds(traci) == 20


def test_force_phase_cross_axis_uses_yellow():
    traci = make_mock_traci(phase_idx=0)  # NS_GREEN
    sc = SumoSignalController("J1")
    sc.force_phase(traci, "EW_GREEN")
    # Should set yellow first
    traci.trafficlight.setPhase.assert_called_with("J1", 1)  # NS_YELLOW
    assert sc._pending_target == "EW_GREEN"


# ── 2. Direction mapping ────────────────────────────────────────────

def test_direction_approach_edges_j1():
    assert cfg.APPROACH_EDGES["J1"]["North"] == "J3J1"
    assert cfg.APPROACH_EDGES["J1"]["East"] == "J2J1"
    assert cfg.APPROACH_EDGES["J1"]["South"] == "S1J1"
    assert cfg.APPROACH_EDGES["J1"]["West"] == "W1J1"
    assert cfg.NODE_TO_TLS["A"] == "J1"
    assert cfg.approach_lane_ids("J1", "North") == ["J3J1_0", "J3J1_1"]


# ── 3. PCU calculation ──────────────────────────────────────────────

@pytest.mark.parametrize(
    "vtype,expected",
    [
        ("motorcycle", 0.24),
        ("car", 1.00),
        ("bus", 2.50),
        ("truck", 2.50),
        ("container", 2.50),
        ("ambulance", 1.00),
        ("police", 1.00),
        ("firetruck", 2.50),
    ],
)
def test_pcu_for_each_vtype(vtype, expected):
    assert pcu_for_vtype(vtype) == expected


def test_pcu_unknown_uses_fallback_and_warns(caplog):
    get_unknown_vtypes().discard("hoverboard")
    with caplog.at_level("WARNING"):
        val = pcu_for_vtype("hoverboard")
    assert val == cfg.PCU_FALLBACK
    assert "hoverboard" in get_unknown_vtypes()


def test_pcu_aggregate_in_snapshot():
    vehicles_by_lane = {
        "J3J1_0": ["m1", "c1"],
        "J3J1_1": ["b1"],
    }
    meta = {
        "m1": {"type": "motorcycle", "speed": 0.0, "lane": "J3J1_0", "pos": 95, "route": ["J3J1", "J1S1"]},
        "c1": {"type": "car", "speed": 0.0, "lane": "J3J1_0", "pos": 90, "route": ["J3J1", "J1S1"]},
        "b1": {"type": "bus", "speed": 2.0, "lane": "J3J1_1", "pos": 80, "route": ["J3J1", "J1J2"]},
    }
    traci = make_mock_traci(vehicles_by_lane=vehicles_by_lane, vehicle_meta=meta)
    sc = SumoSignalController("J1")
    from sumo_scenario_manager import SumoScenarioManager
    sm = SumoScenarioManager("J1")
    sp = SumoSnapshotProvider("J1", "A")
    snap = sp.build_snapshot(traci, sc, sm)
    north = snap["directions"]["North"]
    # 0.24 + 1.0 + 2.5 = 3.74
    assert north["pcu_equivalent"] == pytest.approx(3.74)
    assert north["vehicle_count"] == 3


# ── 4–5. Snapshot keys ──────────────────────────────────────────────

def test_snapshot_top_level_and_direction_keys():
    traci = make_mock_traci()
    sc = SumoSignalController("J1")
    from sumo_scenario_manager import SumoScenarioManager
    sm = SumoScenarioManager("J1")
    sp = SumoSnapshotProvider("J1", "A")
    snap = sp.build_snapshot(traci, sc, sm)
    assert TOP_LEVEL_KEYS.issubset(snap.keys())
    assert snap["node_id"] == "A"
    for d in cfg.DIRECTIONS:
        assert d in snap["directions"]
        assert DIRECTION_KEYS.issubset(snap["directions"][d].keys())
        assert "straight" in snap["directions"][d]["queue_by_movement"]


# ── 6–8. Entity generator ───────────────────────────────────────────

def test_build_all_entities_ten_with_urn_a():
    cfg.ensure_simulator_on_path()
    from entity_generator import build_all_entities

    traci = make_mock_traci(phase_idx=2)
    sc = SumoSignalController("J1")
    from sumo_scenario_manager import SumoScenarioManager
    sm = SumoScenarioManager("J1")
    sp = SumoSnapshotProvider("J1", "A")
    snap = sp.build_snapshot(traci, sc, sm)

    entities = build_all_entities("A", snap)
    assert len(entities) == 10
    types = [e["type"] for e in entities]
    assert types.count("Intersection") == 1
    assert types.count("Camera") == 1
    assert types.count("TrafficLight") == 4
    assert types.count("VehicleSensor") == 4

    ids = [e["id"] for e in entities]
    assert "urn:ngsi-ld:Intersection:A" in ids
    assert "urn:ngsi-ld:Camera:A" in ids
    assert any("J1" in i for i in ids) is False
    for e in entities:
        assert "@context" in e
        assert e["id"].startswith("urn:ngsi-ld:")


def test_city_network_snapshot_still_builds_entities():
    """Backward compatibility: 1D engine snapshot shape still works."""
    cfg.ensure_simulator_on_path()
    from entity_generator import build_all_entities
    from traffic_engine import CityNetworkEngine

    engine = CityNetworkEngine()
    snap = engine.get_snapshot("A")
    entities = build_all_entities("A", snap)
    assert len(entities) == 10
    assert entities[0]["id"] == "urn:ngsi-ld:Intersection:A"


# ── 9. Orion error does not crash publish helper ────────────────────

def test_orion_error_does_not_crash_runner_publish():
    from traci_runner import publish_once

    backend = MagicMock()
    backend.publish_node = "A"
    backend.get_snapshot.return_value = {
        "node_id": "A",
        "directions": {
            d: {
                "vehicle_count": 0, "pcu_equivalent": 0.0,
                "left_count": 0, "straight_count": 0, "right_count": 0,
                "average_speed_kmh": 0.0, "waiting_vehicle_count": 0,
                "queue_length_m": 0.0,
                "queue_by_movement": {"straight": 0.0, "left": 0.0, "right": 0.0},
                "occupancy_pct": 0.0, "density": "LOW",
                "arrival_rate_pcu_per_sec": 0.0,
                "waiting_reason_counts": {"RED_PHASE": 0, "CONGESTION": 0},
                "dominant_waiting_reason": None,
                "theoretical_speed_kmh": 50.0, "moto_front_pct": 0.0,
            }
            for d in cfg.DIRECTIONS
        },
        "phase": "NS_GREEN", "next_phase": "NS_YELLOW",
        "phase_remaining": 10, "phase_duration": 42,
        "colors": cfg.PHASE_COLORS["NS_GREEN"],
        "scenario": "normal", "blocked_direction": None, "incidents": [],
        "simulation_time_sec": 1.0,
        "downstream_edge_full": False, "spillback_pressure": 0.0,
        "spillback_detected": False, "intersection_box_blocked": False,
        "vehicles_blocked_at_entry": 0, "yellow_commitment_count": 0,
        "preemption_active": False,
    }

    def boom(_entity):
        raise ConnectionError("Orion down")

    cfg.ensure_simulator_on_path()
    from entity_generator import build_all_entities

    # Should not raise
    n = publish_once(backend, boom, build_all_entities)
    assert n == 0


# ── 10. Shutdown always closes TraCI ────────────────────────────────

def test_backend_stop_closes_traci():
    backend = SumoBackend.__new__(SumoBackend)
    mock_traci = MagicMock()
    backend._traci = mock_traci
    backend._started = True
    backend.stop()
    mock_traci.close.assert_called_once()
    # second stop safe
    backend.stop()
    assert backend._traci is None


def test_sumo_config_path_exists():
    assert cfg.DEFAULT_SUMO_CONFIG.is_file(), f"Missing {cfg.DEFAULT_SUMO_CONFIG}"


def test_resolve_sumo_binary_raises_without_sumo(monkeypatch):
    monkeypatch.delenv("SUMO_HOME", raising=False)
    monkeypatch.setattr("shutil.which", lambda n: None)
    with pytest.raises(RuntimeError, match="SUMO binary not found"):
        cfg.resolve_sumo_binary(use_gui=False)
