"""
Unit tests for Visualize SUMO backend (mock TraCI — no SUMO binary required for most).
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List
from unittest.mock import MagicMock

import pytest

VISUALIZE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(VISUALIZE))

import configuration.config as cfg
from integration.orion.entity_mapper import build_all_entities
from simulation.signal_controller import SumoSignalController
from observation.snapshot_provider import (
    SumoSnapshotProvider,
    pcu_for_vtype,
    get_unknown_vtypes,
)
from simulation.backend import SumoBackend
from runtime.command_queue import CommandQueue
from configuration.demand_profiles import VEHICLE_CLASS_WEIGHTS, split_flow


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
    # Phase 3.8 additive queue/storage fidelity
    "queue_length_vehicles", "approach_length_m",
    "full_link_halting_count", "full_link_jam_length_m",
    "storage_utilization_lane", "storage_utilization_pcu",
    "nominal_storage_pcu_geometric", "estimated_density_veh_per_km",
}

FORBIDDEN_ATTRS = {
    "status", "color", "mode", "direction", "snapshotTime", "observedAt",
}


def make_mock_traci(phase_idx: int = 0, sim_t: float = 10.0,
                    vehicles_by_lane: Dict[str, List[str]] | None = None,
                    vehicle_meta: Dict[str, dict] | None = None):
    vehicles_by_lane = vehicles_by_lane or {}
    vehicle_meta = vehicle_meta or {}
    traci = MagicMock()
    traci.simulation.getTime.return_value = sim_t
    traci.simulation.getMinExpectedNumber.return_value = 1
    traci.simulation.getDepartedIDList.return_value = []
    traci.simulation.getArrivedIDList.return_value = []
    traci.simulation.getArrivedNumber.return_value = 0
    traci.trafficlight.getPhase.return_value = phase_idx
    traci.trafficlight.getNextSwitch.return_value = sim_t + 20.0
    phase0 = SimpleNamespace(duration=42.0, state="GGGGgrrrrrGGGGgrrrrr", minDur=42, maxDur=42)
    phase1 = SimpleNamespace(duration=3.0, state="yyyyyrrrrryyyyyrrrrr", minDur=3, maxDur=3)
    phase2 = SimpleNamespace(duration=42.0, state="rrrrrGGGGgrrrrrGGGGg", minDur=42, maxDur=42)
    phase3 = SimpleNamespace(duration=3.0, state="rrrrryyyyyrrrrryyyyy", minDur=3, maxDur=3)
    logic = SimpleNamespace(phases=[phase0, phase1, phase2, phase3], programID="0")
    traci.trafficlight.getAllProgramLogics.return_value = [logic]
    traci.lanearea.getIDList.return_value = []

    def lane_vehicle_ids(lane_id):
        return list(vehicles_by_lane.get(lane_id, []))

    traci.lane.getLastStepVehicleIDs.side_effect = lane_vehicle_ids
    traci.lane.getLastStepOccupancy.return_value = 0.1
    traci.lane.getLength.return_value = 100.0

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
    traci.vehicle.getAccumulatedWaitingTime.return_value = 0.0
    return traci


def _sample_snapshot(phase: str = "NS_GREEN") -> dict:
    return {
        "node_id": "A",
        "directions": {
            d: {
                "vehicle_count": 2, "pcu_equivalent": 1.24,
                "left_count": 0, "straight_count": 2, "right_count": 0,
                "average_speed_kmh": 12.0, "waiting_vehicle_count": 1,
                "queue_length_m": 8.0,
                "queue_by_movement": {"straight": 8.0, "left": 0.0, "right": 0.0},
                "occupancy_pct": 10.0, "density": "LOW",
                "arrival_rate_pcu_per_sec": 0.1,
                "waiting_reason_counts": {"RED_PHASE": 1, "CONGESTION": 0},
                "dominant_waiting_reason": "RED_PHASE",
                "theoretical_speed_kmh": 45.0, "moto_front_pct": 0.5,
                "vehicle_class_composition": {"motorcycle": 0.5, "car": 0.5},
            }
            for d in cfg.DIRECTIONS
        },
        "phase": phase, "next_phase": "NS_YELLOW",
        "phase_remaining": 10, "phase_duration": 42,
        "green_duration": 42, "yellow_duration": 3, "red_duration": 42,
        "colors": cfg.PHASE_COLORS[phase],
        "scenario": "normal", "blocked_direction": None, "incidents": [],
        "simulation_time_sec": 1.0,
        "downstream_edge_full": False, "spillback_pressure": 0.0,
        "spillback_detected": False, "intersection_box_blocked": False,
        "vehicles_blocked_at_entry": 0, "yellow_commitment_count": 0,
        "preemption_active": False,
    }


def test_phase_index_mapping():
    assert cfg.PHASE_INDEX_TO_NAME[0] == "NS_GREEN"
    assert cfg.PHASE_NAME_TO_INDEX["EW_YELLOW"] == 3


def test_three_lane_convention():
    assert cfg.LANES_PER_APPROACH == 3
    assert cfg.approach_lane_ids("J1", "North") == ["J3J1_0", "J3J1_1", "J3J1_2"]
    assert cfg.MOVEMENT_BY_LANE_INDEX[0] == "right"
    assert cfg.MOVEMENT_BY_LANE_INDEX[2] == "left"


def test_multi_node_topology_config():
    assert cfg.NODE_TO_TLS == {"A": "J1", "B": "J2", "C": "J3", "D": "J4"}
    for tls in ("J1", "J2", "J3", "J4"):
        assert set(cfg.APPROACH_EDGES[tls]) == set(cfg.DIRECTIONS)


def test_detector_naming():
    assert cfg.detector_id_e1("J1", "North", 0) == "E1_J1_N_0"
    assert cfg.detector_id_e2("J1", "East", 2) == "E2_J1_E_2"
    assert cfg.detector_id_e2_out("J1", "J1J2", 1) == "E2OUT_J1_J1J2_1"


def test_demand_weights_experimental():
    assert abs(sum(VEHICLE_CLASS_WEIGHTS.values()) - 1.0) < 1e-6
    assert VEHICLE_CLASS_WEIGHTS["motorcycle"] == 0.85
    flow = split_flow(160)
    assert flow["motorcycle"] >= flow["car"]


def test_pcu_table():
    assert pcu_for_vtype("motorcycle") == 0.24
    assert pcu_for_vtype("firetruck") == 2.5


def test_signal_and_snapshot_keys():
    traci = make_mock_traci(phase_idx=0)
    sc = SumoSignalController("J1")
    assert sc.current_phase_name(traci) == "NS_GREEN"
    assert sc.yellow_duration(traci) == 3
    assert sc.green_duration(traci) == 42
    from simulation.scenario_manager import SumoScenarioManager
    sm = SumoScenarioManager("J1")
    sp = SumoSnapshotProvider("J1", "A")
    snap = sp.build_snapshot(traci, sc, sm)
    assert TOP_LEVEL_KEYS.issubset(snap.keys())
    for d in cfg.DIRECTIONS:
        assert DIRECTION_KEYS.issubset(snap["directions"][d].keys())


def test_yellow_commitment_counts():
    meta = {
        "v1": {"type": "car", "speed": 0.0, "lane": "J3J1_0", "pos": 95, "route": ["J3J1", "J1S1"]},
    }
    traci = make_mock_traci(
        phase_idx=1,
        vehicles_by_lane={"J3J1_0": ["v1"]},
        vehicle_meta=meta,
    )
    sc = SumoSignalController("J1")
    from simulation.scenario_manager import SumoScenarioManager
    sm = SumoScenarioManager("J1")
    snap = SumoSnapshotProvider("J1", "A").build_snapshot(traci, sc, sm)
    assert snap["phase"] == "NS_YELLOW"
    assert snap["yellow_commitment_count"] >= 1


def test_entities_spec_and_durations_from_snapshot():
    snap = _sample_snapshot()
    entities = build_all_entities("A", snap)
    assert len(entities) == 10
    tls = [e for e in entities if e["type"] == "TrafficLight"]
    assert tls[0]["yellowDuration"]["value"] == 3
    assert tls[0]["greenDurationCurrent"]["value"] == 42
    for e in entities:
        assert FORBIDDEN_ATTRS.isdisjoint(e.keys())


def test_build_all_nodes_entities():
    snap = _sample_snapshot()
    all_ents = []
    for node in ("A", "B", "C", "D"):
        all_ents.extend(build_all_entities(node, snap))
    assert len(all_ents) == 40
    ids = [e["id"] for e in all_ents]
    assert len(ids) == len(set(ids))


def test_command_queue_drain():
    q = CommandQueue()
    seen = []
    q.enqueue("force_phase", node_id="A", phase="EW_GREEN")
    n = q.drain({"force_phase": lambda node_id, phase: seen.append((node_id, phase))})
    assert n == 1
    assert seen == [("A", "EW_GREEN")]


def test_orion_publish_helper():
    from traci_runner import publish_once
    backend = MagicMock()
    backend.publish_nodes = ["A"]
    backend.publish_node = "A"
    backend.simulation_time_sec = 1.0
    backend.get_snapshot.return_value = _sample_snapshot()

    def boom(_):
        raise ConnectionError("down")

    assert publish_once(backend, boom, build_all_entities) == 0


def test_backend_stop_closes_traci():
    backend = SumoBackend.__new__(SumoBackend)
    mock_traci = MagicMock()
    backend._traci = mock_traci
    backend._started = True
    backend.stop()
    mock_traci.close.assert_called_once()


def test_sumo_assets_exist():
    assert cfg.DEFAULT_SUMO_CONFIG.is_file()
    assert (cfg.SUMO_ASSETS_DIR / "detectors.add.xml").is_file()
    assert (cfg.SUMO_ASSETS_DIR / "intersection.net.xml").is_file()


def test_docs_phase0_exist():
    docs = VISUALIZE / "docs"
    for name in ("adr.md", "data_dictionary.md", "limitations.md", "out_of_scope.md"):
        assert (docs / name).is_file()


def test_pcu_aggregate():
    vehicles_by_lane = {"J3J1_0": ["m1"], "J3J1_1": ["c1"], "J3J1_2": ["b1"]}
    meta = {
        "m1": {"type": "motorcycle", "speed": 0.0, "lane": "J3J1_0", "pos": 95, "route": ["J3J1", "J1S1"]},
        "c1": {"type": "car", "speed": 0.0, "lane": "J3J1_1", "pos": 90, "route": ["J3J1", "J1S1"]},
        "b1": {"type": "bus", "speed": 2.0, "lane": "J3J1_2", "pos": 80, "route": ["J3J1", "J1J2"]},
    }
    traci = make_mock_traci(vehicles_by_lane=vehicles_by_lane, vehicle_meta=meta)
    sc = SumoSignalController("J1")
    from simulation.scenario_manager import SumoScenarioManager
    snap = SumoSnapshotProvider("J1", "A").build_snapshot(traci, sc, SumoScenarioManager("J1"))
    assert snap["directions"]["North"]["pcu_equivalent"] == pytest.approx(3.74)
    assert snap["directions"]["North"]["vehicle_count"] == 3
