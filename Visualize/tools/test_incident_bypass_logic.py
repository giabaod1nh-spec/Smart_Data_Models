"""Mocked TraCI checks for incident bypass zone + cooldown behaviour."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from typing import Any, Dict, List

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from actuators.incident import BypassZoneState, IncidentActuator  # noqa: E402


class _FakeLane:
    def __init__(self, vehicles: Dict[str, List[str]], lengths: Dict[str, float]):
        self._vehicles = vehicles
        self._lengths = lengths

    def getLastStepVehicleIDs(self, lane_id: str) -> List[str]:
        return list(self._vehicles.get(lane_id, []))

    def getLength(self, lane_id: str) -> float:
        return float(self._lengths.get(lane_id, 100.0))


class _FakeEdge:
    def __init__(self, vehicles: Dict[str, List[str]], lane_counts: Dict[str, int]):
        self._vehicles = vehicles
        self._lane_counts = lane_counts

    def getLastStepVehicleIDs(self, edge: str) -> List[str]:
        return list(self._vehicles.get(edge, []))

    def getLaneNumber(self, edge: str) -> int:
        return int(self._lane_counts.get(edge, 3))


class _FakeVehicle:
    def __init__(self, state: "FakeTraCI"):
        self.s = state
        self.change_calls: List[tuple] = []

    def getIDList(self) -> List[str]:
        return list(self.s.vehicles.keys())

    def getLaneID(self, vid: str) -> str:
        return self.s.vehicles[vid]["lane"]

    def getRoadID(self, vid: str) -> str:
        return self.s.vehicles[vid]["road"]

    def getLanePosition(self, vid: str) -> float:
        return float(self.s.vehicles[vid]["pos"])

    def getLength(self, vid: str) -> float:
        return float(self.s.vehicles[vid].get("length", 4.5))

    def getLaneIndex(self, vid: str) -> int:
        lane = self.s.vehicles[vid]["lane"]
        return int(lane.rsplit("_", 1)[1])

    def getLaneChangeMode(self, vid: str) -> int:
        return int(self.s.vehicles[vid].get("lc_mode", 1621))

    def setLaneChangeMode(self, vid: str, mode: int) -> None:
        self.s.vehicles[vid]["lc_mode"] = int(mode)

    def setSpeed(self, vid: str, speed: float) -> None:
        self.s.vehicles[vid]["speed"] = float(speed)

    def setSpeedMode(self, vid: str, mode: int) -> None:
        self.s.vehicles[vid]["speed_mode"] = int(mode)

    def couldChangeLane(self, vid: str, direction: int) -> bool:
        return bool(self.s.vehicles[vid].get("could_change", True))

    def changeLane(self, vid: str, lane_index: int, duration: float) -> None:
        self.change_calls.append((vid, int(lane_index), float(duration)))
        self.s.vehicles[vid]["pending_lane"] = int(lane_index)

    def remove(self, vid: str) -> None:
        self.s.vehicles.pop(vid, None)


class _FakeSim:
    def __init__(self, t: float = 0.0):
        self.t = t

    def getTime(self) -> float:
        return float(self.t)


class FakeTraCI:
    def __init__(self) -> None:
        self.vehicles: Dict[str, Dict[str, Any]] = {}
        self.edge_vehicles: Dict[str, List[str]] = {}
        self.lane_vehicles: Dict[str, List[str]] = {}
        self.vehicle = _FakeVehicle(self)
        self.edge = _FakeEdge(self.edge_vehicles, {"E": 3})
        self.lane = _FakeLane(self.lane_vehicles, {"E_0": 120, "E_1": 120, "E_2": 120})
        self.simulation = _FakeSim(0.0)
        self.polygon = SimpleNamespace(getIDList=lambda: [], remove=lambda *_: None, add=lambda *_a, **_k: None)
        self.poi = SimpleNamespace(getIDList=lambda: [], remove=lambda *_: None)
        self.route = SimpleNamespace(getIDList=lambda: ["r"], add=lambda *_a, **_k: None)

    def sync_lists(self) -> None:
        self.edge_vehicles.clear()
        self.lane_vehicles.clear()
        for vid, meta in self.vehicles.items():
            self.edge_vehicles.setdefault(meta["road"], []).append(vid)
            self.lane_vehicles.setdefault(meta["lane"], []).append(vid)


def _zone() -> BypassZoneState:
    # start=70, deadline=90, blocker=95
    return BypassZoneState(
        edge="E",
        crash_lane="E_1",
        crash_lane_index=1,
        lane_count=3,
        blocker_rear_pos=95.0,
        bypass_start_pos=70.0,
        bypass_deadline_pos=90.0,
    )


def test_active_zone_issues_change_lane() -> None:
    traci = FakeTraCI()
    act = IncidentActuator()
    act.active_by_node["A"] = ["incident_A_0", "incident_A_1"]
    act.zone_by_node["A"] = _zone()
    traci.vehicles = {
        "incident_A_0": {"road": "E", "lane": "E_1", "pos": 97.0, "length": 4.5},
        "incident_A_1": {"road": "E", "lane": "E_1", "pos": 102.0, "length": 4.5},
        "car1": {"road": "E", "lane": "E_1", "pos": 75.0, "length": 4.5, "could_change": True},
    }
    traci.sync_lists()
    traci.simulation.t = 10.0
    act.tick(traci)
    assert traci.vehicle.change_calls, "expected changeLane in active zone"
    assert traci.vehicle.change_calls[0][0] == "car1"
    assert traci.vehicle.change_calls[0][1] in (0, 2)


def test_upstream_of_red_zone_no_change() -> None:
    traci = FakeTraCI()
    act = IncidentActuator()
    act.active_by_node["A"] = ["incident_A_0"]
    act.zone_by_node["A"] = _zone()
    traci.vehicles = {
        "incident_A_0": {"road": "E", "lane": "E_1", "pos": 97.0, "length": 4.5},
        "car_far": {"road": "E", "lane": "E_1", "pos": 20.0, "length": 4.5, "could_change": True},
    }
    traci.sync_lists()
    traci.simulation.t = 5.0
    act.tick(traci)
    assert traci.vehicle.change_calls == []


def test_past_deadline_still_retries() -> None:
    traci = FakeTraCI()
    act = IncidentActuator()
    act.active_by_node["A"] = ["incident_A_0"]
    act.zone_by_node["A"] = _zone()
    traci.vehicles = {
        "incident_A_0": {"road": "E", "lane": "E_1", "pos": 97.0, "length": 4.5},
        "car_late": {
            "road": "E",
            "lane": "E_1",
            "pos": 92.0,
            "length": 4.5,
            "could_change": True,
        },
    }
    traci.sync_lists()
    traci.simulation.t = 12.0
    act.tick(traci)
    assert traci.vehicle.change_calls, "past deadline must still retry bypass"
    assert act.bypass_cmds["car_late"].conservative is True


def test_cooldown_prevents_spam() -> None:
    traci = FakeTraCI()
    act = IncidentActuator()
    act.active_by_node["A"] = ["incident_A_0"]
    act.zone_by_node["A"] = _zone()
    traci.vehicles = {
        "incident_A_0": {"road": "E", "lane": "E_1", "pos": 97.0, "length": 4.5},
        "car1": {"road": "E", "lane": "E_1", "pos": 75.0, "length": 4.5, "could_change": True},
    }
    traci.sync_lists()
    traci.simulation.t = 1.0
    act.tick(traci)
    first = list(traci.vehicle.change_calls)
    assert first
    traci.vehicle.change_calls.clear()
    traci.simulation.t = 1.2  # within cooldown
    act.tick(traci)
    assert traci.vehicle.change_calls == [], "must not spam changeLane inside cooldown"


def test_clear_drops_bypass_state() -> None:
    from actuators.incident import BypassCommandState

    traci = FakeTraCI()
    act = IncidentActuator()
    act.active_by_node["A"] = ["incident_A_0"]
    act.zone_by_node["A"] = _zone()
    traci.vehicles = {
        "incident_A_0": {"road": "E", "lane": "E_1", "pos": 97.0, "length": 4.5, "lc_mode": 0},
        "car1": {"road": "E", "lane": "E_1", "pos": 75.0, "length": 4.5, "lc_mode": 1621},
    }
    traci.sync_lists()
    act._lc_mode_restore["car1"] = 99
    act.bypass_cmds["car1"] = BypassCommandState(target_lane=2, last_attempt_t=1.0)
    act.clear(traci, "A")
    assert "A" not in act.zone_by_node
    assert "A" not in act.active_by_node
    assert "car1" not in act.bypass_cmds
    assert "incident_A_0" not in traci.vehicles
    assert traci.vehicles["car1"]["lc_mode"] == 99


if __name__ == "__main__":
    test_active_zone_issues_change_lane()
    test_upstream_of_red_zone_no_change()
    test_past_deadline_still_retries()
    test_cooldown_prevents_spam()
    test_clear_drops_bypass_state()
    print("ok")
