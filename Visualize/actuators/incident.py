"""IncidentActuator — staged deterministic accident vehicles per intersection."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import configuration.config as cfg
from configuration.model_params import get_registry

log = logging.getLogger(__name__)


def _staging_config() -> Dict[str, Any]:
    raw = get_registry().export_effective_config().get("incident_staging") or {}
    return {
        "placement": str(raw.get("placement") or "stopline").lower(),
        "front_offset_from_stopline_m": float(raw.get("front_offset_from_stopline_m", 3.0)),
        "rear_bumper_gap_m": float(raw.get("rear_bumper_gap_m", 0.2)),
        "car_length_m": float(raw.get("car_length_m", 4.5)),
        "junction_position_ratio": float(raw.get("junction_position_ratio", 0.42)),
    }


def _crash_positions(
    lane_len: float,
    *,
    front_offset: float,
    car_len: float,
    gap: float,
) -> Tuple[float, float]:
    """Front (near stop line) and rear vehicle centres — bumper-to-bumper."""
    front = max(5.0, lane_len - front_offset)
    rear = max(3.0, front - car_len - gap)
    return front, rear


class IncidentActuator:
    def __init__(self) -> None:
        self.active_by_node: Dict[str, List[str]] = {}

    def vehicle_ids(self, node_id: str) -> List[str]:
        return list(self.active_by_node.get(node_id, []))

    def crash_lane_id(self, node_id: str, direction: Optional[str] = None) -> str:
        direction = direction or cfg.incident_approach_direction(node_id)
        tls = cfg.NODE_TO_TLS[node_id]
        edge = cfg.APPROACH_EDGES[tls][direction]
        return f"{edge}_1"

    def clear(self, traci_module, node_id: str) -> None:
        for vid in list(self.active_by_node.get(node_id, [])):
            try:
                if vid in traci_module.vehicle.getIDList():
                    traci_module.vehicle.remove(vid)
            except Exception as e:
                log.debug("incident remove %s: %s", vid, e)
        self.active_by_node.pop(node_id, None)

    def _spawn_frozen(
        self,
        traci_module,
        vid: str,
        route_id: str,
        lane_id: str,
        pos: float,
        lane_index: int,
    ) -> None:
        if vid in traci_module.vehicle.getIDList():
            traci_module.vehicle.remove(vid)
        edge = lane_id.rsplit("_", 1)[0]
        if route_id not in traci_module.route.getIDList():
            traci_module.route.add(route_id, [edge])
        traci_module.vehicle.add(
            vid,
            route_id,
            typeID="car",
            depart="now",
            departLane=str(lane_index),
            departPos="0",
            departSpeed="0",
        )
        lane_len = float(traci_module.lane.getLength(lane_id))
        safe_pos = max(1.0, min(pos, lane_len - 1.0))
        traci_module.vehicle.moveTo(vid, lane_id, safe_pos)
        traci_module.vehicle.setSpeed(vid, 0)
        traci_module.vehicle.setSpeedMode(vid, 0)
        traci_module.vehicle.setLaneChangeMode(vid, 0)

    def stage(
        self,
        traci_module,
        node_id: str,
        direction: Optional[str] = None,
        sim_t: float = 0.0,
    ) -> List[str]:
        """Spawn two stopped cars bumper-to-bumper just before the stop line."""
        self.clear(traci_module, node_id)
        conf = _staging_config()
        direction = direction or cfg.incident_approach_direction(node_id)
        tls = cfg.NODE_TO_TLS[node_id]
        edge = cfg.APPROACH_EDGES[tls][direction]
        lane_index = 1
        approach_lane = f"{edge}_{lane_index}"
        car_len = conf["car_length_m"]
        gap = conf["rear_bumper_gap_m"]

        app_len = float(traci_module.lane.getLength(approach_lane))
        front_pos, rear_pos = _crash_positions(
            app_len,
            front_offset=conf["front_offset_from_stopline_m"],
            car_len=car_len,
            gap=gap,
        )

        rid = f"incident_route_{node_id}_{edge}"
        vids: List[str] = []
        try:
            for i, pos in enumerate((rear_pos, front_pos)):
                vid = f"incident_{node_id}_{i}"
                self._spawn_frozen(traci_module, vid, rid, approach_lane, pos, lane_index)
                vids.append(vid)
        except Exception as e:
            for staged in vids:
                try:
                    traci_module.vehicle.remove(staged)
                except Exception:
                    pass
            raise RuntimeError(
                f"incident vehicle staging failed at {node_id} {direction}"
            ) from e

        if len(vids) != 2:
            raise RuntimeError(f"incident requires 2 vehicles at {node_id}, got {len(vids)}")

        self.active_by_node[node_id] = vids
        log.info(
            "Incident staged node=%s dir=%s lane=%s pos=%.1f/%.1f gap=%.2f vehicles=%s",
            node_id,
            direction,
            approach_lane,
            rear_pos,
            front_pos,
            gap,
            vids,
        )
        return vids

    def status(self) -> Dict[str, Any]:
        return {node: list(vids) for node, vids in self.active_by_node.items()}

    def tick(self, traci_module) -> None:
        for node_id, vids in list(self.active_by_node.items()):
            alive: List[str] = []
            for vid in vids:
                try:
                    if vid not in traci_module.vehicle.getIDList():
                        continue
                    traci_module.vehicle.setSpeed(vid, 0)
                    traci_module.vehicle.setSpeedMode(vid, 0)
                    alive.append(vid)
                except Exception:
                    pass
            if alive:
                self.active_by_node[node_id] = alive
            else:
                self.active_by_node.pop(node_id, None)
