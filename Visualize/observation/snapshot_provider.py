"""
Build get_snapshot()-compatible dicts from TraCI + optional E1/E2 detectors.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set

import configuration.config as cfg
from observation.detector_manager import DetectorManager
from configuration.model_params import get_pcu

log = logging.getLogger(__name__)

_UNKNOWN_VTYPES: Set[str] = set()


def get_unknown_vtypes() -> Set[str]:
    return set(_UNKNOWN_VTYPES)


def pcu_for_vtype(vtype_id: str) -> float:
    return get_pcu(vtype_id)


from observation.metrics_derivation import (
    estimated_density_veh_per_km,
    greenshields_speed_kmh as _gs_speed,
    queue_length_vehicles_from_jam_m,
    storage_utilization_lane,
    storage_utilization_pcu,
)


def density_label(pcu_equivalent: float) -> str:
    """
    Traffic Load Class from approach PCU count — SEMANTIC legacy dual-write (Strategy C).
    Ownership: semantic context; kept on snapshot for entity_generator trafficStatus.
    """
    for label, (lo, hi) in cfg.DENSITY_THRESHOLDS_PER_DIRECTION.items():
        if lo <= pcu_equivalent < hi:
            return label
    return "HIGH"


def greenshields_speed_kmh(v_free_kmh: float, density_pcu_per_km: float) -> float:
    """Physical aggregate estimate (not a semantic class)."""
    return round(
        _gs_speed(v_free_kmh, density_pcu_per_km, cfg.K_JAM_PCU_PER_KM),
        1,
    )


class SumoSnapshotProvider:
    def __init__(self, tls_id: str = "J1", node_id: str = "A"):
        self.tls_id = tls_id
        self.node_id = node_id
        self.detectors = DetectorManager(tls_id)
        self._prev_ids: Dict[str, Set[str]] = {d: set() for d in cfg.DIRECTIONS}
        self._arrival_pcu_window: Dict[str, float] = {d: 0.0 for d in cfg.DIRECTIONS}
        self._arrival_rate: Dict[str, float] = {d: 0.0 for d in cfg.DIRECTIONS}
        self._last_arrival_flush_sim_t: float = 0.0
        self.simulation_run_id: Optional[str] = None

    def update_arrival_rates(self, traci_module, sim_t: float) -> None:
        for direction in cfg.DIRECTIONS:
            ids_now = self._vehicle_ids_on_approach(traci_module, direction)
            new_ids = ids_now - self._prev_ids[direction]
            for vid in new_ids:
                try:
                    vt = traci_module.vehicle.getTypeID(vid)
                    self._arrival_pcu_window[direction] += pcu_for_vtype(vt)
                except Exception:
                    self._arrival_pcu_window[direction] += cfg.PCU_FALLBACK
            self._prev_ids[direction] = ids_now

        if sim_t - self._last_arrival_flush_sim_t >= 1.0:
            dt = max(1e-6, sim_t - self._last_arrival_flush_sim_t)
            for d in cfg.DIRECTIONS:
                self._arrival_rate[d] = round(self._arrival_pcu_window[d] / dt, 3)
                self._arrival_pcu_window[d] = 0.0
            self._last_arrival_flush_sim_t = sim_t

    def build_snapshot(self, traci_module, signal_controller, scenario_manager) -> dict:
        sim_t = float(traci_module.simulation.getTime())
        self.update_arrival_rates(traci_module, sim_t)

        phase = signal_controller.current_phase_name(traci_module)
        colors = signal_controller.colors(traci_module)
        directions_data = {
            d: self._build_direction(traci_module, d, colors.get(d, "red"))
            for d in cfg.DIRECTIONS
        }

        spill = self._spillback_metrics(traci_module)
        box_blocked = spill["downstream_edge_full"] or spill["spillback_pressure"] >= cfg.BOX_OCCUPANCY_THRESHOLD
        blocked_at_entry = self._count_blocked_at_entry(traci_module, box_blocked)
        yellow_commit = self._yellow_commitment_count(traci_module, phase)
        preemption = bool(getattr(signal_controller, "preemption_active", False))

        return {
            "node_id": self.node_id,
            "directions": directions_data,
            "phase": phase,
            "next_phase": signal_controller.next_phase_name(traci_module),
            "phase_remaining": signal_controller.phase_remaining_seconds(traci_module),
            "phase_duration": signal_controller.phase_duration(traci_module),
            "yellow_duration": signal_controller.yellow_duration(traci_module),
            "green_duration": signal_controller.green_duration(traci_module),
            "red_duration": signal_controller.red_duration(traci_module),
            "colors": colors,
            "scenario": scenario_manager.current_scenario,
            "blocked_direction": scenario_manager.blocked_direction,
            "incidents": scenario_manager.recent_incidents(),
            "simulation_time_sec": round(sim_t, 2),
            "downstream_edge_full": spill["downstream_edge_full"],
            "spillback_pressure": spill["spillback_pressure"],
            "spillback_detected": spill["spillback_detected"],
            "intersection_box_blocked": box_blocked,
            "vehicles_blocked_at_entry": blocked_at_entry,
            "yellow_commitment_count": yellow_commit,
            "preemption_active": preemption,
            "simulation_run_id": self.simulation_run_id,
            "schema_version": getattr(cfg, "MODEL_SCHEMA_VERSION", None),
            "pcu_profile_id": getattr(cfg, "PCU_PROFILE_ID", None),
            "config_hash": getattr(cfg, "CONFIG_HASH", None),
        }

    def _build_direction(self, traci_module, direction: str, color: str) -> dict:
        lane_ids = cfg.approach_lane_ids(self.tls_id, direction)
        vehicle_ids: List[str] = []
        for lid in lane_ids:
            try:
                vehicle_ids.extend(traci_module.lane.getLastStepVehicleIDs(lid))
            except Exception as e:
                log.debug("getLastStepVehicleIDs(%s): %s", lid, e)

        seen: Set[str] = set()
        unique_ids: List[str] = []
        for vid in vehicle_ids:
            if vid not in seen:
                seen.add(vid)
                unique_ids.append(vid)

        class_counts: Dict[str, int] = {}
        pcu_sum = 0.0
        speeds_kmh: List[float] = []
        waiting = 0
        left_n = straight_n = right_n = 0
        queue_by_movement = {"straight": 0.0, "left": 0.0, "right": 0.0}
        near_stop: List[str] = []
        approach_edge = cfg.APPROACH_EDGES[self.tls_id][direction]

        for vid in unique_ids:
            try:
                vt = traci_module.vehicle.getTypeID(vid)
                speed_ms = float(traci_module.vehicle.getSpeed(vid))
                pcu = pcu_for_vtype(vt)
                pcu_sum += pcu
                class_counts[vt] = class_counts.get(vt, 0) + 1

                if speed_ms >= cfg.HALTING_SPEED_MS:
                    speeds_kmh.append(speed_ms * 3.6)
                else:
                    waiting += 1

                movement = self._infer_movement(traci_module, vid, approach_edge)
                if movement == "left":
                    left_n += 1
                elif movement == "right":
                    right_n += 1
                else:
                    straight_n += 1

                if speed_ms < cfg.HALTING_SPEED_MS:
                    length = float(traci_module.vehicle.getLength(vid))
                    queue_by_movement[movement] = queue_by_movement.get(movement, 0.0) + length + 1.5

                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    if lane_len > 0 and pos / lane_len >= cfg.MOTO_FRONT_ZONE_START_RATIO:
                        near_stop.append(vid)
                except Exception:
                    pass
            except Exception as e:
                log.debug("vehicle metrics %s: %s", vid, e)

        # Prefer E2 stop-line jam / occupancy (ADR-004); full-link metrics separate.
        e2_q = self.detectors.approach_queue_by_movement(traci_module, direction)
        if e2_q and any(e2_q.values()):
            queue_by_movement = {k: round(float(e2_q.get(k, 0.0)), 1) for k in ("straight", "left", "right")}
        else:
            for k in queue_by_movement:
                queue_by_movement[k] = round(queue_by_movement[k], 1)

        e2_occ = self.detectors.approach_occupancy_pct(traci_module, direction)
        if e2_occ is not None:
            occupancy = e2_occ
        else:
            occ_vals: List[float] = []
            for lid in lane_ids:
                try:
                    occ = float(traci_module.lane.getLastStepOccupancy(lid))
                    if occ <= 1.0:
                        occ *= 100.0
                    occ_vals.append(max(0.0, min(100.0, occ)))
                except Exception:
                    pass
            occupancy = round(sum(occ_vals) / len(occ_vals), 1) if occ_vals else 0.0

        queue_m = max(queue_by_movement.values()) if any(queue_by_movement.values()) else 0.0
        avg_speed = round(sum(speeds_kmh) / len(speeds_kmh), 1) if speeds_kmh else 0.0
        pcu_equivalent = round(pcu_sum, 2)
        dens = density_label(pcu_equivalent)

        # Full-link TraCI metrics (entire approach; not capped at E2 cover)
        full_link = self._full_link_queue_metrics(traci_module, lane_ids, waiting, pcu_equivalent)
        approach_len_m = full_link["approach_length_m"]
        car_len = float(getattr(cfg, "PASSENGER_CAR_LENGTH_M", 7.5))
        queue_veh = queue_length_vehicles_from_jam_m(queue_m, car_len)
        if waiting > 0 and queue_m <= 0:
            queue_veh = float(waiting)

        if color in ("red", "yellow"):
            red_phase_n, congestion_n = waiting, 0
        else:
            red_phase_n, congestion_n = 0, waiting
        if red_phase_n == 0 and congestion_n == 0:
            dominant = None
        elif red_phase_n >= congestion_n:
            dominant = "RED_PHASE"
        else:
            dominant = "CONGESTION"

        if not near_stop:
            near_stop = unique_ids[-max(1, len(unique_ids) // 5) :] if unique_ids else []
        moto_n = 0
        for vid in near_stop:
            try:
                if traci_module.vehicle.getTypeID(vid) == "motorcycle":
                    moto_n += 1
            except Exception:
                pass
        moto_front = round(100.0 * moto_n / len(near_stop), 1) if near_stop else 0.0

        total_v = len(unique_ids) or 1
        composition = {k: round(v / total_v, 4) for k, v in sorted(class_counts.items())}
        if not unique_ids:
            composition = {}

        pcu_per_km = estimated_density_veh_per_km(pcu_equivalent, approach_len_m)
        theoretical = greenshields_speed_kmh(cfg.BASE_V_FREE_KMH, pcu_per_km)

        return {
            "vehicle_count": len(unique_ids),
            "pcu_equivalent": pcu_equivalent,
            "left_count": left_n,
            "straight_count": straight_n,
            "right_count": right_n,
            "average_speed_kmh": avg_speed,
            "waiting_vehicle_count": waiting,
            "queue_length_m": round(queue_m, 1),
            "queue_length_vehicles": queue_veh,
            "queue_by_movement": queue_by_movement,
            "occupancy_pct": occupancy,
            "density": dens,  # SEMANTIC dual-write for NGSI trafficStatus
            "estimated_density_veh_per_km": round(pcu_per_km, 3),
            "arrival_rate_pcu_per_sec": self._arrival_rate.get(direction, 0.0),
            "waiting_reason_counts": {"RED_PHASE": red_phase_n, "CONGESTION": congestion_n},
            "dominant_waiting_reason": dominant,
            "theoretical_speed_kmh": theoretical,
            "moto_front_pct": moto_front,
            "vehicle_class_composition": composition,
            "approach_length_m": approach_len_m,
            "full_link_halting_count": full_link["full_link_halting_count"],
            "full_link_jam_length_m": full_link["full_link_jam_length_m"],
            "storage_utilization_lane": full_link["storage_utilization_lane"],
            "storage_utilization_pcu": full_link["storage_utilization_pcu"],
            "nominal_storage_pcu_geometric": full_link["nominal_storage_pcu_geometric"],
        }

    def _full_link_queue_metrics(
        self,
        traci_module,
        lane_ids: List[str],
        waiting_count: int,
        queued_pcu: float,
    ) -> dict:
        """Full-approach TraCI spillback metrics (not limited to E2 cover length)."""
        car_len = float(getattr(cfg, "PASSENGER_CAR_LENGTH_M", 7.5))
        lane_lengths: List[float] = []
        lane_jam_m: List[float] = []
        halting = 0
        for lid in lane_ids:
            try:
                L = float(traci_module.lane.getLength(lid))
            except Exception:
                L = float(cfg.APPROACH_LENGTH_M)
            lane_lengths.append(L)
            jam = 0.0
            try:
                for vid in traci_module.lane.getLastStepVehicleIDs(lid):
                    try:
                        if float(traci_module.vehicle.getSpeed(vid)) < cfg.HALTING_SPEED_MS:
                            jam += float(traci_module.vehicle.getLength(vid)) + 1.5
                            halting += 1
                    except Exception:
                        pass
            except Exception:
                pass
            lane_jam_m.append(jam)

        approach_len = max(lane_lengths) if lane_lengths else float(cfg.APPROACH_LENGTH_M)
        util_lane = 0.0
        for jam, L in zip(lane_jam_m, lane_lengths):
            util_lane = max(util_lane, storage_utilization_lane(jam, L))
        full_jam = max(lane_jam_m) if lane_jam_m else 0.0
        if waiting_count > 0 and halting == 0:
            halting = int(waiting_count)
        nominal = round(sum(max(0.0, L) / car_len for L in lane_lengths), 2) if lane_lengths else 0.0
        util_pcu = storage_utilization_pcu(queued_pcu, lane_lengths, car_len)
        return {
            "approach_length_m": round(approach_len, 2),
            "full_link_halting_count": int(halting),
            "full_link_jam_length_m": round(full_jam, 1),
            "storage_utilization_lane": round(util_lane, 4),
            "storage_utilization_pcu": round(util_pcu, 4),
            "nominal_storage_pcu_geometric": nominal,
        }

    def _infer_movement(self, traci_module, vid: str, approach_edge: str) -> str:
        try:
            route = list(traci_module.vehicle.getRoute(vid))
            idx = int(traci_module.vehicle.getRouteIndex(vid))
            if approach_edge in route:
                ai = route.index(approach_edge)
                if ai + 1 < len(route):
                    return cfg.TURN_BY_OD.get((approach_edge, route[ai + 1]), "straight")
            if idx + 1 < len(route):
                return cfg.TURN_BY_OD.get((route[idx], route[idx + 1]), "straight")
        except Exception:
            pass
        return "straight"

    def _vehicle_ids_on_approach(self, traci_module, direction: str) -> Set[str]:
        ids: Set[str] = set()
        for lid in cfg.approach_lane_ids(self.tls_id, direction):
            try:
                ids.update(traci_module.lane.getLastStepVehicleIDs(lid))
            except Exception:
                pass
        return ids

    def _spillback_metrics(self, traci_module) -> dict:
        pressure, full = self.detectors.outbound_occupancy_ratio(traci_module)
        if not self.detectors.available:
            max_pressure = 0.0
            any_full = False
            for edge in cfg.OUTGOING_EDGES.get(self.tls_id, []):
                occs = []
                for lane_idx in range(cfg.LANES_PER_APPROACH):
                    lid = f"{edge}_{lane_idx}"
                    try:
                        o = float(traci_module.lane.getLastStepOccupancy(lid))
                        if o > 1.0:
                            o = o / 100.0
                        occs.append(o)
                    except Exception:
                        pass
                if not occs:
                    continue
                ratio = sum(occs) / len(occs)
                max_pressure = max(max_pressure, ratio)
                if ratio >= cfg.BOX_OCCUPANCY_THRESHOLD:
                    any_full = True
            pressure, full = round(max_pressure, 3), any_full

        return {
            "downstream_edge_full": full,
            "spillback_pressure": pressure,
            "spillback_detected": full or pressure >= cfg.BOX_OCCUPANCY_THRESHOLD,
        }

    def _count_blocked_at_entry(self, traci_module, box_blocked: bool) -> int:
        if not box_blocked:
            return 0
        count = 0
        for direction in cfg.DIRECTIONS:
            for vid in self._vehicle_ids_on_approach(traci_module, direction):
                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    speed = float(traci_module.vehicle.getSpeed(vid))
                    if lane_len > 0 and pos / lane_len >= cfg.BLOCKED_ENTRY_POSITION_RATIO and speed < cfg.HALTING_SPEED_MS:
                        count += 1
                except Exception:
                    pass
        return count

    def _yellow_commitment_count(self, traci_module, phase: str) -> int:
        if "YELLOW" not in phase:
            return 0
        count = 0
        for direction in cfg.DIRECTIONS:
            for vid in self._vehicle_ids_on_approach(traci_module, direction):
                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    if lane_len > 0 and pos / lane_len >= cfg.YELLOW_COMMITMENT_RATIO:
                        count += 1
                except Exception:
                    pass
        return count
