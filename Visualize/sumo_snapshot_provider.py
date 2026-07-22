"""
Build get_snapshot()-compatible dicts from TraCI lane / vehicle data.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import config as cfg

log = logging.getLogger(__name__)

# Track unknown vTypes for TODO reporting
_UNKNOWN_VTYPES: Set[str] = set()


def get_unknown_vtypes() -> Set[str]:
    return set(_UNKNOWN_VTYPES)


def pcu_for_vtype(vtype_id: str) -> float:
    if vtype_id in cfg.PCU_FACTORS:
        return cfg.PCU_FACTORS[vtype_id]
    if vtype_id not in _UNKNOWN_VTYPES:
        _UNKNOWN_VTYPES.add(vtype_id)
        log.warning(
            "Unknown SUMO vType '%s' — using PCU_FALLBACK=%.2f. Add it to config.PCU_FACTORS.",
            vtype_id, cfg.PCU_FALLBACK,
        )
    return cfg.PCU_FALLBACK


def density_label(pcu_equivalent: float) -> str:
    for label, (lo, hi) in cfg.DENSITY_THRESHOLDS_PER_DIRECTION.items():
        if lo <= pcu_equivalent < hi:
            return label
    return "HIGH"


def greenshields_speed_kmh(v_free_kmh: float, density_pcu_per_km: float) -> float:
    ratio = min(1.0, max(0.0, density_pcu_per_km / cfg.K_JAM_PCU_PER_KM))
    return round(max(0.0, v_free_kmh * (1.0 - ratio)), 1)


class SumoSnapshotProvider:
    """
    Observation layer: TraCI raw → snapshot contract matching CityNetworkEngine.get_snapshot.
    """

    def __init__(self, tls_id: str = "J1", node_id: str = "A"):
        self.tls_id = tls_id
        self.node_id = node_id
        # Arrival tracking: vehicle IDs seen on each approach in previous window
        self._prev_ids: Dict[str, Set[str]] = {d: set() for d in cfg.DIRECTIONS}
        self._arrival_pcu_window: Dict[str, float] = {d: 0.0 for d in cfg.DIRECTIONS}
        self._arrival_rate: Dict[str, float] = {d: 0.0 for d in cfg.DIRECTIONS}
        self._last_arrival_flush_sim_t: float = 0.0

    def update_arrival_rates(self, traci_module, sim_t: float) -> None:
        """Call every step (or each publish) to accumulate new arrivals; flush each 1s sim."""
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

    def build_snapshot(
        self,
        traci_module,
        signal_controller,
        scenario_manager,
    ) -> dict:
        sim_t = float(traci_module.simulation.getTime())
        self.update_arrival_rates(traci_module, sim_t)

        phase = signal_controller.current_phase_name(traci_module)
        colors = signal_controller.colors(traci_module)
        directions_data: Dict[str, dict] = {}

        for direction in cfg.DIRECTIONS:
            directions_data[direction] = self._build_direction(
                traci_module, direction, colors.get(direction, "red")
            )

        spill = self._spillback_metrics(traci_module)

        return {
            "node_id": self.node_id,
            "directions": directions_data,
            "phase": phase,
            "next_phase": signal_controller.next_phase_name(traci_module),
            "phase_remaining": signal_controller.phase_remaining_seconds(traci_module),
            "phase_duration": signal_controller.phase_duration(traci_module),
            "colors": colors,
            "scenario": scenario_manager.current_scenario,
            "blocked_direction": scenario_manager.blocked_direction,
            "incidents": scenario_manager.recent_incidents(),
            "simulation_time_sec": round(sim_t, 2),
            "downstream_edge_full": spill["downstream_edge_full"],
            "spillback_pressure": spill["spillback_pressure"],
            "spillback_detected": spill["spillback_detected"],
            # TODO(v1): junction box blocking not inferred from TraCI yet
            "intersection_box_blocked": False,
            # TODO(v1): blocked-at-entry count requires entry-zone logic
            "vehicles_blocked_at_entry": 0,
            # TODO(v1): yellow commitment needs position-vs-stop-line threshold
            "yellow_commitment_count": 0,
            # TODO(v1): emergency preemption deferred
            "preemption_active": False,
        }

    # ── per-direction ───────────────────────────────────────────────

    def _build_direction(self, traci_module, direction: str, color: str) -> dict:
        lane_ids = cfg.approach_lane_ids(self.tls_id, direction)
        vehicle_ids: List[str] = []
        for lid in lane_ids:
            try:
                vehicle_ids.extend(traci_module.lane.getLastStepVehicleIDs(lid))
            except Exception as e:
                log.debug("getLastStepVehicleIDs(%s): %s", lid, e)

        # Unique preserve order
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

                # Queue contribution: halting vehicles by length
                if speed_ms < cfg.HALTING_SPEED_MS:
                    length = float(traci_module.vehicle.getLength(vid))
                    queue_by_movement[movement] = queue_by_movement.get(movement, 0.0) + length + 1.5

                # Near stop-line: lane position close to lane length
                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    if lane_len > 0 and pos / lane_len >= 0.8:
                        near_stop.append(vid)
                except Exception:
                    pass
            except Exception as e:
                log.debug("vehicle metrics %s: %s", vid, e)

        # Occupancy: mean of lane last-step occupancy (SUMO returns 0–1 or sometimes %)
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

        # Prefer jam length if available via lane (approximation: sum halting lengths)
        queue_m = max(queue_by_movement.values()) if any(queue_by_movement.values()) else 0.0
        # Also try getLastStepHaltingNumber aggregate length via mean vehicle length
        try:
            jam_est = 0.0
            for lid in lane_ids:
                # No native jam meters on lane without E2; keep our estimate
                jam_est = max(jam_est, queue_m)
            queue_m = round(jam_est, 1)
        except Exception:
            queue_m = round(queue_m, 1)

        for k in queue_by_movement:
            queue_by_movement[k] = round(queue_by_movement[k], 1)

        avg_speed = round(sum(speeds_kmh) / len(speeds_kmh), 1) if speeds_kmh else 0.0
        pcu_equivalent = round(pcu_sum, 2)
        dens = density_label(pcu_equivalent)

        # waiting reason
        red_phase_n = 0
        congestion_n = 0
        if color in ("red", "yellow"):
            red_phase_n = waiting
        else:
            congestion_n = waiting
        if red_phase_n == 0 and congestion_n == 0:
            dominant = None
        elif red_phase_n >= congestion_n:
            dominant = "RED_PHASE"
        else:
            dominant = "CONGESTION"

        # moto front pct
        if not near_stop:
            near_stop = unique_ids[-max(1, len(unique_ids) // 5):] if unique_ids else []
        moto_n = 0
        for vid in near_stop:
            try:
                if traci_module.vehicle.getTypeID(vid) == "motorcycle":
                    moto_n += 1
            except Exception:
                pass
        moto_front = round(100.0 * moto_n / len(near_stop), 1) if near_stop else 0.0

        # composition fractions by actual SUMO vType
        total_v = len(unique_ids) or 1
        composition = {k: round(v / total_v, 4) for k, v in sorted(class_counts.items())}
        if not unique_ids:
            composition = {}

        pcu_per_km = (pcu_equivalent / cfg.APPROACH_LENGTH_M) * 1000.0
        theoretical = greenshields_speed_kmh(cfg.BASE_V_FREE_KMH, pcu_per_km)

        return {
            "vehicle_count": len(unique_ids),
            "pcu_equivalent": pcu_equivalent,
            "left_count": left_n,
            "straight_count": straight_n,
            "right_count": right_n,
            "average_speed_kmh": avg_speed,
            "waiting_vehicle_count": waiting,
            "queue_length_m": queue_m,
            "queue_by_movement": queue_by_movement,
            "occupancy_pct": occupancy,
            "density": dens,
            "arrival_rate_pcu_per_sec": self._arrival_rate.get(direction, 0.0),
            "waiting_reason_counts": {"RED_PHASE": red_phase_n, "CONGESTION": congestion_n},
            "dominant_waiting_reason": dominant,
            "theoretical_speed_kmh": theoretical,
            "moto_front_pct": moto_front,
            "vehicle_class_composition": composition,
        }

    def _infer_movement(self, traci_module, vid: str, approach_edge: str) -> str:
        try:
            route = list(traci_module.vehicle.getRoute(vid))
            idx = int(traci_module.vehicle.getRouteIndex(vid))
            # Find approach_edge in route and look at next edge
            if approach_edge in route:
                ai = route.index(approach_edge)
                if ai + 1 < len(route):
                    nxt = route[ai + 1]
                    return cfg.TURN_BY_OD.get((approach_edge, nxt), "straight")
            if idx + 1 < len(route):
                nxt = route[idx + 1]
                cur = route[idx] if idx < len(route) else approach_edge
                return cfg.TURN_BY_OD.get((cur, nxt), "straight")
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
        """
        Approximate spillback from outgoing edge occupancy.
        TODO(v1): refine with E2 detectors / junction occupancy.
        """
        max_pressure = 0.0
        any_full = False
        for edge in cfg.OUTGOING_EDGES.get(self.tls_id, []):
            try:
                # Mean occupancy across lanes
                occs = []
                for lane_idx in (0, 1):
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
                if ratio >= 0.95:
                    any_full = True
            except Exception:
                pass

        return {
            "downstream_edge_full": any_full,
            "spillback_pressure": round(max_pressure, 3),
            "spillback_detected": any_full or max_pressure >= 0.85,
        }
