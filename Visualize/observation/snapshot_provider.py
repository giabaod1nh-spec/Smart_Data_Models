"""
Build get_snapshot()-compatible dicts from TraCI + optional E1/E2 detectors.
"""
from __future__ import annotations

import logging
import hashlib
import json
import math
from typing import Dict, List, Optional, Set

import configuration.config as cfg
from observation.detector_manager import DetectorManager, occupancy_percent
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


def mean_moving_speed_kmh(speeds_kmh: List[float]) -> float:
    """Mean speed (km/h) of vehicles above HALTING threshold only; full float precision."""
    if not speeds_kmh:
        return 0.0
    return sum(speeds_kmh) / len(speeds_kmh)


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
    def __init__(self, tls_id: str = "411926415", node_id: str = "A", *,
                 stage_a_measurements: bool = False):
        self.tls_id = tls_id
        self.node_id = node_id
        self.directions = cfg.directions_for_node(node_id)
        self.stage_a_measurements = stage_a_measurements
        self.detectors = DetectorManager(tls_id, stage_a_measurements=stage_a_measurements)
        self._queue_scope_digests: dict[str, tuple[tuple, str]] = {}
        self._prev_ids: Dict[str, Set[str]] = {d: set() for d in self.directions}
        self._arrival_pcu_window: Dict[str, float] = {d: 0.0 for d in self.directions}
        self._arrival_rate: Dict[str, float] = {d: 0.0 for d in self.directions}
        self._arrival_window_incomplete: Dict[str, bool] = {d: False for d in self.directions}
        self._arrival_rate_incomplete: Dict[str, bool] = {d: False for d in self.directions}
        self._last_arrival_flush_sim_t: float = 0.0
        self.simulation_run_id: Optional[str] = None

    def update_arrival_rates(self, traci_module, sim_t: float, *, quality: Optional[dict] = None) -> None:
        for direction in self.directions:
            errors = []
            ids_now = self._vehicle_ids_on_approach(traci_module, direction, errors=errors)
            new_ids = ids_now - self._prev_ids[direction]
            for vid in new_ids:
                try:
                    vt = traci_module.vehicle.getTypeID(vid)
                    self._arrival_pcu_window[direction] += pcu_for_vtype(vt)
                except Exception:
                    errors.append(vid)
                    self._arrival_pcu_window[direction] += cfg.PCU_FALLBACK
            if errors:
                self._arrival_window_incomplete[direction] = True
            self._prev_ids[direction] = ids_now

        if sim_t - self._last_arrival_flush_sim_t >= 1.0:
            dt = max(1e-6, sim_t - self._last_arrival_flush_sim_t)
            for d in self.directions:
                self._arrival_rate[d] = round(self._arrival_pcu_window[d] / dt, 3)
                self._arrival_rate_incomplete[d] = self._arrival_window_incomplete[d]
                self._arrival_window_incomplete[d] = False
                self._arrival_pcu_window[d] = 0.0
            self._last_arrival_flush_sim_t = sim_t
        if quality is not None:
            for direction in self.directions:
                quality[direction] = {
                    "arrival_rate_unavailable": self._arrival_rate_incomplete[direction],
                }

    def build_snapshot(self, traci_module, signal_controller, scenario_manager, *, capture_quality: bool = False) -> dict:
        sim_t = float(traci_module.simulation.getTime())
        quality: dict = {}
        self.update_arrival_rates(traci_module, sim_t, quality=quality if capture_quality else None)

        phase = signal_controller.current_phase_name(traci_module)
        colors = signal_controller.colors(traci_module)
        directions_data = {
            d: self._build_direction(
                traci_module, d, colors.get(d, "red"),
                quality=quality.setdefault(d, {}) if capture_quality else None,
            )
            for d in self.directions
        }

        top_quality: dict = {}
        spill = self._spillback_metrics(
            traci_module, **({"quality": top_quality} if capture_quality else {}),
        )
        box_blocked = spill["downstream_edge_full"] or spill["spillback_pressure"] >= cfg.BOX_OCCUPANCY_THRESHOLD
        blocked_at_entry = self._count_blocked_at_entry(
            traci_module, box_blocked, **({"quality": top_quality} if capture_quality else {}),
        )
        yellow_commit = self._yellow_commitment_count(
            traci_module, phase, **({"quality": top_quality} if capture_quality else {}),
        )
        preemption = bool(getattr(signal_controller, "preemption_active", False))

        result = {
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
            **({"spillback_occupancy_method": spill["spillback_occupancy_method"]}
               if spill.get("spillback_occupancy_method") is not None else {}),
            **({"analytics_spillback_evidence": spill["analytics_spillback_evidence"]}
               if spill.get("analytics_spillback_evidence") is not None else {}),
            "intersection_box_blocked": box_blocked,
            "vehicles_blocked_at_entry": blocked_at_entry,
            "yellow_commitment_count": yellow_commit,
            "preemption_active": preemption,
            "simulation_run_id": self.simulation_run_id,
            "schema_version": getattr(cfg, "MODEL_SCHEMA_VERSION", None),
            "pcu_profile_id": getattr(cfg, "PCU_PROFILE_ID", None),
            "config_hash": getattr(cfg, "CONFIG_HASH", None),
        }
        if capture_quality:
            quality["_top_level"] = top_quality
            result["_measurement_quality"] = quality
        if self.stage_a_measurements:
            result["analytics_capture_seconds"] = sim_t
        return result

    def _build_direction(self, traci_module, direction: str, color: str, *, quality: Optional[dict] = None) -> dict:
        lane_ids = cfg.approach_lane_ids(self.tls_id, direction)
        if quality is not None and not lane_ids:
            raise ValueError(f"{self.node_id}/{direction}: no approach lanes")
        failed_lanes = False
        vehicle_ids: List[str] = []
        for lid in lane_ids:
            try:
                vehicle_ids.extend(traci_module.lane.getLastStepVehicleIDs(lid))
            except Exception as e:
                failed_lanes = True
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
        approach_edge = cfg.APPROACH_EDGES.get(self.tls_id, {}).get(direction)
        failed_vehicle_metrics = False
        failed_queue_length = False
        failed_movement = False
        failed_near_stop = False
        route_queue_samples = [] if self.stage_a_measurements else None

        for vid in unique_ids:
            try:
                vt = traci_module.vehicle.getTypeID(vid)
                speed_ms = float(traci_module.vehicle.getSpeed(vid))
                if self.stage_a_measurements and (not math.isfinite(speed_ms) or speed_ms < 0):
                    raise ValueError("invalid vehicle speed")
                pcu = pcu_for_vtype(vt)
                pcu_sum += pcu
                class_counts[vt] = class_counts.get(vt, 0) + 1

                if speed_ms >= cfg.HALTING_SPEED_MS:
                    speeds_kmh.append(speed_ms * 3.6)
                else:
                    waiting += 1

                movement_errors = [] if self.stage_a_measurements or quality is not None else None
                movement = self._infer_movement(traci_module, vid, approach_edge, errors=movement_errors)
                if movement_errors:
                    failed_movement = True
                if movement == "left":
                    left_n += 1
                elif movement == "right":
                    right_n += 1
                else:
                    straight_n += 1

                if speed_ms < cfg.HALTING_SPEED_MS:
                    if not self.stage_a_measurements:
                        length = float(traci_module.vehicle.getLength(vid))
                        queue_by_movement[movement] = queue_by_movement.get(movement, 0.0) + length + 1.5
                    else:
                        try:
                            length = float(traci_module.vehicle.getLength(vid))
                            if movement_errors or not math.isfinite(length) or length <= 0:
                                raise ValueError("stopped vehicle lacks a valid route movement or length")
                            queue_by_movement[movement] += length + 1.5
                            route_queue_samples.append([movement, length, speed_ms])
                        except Exception:
                            failed_queue_length = True

                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    if lane_len > 0 and pos / lane_len >= cfg.MOTO_FRONT_ZONE_START_RATIO:
                        near_stop.append(vid)
                except Exception:
                    failed_near_stop = True
                    pass
            except Exception as e:
                failed_vehicle_metrics = True
                failed_queue_length = True
                log.debug("vehicle metrics %s: %s", vid, e)

        if not self.stage_a_measurements:
            e2_q = self.detectors.approach_queue_by_movement(
                traci_module, direction, **({"quality": quality} if quality is not None else {}),
            )
            legacy_e2 = bool(e2_q) and any(e2_q.values())
            if legacy_e2:
                queue_by_movement = {k: round(float(e2_q.get(k, 0.0)), 1) for k in queue_by_movement}
            else:
                queue_by_movement = {k: round(v, 1) for k, v in queue_by_movement.items()}
            queue_valid = not ((failed_lanes or failed_queue_length or failed_movement) and not legacy_e2)
            if legacy_e2 and quality and quality.get("queue_detector_incomplete"):
                queue_valid = False
        else:
            configured_lanes = cfg.edge_lane_ids(approach_edge) if approach_edge else ()
            use_e2 = bool(configured_lanes) and all(
                cfg.movement_for_lane(approach_edge, index) in ("straight", "left", "right")
                for index, _ in enumerate(configured_lanes)
            )
            if use_e2:
                queue_method = "e2_jam_movement_v1"
                queue_quality = quality if quality is not None else {}
                e2_q = self.detectors.approach_queue_by_movement(traci_module, direction, quality=queue_quality)
                queue_valid = (bool(e2_q) and not queue_quality.get("queue_detector_incomplete")
                               and len(queue_quality.get("analytics_e2_queue", ())) == len(configured_lanes))
                queue_samples = queue_quality.get("analytics_e2_queue", [])
                # A complete zero is an E2 measurement in its own right.
                queue_by_movement = {k: round(float(e2_q.get(k, 0.0)), 1) for k in queue_by_movement}
            else:
                queue_method = "route_stopped_length_v1"
                queue_valid = bool(lane_ids) and not (failed_lanes or failed_queue_length)
                queue_samples = route_queue_samples
                queue_by_movement = {k: round(v, 1) for k, v in queue_by_movement.items()}

        occupancy_quality = quality if quality is not None else {}
        e2_occ = self.detectors.approach_occupancy_pct(
            traci_module, direction, **({"quality": occupancy_quality}
                                       if quality is not None or self.stage_a_measurements else {}),
        )
        failed_occupancy = False
        if e2_occ is not None and (
                not self.stage_a_measurements or not occupancy_quality.get("occupancy_detector_incomplete")):
            occupancy = e2_occ
            occupancy_method = "e2_percent_v2"
            failed_occupancy = bool(occupancy_quality.get("occupancy_detector_incomplete"))
            occupancy_evidence = occupancy_quality.get("analytics_e2_occupancy_percent")
        else:
            occ_vals: List[float] = []
            for lid in lane_ids:
                try:
                    raw = traci_module.lane.getLastStepOccupancy(lid)
                    if self.stage_a_measurements:
                        occ_vals.append(occupancy_percent(raw))
                    else:
                        occ = float(raw)
                        occ_vals.append(max(0.0, min(100.0, occ * 100.0 if occ <= 1.0 else occ)))
                except Exception:
                    pass
            failed_occupancy = len(occ_vals) != len(lane_ids) or (self.stage_a_measurements and not occ_vals)
            occupancy = (
                round(sum(occ_vals) / len(occ_vals), 1)
                if occ_vals and (not self.stage_a_measurements or not failed_occupancy) else 0.0
            )
            occupancy_method = None if failed_occupancy else "lane_percent_v2"
            occupancy_evidence = occ_vals

        queue_m = max(queue_by_movement.values()) if any(queue_by_movement.values()) else 0.0
        # Moving-vehicles-only mean; precision preserved for publish — UI rounds for display.
        avg_speed = mean_moving_speed_kmh(speeds_kmh)
        pcu_equivalent = round(pcu_sum, 2)
        dens = density_label(pcu_equivalent)

        # Full-link TraCI metrics (entire approach; not capped at E2 cover)
        full_link = self._full_link_queue_metrics(
            traci_module, lane_ids, waiting, pcu_equivalent,
            **({"quality": quality} if quality is not None else {}),
        )
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
            if unique_ids:
                failed_near_stop = True
            near_stop = unique_ids[-max(1, len(unique_ids) // 5) :] if unique_ids else []
        moto_n = 0
        for vid in near_stop:
            try:
                if traci_module.vehicle.getTypeID(vid) == "motorcycle":
                    moto_n += 1
            except Exception:
                failed_near_stop = True
                pass
        moto_front = round(100.0 * moto_n / len(near_stop), 1) if near_stop else 0.0

        total_v = len(unique_ids) or 1
        composition = {k: round(v / total_v, 4) for k, v in sorted(class_counts.items())}
        if not unique_ids:
            composition = {}

        pcu_per_km = estimated_density_veh_per_km(pcu_equivalent, approach_len_m)
        theoretical = greenshields_speed_kmh(cfg.BASE_V_FREE_KMH, pcu_per_km)

        result = {
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
            **({"queue_measurement": {
                "method": queue_method,
                "scope_digest": self._queue_scope_digest(direction, queue_method, lane_ids, configured_lanes),
            }} if self.stage_a_measurements and queue_valid else {}),
            "occupancy_pct": occupancy,
            **({"occupancy_method": occupancy_method}
               if self.stage_a_measurements and occupancy_method is not None else {}),
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
        if self.stage_a_measurements:
            evidence = {}
            if occupancy_method is not None and occupancy_evidence:
                evidence["occupancy"] = {"method": occupancy_method, "percent": occupancy_evidence}
            if queue_valid:
                evidence["queue"] = {
                    "method": queue_method, "scopeDigest": result["queue_measurement"]["scope_digest"],
                    "samples": queue_samples,
                }
                if queue_method == "route_stopped_length_v1":
                    evidence["queue"]["haltingThreshold"] = cfg.HALTING_SPEED_MS
            if evidence:
                result["analytics_measurement_evidence"] = evidence
        if quality is not None:
            unavailable = set()
            if failed_lanes:
                unavailable.update((
                    "vehicle_count", "pcu_equivalent", "left_count", "straight_count",
                    "right_count", "average_speed_kmh", "waiting_vehicle_count",
                ))
                failed_queue_length = True
            if failed_vehicle_metrics:
                unavailable.update((
                    "pcu_equivalent", "left_count", "straight_count", "right_count",
                    "average_speed_kmh", "waiting_vehicle_count",
                ))
            if failed_movement:
                unavailable.update(("left_count", "straight_count", "right_count"))
            if not queue_valid:
                unavailable.add("queue_length_m")
            if failed_occupancy:
                unavailable.add("occupancy_pct")
            quality["unavailable_core_metrics"] = sorted(unavailable)
            quality["failed_lane_reads"] = failed_lanes
            quality["failed_vehicle_reads"] = failed_vehicle_metrics
            quality["failed_movement_reads"] = failed_movement
            quality["failed_occupancy_reads"] = failed_occupancy
            quality["moto_front_unavailable"] = failed_near_stop
        return result

    def _queue_scope_digest(self, direction, method, lanes, configured_lanes):
        # The event also pins the archived run manifest and topology. This
        # digest distinguishes the provider's measurement population/method.
        key = (direction, method, tuple(sorted(lanes)), tuple(configured_lanes), cfg.HALTING_SPEED_MS)
        cached = self._queue_scope_digests.get(direction)
        if cached is None or cached[0] != key:
            material = json.dumps([self.tls_id, *key], separators=(",", ":"), ensure_ascii=False)
            cached = key, hashlib.sha256(material.encode("utf-8")).hexdigest()
            self._queue_scope_digests[direction] = cached
        return cached[1]

    def _full_link_queue_metrics(
        self,
        traci_module,
        lane_ids: List[str],
        waiting_count: int,
        queued_pcu: float,
        *, quality: Optional[dict] = None,
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
                if quality is not None:
                    quality["full_link_incomplete"] = True
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
                        if quality is not None:
                            quality["full_link_incomplete"] = True
                        pass
            except Exception:
                if quality is not None:
                    quality["full_link_incomplete"] = True
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

    def _infer_movement(self, traci_module, vid: str, approach_edge: Optional[str], *, errors: Optional[list] = None) -> str:
        try:
            route = list(traci_module.vehicle.getRoute(vid))
            idx = int(traci_module.vehicle.getRouteIndex(vid))
            offset = max(0, idx) if self.stage_a_measurements else 0
            if approach_edge and approach_edge in (route[offset:] if offset else route):
                ai = route.index(approach_edge, offset)
                if ai + 1 < len(route):
                    pair = (approach_edge, route[ai + 1])
                    if errors is not None and pair not in cfg.TURN_BY_OD:
                        errors.append(vid)
                    return cfg.TURN_BY_OD.get(pair, "straight")
            if idx + 1 < len(route) and (idx >= 0 or not self.stage_a_measurements):
                pair = (route[idx], route[idx + 1])
                if errors is not None and pair not in cfg.TURN_BY_OD:
                    errors.append(vid)
                return cfg.TURN_BY_OD.get(pair, "straight")
        except Exception:
            if errors is not None:
                errors.append(vid)
            return "straight"
        if errors is not None:
            errors.append(vid)
        return "straight"

    def _vehicle_ids_on_approach(self, traci_module, direction: str, *, errors: Optional[list] = None) -> Set[str]:
        ids: Set[str] = set()
        for lid in cfg.approach_lane_ids(self.tls_id, direction):
            try:
                ids.update(traci_module.lane.getLastStepVehicleIDs(lid))
            except Exception:
                if errors is not None:
                    errors.append(lid)
                pass
        return ids

    def _spillback_metrics(self, traci_module, *, quality: Optional[dict] = None) -> dict:
        occupancy_quality = quality if quality is not None else {}
        pressure, full = self.detectors.outbound_occupancy_ratio(
            traci_module, **({"quality": occupancy_quality}
                             if quality is not None or self.stage_a_measurements else {}),
        )
        method = "e2_ratio_v2"
        raw_groups = occupancy_quality.get("analytics_outbound_e2_percent")
        if (self.detectors.available and occupancy_quality.get("spillback_detector_incomplete")
                and not self.stage_a_measurements):
            occupancy_quality["spillback_unavailable"] = True
        if not self.detectors.available or (
                self.stage_a_measurements and occupancy_quality.get("spillback_detector_incomplete")):
            max_pressure = 0.0
            any_full = False
            failed_fallback = False
            read_count = 0
            raw_groups = [] if self.stage_a_measurements else None
            for edge in cfg.OUTGOING_EDGES.get(self.tls_id, []):
                occs = []
                for lid in cfg.edge_lane_ids(edge):
                    try:
                        raw = traci_module.lane.getLastStepOccupancy(lid)
                        if self.stage_a_measurements:
                            occs.append(occupancy_percent(raw))
                        else:
                            value = float(raw)
                            occs.append(value / 100.0 if value > 1.0 else value)
                        read_count += 1
                    except Exception:
                        failed_fallback = True
                        pass
                if not occs:
                    continue
                if raw_groups is not None:
                    raw_groups.append(occs)
                    occs = [value / 100.0 for value in occs]
                ratio = sum(occs) / len(occs)
                max_pressure = max(max_pressure, ratio)
                if ratio >= cfg.BOX_OCCUPANCY_THRESHOLD:
                    any_full = True
            if failed_fallback or read_count == 0:
                method = None
                pressure, full = (0.0, False) if self.stage_a_measurements else (round(max_pressure, 3), any_full)
                occupancy_quality["spillback_unavailable"] = True
            else:
                method = "lane_ratio_v2"
                pressure, full = round(max_pressure, 3), any_full

        return {
            "downstream_edge_full": full,
            "spillback_pressure": pressure,
            "spillback_detected": full or pressure >= cfg.BOX_OCCUPANCY_THRESHOLD,
            **({"spillback_occupancy_method": method} if self.stage_a_measurements else {}),
            **({"analytics_spillback_evidence": {
                "method": method, "percentGroups": raw_groups, "threshold": cfg.BOX_OCCUPANCY_THRESHOLD,
            }} if self.stage_a_measurements and method is not None and raw_groups else {}),
        }

    def _count_blocked_at_entry(self, traci_module, box_blocked: bool, *, quality: Optional[dict] = None) -> int:
        if not box_blocked:
            return 0
        count = 0
        for direction in self.directions:
            errors = [] if quality is not None else None
            for vid in self._vehicle_ids_on_approach(traci_module, direction, errors=errors):
                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    speed = float(traci_module.vehicle.getSpeed(vid))
                    if lane_len > 0 and pos / lane_len >= cfg.BLOCKED_ENTRY_POSITION_RATIO and speed < cfg.HALTING_SPEED_MS:
                        count += 1
                except Exception:
                    if errors is not None:
                        errors.append(vid)
                    pass
            if errors:
                quality["blocked_entry_unavailable"] = True
        return count

    def _yellow_commitment_count(self, traci_module, phase: str, *, quality: Optional[dict] = None) -> int:
        if "YELLOW" not in phase:
            return 0
        count = 0
        for direction in self.directions:
            errors = [] if quality is not None else None
            for vid in self._vehicle_ids_on_approach(traci_module, direction, errors=errors):
                try:
                    lane_id = traci_module.vehicle.getLaneID(vid)
                    lane_len = float(traci_module.lane.getLength(lane_id))
                    pos = float(traci_module.vehicle.getLanePosition(vid))
                    if lane_len > 0 and pos / lane_len >= cfg.YELLOW_COMMITMENT_RATIO:
                        count += 1
                except Exception:
                    if errors is not None:
                        errors.append(vid)
                    pass
            if errors:
                quality["yellow_commitment_unavailable"] = True
        return count
