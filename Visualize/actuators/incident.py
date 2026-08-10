"""IncidentActuator — staged deterministic accident vehicles per intersection."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import configuration.config as cfg
from configuration.model_params import get_registry

log = logging.getLogger(__name__)


def _staging_config() -> Dict[str, Any]:
    raw = get_registry().export_effective_config().get("incident_staging") or {}
    return {
        "placement": str(raw.get("placement") or "stopline").lower(),
        "front_offset_from_stopline_m": float(raw.get("front_offset_from_stopline_m", 3.0)),
        "rear_bumper_gap_m": float(raw.get("rear_bumper_gap_m", 1.2)),
        "car_length_m": float(raw.get("car_length_m", 4.5)),
        "junction_position_ratio": float(raw.get("junction_position_ratio", 0.42)),
    }


def _markers_config() -> Dict[str, Any]:
    raw = get_registry().export_effective_config().get("incident_markers") or {}
    return {
        "enabled": bool(raw.get("enabled", True)),
        "polygon_color": tuple(raw.get("polygon_color") or (255, 0, 0, 100)),
        "zone_length_m": float(raw.get("zone_length_m", 25.0)),
        "warning_sign_enabled": bool(raw.get("warning_sign_enabled", True)),
        "warning_sign_size_m": float(raw.get("warning_sign_size_m", 1.2)),
        "warning_sign_fill": tuple(raw.get("warning_sign_fill") or (255, 193, 7, 255)),
        "warning_sign_border": tuple(raw.get("warning_sign_border") or (220, 38, 38, 255)),
        "warning_sign_mark": tuple(raw.get("warning_sign_mark") or (20, 20, 20, 255)),
        "vehicle_highlight_size_m": float(raw.get("vehicle_highlight_size_m", 0.0)),
    }


def _bypass_config() -> Dict[str, Any]:
    raw = get_registry().export_effective_config().get("incident_bypass") or {}
    return {
        "enabled": bool(raw.get("enabled", True)),
        "lookahead_m": float(raw.get("lookahead_m", 25.0)),
        "safety_buffer_m": float(raw.get("safety_buffer_m", 5.0)),
        "lane_change_duration_s": float(raw.get("lane_change_duration_s", 3.0)),
        "attempt_cooldown_s": float(raw.get("attempt_cooldown_s", 1.5)),
        "min_front_gap_m": float(raw.get("min_front_gap_m", 8.0)),
        "min_rear_gap_m": float(raw.get("min_rear_gap_m", 6.0)),
        "neighbor_window_m": float(raw.get("neighbor_window_m", 30.0)),
    }


def blocker_rear_from_front_pos(front_lane_pos: float, vehicle_length_m: float) -> float:
    """Upstream bumper of a vehicle given TraCI front lanePosition + length."""
    return float(front_lane_pos) - float(vehicle_length_m)


def adjacent_lane_indices(crash_lane_index: int, lane_count: int) -> List[int]:
    """Neighbor lane indices derived from crash lane (not hard-coded [0, 2])."""
    left = crash_lane_index + 1
    right = crash_lane_index - 1
    return [i for i in (left, right) if 0 <= i < lane_count]


@dataclass
class BypassZoneState:
    edge: str
    crash_lane: str
    crash_lane_index: int
    lane_count: int
    blocker_rear_pos: float
    bypass_start_pos: float
    bypass_deadline_pos: float


@dataclass
class BypassCommandState:
    target_lane: int
    last_attempt_t: float
    conservative: bool = False


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


def _gap_center_lane_pos(rear_pos: float, gap: float) -> float:
    """Lane position at the centre of the bumper gap between staged vehicles."""
    return rear_pos + gap / 2.0


def _lane_polyline(
    traci_module,
    lane_id: str,
) -> Tuple[List[Tuple[float, float]], List[float]]:
    raw_shape = traci_module.lane.getShape(lane_id)
    shape = [(float(p[0]), float(p[1])) for p in raw_shape]
    if len(shape) < 2:
        return [], []
    dists = [0.0]
    for i in range(1, len(shape)):
        dists.append(
            dists[-1]
            + math.hypot(shape[i][0] - shape[i - 1][0], shape[i][1] - shape[i - 1][1])
        )
    return shape, dists


def _point_on_shape(
    shape: List[Tuple[float, float]],
    dists: List[float],
    target_dist: float,
) -> Tuple[float, float]:
    if not shape or not dists:
        return 0.0, 0.0
    if target_dist <= 0:
        return shape[0]
    if target_dist >= dists[-1]:
        return shape[-1]
    for i in range(1, len(shape)):
        if dists[i] >= target_dist:
            span = dists[i] - dists[i - 1]
            t = (target_dist - dists[i - 1]) / (span or 1.0)
            x = shape[i - 1][0] + t * (shape[i][0] - shape[i - 1][0])
            y = shape[i - 1][1] + t * (shape[i][1] - shape[i - 1][1])
            return x, y
    return shape[-1]


def _gap_sign_anchor(
    traci_module,
    lane_id: str,
    rear_pos: float,
    gap: float,
) -> Optional[Tuple[float, float, float]]:
    """XY + heading at the bumper gap centre (lane geometry, not vehicle sprites)."""
    shape, dists = _lane_polyline(traci_module, lane_id)
    if len(shape) < 2:
        return None
    gap_pos = _gap_center_lane_pos(rear_pos, gap)
    gap_pos = max(0.0, min(gap_pos, dists[-1]))
    cx, cy = _point_on_shape(shape, dists, gap_pos)
    eps = 0.5
    p0 = _point_on_shape(shape, dists, max(0.0, gap_pos - eps))
    p1 = _point_on_shape(shape, dists, min(dists[-1], gap_pos + eps))
    angle = math.degrees(math.atan2(p1[1] - p0[1], p1[0] - p0[0]))
    return cx, cy, angle


def _lane_zone_shape(
    traci_module,
    lane_id: str,
    zone_length_m: float,
) -> List[Tuple[float, float]]:
    """Closed polygon band on the lane near the stop line (uses lane geometry)."""
    shape, dists = _lane_polyline(traci_module, lane_id)
    if len(shape) < 2:
        return []

    half_w = float(traci_module.lane.getWidth(lane_id)) / 2.0
    total = dists[-1]
    start_dist = max(0.0, total - zone_length_m)
    samples = max(4, int(zone_length_m / 3.0))

    center: List[Tuple[float, float]] = []
    for i in range(samples + 1):
        d = start_dist + (total - start_dist) * i / samples
        center.append(_point_on_shape(shape, dists, d))
    if len(center) < 2:
        return []

    left_pts: List[Tuple[float, float]] = []
    right_pts: List[Tuple[float, float]] = []
    for i, (x, y) in enumerate(center):
        if i < len(center) - 1:
            x2, y2 = center[i + 1]
        else:
            x0, y0 = center[i - 1]
            x2, y2 = x + (x - x0), y + (y - y0)
        dx, dy = x2 - x, y2 - y
        ln = math.hypot(dx, dy) or 1.0
        nx, ny = -dy / ln, dx / ln
        left_pts.append((x + nx * half_w, y + ny * half_w))
        right_pts.append((x - nx * half_w, y - ny * half_w))

    return left_pts + list(reversed(right_pts))


def _warning_triangle_vertices(
    cx: float,
    cy: float,
    angle_deg: float,
    size_m: float,
) -> List[Tuple[float, float]]:
    """Equilateral warning triangle; apex points along lane heading."""
    rad = math.radians(angle_deg)
    perp = rad + math.pi / 2
    half_base = size_m * 0.52
    apex = (
        cx + size_m * 0.42 * math.cos(rad),
        cy + size_m * 0.42 * math.sin(rad),
    )
    base_c = (
        cx - size_m * 0.38 * math.cos(rad),
        cy - size_m * 0.38 * math.sin(rad),
    )
    left = (
        base_c[0] + half_base * math.cos(perp),
        base_c[1] + half_base * math.sin(perp),
    )
    right = (
        base_c[0] - half_base * math.cos(perp),
        base_c[1] - half_base * math.sin(perp),
    )
    return [apex, left, right]


def _scale_shape(
    shape: List[Tuple[float, float]],
    cx: float,
    cy: float,
    scale: float,
) -> List[Tuple[float, float]]:
    return [((x - cx) * scale + cx, (y - cy) * scale + cy) for x, y in shape]


def _exclamation_shapes(
    cx: float,
    cy: float,
    angle_deg: float,
    size_m: float,
) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """Bar + square-dot for the black '!' mark inside the warning triangle."""
    rad = math.radians(angle_deg)
    perp = rad + math.pi / 2
    # Vertical axis of triangle (apex direction = rad).
    bar_half_len = size_m * 0.16
    bar_half_w = size_m * 0.045
    bar_cx = cx + size_m * 0.06 * math.cos(rad)
    bar_cy = cy + size_m * 0.06 * math.sin(rad)
    bar = [
        (
            bar_cx + bar_half_len * math.cos(rad) + bar_half_w * math.cos(perp),
            bar_cy + bar_half_len * math.sin(rad) + bar_half_w * math.sin(perp),
        ),
        (
            bar_cx + bar_half_len * math.cos(rad) - bar_half_w * math.cos(perp),
            bar_cy + bar_half_len * math.sin(rad) - bar_half_w * math.sin(perp),
        ),
        (
            bar_cx - bar_half_len * math.cos(rad) - bar_half_w * math.cos(perp),
            bar_cy - bar_half_len * math.sin(rad) - bar_half_w * math.sin(perp),
        ),
        (
            bar_cx - bar_half_len * math.cos(rad) + bar_half_w * math.cos(perp),
            bar_cy - bar_half_len * math.sin(rad) + bar_half_w * math.sin(perp),
        ),
    ]
    dot_r = size_m * 0.055
    dot_cx = cx - size_m * 0.18 * math.cos(rad)
    dot_cy = cy - size_m * 0.18 * math.sin(rad)
    # Axis-aligned square in local rad/perp frame.
    dot = [
        (
            dot_cx + dot_r * math.cos(rad) + dot_r * math.cos(perp),
            dot_cy + dot_r * math.sin(rad) + dot_r * math.sin(perp),
        ),
        (
            dot_cx + dot_r * math.cos(rad) - dot_r * math.cos(perp),
            dot_cy + dot_r * math.sin(rad) - dot_r * math.sin(perp),
        ),
        (
            dot_cx - dot_r * math.cos(rad) - dot_r * math.cos(perp),
            dot_cy - dot_r * math.sin(rad) - dot_r * math.sin(perp),
        ),
        (
            dot_cx - dot_r * math.cos(rad) + dot_r * math.cos(perp),
            dot_cy - dot_r * math.sin(rad) + dot_r * math.sin(perp),
        ),
    ]
    return bar, dot


def _add_polygon(
    traci_module,
    polygon_id: str,
    shape: List[Tuple[float, float]],
    color: Tuple[int, ...],
    *,
    layer: int = 110,
    line_width: float = 1.0,
) -> None:
    if polygon_id in traci_module.polygon.getIDList():
        traci_module.polygon.remove(polygon_id)
    traci_module.polygon.add(
        polygon_id,
        shape,
        color=color,
        fill=True,
        polygonType="incident_sign",
        layer=layer,
        lineWidth=line_width,
    )


class IncidentActuator:
    def __init__(self) -> None:
        self.active_by_node: Dict[str, List[str]] = {}
        self.markers_by_node: Dict[str, Dict[str, str]] = {}
        self.zone_by_node: Dict[str, BypassZoneState] = {}
        self.bypass_cmds: Dict[str, BypassCommandState] = {}
        self._lc_mode_restore: Dict[str, int] = {}

    def vehicle_ids(self, node_id: str) -> List[str]:
        return list(self.active_by_node.get(node_id, []))

    def crash_lane_id(self, node_id: str, direction: Optional[str] = None) -> str:
        direction = direction or cfg.incident_approach_direction(node_id)
        tls = cfg.NODE_TO_TLS[node_id]
        edge = cfg.APPROACH_EDGES[tls][direction]
        return f"{edge}_1"

    def _clear_markers(self, traci_module, node_id: str) -> None:
        state = self.markers_by_node.pop(node_id, None)
        if not state:
            return
        for key in (
            "polygon_id",
            "sign_border_id",
            "sign_fill_id",
            "sign_bang_id",
            "sign_dot_id",
            # legacy keys from earlier marker versions
            "sign_id",
        ):
            polygon_id = state.get(key)
            if not polygon_id:
                continue
            try:
                if polygon_id in traci_module.polygon.getIDList():
                    traci_module.polygon.remove(polygon_id)
            except Exception as e:
                log.debug("incident polygon remove %s: %s", polygon_id, e)
        poi_id = state.get("poi_id")
        if poi_id:
            try:
                if poi_id in traci_module.poi.getIDList():
                    traci_module.poi.remove(poi_id)
            except Exception as e:
                log.debug("incident poi remove %s: %s", poi_id, e)

    def _restore_lc_modes(self, traci_module, vehicle_ids: Optional[List[str]] = None) -> None:
        vids = (
            list(vehicle_ids)
            if vehicle_ids is not None
            else list(self._lc_mode_restore.keys())
        )
        for vid in vids:
            mode = self._lc_mode_restore.pop(vid, None)
            if mode is None:
                continue
            try:
                if vid in traci_module.vehicle.getIDList():
                    traci_module.vehicle.setLaneChangeMode(vid, mode)
            except Exception as e:
                log.debug("restore lc mode %s: %s", vid, e)

    def _clear_bypass_state(self, traci_module, node_id: str) -> None:
        zone = self.zone_by_node.pop(node_id, None)
        remaining_edges = {z.edge for z in self.zone_by_node.values()}
        stale: List[str] = []
        for vid in list(self.bypass_cmds.keys()):
            try:
                if vid not in traci_module.vehicle.getIDList():
                    stale.append(vid)
                    continue
                road = str(traci_module.vehicle.getRoadID(vid))
                if zone is not None and road == zone.edge:
                    stale.append(vid)
                elif road not in remaining_edges:
                    # Not on another active incident approach — drop bookkeeping.
                    stale.append(vid)
            except Exception:
                stale.append(vid)
        for vid in stale:
            self.bypass_cmds.pop(vid, None)
        self._restore_lc_modes(traci_module, stale)

    def _apply_warning_sign(
        self,
        traci_module,
        node_id: str,
        lane_id: str,
        rear_pos: float,
        gap: float,
        conf: Dict[str, Any],
    ) -> Dict[str, str]:
        """Draw classic warning triangle: red border, yellow fill, black !."""
        ids = {
            "sign_border_id": f"incident_sign_border_{node_id}",
            "sign_fill_id": f"incident_sign_fill_{node_id}",
            "sign_bang_id": f"incident_sign_bang_{node_id}",
            "sign_dot_id": f"incident_sign_dot_{node_id}",
        }
        anchor = _gap_sign_anchor(traci_module, lane_id, rear_pos, gap)
        if anchor is None:
            return {k: "" for k in ids}
        cx, cy, angle = anchor
        size_m = conf["warning_sign_size_m"]
        tri_outer = _warning_triangle_vertices(cx, cy, angle, size_m)
        tri_inner = _scale_shape(tri_outer, cx, cy, 0.72)
        bang, dot = _exclamation_shapes(cx, cy, angle, size_m)
        _add_polygon(
            traci_module,
            ids["sign_border_id"],
            tri_outer,
            conf["warning_sign_border"],
            layer=112,
        )
        _add_polygon(
            traci_module,
            ids["sign_fill_id"],
            tri_inner,
            conf["warning_sign_fill"],
            layer=113,
        )
        _add_polygon(
            traci_module,
            ids["sign_bang_id"],
            bang,
            conf["warning_sign_mark"],
            layer=114,
        )
        _add_polygon(
            traci_module,
            ids["sign_dot_id"],
            dot,
            conf["warning_sign_mark"],
            layer=115,
        )
        return ids

    def clear(self, traci_module, node_id: str) -> None:
        self._clear_bypass_state(traci_module, node_id)
        self._clear_markers(traci_module, node_id)
        for vid in list(self.active_by_node.get(node_id, [])):
            try:
                if vid in traci_module.vehicle.getIDList():
                    traci_module.vehicle.remove(vid)
            except Exception as e:
                log.debug("incident remove %s: %s", vid, e)
        self.active_by_node.pop(node_id, None)

    def _apply_markers(
        self,
        traci_module,
        node_id: str,
        lane_id: str,
        vehicle_ids: List[str],
        rear_pos: float,
        gap: float,
    ) -> None:
        """Visual-only markers — must never break incident staging."""
        conf = _markers_config()
        if not conf["enabled"]:
            return

        polygon_id = f"incident_zone_{node_id}"
        sign_ids: Dict[str, str] = {
            "sign_border_id": "",
            "sign_fill_id": "",
            "sign_bang_id": "",
            "sign_dot_id": "",
        }
        try:
            shape = _lane_zone_shape(traci_module, lane_id, conf["zone_length_m"])
            if shape:
                try:
                    _add_polygon(
                        traci_module,
                        polygon_id,
                        shape,
                        conf["polygon_color"],
                        layer=100,
                    )
                except Exception as e:
                    log.warning("incident polygon add failed node=%s: %s", node_id, e)
                    polygon_id = ""

            if conf["warning_sign_enabled"]:
                try:
                    sign_ids = self._apply_warning_sign(
                        traci_module,
                        node_id,
                        lane_id,
                        rear_pos,
                        gap,
                        conf,
                    )
                except Exception as e:
                    log.warning("incident warning sign failed node=%s: %s", node_id, e)

            hl_size = conf["vehicle_highlight_size_m"]
            if hl_size > 0:
                for vid in vehicle_ids:
                    try:
                        if vid in traci_module.vehicle.getIDList():
                            traci_module.vehicle.highlight(
                                vid,
                                color=conf["warning_sign_border"],
                                size=hl_size,
                            )
                    except Exception:
                        pass

            self.markers_by_node[node_id] = {
                "polygon_id": polygon_id,
                **sign_ids,
                "lane_id": lane_id,
            }
        except Exception as e:
            log.warning("incident markers skipped node=%s: %s", node_id, e)

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

    def _measure_blocker_rear_pos(
        self,
        traci_module,
        vehicle_ids: List[str],
        fallback_front_pos: float,
        fallback_length: float,
    ) -> float:
        """Upstream edge of nearest blocker (TraCI front lanePosition − length)."""
        rears: List[float] = []
        for vid in vehicle_ids:
            try:
                if vid not in traci_module.vehicle.getIDList():
                    continue
                front = float(traci_module.vehicle.getLanePosition(vid))
                length = float(traci_module.vehicle.getLength(vid))
                rears.append(blocker_rear_from_front_pos(front, length))
            except Exception:
                continue
        if rears:
            return min(rears)
        return blocker_rear_from_front_pos(fallback_front_pos, fallback_length)

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
        bypass_conf = _bypass_config()
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
                self._spawn_frozen(
                    traci_module, vid, rid, approach_lane, pos, lane_index
                )
                vids.append(vid)
        except Exception as e:
            for staged in vids:
                try:
                    traci_module.vehicle.remove(staged)
                except Exception:
                    pass
            self._clear_markers(traci_module, node_id)
            raise RuntimeError(
                f"incident vehicle staging failed at {node_id} {direction}"
            ) from e

        if len(vids) != 2:
            raise RuntimeError(f"incident requires 2 vehicles at {node_id}, got {len(vids)}")

        self.active_by_node[node_id] = vids

        measured_len = car_len
        try:
            measured_len = float(traci_module.vehicle.getLength(vids[0]))
        except Exception:
            pass
        blocker_rear = self._measure_blocker_rear_pos(
            traci_module, vids, rear_pos, measured_len
        )
        lookahead = max(1.0, bypass_conf["lookahead_m"])
        safety = max(0.5, bypass_conf["safety_buffer_m"])
        if safety >= lookahead:
            safety = max(0.5, lookahead * 0.2)
        deadline = blocker_rear - safety
        start = blocker_rear - lookahead
        if start >= deadline:
            start = deadline - 1.0
        start = max(0.0, start)
        deadline = max(start + 0.5, deadline)

        try:
            lane_count = int(traci_module.edge.getLaneNumber(edge))
        except Exception:
            lane_count = int(cfg.LANES_PER_APPROACH)

        self.zone_by_node[node_id] = BypassZoneState(
            edge=edge,
            crash_lane=approach_lane,
            crash_lane_index=lane_index,
            lane_count=lane_count,
            blocker_rear_pos=blocker_rear,
            bypass_start_pos=start,
            bypass_deadline_pos=deadline,
        )

        self._apply_markers(
            traci_module,
            node_id,
            approach_lane,
            vids,
            rear_pos,
            gap,
        )
        log.info(
            "Incident staged node=%s dir=%s lane=%s pos=%.1f/%.1f gap=%.2f "
            "blocker_rear=%.1f bypass=[%.1f, %.1f) vehicles=%s",
            node_id,
            direction,
            approach_lane,
            rear_pos,
            front_pos,
            gap,
            blocker_rear,
            start,
            deadline,
            vids,
        )
        return vids

    def status(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for node, vids in self.active_by_node.items():
            zone = self.zone_by_node.get(node)
            out[node] = {
                "vehicles": list(vids),
                "markers": dict(self.markers_by_node.get(node, {})),
                "zone": None
                if zone is None
                else {
                    "edge": zone.edge,
                    "crash_lane": zone.crash_lane,
                    "crash_lane_index": zone.crash_lane_index,
                    "lane_count": zone.lane_count,
                    "blocker_rear_pos": zone.blocker_rear_pos,
                    "bypass_start_pos": zone.bypass_start_pos,
                    "bypass_deadline_pos": zone.bypass_deadline_pos,
                },
            }
        return out

    def _neighbor_gaps(
        self,
        traci_module,
        edge: str,
        lane_index: int,
        ego_pos: float,
        ego_len: float,
        window_m: float,
        incident_ids: set,
    ) -> Tuple[float, float, int]:
        """Return (front_gap, rear_gap, nearby_count) on a candidate lane."""
        lane_id = f"{edge}_{lane_index}"
        front_gap = window_m
        rear_gap = window_m
        nearby = 0
        try:
            vids = list(traci_module.lane.getLastStepVehicleIDs(lane_id))
        except Exception:
            return front_gap, rear_gap, nearby
        for oid in vids:
            if oid in incident_ids:
                continue
            try:
                opos = float(traci_module.vehicle.getLanePosition(oid))
                olen = float(traci_module.vehicle.getLength(oid))
            except Exception:
                continue
            # Ego front at ego_pos; other front at opos (SUMO TraCI semantics).
            delta = opos - ego_pos
            if abs(delta) <= window_m:
                nearby += 1
            if delta > 0:
                gap = delta - olen
                if gap < front_gap:
                    front_gap = max(0.0, gap)
            else:
                gap = -delta - ego_len
                if gap < rear_gap:
                    rear_gap = max(0.0, gap)
        return front_gap, rear_gap, nearby

    def _pick_bypass_lane(
        self,
        traci_module,
        vid: str,
        zone: BypassZoneState,
        conf: Dict[str, Any],
        incident_ids: set,
        *,
        conservative: bool,
    ) -> Optional[int]:
        try:
            ego_pos = float(traci_module.vehicle.getLanePosition(vid))
            ego_len = float(traci_module.vehicle.getLength(vid))
            ego_lane_idx = int(traci_module.vehicle.getLaneIndex(vid))
        except Exception:
            return None

        candidates = adjacent_lane_indices(zone.crash_lane_index, zone.lane_count)
        if not candidates:
            return None

        min_front = conf["min_front_gap_m"]
        min_rear = conf["min_rear_gap_m"]
        if conservative:
            min_front *= 1.25
            min_rear *= 1.25
        window = conf["neighbor_window_m"]

        scored: List[Tuple[float, int]] = []
        for target in candidates:
            direction = 1 if target > ego_lane_idx else -1
            try:
                if hasattr(traci_module.vehicle, "couldChangeLane"):
                    if not traci_module.vehicle.couldChangeLane(vid, direction):
                        continue
            except Exception:
                pass

            front_gap, rear_gap, nearby = self._neighbor_gaps(
                traci_module,
                zone.edge,
                target,
                ego_pos,
                ego_len,
                window,
                incident_ids,
            )
            if front_gap < min_front or rear_gap < min_rear:
                continue
            # Higher score = freer lane (larger gaps, fewer nearby vehicles).
            score = front_gap + rear_gap - 2.0 * nearby
            scored.append((score, target))

        if not scored:
            return None
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]

    def _issue_change_lane(
        self,
        traci_module,
        vid: str,
        target_lane: int,
        sim_t: float,
        conf: Dict[str, Any],
        *,
        conservative: bool,
    ) -> None:
        duration = conf["lane_change_duration_s"]
        try:
            if vid not in self._lc_mode_restore:
                try:
                    self._lc_mode_restore[vid] = int(
                        traci_module.vehicle.getLaneChangeMode(vid)
                    )
                except Exception:
                    self._lc_mode_restore[vid] = 1621
            # Mild strategic+cooperative enablement without full permissive takeover.
            traci_module.vehicle.setLaneChangeMode(vid, 1621)
            traci_module.vehicle.changeLane(vid, target_lane, duration)
            self.bypass_cmds[vid] = BypassCommandState(
                target_lane=target_lane,
                last_attempt_t=sim_t,
                conservative=conservative,
            )
        except Exception as e:
            log.debug("changeLane failed %s -> %s: %s", vid, target_lane, e)

    def _try_bypass(
        self,
        traci_module,
        vid: str,
        zone: BypassZoneState,
        conf: Dict[str, Any],
        incident_ids: set,
        sim_t: float,
        *,
        conservative: bool,
    ) -> None:
        cmd = self.bypass_cmds.get(vid)
        cooldown = conf["attempt_cooldown_s"]
        if conservative:
            cooldown = max(cooldown, conf["lane_change_duration_s"])

        if cmd is not None:
            try:
                cur_lane = int(traci_module.vehicle.getLaneIndex(vid))
                if cur_lane == cmd.target_lane:
                    # Completed — restore LC mode and drop command state.
                    self._restore_lc_modes(traci_module, [vid])
                    self.bypass_cmds.pop(vid, None)
                    return
            except Exception:
                self.bypass_cmds.pop(vid, None)
                return
            if sim_t - cmd.last_attempt_t < cooldown:
                return

        target = self._pick_bypass_lane(
            traci_module,
            vid,
            zone,
            conf,
            incident_ids,
            conservative=conservative,
        )
        if target is None:
            return
        self._issue_change_lane(
            traci_module,
            vid,
            target,
            sim_t,
            conf,
            conservative=conservative,
        )

    def _tick_bypass_for_node(
        self,
        traci_module,
        node_id: str,
        zone: BypassZoneState,
        incident_ids: set,
        conf: Dict[str, Any],
        sim_t: float,
    ) -> None:
        try:
            on_edge = list(traci_module.edge.getLastStepVehicleIDs(zone.edge))
        except Exception:
            return

        for vid in on_edge:
            if vid in incident_ids:
                continue
            try:
                lane_id = str(traci_module.vehicle.getLaneID(vid))
                if lane_id != zone.crash_lane:
                    # Left crash lane — clear pending bypass bookkeeping.
                    if vid in self.bypass_cmds:
                        self._restore_lc_modes(traci_module, [vid])
                        self.bypass_cmds.pop(vid, None)
                    continue
                pos = float(traci_module.vehicle.getLanePosition(vid))
            except Exception:
                continue

            if zone.bypass_start_pos <= pos < zone.bypass_deadline_pos:
                self._try_bypass(
                    traci_module,
                    vid,
                    zone,
                    conf,
                    incident_ids,
                    sim_t,
                    conservative=False,
                )
            elif zone.bypass_deadline_pos <= pos < zone.blocker_rear_pos:
                # Past deadline: still track / retry conservatively; do not drop.
                self._try_bypass(
                    traci_module,
                    vid,
                    zone,
                    conf,
                    incident_ids,
                    sim_t,
                    conservative=True,
                )

    def tick(self, traci_module) -> None:
        marker_conf = _markers_config()
        bypass_conf = _bypass_config()
        hl_size = marker_conf["vehicle_highlight_size_m"]
        try:
            sim_t = float(traci_module.simulation.getTime())
        except Exception:
            sim_t = 0.0

        for node_id, vids in list(self.active_by_node.items()):
            alive: List[str] = []
            for vid in vids:
                try:
                    if vid not in traci_module.vehicle.getIDList():
                        continue
                    traci_module.vehicle.setSpeed(vid, 0)
                    traci_module.vehicle.setSpeedMode(vid, 0)
                    traci_module.vehicle.setLaneChangeMode(vid, 0)
                    if hl_size > 0:
                        traci_module.vehicle.highlight(
                            vid,
                            color=marker_conf["warning_sign_border"],
                            size=hl_size,
                        )
                    alive.append(vid)
                except Exception:
                    pass
            if alive:
                self.active_by_node[node_id] = alive
                zone = self.zone_by_node.get(node_id)
                if zone is not None and bypass_conf["enabled"]:
                    self._tick_bypass_for_node(
                        traci_module,
                        node_id,
                        zone,
                        set(alive),
                        bypass_conf,
                        sim_t,
                    )
            else:
                self._clear_bypass_state(traci_module, node_id)
                self._clear_markers(traci_module, node_id)
                self.active_by_node.pop(node_id, None)
