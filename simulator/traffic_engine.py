"""
traffic_engine.py — Tang Simulation Layer chinh

Gom:
  PhaseController     — dieu khien pha den, 1 instance / intersection
  IntersectionRuntime  — trang thai runtime cua 1 giao lo (4 huong x 3 lan)
  CityNetworkEngine    — dieu phoi toan bo mang luoi 4 giao lo + 5 boundary

Phase 2.5+ (P1-P4):
  P1 weather factor ap dung 1 lan
  P2 simulation_time_sec, segment_travel_times, arrival_rate, waiting_reason
  P3 multi-lane separation + occupancy lateral
  P4 phase offset theo chu ky

Phase 3 (U1-U9):
  U1 capacitated edge  U2 box blocking  U3 discharge headway
  U4 moto filter-to-front  U5 right-on-red conflict  U6 yellow commitment
  U7 OD matrix  U8 emergency preemption  U9 weather headway/spawn
"""
import math
import random
import time
import threading
import logging
from typing import Dict, List, Optional

from models import (
    Vehicle, VehicleState, VEHICLE_CLASS_WEIGHTS, VEHICLE_LENGTH_METERS,
)
from scenarios import (
    SCENARIOS, DIRECTIONS, VALID_DESTINATIONS, MOVEMENT_TYPE, TURN_PROBABILITIES,
    INTERSECTION_TURN_RULES, REROUTE_PROBABILITY,
    DENSITY_THRESHOLDS_PCU, DENSITY_THRESHOLDS_PER_DIRECTION, DENSITY_THRESHOLDS_INTERSECTION,
    PHASE_SEQUENCE, PHASE_DURATIONS, PHASE_COLORS,
    INTER_VEHICLE_GAP_M, MOTORCYCLE_LATERAL_FACTOR, APPROACH_LANE_LENGTH_M,
    CROSSING_LENGTH_M, PRIORITY_VEHICLE_PROBABILITY,
    BUS_DWELL_PROBABILITY_PER_SEC, BUS_DWELL_DURATION_SEC, BUS_DWELL_SPEED_KMH,
    SCENARIOS_WITH_RAMP, SPAWN_CURVE_DURATION_SEC,
    DISCHARGE_HEADWAY_SEC, BOX_OCCUPANCY_THRESHOLD, RIGHT_ON_RED_CONFLICT,
    YELLOW_COMMITMENT_POINT, DIRECTION_TO_GREEN_PHASE,
)
from traffic_flow_speed import (
    WEATHER_CAPACITY_FACTOR, WEATHER_HEADWAY_FACTOR, WEATHER_SPAWN_FACTOR,
    greenshields_speed, K_JAM_PCU_PER_KM,
)
import network as net

log = logging.getLogger(__name__)

STOP_LINE  = 1.0   # vi tri vach dung trong khong gian intersection (0.0 -> 2.0)
EXIT_POINT = 2.0   # vi tri thoat khoi intersection
LANES = ("straight", "left", "right")

# Phase offset: pha dong bo gia tao (chua phai Green Wave toi uu)
PHASE_OFFSETS = {"A": 0.0, "B": 15.0, "C": 12.0, "D": 25.0}


def density_label(pcu_equivalent: float) -> str:
    """Nguong per-direction — dung trong snapshot VehicleSensor."""
    for label, (lo, hi) in DENSITY_THRESHOLDS_PER_DIRECTION.items():
        if lo <= pcu_equivalent < hi:
            return label
    return "HIGH"


def density_label_intersection(total_pcu: float) -> str:
    """Nguong tong giao lo — dung cho re-route va Intersection entity."""
    for label, (lo, hi) in DENSITY_THRESHOLDS_INTERSECTION.items():
        if lo <= total_pcu < hi:
            return label
    return "HIGH"


class PhaseController:
    """State machine dieu khien pha den cho 1 giao lo. Cac TrafficLight chi doc ket qua.

    QUAN TRONG: dung thoi gian TICH LUY THEO dt MO PHONG, khong dung
    time.time() (wall-clock). Neu dung wall-clock, khi vong lap mo phong
    chay nhanh hon thoi gian thuc (vi du trong test, hoac khi may cham
    khien simulation loop bi cham lai), pha den se lech hoan toan khoi
    logic mo phong — day la 1 loi thuc te da phat hien khi debug (den
    khong bao gio chuyen pha trong pytest vi test chay het 200s mo phong
    chi trong vai chuc mili-giay thuc).
    """

    def __init__(self, initial_offset_sec: float = 0.0):
        self._lock = threading.Lock()
        self._custom_green: Optional[int] = None

        # An dan offset qua cac pha trong chu ky (an toan voi moi gia tri)
        cycle_length = sum(PHASE_DURATIONS.values())  # 45+5+45+5 = 100s
        remaining = initial_offset_sec % cycle_length
        self._phase_idx = 0
        self._elapsed_in_phase = 0.0
        for i, phase in enumerate(PHASE_SEQUENCE):
            dur = PHASE_DURATIONS[phase]
            if remaining < dur:
                self._phase_idx = i
                self._elapsed_in_phase = remaining
                break
            remaining -= dur

    @property
    def current_phase(self) -> str:
        return PHASE_SEQUENCE[self._phase_idx]

    @property
    def next_phase(self) -> str:
        return PHASE_SEQUENCE[(self._phase_idx + 1) % len(PHASE_SEQUENCE)]

    @property
    def phase_duration(self) -> int:
        base = PHASE_DURATIONS[self.current_phase]
        if self._custom_green and "GREEN" in self.current_phase:
            return self._custom_green
        return base

    @property
    def remaining_seconds(self) -> int:
        return max(0, int(self.phase_duration - self._elapsed_in_phase))

    def get_color(self, direction: str) -> str:
        return PHASE_COLORS[self.current_phase][direction]

    def is_green(self, direction: str) -> bool:
        return self.get_color(direction) == "green"

    def tick(self, dt: float = 0.0):
        with self._lock:
            self._elapsed_in_phase += dt
            if self._elapsed_in_phase >= self.phase_duration:
                overflow = self._elapsed_in_phase - self.phase_duration
                self._phase_idx = (self._phase_idx + 1) % len(PHASE_SEQUENCE)
                self._elapsed_in_phase = overflow

    def force_phase(self, phase: str):
        with self._lock:
            if phase in PHASE_SEQUENCE:
                self._phase_idx = PHASE_SEQUENCE.index(phase)
                self._elapsed_in_phase = 0.0

    def set_green_duration(self, seconds: int):
        with self._lock:
            self._custom_green = max(10, min(120, seconds))


class IntersectionRuntime:
    """Trang thai runtime cua 1 giao lo: 4 huong x 3 lan + PhaseController rieng."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.vehicles: Dict[str, Dict[str, List[Vehicle]]] = {
            d: {lane: [] for lane in LANES} for d in DIRECTIONS
        }
        self.phase_controller = PhaseController(
            initial_offset_sec=PHASE_OFFSETS.get(node_id, 0.0)
        )
        self.blocked_direction: Optional[str] = None
        self.incidents: List[dict] = []
        self.preemption_active: bool = False  # Phase3 U8
        # Arrival rate real-time (PCU/giay), flush moi 1s mo phong
        self._pcu_arrived_this_sec: Dict[str, float] = {d: 0.0 for d in DIRECTIONS}
        self._pcu_arrival_rate: Dict[str, float] = {d: 0.0 for d in DIRECTIONS}
        self._arrival_reset_timer: float = 0.0

    def get_all_vehicles_in_direction(self, direction: str) -> List[Vehicle]:
        return (
            self.vehicles[direction]["straight"]
            + self.vehicles[direction]["left"]
            + self.vehicles[direction]["right"]
        )

    def get_density_level(self) -> str:
        total_pcu = sum(
            v.pcu_factor
            for d in DIRECTIONS
            for v in self.get_all_vehicles_in_direction(d)
            if v.state != VehicleState.EXITED_NETWORK
        )
        return density_label_intersection(total_pcu)


class CityNetworkEngine:
    """
    Dieu phoi toan bo mang luoi: 4 IntersectionRuntime + xe TRANSIT tren edges.

    KHONG import bat ky thu gi lien quan NGSI-LD/Orion — day la tang Simulation
    Layer thuan tuy, co the test doc lap bang pytest (xem test_simulation_standalone.py).
    """

    def __init__(self):
        self.intersections: Dict[str, IntersectionRuntime] = {
            node: IntersectionRuntime(node) for node in net.INTERSECTION_NODES
        }
        self.edges_vehicles: Dict[str, List[Vehicle]] = {eid: [] for eid in net.EDGES}
        self.current_scenario = "normal"
        self.per_node_scenario: Dict[str, str] = {}
        self.trip_records: List[dict] = []
        self._lock = threading.Lock()
        self.last_spawn_count = 0
        self.simulation_time_sec: float = 0.0

    # ── Scenario control ────────────────────────────────────
    def set_scenario(self, scenario: str, target_intersection: Optional[str] = None,
                     target_direction: Optional[str] = None):
        with self._lock:
            self.current_scenario = scenario
            self.per_node_scenario = {}  # reset per-node khi set global moi
            for ix in self.intersections.values():
                ix.blocked_direction = None

            if scenario == "accident":
                node = target_intersection or random.choice(net.INTERSECTION_NODES)
                direction = target_direction or random.choice(DIRECTIONS)
                self.intersections[node].blocked_direction = direction
                ix = self.intersections[node]
                ix.incidents.append({
                    "type": "MINOR_ACCIDENT", "direction": direction, "time": time.time(),
                })
                now = time.time()
                ix.incidents = [i for i in ix.incidents if now - i["time"] < 3600][-200:]
                log.info(f"Accident scenario: {node}-{direction} blocked")

    def _weather(self) -> str:
        return SCENARIOS[self.current_scenario]["weather"]

    def _capacity_factor(self) -> float:
        """Giu lai de tuong thich; P1-A khong con nhan kep voi WEATHER_CAPACITY_FACTOR."""
        return WEATHER_CAPACITY_FACTOR.get(self._weather(), 1.0)

    def _get_spawn_multiplier(self, effective_scenario: str) -> float:
        """P7-B: he so spawn curve theo effective scenario (peak-only)."""
        if effective_scenario not in SCENARIOS_WITH_RAMP:
            return 1.0
        t = self.simulation_time_sec
        D = SPAWN_CURVE_DURATION_SEC
        if D <= 0:
            return 1.0
        ramp_up = D * 0.15
        peak_end = D * 0.85
        if t < ramp_up:
            return 0.3 + 0.7 * (t / ramp_up)
        elif t < peak_end:
            return 1.0
        elif t < D:
            return 1.0 - 0.7 * ((t - peak_end) / (D - peak_end))
        return 0.3

    # LUU Y: da XOA ham _local_density_pcu_per_km() va viec dung
    # compute_effective_speed() (Greenshields ap dung tren toan bo
    # hang doi/edge) o day. Day chinh la nguyen nhan gay 2 bug deadlock
    # gia da phat hien va fix (xem PHASE2-V3-TEST-GUIDE.md).

    # ── Phase 3 U1 — capacitated edge helpers ────────────────
    def edge_max_pcu(self, edge_id: str) -> float:
        return net.EDGES[edge_id]["length_m"] * K_JAM_PCU_PER_KM / 1000.0

    def edge_current_pcu(self, edge_id: str) -> float:
        return sum(
            v.pcu_factor for v in self.edges_vehicles[edge_id]
            if v.state != VehicleState.EXITED_NETWORK
        )

    def _edge_occupancy_ratio(self, edge_id: str) -> float:
        mx = self.edge_max_pcu(edge_id)
        if mx <= 0:
            return 0.0
        return self.edge_current_pcu(edge_id) / mx

    def _edge_is_full(self, edge_id: str, threshold: float = 1.0) -> bool:
        return self.edge_current_pcu(edge_id) >= self.edge_max_pcu(edge_id) * threshold

    def _predicted_exit_edge(self, vehicle: Vehicle, node_id: str) -> Optional[str]:
        """Edge xe sẽ ra sau khi qua nút (từ route). None nếu sắp EXIT network."""
        # Khi còn trên edge vào nút: route_index trỏ node hiện tại trên route (boundary/prev).
        # next_node = node_id = route[route_index+1]; exit = route[route_index+2]
        ri = vehicle.route_index
        if ri + 2 >= len(vehicle.route):
            return None
        next_after = vehicle.route[ri + 2]
        try:
            return net.get_edge_between(node_id, next_after)
        except ValueError:
            return None

    def _min_gap_units(self, v: Vehicle, ahead: Vehicle, space_scale: float) -> float:
        """U3+U9: headway theo loại xe + hệ số mưa; đứng yên → gap cố định."""
        headway_s = DISCHARGE_HEADWAY_SEC.get(v.vehicle_class, 2.0)
        weather_hw = WEATHER_HEADWAY_FACTOR.get(self._weather(), 1.0)
        headway_s *= weather_hw
        if ahead.speed_kmh < 1.0 and v.speed_kmh < 1.0:
            min_gap_m = v.length_meters + INTER_VEHICLE_GAP_M
        else:
            ref_speed = max(ahead.speed_kmh, v.speed_kmh, 5.0)
            min_gap_m = v.length_meters + (ref_speed / 3.6) * headway_s
        return min_gap_m / max(space_scale, 1.0)

    # ══════════════════════════════════════════════════════════
    # SPAWN LOOP — chi spawn tai Boundary Node
    # ══════════════════════════════════════════════════════════
    def tick_spawn(self):
        """
        P5+P7+U1+U7+U9: route trước, OD theo scenario, weather spawn factor,
        skip nếu first_edge đầy PCU.
        """
        global_cfg = SCENARIOS[self.current_scenario]
        spawned = 0
        weather_spawn = WEATHER_SPAWN_FACTOR.get(self._weather(), 1.0)

        with self._lock:
            for boundary in net.BOUNDARY_NODES:
                first_hops = {
                    e["to"] for e in net.EDGES.values()
                    if e["from"] == boundary and e["to"] in net.INTERSECTION_NODES
                }
                rates_to_consider = [global_cfg["boundary_spawn_rate"].get(boundary, 3)]
                for hop in first_hops:
                    if hop in self.per_node_scenario:
                        sc = self.per_node_scenario[hop]
                        rates_to_consider.append(
                            SCENARIOS[sc]["boundary_spawn_rate"].get(boundary, 0)
                        )
                cap = max(rates_to_consider) if rates_to_consider else 3
                n = max(0, int(cap + random.uniform(-cap * 0.2, cap * 0.2)))

                for _ in range(n):
                    destination = net.pick_destination(boundary, scenario=self.current_scenario)
                    route = net.compute_route(boundary, destination)
                    if len(route) < 2:
                        continue

                    first_intersection = (
                        route[1] if route[1] in net.INTERSECTION_NODES else None
                    )
                    effective_scenario = (
                        self.per_node_scenario.get(first_intersection)
                        if first_intersection else None
                    ) or self.current_scenario
                    cfg = SCENARIOS.get(effective_scenario, global_cfg)
                    rate = cfg["boundary_spawn_rate"].get(boundary, 3)
                    multiplier = self._get_spawn_multiplier(effective_scenario)
                    effective_rate = rate * multiplier * weather_spawn

                    if cap > 0 and random.random() > min(1.0, effective_rate / max(cap, 1e-9)):
                        continue

                    first_edge = net.get_edge_between(route[0], route[1])
                    # U1: không spawn vào edge đã đầy
                    if self._edge_is_full(first_edge):
                        continue

                    vehicle = self._create_vehicle(route, cfg)
                    vehicle.state = VehicleState.TRANSIT
                    vehicle.current_context_type = "edge"
                    vehicle.current_context_id = first_edge
                    vehicle._segment_enter_sim_time = self.simulation_time_sec
                    vehicle.position = 0.0
                    self.edges_vehicles[first_edge].append(vehicle)
                    spawned += 1

        self.last_spawn_count = spawned

    def _create_vehicle(self, route: list, cfg: dict) -> Vehicle:
        classes = list(VEHICLE_CLASS_WEIGHTS.keys())
        weights = list(VEHICLE_CLASS_WEIGHTS.values())
        vclass = random.choices(classes, weights=weights, k=1)[0]

        min_s, max_s = cfg["base_speed_kmh"]["min"], cfg["base_speed_kmh"]["max"]
        v_free = random.uniform(min_s, max_s) * random.uniform(0.9, 1.1)

        is_priority = random.random() < PRIORITY_VEHICLE_PROBABILITY
        if is_priority:
            v_free *= 1.3

        return Vehicle(
            route=route,
            route_index=0,
            vehicle_class=vclass,
            is_priority=is_priority,
            v_free_kmh=max(10.0, v_free),
            speed_kmh=max(10.0, v_free),
        )

    # ══════════════════════════════════════════════════════════
    # SIMULATION LOOP
    # ══════════════════════════════════════════════════════════
    def tick_simulation(self, dt: float):
        self.simulation_time_sec += dt
        for ix in self.intersections.values():
            ix.phase_controller.tick(dt)

        with self._lock:
            self._tick_edges(dt)
            self._tick_intersections(dt)
            self._tick_arrival_rates(dt)

    def _tick_arrival_rates(self, dt: float):
        for ix in self.intersections.values():
            ix._arrival_reset_timer += dt
            if ix._arrival_reset_timer >= 1.0:
                ix._pcu_arrival_rate = dict(ix._pcu_arrived_this_sec)
                ix._pcu_arrived_this_sec = {d: 0.0 for d in DIRECTIONS}
                ix._arrival_reset_timer -= 1.0

    def _close_segment(self, vehicle: Vehicle):
        """Flush thoi gian doan hien tai. Goi TRUOC moi doi context / EXIT."""
        if not math.isnan(vehicle._segment_enter_sim_time) and vehicle.current_context_id:
            elapsed = self.simulation_time_sec - vehicle._segment_enter_sim_time
            prev = vehicle.segment_times.get(vehicle.current_context_id, 0.0)
            vehicle.segment_times[vehicle.current_context_id] = round(prev + elapsed, 2)
        vehicle._segment_enter_sim_time = self.simulation_time_sec

    # ── Edge (TRANSIT / BLOCKED_AT_ENTRY) ────────────────────
    def _tick_edges(self, dt: float):
        for edge_id, vehicles in list(self.edges_vehicles.items()):
            edge = net.EDGES[edge_id]
            edge_length = edge["length_m"]
            to_remove = []

            vehicles.sort(key=lambda v: v.position, reverse=True)

            for i, v in enumerate(vehicles):
                # U2: thử vào nút lại nếu đang bị chặn cuối edge
                if v.state == VehicleState.BLOCKED_AT_ENTRY:
                    v.record_tick(dt)
                    v.speed_kmh = 0.0
                    v.position = min(v.position, edge_length - 0.01)
                    if self._try_enter_intersection(v, edge_id):
                        to_remove.append(v)
                    continue

                if v.vehicle_class == "bus" and v.congested_timer_sec <= 0:
                    if random.random() < BUS_DWELL_PROBABILITY_PER_SEC * dt:
                        v.congested_timer_sec = BUS_DWELL_DURATION_SEC
                        v.state = VehicleState.CONGESTED

                if v.congested_timer_sec > 0:
                    v.congested_timer_sec -= dt
                    speed = BUS_DWELL_SPEED_KMH
                else:
                    speed = v.v_free_kmh
                    speed *= WEATHER_CAPACITY_FACTOR.get(self._weather(), 1.0)
                    from models import VEHICLE_SPEED_AGILITY_FACTOR
                    speed *= VEHICLE_SPEED_AGILITY_FACTOR.get(v.vehicle_class, 1.0)

                    if i > 0:
                        ahead = vehicles[i - 1]
                        gap = ahead.position - v.position
                        # U3+U9 headway trên edge (mét thật)
                        headway_s = DISCHARGE_HEADWAY_SEC.get(v.vehicle_class, 2.0)
                        headway_s *= WEATHER_HEADWAY_FACTOR.get(self._weather(), 1.0)
                        if ahead.speed_kmh < 1.0 and speed < 3.6:
                            min_gap = v.length_meters + INTER_VEHICLE_GAP_M
                        else:
                            min_gap = v.length_meters + max(ahead.speed_kmh, speed, 5.0) / 3.6 * headway_s
                        if gap < min_gap:
                            slow_ratio = max(0.15, gap / max(min_gap, 0.001))
                            speed *= slow_ratio
                            v.state = VehicleState.CONGESTED
                        else:
                            v.state = VehicleState.TRANSIT
                    else:
                        v.state = VehicleState.TRANSIT

                v.speed_kmh = speed
                v.record_tick(dt)
                delta_m = (speed / 3.6) * dt
                v.position += delta_m
                v.distance_traveled_m += delta_m

                if v.position >= edge_length:
                    if self._handle_edge_arrival(v, edge_id):
                        to_remove.append(v)
                    else:
                        # U2 deferred: giữ trên edge
                        v.position = edge_length - 0.01

            for v in to_remove:
                try:
                    vehicles.remove(v)
                except ValueError:
                    pass

    def _record_trip(self, vehicle: Vehicle):
        self.trip_records.append(vehicle.to_trip_record())
        if len(self.trip_records) > 2000:
            self.trip_records = self.trip_records[-2000:]

    def _try_enter_intersection(self, vehicle: Vehicle, edge_id: str) -> bool:
        """Thử đưa xe từ cuối edge vào nút (dùng lại logic arrival)."""
        return self._handle_edge_arrival(vehicle, edge_id)

    def _handle_edge_arrival(self, vehicle: Vehicle, edge_id: str) -> bool:
        """
        Returns True nếu xe đã rời edge (vào nút / EXIT).
        Returns False nếu U2 box-block → giữ trên edge (BLOCKED_AT_ENTRY).
        """
        edge = net.EDGES[edge_id]
        next_node = edge["to"]

        if next_node in net.BOUNDARY_NODES:
            self._close_segment(vehicle)
            vehicle.state = VehicleState.EXITED_NETWORK
            self._record_trip(vehicle)
            return True

        # U2: box blocking — không vào nút nếu edge ra gần đầy
        exit_edge = self._predicted_exit_edge(vehicle, next_node)
        if exit_edge and self._edge_is_full(exit_edge, BOX_OCCUPANCY_THRESHOLD):
            vehicle.state = VehicleState.BLOCKED_AT_ENTRY
            vehicle.position = edge["length_m"] - 0.01
            return False

        self._close_segment(vehicle)
        entry_direction = edge["to_dir"]
        vehicle.current_context_type = "intersection"
        vehicle.current_context_id = next_node
        vehicle.current_direction = entry_direction
        vehicle.state = VehicleState.MOVING
        vehicle.position = 0.0
        vehicle.intersections_visited.append(next_node)

        vehicle.movement_type = self._compute_movement_type(vehicle, next_node, entry_direction)
        lane = vehicle.movement_type if vehicle.movement_type in LANES else "straight"

        ix = self.intersections[next_node]
        ix._pcu_arrived_this_sec[entry_direction] += vehicle.pcu_factor
        ix.vehicles[entry_direction][lane].append(vehicle)

        # U8: emergency preemption khi ưu tiên vào nút
        if vehicle.is_priority and entry_direction in DIRECTION_TO_GREEN_PHASE:
            ix.phase_controller.force_phase(DIRECTION_TO_GREEN_PHASE[entry_direction])
            ix.preemption_active = True

        return True

    def _compute_movement_type(self, vehicle: Vehicle, node_id: str, entry_direction: str) -> str:
        if vehicle.route_index + 1 >= len(vehicle.route) - 1:
            return "straight"
        next_node = vehicle.route[vehicle.route_index + 2] if vehicle.route_index + 2 < len(vehicle.route) else None
        if next_node is None:
            return "straight"
        try:
            exit_edge = net.get_edge_between(node_id, next_node)
        except ValueError:
            return "straight"
        exit_direction = net.EDGES[exit_edge]["from_dir"]
        if exit_direction is None:
            return "straight"
        return MOVEMENT_TYPE.get(entry_direction, {}).get(exit_direction, "straight")

    # ── Intersection (MOVING/WAITING/CONGESTED/BLOCKED_AT_EXIT) ─
    def _tick_intersections(self, dt: float):
        for node_id, ix in self.intersections.items():
            is_incident_dir = ix.blocked_direction
            # Clear preemption flag mỗi tick nếu không còn xe ưu tiên trong nút
            has_priority = any(
                v.is_priority
                for d in DIRECTIONS
                for v in ix.get_all_vehicles_in_direction(d)
                if v.state != VehicleState.EXITED_NETWORK
            )
            if not has_priority:
                ix.preemption_active = False

            for direction in DIRECTIONS:
                color = ix.phase_controller.get_color(direction)
                is_green = color == "green"

                for lane in LANES:
                    lane_vehicles = ix.vehicles[direction][lane]
                    queue = sorted(lane_vehicles, key=lambda v: v.position, reverse=True)
                    to_remove_lane = []

                    for i, v in enumerate(queue):
                        if v.state == VehicleState.EXITED_NETWORK:
                            to_remove_lane.append(v)
                            continue

                        # U1: đang chờ edge ra có chỗ
                        if v.state == VehicleState.BLOCKED_AT_EXIT:
                            v.record_tick(dt)
                            v.speed_kmh = 0.0
                            v.position = min(v.position, EXIT_POINT - 0.01)
                            if self._handle_intersection_exit(v, node_id, direction):
                                to_remove_lane.append(v)
                            continue

                        # U6: yellow commitment — chỉ xe đã qua điểm commit mới đi tiếp
                        if color == "yellow":
                            can_proceed = (
                                v.position >= YELLOW_COMMITMENT_POINT or v.is_priority
                            )
                        elif color == "green":
                            can_proceed = True
                        else:
                            can_proceed = False
                        if not can_proceed:
                            can_proceed = v.is_priority or self._right_on_red_ok(node_id, v)

                        # Vùng dừng: đỏ → STOP_LINE; vàng chưa commit → trước commitment
                        if not can_proceed:
                            stop_at = (
                                YELLOW_COMMITMENT_POINT - 0.01
                                if color == "yellow"
                                else STOP_LINE
                            )
                            if v.position >= stop_at or v.state == VehicleState.WAITING:
                                v.state = VehicleState.WAITING
                                v.position = min(v.position, stop_at)
                                v.speed_kmh = 0.0
                                v.record_tick(dt)
                                # U4: moto filter-to-front — len dần lên trước ô tô (1 xe/tick)
                                if v.vehicle_class == "motorcycle" and i > 0:
                                    ahead = queue[i - 1]
                                    filter_kmh = 5.0
                                    delta = (filter_kmh / 3.6) * dt / APPROACH_LANE_LENGTH_M
                                    if ahead.vehicle_class != "motorcycle":
                                        # Len lên trước xe 4 bánh (đặc thù VN), không teleport cả hàng
                                        v.position = min(
                                            v.position + delta,
                                            ahead.position + 0.03,
                                            stop_at,
                                        )
                                    elif ahead.position <= STOP_LINE:
                                        moto_gap = (v.length_meters + 0.3) / APPROACH_LANE_LENGTH_M
                                        if ahead.position - v.position > moto_gap * 1.5:
                                            v.position = min(
                                                v.position + delta,
                                                ahead.position - moto_gap,
                                                stop_at,
                                            )
                                continue
                            # Chưa tới vạch: tiến dần rồi kẹp tại stop_at (không “vượt commit”)
                            v.record_tick(dt)
                            creep = min(v.v_free_kmh, 15.0)
                            creep *= WEATHER_CAPACITY_FACTOR.get(self._weather(), 1.0)
                            if i > 0:
                                ahead = queue[i - 1]
                                gap_units = ahead.position - v.position
                                min_gap_units = self._min_gap_units(v, ahead, APPROACH_LANE_LENGTH_M)
                                if gap_units < min_gap_units:
                                    creep *= max(0.1, gap_units / max(min_gap_units, 0.001))
                                    v.state = VehicleState.CONGESTED
                                else:
                                    v.state = VehicleState.MOVING
                            else:
                                v.state = VehicleState.MOVING
                            # U4: moto len khi tiếp cận hàng chờ
                            if v.vehicle_class == "motorcycle" and i > 0:
                                ahead = queue[i - 1]
                                filter_kmh = 5.0
                                delta = (filter_kmh / 3.6) * dt / APPROACH_LANE_LENGTH_M
                                if ahead.vehicle_class != "motorcycle":
                                    v.position = min(v.position + delta, ahead.position + 0.03, stop_at)
                                    v.speed_kmh = filter_kmh
                                    continue
                                moto_gap = (v.length_meters + 0.3) / APPROACH_LANE_LENGTH_M
                                if ahead.position - v.position > moto_gap * 1.5 and ahead.position <= STOP_LINE:
                                    v.position = min(v.position + delta, ahead.position - moto_gap, stop_at)
                                    v.speed_kmh = filter_kmh
                                    continue
                            delta_pos = (creep / 3.6) * dt / APPROACH_LANE_LENGTH_M
                            v.position = min(v.position + delta_pos, stop_at)
                            v.distance_traveled_m += (creep / 3.6) * dt
                            v.speed_kmh = creep
                            if v.position >= stop_at:
                                v.state = VehicleState.WAITING
                                v.speed_kmh = 0.0
                            continue

                        just_turned_green = v.was_waiting_last_tick and can_proceed
                        if just_turned_green and v.startup_timer_sec <= 0:
                            v.startup_timer_sec = 2.5

                        v.record_tick(dt)

                        eff_speed = v.v_free_kmh
                        eff_speed *= WEATHER_CAPACITY_FACTOR.get(self._weather(), 1.0)
                        from models import VEHICLE_SPEED_AGILITY_FACTOR
                        eff_speed *= VEHICLE_SPEED_AGILITY_FACTOR.get(v.vehicle_class, 1.0)

                        if v.startup_timer_sec > 0:
                            v.startup_timer_sec -= dt
                            startup_ratio = max(0.0, 1.0 - v.startup_timer_sec / 2.5)
                            eff_speed *= max(0.3, startup_ratio)

                        if direction == is_incident_dir:
                            eff_speed *= 0.05

                        if v.position >= STOP_LINE:
                            if v.movement_type == "left":
                                eff_speed *= 0.6
                            elif v.movement_type == "right":
                                eff_speed *= 0.8

                        is_congested_by_headway = False
                        if i > 0:
                            ahead = queue[i - 1]
                            gap_units = ahead.position - v.position
                            space_scale = (
                                APPROACH_LANE_LENGTH_M if v.position < STOP_LINE else CROSSING_LENGTH_M
                            )
                            min_gap_units = self._min_gap_units(v, ahead, space_scale)
                            if gap_units < min_gap_units:
                                slow_ratio = max(0.1, gap_units / max(min_gap_units, 0.001))
                                eff_speed *= slow_ratio
                                is_congested_by_headway = True

                        if is_congested_by_headway or (v.position < STOP_LINE and eff_speed < v.v_free_kmh * 0.5):
                            v.state = VehicleState.CONGESTED
                        else:
                            v.state = VehicleState.MOVING

                        space_scale = APPROACH_LANE_LENGTH_M if v.position < STOP_LINE else CROSSING_LENGTH_M
                        delta_pos = (eff_speed / 3.6) * dt / space_scale
                        v.position += delta_pos
                        v.distance_traveled_m += (eff_speed / 3.6) * dt
                        v.speed_kmh = eff_speed

                        if v.position >= EXIT_POINT:
                            if self._handle_intersection_exit(v, node_id, direction):
                                to_remove_lane.append(v)
                            else:
                                v.position = EXIT_POINT - 0.01

                    for v in to_remove_lane:
                        try:
                            ix.vehicles[direction][lane].remove(v)
                        except ValueError:
                            pass

    def _right_on_red_ok(self, node_id: str, vehicle: Vehicle) -> bool:
        """U5: right-on-red chỉ khi rule cho phép VÀ không có xe thẳng xung đột trong hộp."""
        rules = INTERSECTION_TURN_RULES.get(node_id, {})
        if not (vehicle.movement_type == "right" and rules.get("right_on_red", False)):
            return False
        conflict_dir = RIGHT_ON_RED_CONFLICT.get(vehicle.current_direction)
        if conflict_dir is None:
            return True
        ix = self.intersections[node_id]
        if ix.phase_controller.is_green(conflict_dir):
            conflict = [
                v for v in ix.get_all_vehicles_in_direction(conflict_dir)
                if v.movement_type == "straight"
                and v.position >= STOP_LINE - 0.05
                and v.state in (VehicleState.MOVING, VehicleState.CONGESTED)
            ]
            if conflict:
                return False
        return True

    def _handle_intersection_exit(self, vehicle: Vehicle, node_id: str, exit_direction_hint: str) -> bool:
        """
        Returns True nếu xe đã rời intersection.
        Returns False nếu U1 edge đầy → BLOCKED_AT_EXIT, giữ trong nút.
        """
        # Peek next edge trước khi bump route_index (idempotent khi retry BLOCKED_AT_EXIT)
        peek_index = vehicle.route_index + 1
        if peek_index >= len(vehicle.route) - 1:
            self._close_segment(vehicle)
            vehicle.route_index = peek_index
            vehicle.state = VehicleState.EXITED_NETWORK
            self._record_trip(vehicle)
            return True

        # Tạm tính next sau maybe_reroute — cần bump index trước reroute như cũ
        already_blocked = vehicle.state == VehicleState.BLOCKED_AT_EXIT
        if not already_blocked:
            self._close_segment(vehicle)
            vehicle.route_index += 1
            self._maybe_reroute(vehicle, node_id)

        if vehicle.route_index >= len(vehicle.route) - 1:
            vehicle.state = VehicleState.EXITED_NETWORK
            self._record_trip(vehicle)
            return True

        next_node = vehicle.route[vehicle.route_index + 1]
        try:
            edge_id = net.get_edge_between(node_id, next_node)
        except ValueError:
            vehicle.state = VehicleState.EXITED_NETWORK
            self._record_trip(vehicle)
            return True

        # U1: không vào edge đầy
        if self._edge_is_full(edge_id):
            vehicle.state = VehicleState.BLOCKED_AT_EXIT
            vehicle.position = EXIT_POINT - 0.01
            return False

        vehicle.current_context_type = "edge"
        vehicle.current_context_id = edge_id
        vehicle.state = VehicleState.TRANSIT
        vehicle.position = 0.0
        self.edges_vehicles[edge_id].append(vehicle)
        return True

    def _maybe_reroute(self, vehicle: Vehicle, current_node: str):
        if vehicle.route_index + 1 >= len(vehicle.route):
            return
        next_node = vehicle.route[vehicle.route_index + 1]
        if next_node not in net.INTERSECTION_NODES:
            return

        next_density = self.intersections[next_node].get_density_level()
        prob = REROUTE_PROBABILITY.get(vehicle.vehicle_class, 0.1)

        if next_density == "HIGH" and random.random() < prob:
            final_dest = vehicle.route[-1]
            new_route = net.compute_route_avoiding(current_node, final_dest, avoid_node=next_node)
            if len(new_route) > 1 and new_route[1] != next_node:
                vehicle.route = vehicle.route[:vehicle.route_index + 1] + new_route[1:]
                log.debug(f"Vehicle {vehicle.id} re-routed at {current_node} avoiding {next_node}")

    # ══════════════════════════════════════════════════════════
    # QUEUE LENGTH — cong thuc rieng oto/xe may (dac thu VN)
    # ══════════════════════════════════════════════════════════
    @staticmethod
    def _compute_queue_length(waiting_vehicles: list) -> float:
        car_like = [v for v in waiting_vehicles if v.vehicle_class in ("car", "bus", "truck")]
        motorcycles = [v for v in waiting_vehicles if v.vehicle_class == "motorcycle"]

        car_queue_m = sum(v.length_meters + INTER_VEHICLE_GAP_M for v in car_like)
        moto_queue_m = sum(v.length_meters + 0.5 for v in motorcycles) / MOTORCYCLE_LATERAL_FACTOR
        return car_queue_m + moto_queue_m

    def _moto_front_pct(self, active: list) -> float:
        """% moto trong top 20% position gần STOP_LINE (approach+waiting)."""
        near = [v for v in active if v.position >= 0.8 and v.position <= STOP_LINE]
        if not near:
            near = sorted(active, key=lambda v: v.position, reverse=True)[:max(1, len(active) // 5)]
        if not near:
            return 0.0
        motos = sum(1 for v in near if v.vehicle_class == "motorcycle")
        return round(100.0 * motos / len(near), 1)

    def _incoming_blocked_count(self, node_id: str) -> int:
        """Đếm xe BLOCKED_AT_ENTRY trên các edge dẫn vào nút."""
        n = 0
        for eid, e in net.EDGES.items():
            if e["to"] != node_id:
                continue
            n += sum(
                1 for v in self.edges_vehicles[eid]
                if v.state == VehicleState.BLOCKED_AT_ENTRY
            )
        return n

    def _spillback_metrics(self, node_id: str) -> dict:
        """downstream_edge_full / spillback_pressure / spillback_detected."""
        max_pressure = 0.0
        any_full = False
        for eid, e in net.EDGES.items():
            if e["from"] != node_id:
                continue
            ratio = self._edge_occupancy_ratio(eid)
            max_pressure = max(max_pressure, ratio)
            if ratio >= 1.0:
                any_full = True
        blocked_entry = self._incoming_blocked_count(node_id) > 0
        blocked_exit = any(
            v.state == VehicleState.BLOCKED_AT_EXIT
            for d in DIRECTIONS
            for v in self.intersections[node_id].get_all_vehicles_in_direction(d)
        )
        return {
            "downstream_edge_full": any_full,
            "spillback_pressure": round(max_pressure, 3),
            "spillback_detected": any_full or blocked_entry or blocked_exit,
            "intersection_box_blocked": blocked_entry,
            "vehicles_blocked_at_entry": self._incoming_blocked_count(node_id),
        }

    # ══════════════════════════════════════════════════════════
    # SNAPSHOT — dung cho entity_generator (Context Layer)
    # ══════════════════════════════════════════════════════════
    def get_snapshot(self, node_id: str) -> dict:
        with self._lock:
            ix = self.intersections[node_id]
            directions_data = {}
            yellow_commitment_count = 0

            for direction in DIRECTIONS:
                all_vehicles = ix.get_all_vehicles_in_direction(direction)
                active  = [v for v in all_vehicles if v.state != VehicleState.EXITED_NETWORK]
                waiting = [v for v in active if v.state == VehicleState.WAITING]
                moving  = [
                    v for v in active
                    if v.state in (
                        VehicleState.MOVING, VehicleState.CONGESTED, VehicleState.BLOCKED_AT_EXIT
                    )
                ]

                left_n     = sum(1 for v in active if v.movement_type == "left")
                straight_n = sum(1 for v in active if v.movement_type == "straight")
                right_n    = sum(1 for v in active if v.movement_type == "right")

                avg_speed = (sum(v.speed_kmh for v in moving) / len(moving)) if moving else 0.0
                pcu_equivalent = sum(v.pcu_factor for v in active)

                waiting_by_lane = {
                    lane: [v for v in ix.vehicles[direction][lane] if v.state == VehicleState.WAITING]
                    for lane in LANES
                }
                queue_by_movement = {
                    lane: round(self._compute_queue_length(waiting_by_lane[lane]), 1)
                    for lane in LANES
                }
                queue_m = max(queue_by_movement.values()) if any(queue_by_movement.values()) else 0.0

                car_like = [v for v in active if v.vehicle_class in ("car", "bus", "truck")]
                motos = [v for v in active if v.vehicle_class == "motorcycle"]
                occ_car = sum(v.length_meters + INTER_VEHICLE_GAP_M for v in car_like)
                occ_moto = sum(v.length_meters + 0.5 for v in motos) / MOTORCYCLE_LATERAL_FACTOR
                occupancy = min(100.0, ((occ_car + occ_moto) / APPROACH_LANE_LENGTH_M) * 100)

                is_green = ix.phase_controller.is_green(direction)
                color = ix.phase_controller.get_color(direction)
                red_phase_n = 0
                congestion_n = 0
                for v in waiting:
                    if is_green:
                        congestion_n += 1
                    else:
                        red_phase_n += 1
                for v in moving:
                    if is_green and v.state == VehicleState.CONGESTED and v.speed_kmh < 1.0:
                        congestion_n += 1

                if red_phase_n == 0 and congestion_n == 0:
                    dominant_reason = None
                elif red_phase_n >= congestion_n:
                    dominant_reason = "RED_PHASE"
                else:
                    dominant_reason = "CONGESTION"

                if color == "yellow":
                    yellow_commitment_count += sum(
                        1 for v in active if v.position >= YELLOW_COMMITMENT_POINT
                    )

                pcu_per_km = (pcu_equivalent / APPROACH_LANE_LENGTH_M) * 1000.0
                theoretical_speed = greenshields_speed(
                    v_free_kmh=SCENARIOS[self.current_scenario]["base_speed_kmh"]["max"],
                    density_pcu_per_km=pcu_per_km,
                )

                directions_data[direction] = {
                    "vehicle_count":         len(active),
                    "pcu_equivalent":        round(pcu_equivalent, 2),
                    "left_count":            left_n,
                    "straight_count":        straight_n,
                    "right_count":           right_n,
                    "average_speed_kmh":     round(avg_speed, 1),
                    "waiting_vehicle_count": len(waiting),
                    "queue_length_m":        round(queue_m, 1),
                    "queue_by_movement":     queue_by_movement,
                    "occupancy_pct":         round(occupancy, 1),
                    "density":               density_label(pcu_equivalent),
                    "arrival_rate_pcu_per_sec": round(ix._pcu_arrival_rate.get(direction, 0.0), 3),
                    "waiting_reason_counts": {
                        "RED_PHASE": red_phase_n,
                        "CONGESTION": congestion_n,
                    },
                    "dominant_waiting_reason": dominant_reason,
                    "theoretical_speed_kmh": round(theoretical_speed, 1),
                    "moto_front_pct": self._moto_front_pct(active),
                }

            recent_incidents = [i for i in ix.incidents if time.time() - i["time"] < 300]
            spill = self._spillback_metrics(node_id)

            return {
                "node_id":             node_id,
                "directions":          directions_data,
                "phase":               ix.phase_controller.current_phase,
                "next_phase":          ix.phase_controller.next_phase,
                "phase_remaining":     ix.phase_controller.remaining_seconds,
                "phase_duration":      ix.phase_controller.phase_duration,
                "colors":              {d: ix.phase_controller.get_color(d) for d in DIRECTIONS},
                "scenario":            self.current_scenario,
                "blocked_direction":   ix.blocked_direction,
                "incidents":           recent_incidents,
                "simulation_time_sec": round(self.simulation_time_sec, 2),
                "downstream_edge_full": spill["downstream_edge_full"],
                "spillback_pressure": spill["spillback_pressure"],
                "spillback_detected": spill["spillback_detected"],
                "intersection_box_blocked": spill["intersection_box_blocked"],
                "vehicles_blocked_at_entry": spill["vehicles_blocked_at_entry"],
                "yellow_commitment_count": yellow_commitment_count,
                "preemption_active": ix.preemption_active,
            }

    # ══════════════════════════════════════════════════════════
    # DEBUG / TEST HELPERS
    # ══════════════════════════════════════════════════════════
    def count_total_vehicles(self) -> int:
        total = 0
        for ix in self.intersections.values():
            for d in DIRECTIONS:
                total += sum(
                    1 for v in ix.get_all_vehicles_in_direction(d)
                    if v.state != VehicleState.EXITED_NETWORK
                )
        for vehicles in self.edges_vehicles.values():
            total += sum(1 for v in vehicles if v.state != VehicleState.EXITED_NETWORK)
        return total

    def count_exited_network(self) -> int:
        return len(self.trip_records)
