"""
models.py — Vehicle Object (tầng Simulation Layer)

Vehicle KHÔNG bao giờ được gửi lên Orion. Chỉ tồn tại trong RAM
của CityNetworkEngine. VehicleSensor entity (NGSI-LD) được tổng hợp
từ danh sách Vehicle này mỗi giây (xem traffic_engine.get_snapshot()).
"""
from dataclasses import dataclass, field
from enum import Enum
import uuid
import time


class VehicleState(Enum):
    TRANSIT        = "TRANSIT"          # đang di chuyển trên edge giữa 2 node
    MOVING          = "MOVING"          # đang di chuyển trong phạm vi 1 intersection
    WAITING         = "WAITING"         # đứng chờ đèn đỏ tại vạch dừng
    CONGESTED       = "CONGESTED"       # đang di chuyển nhưng bị chậm bởi mật độ cao / dừng đón khách
    BLOCKED_AT_ENTRY = "BLOCKED_AT_ENTRY"  # Phase3 U2: cuối edge, chưa vào nút (lối ra tắc)
    BLOCKED_AT_EXIT  = "BLOCKED_AT_EXIT"   # Phase3 U1: trong nút, chờ edge ra còn chỗ
    EXITED_NETWORK  = "EXITED_NETWORK"  # đã ra khỏi biên mạng lưới, sẽ bị dọn khỏi RAM


# Bảng hệ số quy đổi Việt Nam (có nguồn, xem báo cáo kèm theo)
PCU_FACTORS = {
    "motorcycle": 0.24,
    "car":        1.00,
    "bus":        2.50,
    "truck":      2.50,
}

VEHICLE_LENGTH_METERS = {
    "motorcycle": 2.0,
    "car":        4.5,
    "bus":        12.0,
    "truck":      10.0,
}

VEHICLE_CLASS_WEIGHTS = {
    "motorcycle": 0.85,
    "car":        0.12,
    "bus":        0.02,
    "truck":      0.01,
}

# Hệ số "linh hoạt" khi tính tốc độ Greenshields — xe máy luồn lách nhanh hơn
VEHICLE_SPEED_AGILITY_FACTOR = {
    "motorcycle": 1.15,
    "car":        1.00,
    "bus":        0.85,
    "truck":      0.80,
}


@dataclass
class Vehicle:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    spawn_time: float = field(default_factory=time.time)

    # Route toàn mạng lưới
    route: list = field(default_factory=list)      # vi du: ["W1", "A", "B", "E1"]
    route_index: int = 0

    # Ngu canh hien tai
    current_context_type: str = "edge"    # "edge" hoac "intersection"
    current_context_id: str = ""           # "edge_W1_A" hoac "A"
    current_direction: str = ""            # huong tiep can tai intersection (neu co)

    movement_type: str = "straight"        # straight / left / right
    vehicle_class: str = "motorcycle"
    is_priority: bool = False              # xe cuu thuong/cuu hoa

    v_free_kmh: float = 40.0
    speed_kmh: float = 40.0
    position: float = 0.0                  # met (tren edge) hoac 0.0-2.0 (trong intersection)
    state: VehicleState = VehicleState.TRANSIT
    length_meters: float = 2.0
    pcu_factor: float = 0.24

    congested_timer_sec: float = 0.0       # dem nguoc cho hanh vi dung don khach (bus)

    total_waiting_time_sec: float = 0.0
    total_travel_time_sec: float = 0.0
    distance_traveled_m: float = 0.0
    stop_count: int = 0
    intersections_visited: list = field(default_factory=list)
    was_waiting_last_tick: bool = False

    # Phase 2.5+: thoi gian tung doan (edge / intersection) trong chuyen di
    segment_times: dict = field(default_factory=dict)
    # nan = chua bat dau do; dung sentinel thay vi 0.0
    _segment_enter_sim_time: float = float("nan")
    # P7-A: dem nguoc khoi dong sau den xanh
    startup_timer_sec: float = 0.0

    def __post_init__(self):
        self.pcu_factor = PCU_FACTORS.get(self.vehicle_class, 1.0)
        self.length_meters = VEHICLE_LENGTH_METERS.get(self.vehicle_class, self.length_meters)

    def record_tick(self, dt: float):
        self.total_travel_time_sec += dt
        if self.state == VehicleState.WAITING:
            self.total_waiting_time_sec += dt
            if not self.was_waiting_last_tick:
                self.stop_count += 1
            self.was_waiting_last_tick = True
        else:
            self.was_waiting_last_tick = False

    def to_trip_record(self) -> dict:
        return {
            "vehicle_id":             self.id,
            "vehicle_class":          self.vehicle_class,
            "movement_type":          self.movement_type,
            "total_travel_time_sec":  round(self.total_travel_time_sec, 1),
            "total_waiting_time_sec": round(self.total_waiting_time_sec, 1),
            "distance_traveled_m":    round(self.distance_traveled_m, 1),
            "stop_count":             self.stop_count,
            "intersections_visited":  list(self.intersections_visited),
            "segment_travel_times":   dict(self.segment_times),
            "average_speed_kmh": round(
                (self.distance_traveled_m / max(self.total_travel_time_sec, 0.001)) * 3.6, 1
            ),
        }
