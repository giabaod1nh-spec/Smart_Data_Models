"""
traffic_flow_speed.py — Mo hinh toc do Greenshields (1935)

v = v_free * (1 - k / k_jam)

Dung de tinh toc do hieu dung cua xe dua tren mat do (khong random).
Nguon: Greenshields, B.D. (1935), A Study of Traffic Capacity,
Highway Research Board Proceedings.
"""
from models import VEHICLE_SPEED_AGILITY_FACTOR

K_JAM_PCU_PER_KM = 200.0

WEATHER_SPEED_FACTOR = {
    "SUNNY":      1.00,
    "LIGHT_RAIN": 0.85,
    "HEAVY_RAIN": 0.65,
}

WEATHER_CAPACITY_FACTOR = {
    "SUNNY":      1.00,
    "LIGHT_RAIN": 0.85,
    "HEAVY_RAIN": 0.65,
}

# Phase 3 U9 — mưa làm tăng khoảng cách an toàn / giảm spawn
WEATHER_HEADWAY_FACTOR = {
    "SUNNY":      1.00,
    "LIGHT_RAIN": 1.20,
    "HEAVY_RAIN": 1.60,
}

WEATHER_SPAWN_FACTOR = {
    "SUNNY":      1.00,
    "LIGHT_RAIN": 0.85,
    "HEAVY_RAIN": 0.50,
}


def greenshields_speed(v_free_kmh: float, density_pcu_per_km: float,
                       k_jam: float = K_JAM_PCU_PER_KM) -> float:
    ratio = min(max(density_pcu_per_km, 0.0) / k_jam, 1.0)
    return max(0.0, v_free_kmh * (1 - ratio))


def compute_effective_speed(vehicle, local_density_pcu_per_km: float,
                            weather: str = "SUNNY", is_green: bool = True,
                            capacity_factor: float = 1.0) -> float:
    if not is_green:
        return 0.0

    v = greenshields_speed(vehicle.v_free_kmh, local_density_pcu_per_km)
    v *= WEATHER_SPEED_FACTOR.get(weather, 1.0)
    v *= VEHICLE_SPEED_AGILITY_FACTOR.get(vehicle.vehicle_class, 1.0)
    v *= capacity_factor
    return max(0.0, v)
