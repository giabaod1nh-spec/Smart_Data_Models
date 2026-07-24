"""
metrics_derivation.py — Pure physical aggregate helpers (no TraCI, no semantic labels).
"""
from __future__ import annotations

from typing import Optional


def estimated_density_veh_per_km(pcu_equivalent: float, approach_length_m: float) -> float:
    if approach_length_m <= 0:
        return 0.0
    return (pcu_equivalent / approach_length_m) * 1000.0


def greenshields_speed_kmh(
    v_free_kmh: float, density_pcu_per_km: float, k_jam_pcu_per_km: float
) -> float:
    if k_jam_pcu_per_km <= 0:
        return float(v_free_kmh)
    ratio = min(1.0, max(0.0, density_pcu_per_km / k_jam_pcu_per_km))
    return max(0.0, float(v_free_kmh) * (1.0 - ratio))


def storage_utilization_lane(jam_length_lane_m: float, lane_length_m: float) -> float:
    """Primary storage util: jam meters on one lane / that lane length (clamp 0–1)."""
    if lane_length_m <= 0:
        return 0.0
    return max(0.0, min(1.0, float(jam_length_lane_m) / float(lane_length_m)))


def storage_utilization_pcu(
    queued_pcu: float, lane_lengths_m: list[float], passenger_car_length_m: float = 7.5
) -> float:
    """Secondary: sum(queued_pcu) / sum(lane_length/7.5). Geometric upper bound denom."""
    if passenger_car_length_m <= 0:
        return 0.0
    nominal = sum(max(0.0, L) / passenger_car_length_m for L in lane_lengths_m)
    if nominal <= 0:
        return 0.0
    return max(0.0, min(1.0, float(queued_pcu) / nominal))


def queue_length_vehicles_from_jam_m(
    jam_length_m: float, passenger_car_length_m: float = 7.5
) -> float:
    if passenger_car_length_m <= 0:
        return 0.0
    return round(max(0.0, float(jam_length_m)) / float(passenger_car_length_m), 2)


def discharge_drop_ratio(
    recent_discharge: float, baseline_discharge: float
) -> Optional[float]:
    if baseline_discharge <= 1e-9:
        return None
    return max(0.0, 1.0 - (recent_discharge / baseline_discharge))
