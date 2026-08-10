"""
Feature engineering for VehicleSensor time windows.

Input: a short history of approach-level samples (or already-aggregated
intersection ticks). Output: fixed-length numeric feature vector for RF.

Heuristic alignment (thesis):
  - sudden speed drop + occupancy/queue spike → ACCIDENT-like
  - gradual speed decline + gradual density rise → CONGESTION-like
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np

FEATURE_NAMES: List[str] = [
    "mean_speed",
    "min_speed",
    "speed_drop",
    "speed_slope",
    "max_step_speed_drop",
    "mean_occupancy",
    "max_occupancy",
    "occupancy_slope",
    "mean_queue_m",
    "max_queue_m",
    "queue_slope",
    "mean_vehicle_count",
    "max_vehicle_count",
    "mean_arrival_rate",
    "low_speed_frac",
    "approach_speed_asymmetry",
    "window_len",
]

_LOW_SPEED_KMH = 15.0


def _as_float_series(values: Iterable[Any]) -> np.ndarray:
    out: List[float] = []
    for v in values:
        try:
            if v is None:
                continue
            out.append(float(v))
        except (TypeError, ValueError):
            continue
    return np.asarray(out, dtype=float)


def _safe_slope(y: np.ndarray) -> float:
    if y.size < 2:
        return 0.0
    x = np.arange(y.size, dtype=float)
    # np.polyfit is fine for short windows
    try:
        return float(np.polyfit(x, y, 1)[0])
    except Exception:
        return 0.0


def _max_step_drop(y: np.ndarray) -> float:
    """Largest single-step relative drop (positive number)."""
    if y.size < 2:
        return 0.0
    prev = np.maximum(y[:-1], 1e-3)
    drops = (prev - y[1:]) / prev
    return float(max(0.0, float(np.max(drops))))


def extract_features_from_series(
    *,
    speeds: Sequence[Any],
    occupancies: Sequence[Any],
    queues_m: Sequence[Any],
    vehicle_counts: Sequence[Any],
    arrival_rates: Optional[Sequence[Any]] = None,
    approach_speeds: Optional[Sequence[Any]] = None,
) -> Dict[str, float]:
    """Build one feature dict from parallel time series (same length preferred)."""
    sp = _as_float_series(speeds)
    oc = _as_float_series(occupancies)
    qu = _as_float_series(queues_m)
    vc = _as_float_series(vehicle_counts)
    ar = _as_float_series(arrival_rates or [])
    asp = _as_float_series(approach_speeds or [])

    n = int(max(sp.size, oc.size, qu.size, vc.size, 1))
    if sp.size == 0:
        sp = np.zeros(1)
    if oc.size == 0:
        oc = np.zeros(1)
    if qu.size == 0:
        qu = np.zeros(1)
    if vc.size == 0:
        vc = np.zeros(1)

    speed_drop = float(sp[0] - sp[-1]) if sp.size >= 2 else 0.0
    low_frac = float(np.mean(sp < _LOW_SPEED_KMH)) if sp.size else 0.0
    if asp.size >= 2:
        asym = float((np.max(asp) - np.min(asp)) / max(float(np.mean(asp)), 1e-3))
    else:
        asym = 0.0

    return {
        "mean_speed": float(np.mean(sp)),
        "min_speed": float(np.min(sp)),
        "speed_drop": speed_drop,
        "speed_slope": _safe_slope(sp),
        "max_step_speed_drop": _max_step_drop(sp),
        "mean_occupancy": float(np.mean(oc)),
        "max_occupancy": float(np.max(oc)),
        "occupancy_slope": _safe_slope(oc),
        "mean_queue_m": float(np.mean(qu)),
        "max_queue_m": float(np.max(qu)),
        "queue_slope": _safe_slope(qu),
        "mean_vehicle_count": float(np.mean(vc)),
        "max_vehicle_count": float(np.max(vc)),
        "mean_arrival_rate": float(np.mean(ar)) if ar.size else 0.0,
        "low_speed_frac": low_frac,
        "approach_speed_asymmetry": asym,
        "window_len": float(n),
    }


def features_to_vector(feat: Mapping[str, float]) -> np.ndarray:
    return np.asarray([float(feat.get(name, 0.0)) for name in FEATURE_NAMES], dtype=float)


def extract_features_from_snapshot_history(
    history: Sequence[Mapping[str, Any]],
) -> Dict[str, float]:
    """
    history: list of per-tick aggregates, each like:
      {
        "average_speed_kmh": float,
        "occupancy_pct": float,
        "queue_length_m": float,
        "vehicle_count": float,
        "arrival_rate_pcu_per_sec": float,
        "approach_speeds": [float, ...]  # optional per-approach
      }
    """
    speeds = [h.get("average_speed_kmh") for h in history]
    occs = [h.get("occupancy_pct") for h in history]
    queues = [h.get("queue_length_m") for h in history]
    counts = [h.get("vehicle_count") for h in history]
    arrivals = [h.get("arrival_rate_pcu_per_sec") for h in history]
    # Use latest tick's approach speeds for asymmetry
    approach_speeds = []
    if history:
        approach_speeds = list(history[-1].get("approach_speeds") or [])
    return extract_features_from_series(
        speeds=speeds,
        occupancies=occs,
        queues_m=queues,
        vehicle_counts=counts,
        arrival_rates=arrivals,
        approach_speeds=approach_speeds,
    )


def aggregate_snapshot_tick(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """Collapse one Intersection snapshot (4 approaches) into a tick record."""
    dirs = snapshot.get("directions") or {}
    speeds: List[float] = []
    occs: List[float] = []
    queues: List[float] = []
    counts: List[float] = []
    arrivals: List[float] = []
    for d in dirs.values():
        if not isinstance(d, Mapping):
            continue
        if d.get("average_speed_kmh") is not None:
            speeds.append(float(d["average_speed_kmh"]))
        if d.get("occupancy_pct") is not None:
            occs.append(float(d["occupancy_pct"]))
        if d.get("queue_length_m") is not None:
            queues.append(float(d["queue_length_m"]))
        if d.get("vehicle_count") is not None:
            counts.append(float(d["vehicle_count"]))
        if d.get("arrival_rate_pcu_per_sec") is not None:
            arrivals.append(float(d["arrival_rate_pcu_per_sec"]))
    return {
        "average_speed_kmh": float(np.mean(speeds)) if speeds else 0.0,
        "occupancy_pct": float(np.mean(occs)) if occs else 0.0,
        "queue_length_m": float(np.mean(queues)) if queues else 0.0,
        "vehicle_count": float(np.sum(counts)) if counts else 0.0,
        "arrival_rate_pcu_per_sec": float(np.sum(arrivals)) if arrivals else 0.0,
        "approach_speeds": speeds,
    }
