"""
Synthetic VehicleSensor-window generator for RF training.

Profiles approximate the thesis heuristics:
  NORMAL      — stable speed/occupancy
  CONGESTION  — gradual speed decline + gradual density/queue rise
  ACCIDENT    — sudden speed collapse + sharp occupancy/queue spike + asymmetry
"""
from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np

from .features import extract_features_from_series, features_to_vector, FEATURE_NAMES
from .labels import LABEL_ACCIDENT, LABEL_CONGESTION, LABEL_NORMAL


def _noise(rng: np.random.Generator, scale: float) -> float:
    return float(rng.normal(0.0, scale))


def _gen_normal(rng: np.random.Generator, n: int) -> Dict[str, List[float]]:
    base_speed = float(rng.uniform(32, 48))
    speeds = [max(5.0, base_speed + _noise(rng, 2.5)) for _ in range(n)]
    occs = [max(0.0, min(100.0, 18 + _noise(rng, 4))) for _ in range(n)]
    queues = [max(0.0, 8 + _noise(rng, 3)) for _ in range(n)]
    counts = [max(0.0, 40 + _noise(rng, 8)) for _ in range(n)]
    arrivals = [max(0.0, 0.25 + _noise(rng, 0.05)) for _ in range(n)]
    approach = [
        max(5.0, base_speed + _noise(rng, 3)),
        max(5.0, base_speed + _noise(rng, 3)),
        max(5.0, base_speed + _noise(rng, 3)),
        max(5.0, base_speed + _noise(rng, 3)),
    ]
    return {
        "speeds": speeds,
        "occupancies": occs,
        "queues_m": queues,
        "vehicle_counts": counts,
        "arrival_rates": arrivals,
        "approach_speeds": approach,
    }


def _gen_congestion(rng: np.random.Generator, n: int) -> Dict[str, List[float]]:
    start = float(rng.uniform(35, 45))
    end = float(rng.uniform(8, 18))
    speeds = [
        max(3.0, start + (end - start) * (i / max(n - 1, 1)) + _noise(rng, 1.2))
        for i in range(n)
    ]
    occ0, occ1 = float(rng.uniform(20, 35)), float(rng.uniform(55, 80))
    occs = [
        max(0.0, min(100.0, occ0 + (occ1 - occ0) * (i / max(n - 1, 1)) + _noise(rng, 3)))
        for i in range(n)
    ]
    q0, q1 = float(rng.uniform(10, 25)), float(rng.uniform(60, 120))
    queues = [
        max(0.0, q0 + (q1 - q0) * (i / max(n - 1, 1)) + _noise(rng, 4))
        for i in range(n)
    ]
    c0, c1 = float(rng.uniform(50, 80)), float(rng.uniform(120, 220))
    counts = [
        max(0.0, c0 + (c1 - c0) * (i / max(n - 1, 1)) + _noise(rng, 10))
        for i in range(n)
    ]
    arrivals = [
        max(0.0, 0.35 + 0.15 * (i / max(n - 1, 1)) + _noise(rng, 0.04))
        for i in range(n)
    ]
    # All approaches similarly degraded
    approach = [max(3.0, end + _noise(rng, 2)) for _ in range(4)]
    return {
        "speeds": speeds,
        "occupancies": occs,
        "queues_m": queues,
        "vehicle_counts": counts,
        "arrival_rates": arrivals,
        "approach_speeds": approach,
    }


def _gen_accident(rng: np.random.Generator, n: int) -> Dict[str, List[float]]:
    start = float(rng.uniform(35, 50))
    break_at = int(rng.integers(max(2, n // 4), max(3, (3 * n) // 4)))
    crash_speed = float(rng.uniform(2, 8))
    speeds: List[float] = []
    occs: List[float] = []
    queues: List[float] = []
    counts: List[float] = []
    arrivals: List[float] = []
    for i in range(n):
        if i < break_at:
            speeds.append(max(5.0, start + _noise(rng, 2)))
            occs.append(max(0.0, min(100.0, 25 + _noise(rng, 4))))
            queues.append(max(0.0, 15 + _noise(rng, 4)))
            counts.append(max(0.0, 55 + _noise(rng, 8)))
            arrivals.append(max(0.0, 0.3 + _noise(rng, 0.04)))
        else:
            # Sudden collapse after break
            t = (i - break_at) / max(n - break_at, 1)
            speeds.append(max(1.0, crash_speed + 3 * (1 - t) + _noise(rng, 1)))
            occs.append(max(0.0, min(100.0, 70 + 20 * t + _noise(rng, 5))))
            queues.append(max(0.0, 80 + 60 * t + _noise(rng, 8)))
            counts.append(max(0.0, 140 + 40 * t + _noise(rng, 12)))
            arrivals.append(max(0.0, 0.05 + _noise(rng, 0.03)))
    # Strong asymmetry: one approach nearly stopped
    approach = [
        crash_speed + _noise(rng, 1),
        float(rng.uniform(20, 35)),
        float(rng.uniform(18, 32)),
        float(rng.uniform(22, 40)),
    ]
    return {
        "speeds": speeds,
        "occupancies": occs,
        "queues_m": queues,
        "vehicle_counts": counts,
        "arrival_rates": arrivals,
        "approach_speeds": approach,
    }


def generate_dataset(
    n_per_class: int = 400,
    window: int = 12,
    seed: int = 42,
) -> Tuple[np.ndarray, np.ndarray]:
    """Return X (n, n_features), y (n,) string labels."""
    rng = np.random.default_rng(seed)
    rows: List[np.ndarray] = []
    labels: List[str] = []
    generators = {
        LABEL_NORMAL: _gen_normal,
        LABEL_CONGESTION: _gen_congestion,
        LABEL_ACCIDENT: _gen_accident,
    }
    for label, gen in generators.items():
        for _ in range(n_per_class):
            n = int(rng.integers(max(6, window - 2), window + 3))
            series = gen(rng, n)
            feat = extract_features_from_series(
                speeds=series["speeds"],
                occupancies=series["occupancies"],
                queues_m=series["queues_m"],
                vehicle_counts=series["vehicle_counts"],
                arrival_rates=series["arrival_rates"],
                approach_speeds=series["approach_speeds"],
            )
            rows.append(features_to_vector(feat))
            labels.append(label)
    X = np.vstack(rows)
    y = np.asarray(labels)
    # Shuffle
    idx = rng.permutation(X.shape[0])
    return X[idx], y[idx]


def feature_names() -> List[str]:
    return list(FEATURE_NAMES)
