"""Normalize raw traffic metrics for neural network input."""
from __future__ import annotations

import numpy as np


def clip01(x: float) -> float:
    return float(np.clip(x, 0.0, 1.0))


def norm_queue(q: float, cap: float) -> float:
    return clip01(q / max(cap, 1.0))


def norm_waiting(w: float, cap: float) -> float:
    return clip01(w / max(cap, 1.0))


def norm_occupancy(occ_pct: float) -> float:
    o = occ_pct / 100.0 if occ_pct > 1.0 else occ_pct
    return clip01(o)
