"""PropagationAnalyzer — observation windows, queue slopes, occupancy."""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, Optional, Tuple


class PropagationAnalyzer:
    def __init__(self):
        self._history: Dict[Tuple[str, str], Deque[Tuple[float, float, float]]] = defaultdict(
            lambda: deque(maxlen=120)
        )

    def record(
        self,
        node: str,
        direction: str,
        sim_t: float,
        queue_m: float,
        occupancy: float = 0.0,
    ) -> None:
        self._history[(node, direction)].append((sim_t, queue_m, occupancy))

    def queue_slope(self, node: str, direction: str, window_s: float, sim_t: float) -> float:
        hist = self._history[(node, direction)]
        pts = [(t, q) for t, q, _ in hist if sim_t - t <= window_s]
        if len(pts) < 2:
            return 0.0
        t0, q0 = pts[0]
        t1, q1 = pts[-1]
        dt = max(1e-3, t1 - t0)
        return (q1 - q0) / dt

    def mean_occupancy(self, node: str, direction: str, window_s: float, sim_t: float) -> float:
        hist = self._history[(node, direction)]
        pts = [o for t, _, o in hist if sim_t - t <= window_s]
        if not pts:
            return 0.0
        return sum(pts) / len(pts)

    def discharge_proxy_drop(
        self, node: str, direction: str, window_s: float, sim_t: float
    ) -> Optional[float]:
        """Approximate discharge drop from queue growth (higher queue ⇒ lower discharge)."""
        hist = self._history[(node, direction)]
        pts = [(t, q) for t, q, _ in hist if sim_t - t <= window_s]
        if len(pts) < 4:
            return None
        mid = len(pts) // 2
        early = sum(q for _, q in pts[:mid]) / mid
        late = sum(q for _, q in pts[mid:]) / max(1, len(pts) - mid)
        if early <= 1e-3:
            return None
        # If queue grew, treat as discharge drop ratio capped
        growth = max(0.0, late - early) / max(early, 1.0)
        return min(1.0, growth)
