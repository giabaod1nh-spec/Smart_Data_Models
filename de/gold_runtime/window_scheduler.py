"""Watermark, window eligibility and close scheduling (Gold Runtime Contract v1).

| Output              | Required streams                | Watermark                          |
|---------------------|---------------------------------|------------------------------------|
| Traffic window      | traffic                         | max(traffic.simulation_time_sec)   |
| Intersection/KPI    | traffic, intersection           | min of both maxima                 |
| Signal window       | signal                          | max(signal.simulation_time_sec)    |
| Network overview    | traffic, intersection, signal   | min of the three maxima            |

Eligibility is strict: ``watermark > window_end``. A missing required stream leaves
the watermark undefined and the window ``OPEN``; wall-clock substitution is forbidden.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Optional, Sequence

from de.gold.contracts import canonical_window_id
from de.gold_runtime.config import (
    ALLOWED_LATENESS_SEC,
    WATERMARK_DELAY_SEC,
    LateClass,
    WindowState,
)


class OutputFamily(str, Enum):
    TRAFFIC = "TRAFFIC"
    INTERSECTION = "INTERSECTION"
    SIGNAL = "SIGNAL"
    NETWORK = "NETWORK"


REQUIRED_STREAMS: dict[OutputFamily, tuple[str, ...]] = {
    OutputFamily.TRAFFIC: ("traffic",),
    OutputFamily.INTERSECTION: ("traffic", "intersection"),
    OutputFamily.SIGNAL: ("signal",),
    OutputFamily.NETWORK: ("traffic", "intersection", "signal"),
}

ALLOWED_TRANSITIONS: dict[WindowState, frozenset[WindowState]] = {
    WindowState.OPEN: frozenset({WindowState.OPEN, WindowState.ELIGIBLE}),
    WindowState.ELIGIBLE: frozenset({WindowState.ELIGIBLE, WindowState.PROCESSING}),
    WindowState.PROCESSING: frozenset(
        {WindowState.PROCESSING, WindowState.CLOSED, WindowState.ELIGIBLE}
    ),
    WindowState.CLOSED: frozenset({WindowState.REVISED}),
    WindowState.REVISED: frozenset(),
}


class WindowStateError(ValueError):
    """Illegal window-state transition; a closed window never reopens."""


@dataclass(frozen=True)
class StreamMaxima:
    """Maximum observed simulation time per required stream for one run."""

    traffic: Optional[float] = None
    intersection: Optional[float] = None
    signal: Optional[float] = None
    camera: Optional[float] = None

    def get(self, stream: str) -> Optional[float]:
        return getattr(self, stream)

    def merge(self, other: "StreamMaxima") -> "StreamMaxima":
        def _max(left: Optional[float], right: Optional[float]) -> Optional[float]:
            values = [value for value in (left, right) if value is not None]
            return max(values) if values else None

        return StreamMaxima(
            traffic=_max(self.traffic, other.traffic),
            intersection=_max(self.intersection, other.intersection),
            signal=_max(self.signal, other.signal),
            camera=_max(self.camera, other.camera),
        )


@dataclass(frozen=True)
class WindowIdentity:
    namespace: str
    simulation_run_id: str
    scenario_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    window_id: str

    @property
    def queue_key(self) -> tuple:
        return (
            self.window_end_sim_sec,
            self.window_size_sec,
            self.simulation_run_id,
            self.window_id,
        )

    def as_dict(self) -> dict:
        return {
            "namespace": self.namespace,
            "simulation_run_id": self.simulation_run_id,
            "scenario_id": self.scenario_id,
            "window_size_sec": int(self.window_size_sec),
            "window_start_sim_sec": float(self.window_start_sim_sec),
            "window_end_sim_sec": float(self.window_end_sim_sec),
            "window_id": self.window_id,
        }


def make_window_identity(
    namespace: str,
    simulation_run_id: str,
    scenario_id: str,
    window_size_sec: int,
    window_start_sim_sec: float,
) -> WindowIdentity:
    start = float(window_start_sim_sec)
    end = start + float(window_size_sec)
    return WindowIdentity(
        namespace=namespace,
        simulation_run_id=simulation_run_id,
        scenario_id=scenario_id,
        window_size_sec=int(window_size_sec),
        window_start_sim_sec=start,
        window_end_sim_sec=end,
        window_id=canonical_window_id(
            simulation_run_id, scenario_id, int(window_size_sec), start, end
        ),
    )


def previous_window(window: WindowIdentity) -> WindowIdentity:
    return make_window_identity(
        window.namespace,
        window.simulation_run_id,
        window.scenario_id,
        window.window_size_sec,
        window.window_start_sim_sec - float(window.window_size_sec),
    )


def window_start_for(simulation_time_sec: float, window_size_sec: int) -> float:
    value = float(simulation_time_sec)
    if not math.isfinite(value) or value < 0:
        raise ValueError("simulation_time_sec must be finite and non-negative")
    return float(math.floor(value / window_size_sec) * window_size_sec)


def watermark(
    family: OutputFamily,
    maxima: StreamMaxima,
    *,
    delay_sec: float = WATERMARK_DELAY_SEC,
) -> Optional[float]:
    """Undefined (``None``) when any required stream has no observed row."""
    values = [maxima.get(stream) for stream in REQUIRED_STREAMS[family]]
    if any(value is None for value in values):
        return None
    return min(float(value) for value in values) - float(delay_sec)


def runtime_watermark(
    maxima: StreamMaxima, *, delay_sec: float = WATERMARK_DELAY_SEC
) -> Optional[float]:
    """One transform emits every family, so scheduling uses the network watermark."""
    return watermark(OutputFamily.NETWORK, maxima, delay_sec=delay_sec)


def is_eligible(
    window_end_sim_sec: float,
    current_watermark: Optional[float],
    *,
    allowed_lateness_sec: float = ALLOWED_LATENESS_SEC,
) -> bool:
    if current_watermark is None:
        return False
    return float(current_watermark) > float(window_end_sim_sec) + float(allowed_lateness_sec)


def assert_transition(current: WindowState, new: WindowState) -> WindowState:
    if new not in ALLOWED_TRANSITIONS[current]:
        raise WindowStateError(f"illegal window transition {current.value} -> {new.value}")
    return new


def candidate_windows(
    *,
    namespace: str,
    simulation_run_id: str,
    scenario_id: str,
    maxima: StreamMaxima,
    window_sizes_sec: Sequence[int],
    floor_by_size: Optional[Mapping[int, float]] = None,
    allowed_lateness_sec: float = ALLOWED_LATENESS_SEC,
    delay_sec: float = WATERMARK_DELAY_SEC,
    limit: int = 1,
) -> tuple[WindowIdentity, ...]:
    """Eligible closed windows in deterministic queue order.

    ``floor_by_size`` carries the highest already-closed ``window_end`` per size so a
    closed window is never re-queued.
    """
    mark = runtime_watermark(maxima, delay_sec=delay_sec)
    if mark is None:
        return ()
    floors = dict(floor_by_size or {})
    candidates: list[WindowIdentity] = []
    for size in window_sizes_sec:
        size = int(size)
        floor = float(floors.get(size, 0.0))
        start = float(math.floor(floor / size) * size)
        while True:
            identity = make_window_identity(
                namespace, simulation_run_id, scenario_id, size, start
            )
            if identity.window_end_sim_sec <= floor:
                start += size
                continue
            if not is_eligible(
                identity.window_end_sim_sec, mark, allowed_lateness_sec=allowed_lateness_sec
            ):
                break
            candidates.append(identity)
            if len(candidates) > limit * len(tuple(window_sizes_sec)) + limit:
                break
            start += size
    return queue_order(candidates)[: max(0, int(limit))]


def queue_order(windows: Iterable[WindowIdentity]) -> tuple[WindowIdentity, ...]:
    return tuple(sorted(windows, key=lambda item: item.queue_key))


def classify_late_row(
    *,
    simulation_time_sec: float,
    window_end_sim_sec: float,
    window_state: WindowState,
    source_set_hash_changed: bool,
    identity_conflict: bool = False,
) -> LateClass:
    """Late-data classification under zero allowed lateness."""
    if identity_conflict:
        return LateClass.CONFLICT
    if float(simulation_time_sec) >= float(window_end_sim_sec):
        return LateClass.ON_TIME
    if window_state in {WindowState.CLOSED, WindowState.REVISED}:
        return LateClass.LATE_AFTER_CLOSE if source_set_hash_changed else LateClass.ON_TIME
    return LateClass.LATE_BEFORE_CLOSE


def source_lag(maxima: StreamMaxima, window_end_sim_sec: float) -> dict[str, float]:
    """Per-stream simulation-time lag behind a window end; bounded label set."""
    lag: dict[str, float] = {}
    for stream in ("traffic", "intersection", "signal", "camera"):
        value = maxima.get(stream)
        lag[stream] = 0.0 if value is None else max(0.0, float(window_end_sim_sec) - float(value))
    return lag
