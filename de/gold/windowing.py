"""Gold 60/300 second window assignment."""
from __future__ import annotations

import math
from dataclasses import dataclass

from de.gold.contracts import WINDOW_SIZES_SEC, canonical_window_id
from de.gold.input_models import SilverGoldInput


@dataclass(frozen=True)
class WindowKey:
    simulation_run_id: str
    scenario_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    window_id: str


@dataclass(frozen=True)
class WindowedRecord:
    window: WindowKey
    record: SilverGoldInput


def assign_window(record: SilverGoldInput, size: int) -> WindowKey:
    if size not in WINDOW_SIZES_SEC:
        raise ValueError(f"unsupported window size: {size}")
    value = float(record.simulation_time_sec)
    if not math.isfinite(value) or value < 0:
        raise ValueError("simulation_time_sec must be finite and non-negative")
    start = float(math.floor(value / size) * size)
    end = start + float(size)
    return WindowKey(
        record.simulation_run_id, record.scenario_id, size, start, end,
        canonical_window_id(record.simulation_run_id, record.scenario_id, size, start, end),
    )


def expand_windows(records: tuple[SilverGoldInput, ...]) -> tuple[WindowedRecord, ...]:
    expanded = [WindowedRecord(assign_window(record, size), record) for record in records for size in WINDOW_SIZES_SEC]
    return tuple(sorted(expanded, key=lambda item: (item.window.window_start_sim_sec, item.window.window_size_sec, item.record.intersection_id, item.record.source_partition, item.record.source_offset)))


TUMBLING_SPEC = "tumbling-epoch-zero-v1"


def tumbling_bounds(simulation_time: float | int, duration: int) -> tuple[int, int]:
    """Assign verified simulation time without rounding across boundaries."""
    if duration not in (60, 300):
        raise ValueError("Unsupported tumbling duration")
    if (type(simulation_time) not in (int, float) or simulation_time < 0
            or simulation_time >= (1 << 63) - 300 or not math.isfinite(simulation_time)):
        raise ValueError("Invalid V1 simulation time")
    start = int(simulation_time) // duration * duration
    return start, start + duration
