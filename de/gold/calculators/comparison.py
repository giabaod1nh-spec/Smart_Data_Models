"""Current versus previous window comparison contract."""
from __future__ import annotations

from dataclasses import dataclass

from de.gold.calculators.explanation import round_half_up


@dataclass(frozen=True)
class ComparisonResult:
    current_value: float | None
    previous_value: float | None
    absolute_change: float | None
    percent_change: float | None
    change_direction: str
    comparison_status: str


def compare_values(current: float | None, previous: float | None, *, compatible: bool = True) -> ComparisonResult:
    if not compatible or current is None or previous is None:
        return ComparisonResult(current, previous, None, None, "UNCHANGED", "NOT_COMPARABLE")
    absolute = current - previous
    direction = "INCREASE" if absolute > 0 else "DECREASE" if absolute < 0 else "UNCHANGED"
    if previous == 0:
        return ComparisonResult(current, previous, round_half_up(absolute), None, direction, "NOT_COMPARABLE")
    percent = absolute / abs(previous) * 100.0
    return ComparisonResult(current, previous, round_half_up(absolute), round_half_up(percent), direction, "COMPARABLE")
