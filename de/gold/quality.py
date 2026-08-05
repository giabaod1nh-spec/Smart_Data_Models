"""Approved Gold 1 parent quality, coverage and freshness contracts."""
from __future__ import annotations

from dataclasses import dataclass


QUALITY_PRECEDENCE = {
    "VALID": 0,
    "VALID_WITH_GAPS": 1,
    "PARTIAL": 2,
    "LOW_COVERAGE": 3,
    "INSUFFICIENT_DATA": 4,
    "UNSUPPORTED": 5,
    "CONFLICTED": 6,
}
NUMERIC_ALLOWED = frozenset({"VALID", "VALID_WITH_GAPS", "PARTIAL", "LOW_COVERAGE"})
NUMERIC_BLOCKING = frozenset({"INSUFFICIENT_DATA", "UNSUPPORTED", "CONFLICTED"})


def normalize_quality(value: str) -> str:
    if value == "VALID_WITH_DEFAULT":
        return "VALID_WITH_GAPS"
    return value if value in QUALITY_PRECEDENCE else "UNSUPPORTED"


def combine_quality(statuses: tuple[str, ...]) -> str:
    if not statuses:
        return "INSUFFICIENT_DATA"
    normalized = tuple(normalize_quality(value) for value in statuses)
    return max(normalized, key=lambda value: QUALITY_PRECEDENCE[value])


def numeric_allowed(status: str) -> bool:
    return normalize_quality(status) in NUMERIC_ALLOWED


def sorted_flags(*groups: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted({flag for group in groups for flag in group if flag}))


@dataclass(frozen=True)
class CoverageResult:
    valid_rows: int
    expected_rows: int
    ratio: float | None
    status: str


def pooled_coverage(valid_rows: int, expected_rows: int) -> CoverageResult:
    valid = max(0, int(valid_rows))
    expected = max(0, int(expected_rows))
    if expected == 0:
        return CoverageResult(valid, expected, None, "INSUFFICIENT_DATA")
    ratio = min(1.0, valid / expected)
    if ratio >= 0.80:
        status = "VALID"
    elif ratio >= 0.30:
        status = "LOW_COVERAGE"
    else:
        status = "INSUFFICIENT_DATA"
    return CoverageResult(valid, expected, ratio, status)


def analytical_freshness(*, closed: bool, revision: bool, age_sec: float, stale_after_sec: float, quality_status: str) -> str:
    quality = normalize_quality(quality_status)
    if quality == "CONFLICTED":
        return "CONFLICTED"
    if quality in {"INSUFFICIENT_DATA", "UNSUPPORTED"}:
        return "INSUFFICIENT_DATA"
    if not closed:
        return "PARTIAL_WINDOW"
    if revision:
        return "REVISED"
    if age_sec > stale_after_sec:
        return "STALE_ANALYTICAL"
    return "CLOSED_WITH_GAPS" if quality != "VALID" else "CLOSED_COMPLETE"

