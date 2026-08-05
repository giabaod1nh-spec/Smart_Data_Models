"""Approved BD-1 Congestion Score Window v1."""
from __future__ import annotations

from de.gold.aggregators.intersection_window import IntersectionTrafficRollup
from de.gold.calculators.explanation import (
    ExplanationFactor,
    KpiCalculation,
    assert_contribution_tolerance,
    canonical_explanation,
    decimal_value,
    round_half_up,
)
from de.gold.quality import NUMERIC_BLOCKING


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def calculate_congestion(rollup: IntersectionTrafficRollup) -> KpiCalculation:
    if rollup.quality_status in NUMERIC_BLOCKING:
        return KpiCalculation(
            "CONGESTION_SCORE_WINDOW", "v1.0", None, "SCORE_0_100",
            rollup.quality_status, rollup.quality_status, rollup.quality_flags,
            canonical_explanation((), rule_version="bd1_congestion_window_v1", extra={"missing": ["required_input"]}),
        )
    normalized = (
        ("QUEUE_HIGH", rollup.max_queue_length_m, _clamp(rollup.max_queue_length_m / 100.0), 0.35, "METER"),
        ("SPEED_LOW", rollup.avg_speed_kmh, _clamp(1.0 - rollup.avg_speed_kmh / 50.0), 0.30, "KM_PER_HOUR"),
        ("OCCUPANCY_HIGH", rollup.avg_occupancy_pct, _clamp(rollup.avg_occupancy_pct / 100.0), 0.20, "PERCENT"),
        ("SPILLBACK_PRESENT", rollup.spillback_ratio_pct, _clamp(rollup.spillback_ratio_pct / 100.0), 0.15, "PERCENT"),
    )
    factors = tuple(ExplanationFactor(code, raw, norm, weight, weight * norm * 100.0, unit) for code, raw, norm, weight, unit in normalized)
    raw_score = sum(decimal_value(item.weight) * decimal_value(item.normalized) for item in factors) * decimal_value(100)
    score = round_half_up(raw_score)
    assert_contribution_tolerance(score, factors)
    label = "LOW" if score < 40 else "MEDIUM" if score < 70 else "HIGH"
    return KpiCalculation(
        "CONGESTION_SCORE_WINDOW", "v1.0", score, "SCORE_0_100", label,
        rollup.quality_status, rollup.quality_flags,
        canonical_explanation(factors, rule_version="bd1_congestion_window_v1", extra={"label": label}),
    )
