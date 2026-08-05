"""Approved BD-3 priority calculation and deterministic ranking."""
from __future__ import annotations

from dataclasses import dataclass

from de.gold.aggregators.intersection_window import IntersectionAggregate, IntersectionTrafficRollup
from de.gold.calculators.explanation import ExplanationFactor, KpiCalculation, canonical_explanation, decimal_value, round_half_up
from de.gold.quality import NUMERIC_BLOCKING, combine_quality, sorted_flags


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


@dataclass(frozen=True)
class PriorityResult:
    intersection_id: str
    window_id: str
    score: float | None
    congestion_score: float | None
    max_queue_length_m: float
    calculation: KpiCalculation
    rank: int | None = None


def calculate_priority(
    rollup: IntersectionTrafficRollup,
    congestion: KpiCalculation,
    intersection: IntersectionAggregate | None,
    previous: IntersectionTrafficRollup | None,
) -> PriorityResult:
    quality = combine_quality((rollup.quality_status, congestion.quality_status) + ((intersection.quality_status,) if intersection else ()))
    flags = sorted_flags(rollup.quality_flags, congestion.quality_flags, (() if previous else ("NO_PREV_QUEUE",)))
    if congestion.numeric_value is None or quality in NUMERIC_BLOCKING:
        calculation = KpiCalculation("INTERSECTION_PRIORITY_WINDOW", "v1.0", None, "SCORE_0_100", quality, quality, flags, canonical_explanation((), rule_version="bd3_priority_window_v1"))
        return PriorityResult(rollup.intersection_id, rollup.window.window_id, None, congestion.numeric_value, rollup.max_queue_length_m, calculation)
    c = congestion.numeric_value / 100.0
    ql = _clamp(rollup.max_queue_length_m / 100.0)
    qg = 0.0 if previous is None else _clamp((rollup.max_queue_length_m - previous.max_queue_length_m) / 50.0)
    sp = _clamp(rollup.spillback_ratio_pct / 100.0)
    inc = 1.0 if intersection and intersection.incident_occurrence else 0.0
    penalty = 0.10 if quality in {"VALID_WITH_GAPS", "PARTIAL", "LOW_COVERAGE"} else 0.0
    components = (
        ("CONGESTION", congestion.numeric_value, c, 0.45, 0.45 * c * 100.0),
        ("QUEUE_LEVEL", rollup.max_queue_length_m, ql, 0.20, 0.20 * ql * 100.0),
        ("QUEUE_GROWTH", 0.0 if previous is None else rollup.max_queue_length_m - previous.max_queue_length_m, qg, 0.15, 0.15 * qg * 100.0),
        ("SPILLBACK", rollup.spillback_ratio_pct, sp, 0.10, 0.10 * sp * 100.0),
        ("INCIDENT", inc, inc, 0.10, 0.10 * inc * 100.0),
        ("QUALITY_PENALTY", penalty, penalty, -1.0, -penalty * 100.0),
    )
    factors = tuple(ExplanationFactor(code, raw, normalized, weight, contribution, "SCORE_0_100") for code, raw, normalized, weight, contribution in components)
    raw = (
        decimal_value("0.45") * decimal_value(c)
        + decimal_value("0.20") * decimal_value(ql)
        + decimal_value("0.15") * decimal_value(qg)
        + decimal_value("0.10") * decimal_value(sp)
        + decimal_value("0.10") * decimal_value(inc)
        - decimal_value(penalty)
    )
    score = round_half_up(decimal_value(100) * max(decimal_value(0), min(decimal_value(1), raw)))
    calculation = KpiCalculation("INTERSECTION_PRIORITY_WINDOW", "v1.0", score, "SCORE_0_100", "VALID", quality, flags, canonical_explanation(factors, rule_version="bd3_priority_window_v1"))
    return PriorityResult(rollup.intersection_id, rollup.window.window_id, score, congestion.numeric_value, rollup.max_queue_length_m, calculation)


def rank_priorities(results: tuple[PriorityResult, ...]) -> tuple[PriorityResult, ...]:
    eligible = [item for item in results if item.score is not None]
    eligible.sort(key=lambda item: (-float(item.score), -float(item.congestion_score or 0), -item.max_queue_length_m, item.intersection_id))
    ranks = {item.intersection_id: index + 1 for index, item in enumerate(eligible)}
    return tuple(PriorityResult(item.intersection_id, item.window_id, item.score, item.congestion_score, item.max_queue_length_m, item.calculation, ranks.get(item.intersection_id)) for item in results)
