"""Canonical decimal and explanation JSON helpers."""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP


TWO_PLACES = Decimal("0.01")
CONTRIBUTION_TOLERANCE = Decimal("0.02")


def decimal_value(value: object) -> Decimal:
    return Decimal(str(value))


def round_half_up(value: object, places: Decimal = TWO_PLACES) -> float:
    return float(decimal_value(value).quantize(places, rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class ExplanationFactor:
    factor: str
    raw: float
    normalized: float
    weight: float
    contribution: float
    unit: str


@dataclass(frozen=True)
class KpiCalculation:
    metric_code: str
    metric_version: str
    numeric_value: float | None
    unit_code: str
    status: str
    quality_status: str
    quality_flags: tuple[str, ...]
    explanation_json: str


def canonical_explanation(factors: tuple[ExplanationFactor, ...], *, rule_version: str, extra: dict | None = None) -> str:
    payload = {
        "factors": [
            {
                "factor": item.factor,
                "raw": round_half_up(item.raw),
                "normalized": round_half_up(item.normalized),
                "weight": round_half_up(item.weight),
                "contribution": round_half_up(item.contribution),
                "unit": item.unit,
            }
            for item in factors
        ],
        "rule_version": rule_version,
    }
    if extra:
        payload.update(extra)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def assert_contribution_tolerance(score: float, factors: tuple[ExplanationFactor, ...]) -> None:
    serialized_sum = sum(decimal_value(round_half_up(item.contribution)) for item in factors)
    difference = abs(serialized_sum - decimal_value(round_half_up(score)))
    if difference > CONTRIBUTION_TOLERANCE:
        raise ValueError(f"contribution sum mismatch: {difference}")

