"""Pure analytical validation for Gold 2 typed inputs."""
from __future__ import annotations

import math
from dataclasses import dataclass

from de.gold.input_models import (
    GoldTransformationContext,
    SilverCameraObservationInput,
    SilverGoldInput,
    SilverSignalStateInput,
    SilverTrafficObservationInput,
)


VALID_FOR_GOLD = "VALID_FOR_GOLD"
ANALYTICAL_UNSUPPORTED = "ANALYTICAL_UNSUPPORTED"
ANALYTICAL_CONFLICTED = "ANALYTICAL_CONFLICTED"
ANALYTICAL_INSUFFICIENT_DATA = "ANALYTICAL_INSUFFICIENT_DATA"


@dataclass(frozen=True)
class ValidationResult:
    status: str
    errors: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return self.status == VALID_FOR_GOLD


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def validate_context(context: GoldTransformationContext) -> None:
    if not context.namespace.strip():
        raise ValueError("namespace is required")
    if context.definition_major < 0 or context.definition_minor < 0 or context.revision_seq < 0:
        raise ValueError("definition/revision numbers must be non-negative")
    if context.analytical_age_sec < 0 or context.stale_after_sec <= 0:
        raise ValueError("freshness durations are invalid")


def validate_input(record: SilverGoldInput) -> ValidationResult:
    errors: list[str] = []
    for name in ("simulation_run_id", "scenario_id", "intersection_id", "source_bronze_event_id", "source_payload_hash"):
        if not str(getattr(record, name, "")).strip():
            errors.append(f"MISSING_{name.upper()}")
    if not _finite(record.simulation_time_sec) or float(record.simulation_time_sec) < 0:
        errors.append("INVALID_SIMULATION_TIME")
    if int(record.cycle_sequence) < 0 or int(record.source_partition) < 0 or int(record.source_offset) < 0:
        errors.append("INVALID_SOURCE_POSITION")

    if isinstance(record, SilverTrafficObservationInput):
        numeric = (
            record.vehicle_count, record.pcu_equivalent, record.average_speed_kmh,
            record.queue_length_m, record.waiting_vehicle_count,
            record.arrival_rate_pcu_per_sec,
        )
        if any(not _finite(v) or float(v) < 0 for v in numeric):
            errors.append("INVALID_TRAFFIC_METRIC")
        if not _finite(record.occupancy_pct) or not 0 <= float(record.occupancy_pct) <= 100:
            errors.append("INVALID_OCCUPANCY")
    elif isinstance(record, SilverSignalStateInput):
        for value in (record.green_duration_sec, record.red_duration_sec, record.yellow_duration_sec):
            if value is not None and (not _finite(value) or float(value) < 0):
                errors.append("INVALID_SIGNAL_DURATION")
                break
    elif isinstance(record, SilverCameraObservationInput):
        if not _finite(record.confidence) or not 0 <= float(record.confidence) <= 1:
            errors.append("INVALID_CAMERA_CONFIDENCE")

    if errors:
        return ValidationResult(ANALYTICAL_INSUFFICIENT_DATA, tuple(sorted(set(errors))))
    return ValidationResult(VALID_FOR_GOLD)

