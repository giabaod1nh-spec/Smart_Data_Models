"""Silver row → Gold 2 input mapping, quality derivation and context construction.

Quality follows Gold Runtime Contract v1 / G3-P0-009. Silver carries
``quality_status`` only on the traffic fact; intersection, signal and camera rows
derive it from required-field presence and identity conflicts. ``VALID`` is never a
fallback when a required field is absent. Window-level ``LOW_COVERAGE`` remains the
Gold 2 engine's pooled-coverage decision and is not pre-empted here.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

from de.gold.contracts import DIRECTION_MAPPING_VERSION, canonicalize_direction
from de.gold.input_models import (
    GoldTransformationContext,
    SilverCameraObservationInput,
    SilverGoldInput,
    SilverIntersectionStateInput,
    SilverSignalStateInput,
    SilverTrafficObservationInput,
)
from de.gold_runtime.config import (
    DEFINITION_MAJOR,
    DEFINITION_MINOR,
    SOURCE_TABLE_CAMERA,
    SOURCE_TABLE_INTERSECTION,
    SOURCE_TABLE_SIGNAL,
    SOURCE_TABLE_TRAFFIC,
    GoldSettings,
)
from de.gold_runtime.cursor import normalize_hash, row_identity
from de.gold_runtime.window_scheduler import WindowIdentity

QUALITY_VALID = "VALID"
QUALITY_VALID_WITH_GAPS = "VALID_WITH_GAPS"
QUALITY_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
QUALITY_CONFLICTED = "CONFLICTED"

TRAFFIC_REQUIRED_METRICS = (
    "vehicle_count", "pcu_equivalent", "average_speed_kmh", "queue_length_m",
    "occupancy_pct", "arrival_rate_pcu_per_sec",
)
INTERSECTION_REQUIRED_FIELDS = (
    "overall_traffic_status", "derived_traffic_state", "current_phase",
)
INTERSECTION_OPTIONAL_FIELDS = ("total_vehicle_count",)
SIGNAL_REQUIRED_FIELDS = ("signal_status", "current_phase")
SIGNAL_OPTIONAL_FIELDS = ("green_duration_sec", "red_duration_sec", "yellow_duration_sec")
CAMERA_REQUIRED_FIELDS = ("incident_detected", "confidence")


class ContextBuildError(ValueError):
    """A Silver row cannot be mapped to a Gold 2 input without inventing a value."""


def parse_quality_flags(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    text = str(raw).strip()
    if not text:
        return ()
    return tuple(sorted({part.strip() for part in text.replace(";", ",").split(",") if part.strip()}))


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, float):
        return math.isfinite(value)
    return True


def _as_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ── G3-P0-009 quality derivation ────────────────────────────────────────────


def derive_traffic_quality(row: Mapping[str, Any], *, conflicted: bool = False) -> str:
    if conflicted:
        return QUALITY_CONFLICTED
    if any(not _present(row.get(name)) for name in TRAFFIC_REQUIRED_METRICS):
        return QUALITY_INSUFFICIENT_DATA
    status = str(row.get("quality_status") or "").strip()
    if not status:
        return QUALITY_INSUFFICIENT_DATA
    return status


def derive_intersection_quality(row: Mapping[str, Any], *, conflicted: bool = False) -> str:
    if conflicted:
        return QUALITY_CONFLICTED
    if any(not _present(row.get(name)) for name in INTERSECTION_REQUIRED_FIELDS):
        return QUALITY_INSUFFICIENT_DATA
    if any(not _present(row.get(name)) for name in INTERSECTION_OPTIONAL_FIELDS):
        return QUALITY_VALID_WITH_GAPS
    return QUALITY_VALID


def derive_signal_quality(row: Mapping[str, Any], *, conflicted: bool = False) -> str:
    if conflicted:
        return QUALITY_CONFLICTED
    if any(not _present(row.get(name)) for name in SIGNAL_REQUIRED_FIELDS):
        return QUALITY_INSUFFICIENT_DATA
    if any(not _present(row.get(name)) for name in SIGNAL_OPTIONAL_FIELDS):
        return QUALITY_VALID_WITH_GAPS
    return QUALITY_VALID


def derive_camera_quality(row: Mapping[str, Any], *, conflicted: bool = False) -> str:
    if conflicted:
        return QUALITY_CONFLICTED
    if any(not _present(row.get(name)) for name in CAMERA_REQUIRED_FIELDS):
        return QUALITY_INSUFFICIENT_DATA
    return QUALITY_VALID


QUALITY_DERIVERS = {
    SOURCE_TABLE_TRAFFIC: derive_traffic_quality,
    SOURCE_TABLE_INTERSECTION: derive_intersection_quality,
    SOURCE_TABLE_SIGNAL: derive_signal_quality,
    SOURCE_TABLE_CAMERA: derive_camera_quality,
}


def derive_quality(source_name: str, row: Mapping[str, Any], *, conflicted: bool = False) -> str:
    try:
        deriver = QUALITY_DERIVERS[source_name]
    except KeyError as exc:
        raise ContextBuildError(f"no quality contract for {source_name!r}") from exc
    return deriver(row, conflicted=conflicted)


# ── Silver row → Gold 2 typed input ─────────────────────────────────────────


def _base(row: Mapping[str, Any]) -> dict:
    return {
        "simulation_run_id": str(row["simulation_run_id"]),
        "scenario_id": str(row["scenario_id"]),
        "intersection_id": str(row["intersection_id"]),
        "source_entity_id": str(row["source_entity_id"]),
        "cycle_sequence": int(row["cycle_sequence"]),
        "simulation_time_sec": float(row["simulation_time_sec"]),
        "source_bronze_event_id": normalize_hash(row["source_bronze_event_id"]),
        "source_raw_ingestion_id": normalize_hash(row["source_raw_ingestion_id"]),
        "source_topic": str(row["source_topic"]),
        "source_partition": int(row["source_partition"]),
        "source_offset": int(row["source_offset"]),
        "source_payload_hash": normalize_hash(row["source_payload_hash"]),
        "processed_at": _as_dt(row["processed_at"]),
        "migration_version": str(row.get("migration_version") or "k9-silver-v1"),
    }


def build_traffic_input(
    row: Mapping[str, Any], *, conflicted: bool = False
) -> SilverTrafficObservationInput:
    canonical, source_direction, direction_flags = canonicalize_direction(row.get("direction"))
    flags = tuple(sorted(set(parse_quality_flags(row.get("quality_flags"))) | set(direction_flags)))
    return SilverTrafficObservationInput(
        **_base(row),
        source_direction=source_direction,
        canonical_direction=canonical,
        direction_mapping_version=DIRECTION_MAPPING_VERSION,
        vehicle_count=int(row["vehicle_count"]),
        pcu_equivalent=float(row["pcu_equivalent"]),
        average_speed_kmh=float(row["average_speed_kmh"]),
        queue_length_m=float(row["queue_length_m"]),
        waiting_vehicle_count=int(row.get("waiting_vehicle_count") or 0),
        occupancy_pct=float(row["occupancy_pct"]),
        arrival_rate_pcu_per_sec=float(row["arrival_rate_pcu_per_sec"]),
        traffic_status=str(row.get("traffic_status") or "UNKNOWN"),
        spillback_risk=bool(row.get("spillback_risk")),
        quality_status=derive_traffic_quality(row, conflicted=conflicted),
        quality_flags=flags,
    )


def build_intersection_input(
    row: Mapping[str, Any], *, conflicted: bool = False
) -> SilverIntersectionStateInput:
    total = row.get("total_vehicle_count")
    return SilverIntersectionStateInput(
        **_base(row),
        overall_traffic_status=str(row.get("overall_traffic_status") or ""),
        derived_traffic_state=str(row.get("derived_traffic_state") or ""),
        current_phase=str(row.get("current_phase") or ""),
        has_active_incident=bool(row.get("has_active_incident")),
        has_spillback=bool(row.get("has_spillback")),
        is_box_blocked=bool(row.get("is_box_blocked")),
        total_vehicle_count=None if total is None else int(total),
        quality_status=derive_intersection_quality(row, conflicted=conflicted),
        quality_flags=parse_quality_flags(row.get("quality_flags")),
    )


def build_signal_input(
    row: Mapping[str, Any], *, conflicted: bool = False
) -> SilverSignalStateInput:
    canonical, source_direction, direction_flags = canonicalize_direction(row.get("direction"))
    flags = tuple(sorted(set(parse_quality_flags(row.get("quality_flags"))) | set(direction_flags)))
    optional = {
        name: (None if row.get(name) is None else float(row[name]))
        for name in SIGNAL_OPTIONAL_FIELDS
    }
    return SilverSignalStateInput(
        **_base(row),
        source_direction=source_direction,
        canonical_direction=canonical,
        direction_mapping_version=DIRECTION_MAPPING_VERSION,
        signal_status=str(row.get("signal_status") or ""),
        current_phase=str(row.get("current_phase") or ""),
        timing_mode=str(row.get("timing_mode") or ""),
        quality_status=derive_signal_quality(row, conflicted=conflicted),
        quality_flags=flags,
        **optional,
    )


def build_camera_input(
    row: Mapping[str, Any], *, conflicted: bool = False
) -> SilverCameraObservationInput:
    return SilverCameraObservationInput(
        **_base(row),
        incident_detected=bool(row.get("incident_detected")),
        confidence=float(row.get("confidence") if row.get("confidence") is not None else 0.0),
        quality_status=derive_camera_quality(row, conflicted=conflicted),
        quality_flags=parse_quality_flags(row.get("quality_flags")),
    )


INPUT_BUILDERS = {
    SOURCE_TABLE_TRAFFIC: build_traffic_input,
    SOURCE_TABLE_INTERSECTION: build_intersection_input,
    SOURCE_TABLE_SIGNAL: build_signal_input,
    SOURCE_TABLE_CAMERA: build_camera_input,
}


def build_inputs(
    source_name: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    conflicted_identities: Iterable[tuple] = (),
) -> tuple[SilverGoldInput, ...]:
    builder = INPUT_BUILDERS.get(source_name)
    if builder is None:
        raise ContextBuildError(f"{source_name!r} is not a Gold 2 input source")
    conflicts = set(conflicted_identities)
    return tuple(
        builder(row, conflicted=row_identity(source_name, row) in conflicts) for row in rows
    )


def _record_sort_key(record: SilverGoldInput) -> tuple:
    return (
        record.simulation_run_id,
        record.scenario_id,
        float(record.simulation_time_sec),
        record.intersection_id,
        getattr(record, "canonical_direction", ""),
        type(record).__name__,
        int(record.source_partition),
        int(record.source_offset),
        record.source_bronze_event_id,
    )


def order_records(records: Iterable[SilverGoldInput]) -> tuple[SilverGoldInput, ...]:
    """Deterministic engine input: no duplicate semantic record, canonical order."""
    unique: dict[tuple, SilverGoldInput] = {}
    for record in records:
        key = (
            type(record).__name__,
            record.simulation_run_id,
            record.scenario_id,
            record.intersection_id,
            getattr(record, "canonical_direction", ""),
            record.source_entity_id,
            float(record.simulation_time_sec),
            record.source_payload_hash,
        )
        unique.setdefault(key, record)
    return tuple(sorted(unique.values(), key=_record_sort_key))


# ── G3-P1-002 expected rows ─────────────────────────────────────────────────


def observation_slots(window_size_sec: int, cadence_sec: float) -> int:
    if cadence_sec <= 0:
        raise ContextBuildError("cadence must be > 0")
    return max(1, int(math.floor(float(window_size_sec) / float(cadence_sec))))


def expected_rows_for(
    windows: Sequence[WindowIdentity],
    *,
    intersections: Sequence[str],
    directions: Sequence[str],
    traffic_cadence_sec: float,
    intersection_cadence_sec: float,
    signal_cadence_sec: float,
) -> dict[str, int]:
    """Configured intersection × eligible-direction matrix × cadence slots.

    Keys mirror the Gold 2 lookup strings exactly; camera is evidence-only and never
    enters a denominator. An intentionally absent dimension is the only zero case.
    """
    expected: dict[str, int] = {}
    if not intersections or not directions:
        return expected
    for window in windows:
        size = int(window.window_size_sec)
        start = float(window.window_start_sim_sec)
        traffic_slots = observation_slots(size, traffic_cadence_sec)
        intersection_slots = observation_slots(size, intersection_cadence_sec)
        signal_slots = observation_slots(size, signal_cadence_sec)
        for intersection_id in intersections:
            rollup_key = (
                window.simulation_run_id, window.scenario_id, intersection_id, size, start,
            )
            expected[f"intersection|{rollup_key}"] = intersection_slots
            for direction in directions:
                directional_key = (
                    window.simulation_run_id, window.scenario_id, intersection_id,
                    direction, size, start,
                )
                expected[f"traffic|{directional_key}"] = traffic_slots
                expected[f"signal|{directional_key}"] = signal_slots
                expected[f"direction|{rollup_key}|{direction}"] = traffic_slots
        expected[f"network|{window.window_id}"] = (
            len(intersections) * len(directions) * traffic_slots
        )
    return expected


# ── Context ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WorkUnitInputs:
    target: WindowIdentity
    previous: WindowIdentity
    records: tuple[SilverGoldInput, ...]
    intersections: tuple[str, ...]
    warnings: tuple[str, ...] = ()


def build_context(
    settings: GoldSettings,
    *,
    computed_at: datetime,
    windows: Sequence[WindowIdentity],
    intersections: Sequence[str],
    revision_seq: int = 0,
    analytical_age_sec: float = 0.0,
    directions: Optional[Sequence[str]] = None,
) -> GoldTransformationContext:
    resolved_directions = tuple(directions) if directions is not None else ("N", "S", "E", "W")
    expected = expected_rows_for(
        windows,
        intersections=tuple(intersections),
        directions=resolved_directions,
        traffic_cadence_sec=settings.traffic_expected_cadence_sec,
        intersection_cadence_sec=settings.intersection_expected_cadence_sec,
        signal_cadence_sec=settings.signal_expected_cadence_sec,
    )
    return GoldTransformationContext(
        namespace=settings.namespace,
        computed_at=computed_at.astimezone(timezone.utc),
        definition_version=settings.definition_version,
        definition_major=DEFINITION_MAJOR,
        definition_minor=DEFINITION_MINOR,
        revision_seq=int(revision_seq),
        gold_schema_version=settings.gold_schema_version,
        expected_rows=expected,
        configured_intersections=tuple(intersections),
        configured_directions=resolved_directions,
        window_closed=True,
        is_revision=int(revision_seq) > 0,
        analytical_age_sec=float(analytical_age_sec),
        stale_after_sec=float(settings.analytical_stale_threshold_sec),
    )
