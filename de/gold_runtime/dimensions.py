"""Silver → Gold dimension mapping and the ``gold-lineage-hash-v1`` contract.

Serialization is canonical UTF-8 JSON in the exact field order of Gold Runtime
Contract v1: sorted nested object keys, explicit JSON nulls, trimmed + NFC
normalized strings, deterministic numeric form, no whitespace, SHA-256 lowercase
hex. Fields absent from the verified Silver contract do not participate and are
never replaced by a placeholder; whole-row hashing is forbidden.
"""
from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from de.gold.contracts import DIRECTION_MAPPING_VERSION, canonicalize_direction
from de.gold.models import (
    GoldDimApproach,
    GoldDimIntersection,
    GoldDimRun,
    GoldDimScenario,
    GoldDimWindow,
)
from de.gold_runtime import LINEAGE_HASH_CONTRACT
from de.gold_runtime.config import (
    DEFINITION_MAJOR,
    DEFINITION_MINOR,
    SOURCE_TABLE_DIM_APPROACH,
    SOURCE_TABLE_DIM_INTERSECTION,
    SOURCE_TABLE_DIM_RUN,
    SOURCE_TABLE_DIM_SCENARIO,
    GoldSettings,
)
from de.gold_runtime.window_scheduler import WindowIdentity

HASH_CONTRACT = LINEAGE_HASH_CONTRACT

# Exact Silver fields participating in each dimension hash (Contract v1, in order).
DIM_HASH_FIELDS: dict[str, tuple[str, ...]] = {
    SOURCE_TABLE_DIM_RUN: (
        "simulation_run_id", "scenario_id", "seed", "producer_id", "started_at",
        "ended_at", "run_status", "contract_version", "node_count",
        "source_bronze_run_id", "created_at", "updated_at",
    ),
    SOURCE_TABLE_DIM_SCENARIO: ("scenario_id", "description", "created_at"),
    SOURCE_TABLE_DIM_INTERSECTION: (
        "intersection_id", "intersection_name", "latitude", "longitude", "network_zone",
        "connected_intersections", "source_hash", "source_bronze_event_id", "created_at",
        "updated_at",
    ),
    SOURCE_TABLE_DIM_APPROACH: (
        "intersection_id", "direction", "source_bronze_event_id", "created_at", "updated_at",
    ),
}
SORTED_ARRAY_FIELDS: frozenset[str] = frozenset({"connected_intersections"})

DIM_TARGET_TABLE: dict[str, str] = {
    SOURCE_TABLE_DIM_RUN: "gold_dim_run",
    SOURCE_TABLE_DIM_SCENARIO: "gold_dim_scenario",
    SOURCE_TABLE_DIM_INTERSECTION: "gold_dim_intersection",
    SOURCE_TABLE_DIM_APPROACH: "gold_dim_approach",
}
DIM_BUSINESS_KEY: dict[str, tuple[str, ...]] = {
    "gold_dim_run": ("simulation_run_id",),
    "gold_dim_scenario": ("scenario_id",),
    "gold_dim_intersection": ("intersection_id",),
    "gold_dim_approach": ("intersection_id", "direction"),
    "gold_dim_window": ("window_id",),
    "gold_dim_metric_definition": ("metric_code", "metric_version"),
}


class DimensionMappingError(ValueError):
    """A Gold dimension field cannot be mapped from the verified Silver contract."""


class _Absent:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return "<ABSENT>"


ABSENT = _Absent()


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", value).strip()


def _canonical_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, _Absent):
        raise DimensionMappingError("absent fields must be filtered before serialization")
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", errors="replace").rstrip("\x00")
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return float(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return _normalize_text(str(value))


def canonical_hash_payload(
    source_name: str, row: Mapping[str, Any]
) -> list[list[Any]]:
    """Ordered ``[name, value]`` pairs; fields absent from the row are excluded."""
    fields = DIM_HASH_FIELDS[source_name]
    payload: list[list[Any]] = []
    for name in fields:
        if name not in row:
            continue
        value = row[name]
        if isinstance(value, _Absent):
            continue
        if name in SORTED_ARRAY_FIELDS and isinstance(value, (list, tuple)):
            value = sorted(str(item) for item in value)
        payload.append([name, _canonical_value(value)])
    return payload


def lineage_hash(source_name: str, row: Mapping[str, Any]) -> str:
    payload = canonical_hash_payload(source_name, row)
    if not payload:
        raise DimensionMappingError(f"{source_name}: no Silver field participates in the hash")
    encoded = json.dumps(
        payload, sort_keys=False, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _optional_dt(value: Any) -> Optional[datetime]:
    return None if value is None else _dt(value)


@dataclass(frozen=True)
class DimensionCandidate:
    target_table: str
    business_key: tuple[str, ...]
    source_hash: str
    row: Any

    @property
    def identity(self) -> tuple:
        return (self.target_table,) + self.business_key


def _version_fields(settings: GoldSettings, computed_at: datetime) -> dict:
    return {
        "definition_version": settings.definition_version,
        "definition_major": DEFINITION_MAJOR,
        "definition_minor": DEFINITION_MINOR,
        "computed_at": computed_at.astimezone(timezone.utc),
        "gold_schema_version": settings.gold_schema_version,
    }


def build_dim_run(
    row: Mapping[str, Any], settings: GoldSettings, computed_at: datetime
) -> DimensionCandidate:
    source_hash = lineage_hash(SOURCE_TABLE_DIM_RUN, row)
    model = GoldDimRun(
        simulation_run_id=str(row["simulation_run_id"]),
        scenario_id=str(row["scenario_id"]),
        seed=None if row.get("seed") is None else str(row["seed"]),
        producer_id=str(row["producer_id"]),
        started_at=_dt(row["started_at"]),
        ended_at=_optional_dt(row.get("ended_at")),
        run_status=str(row["run_status"]),
        contract_version=str(row["contract_version"]),
        node_count=None if row.get("node_count") is None else int(row["node_count"]),
        source_bronze_run_id=str(row["source_bronze_run_id"]),
        source_hash=source_hash,
        **_version_fields(settings, computed_at),
    )
    return DimensionCandidate("gold_dim_run", (model.simulation_run_id,), source_hash, model)


def build_dim_scenario(
    row: Mapping[str, Any], settings: GoldSettings, computed_at: datetime
) -> DimensionCandidate:
    source_hash = lineage_hash(SOURCE_TABLE_DIM_SCENARIO, row)
    model = GoldDimScenario(
        scenario_id=str(row["scenario_id"]),
        description=str(row.get("description") or ""),
        source_hash=source_hash,
        **_version_fields(settings, computed_at),
    )
    return DimensionCandidate("gold_dim_scenario", (model.scenario_id,), source_hash, model)


def build_dim_intersection(
    row: Mapping[str, Any], settings: GoldSettings, computed_at: datetime
) -> DimensionCandidate:
    source_hash = lineage_hash(SOURCE_TABLE_DIM_INTERSECTION, row)
    connected = row.get("connected_intersections") or []
    model = GoldDimIntersection(
        intersection_id=str(row["intersection_id"]),
        intersection_name=str(row.get("intersection_name") or ""),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        network_zone=str(row.get("network_zone") or ""),
        connected_intersections=sorted(str(item) for item in connected),
        source_hash=source_hash,
        **_version_fields(settings, computed_at),
    )
    return DimensionCandidate(
        "gold_dim_intersection", (model.intersection_id,), source_hash, model
    )


def build_dim_approach(
    row: Mapping[str, Any], settings: GoldSettings, computed_at: datetime
) -> DimensionCandidate:
    source_hash = lineage_hash(SOURCE_TABLE_DIM_APPROACH, row)
    canonical, source_direction, _flags = canonicalize_direction(row.get("direction"))
    model = GoldDimApproach(
        intersection_id=str(row["intersection_id"]),
        direction=canonical,
        source_direction=source_direction,
        direction_mapping_version=DIRECTION_MAPPING_VERSION,
        source_hash=source_hash,
        **_version_fields(settings, computed_at),
    )
    return DimensionCandidate(
        "gold_dim_approach", (model.intersection_id, model.direction), source_hash, model
    )


DIM_BUILDERS = {
    SOURCE_TABLE_DIM_RUN: build_dim_run,
    SOURCE_TABLE_DIM_SCENARIO: build_dim_scenario,
    SOURCE_TABLE_DIM_INTERSECTION: build_dim_intersection,
    SOURCE_TABLE_DIM_APPROACH: build_dim_approach,
}


def build_dimension_candidates(
    source_name: str,
    rows: Sequence[Mapping[str, Any]],
    settings: GoldSettings,
    computed_at: datetime,
) -> tuple[DimensionCandidate, ...]:
    try:
        builder = DIM_BUILDERS[source_name]
    except KeyError as exc:
        raise DimensionMappingError(f"{source_name!r} is not a Gold dimension source") from exc
    return tuple(builder(row, settings, computed_at) for row in rows)


def build_dim_window(
    window: WindowIdentity, settings: GoldSettings, computed_at: datetime
) -> DimensionCandidate:
    """Window identity comes from the Gold 2 canonical key, never a Silver hash."""
    model = GoldDimWindow(
        window_id=window.window_id,
        window_size_sec=int(window.window_size_sec),
        window_start_sim_sec=float(window.window_start_sim_sec),
        window_end_sim_sec=float(window.window_end_sim_sec),
        computed_at=computed_at.astimezone(timezone.utc),
        gold_schema_version=settings.gold_schema_version,
    )
    return DimensionCandidate("gold_dim_window", (model.window_id,), "", model)


def metric_definition_candidates(definitions: Sequence[Any]) -> tuple[DimensionCandidate, ...]:
    """Metric definitions come from the Gold 2 approved registry, unchanged."""
    return tuple(
        DimensionCandidate(
            "gold_dim_metric_definition",
            (item.metric_code, item.metric_version),
            "",
            item,
        )
        for item in definitions
    )


def detect_dimension_conflicts(
    candidates: Sequence[DimensionCandidate],
    existing: Mapping[tuple, str],
) -> tuple[DimensionCandidate, ...]:
    """An existing identity/version with a different source hash is a conflict."""
    conflicts = []
    for candidate in candidates:
        if not candidate.source_hash:
            continue
        current = existing.get(candidate.identity)
        if current is not None and current != candidate.source_hash:
            conflicts.append(candidate)
    return tuple(conflicts)


def select_new_dimension_versions(
    candidates: Sequence[DimensionCandidate],
    existing: Mapping[tuple, str],
) -> tuple[DimensionCandidate, ...]:
    """Insert only when the exact version is absent."""
    return tuple(
        candidate for candidate in candidates if candidate.identity not in existing
    )
