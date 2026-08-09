"""Canonical Gold source-set hashing and lineage metadata."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from de.gold.input_models import SilverGoldInput, SilverSignalStateInput, SilverTrafficObservationInput
from de.gold.quality import numeric_allowed


def source_table(record: SilverGoldInput) -> str:
    name = type(record).__name__
    return {
        "SilverTrafficObservationInput": "silver_fact_traffic_observation",
        "SilverIntersectionStateInput": "silver_fact_intersection_state",
        "SilverSignalStateInput": "silver_fact_signal_state",
        "SilverCameraObservationInput": "silver_fact_camera_observation",
    }[name]


def canonical_source_row(record: SilverGoldInput) -> dict:
    direction = record.canonical_direction if isinstance(record, (SilverTrafficObservationInput, SilverSignalStateInput)) else ""
    return {
        "source_table": source_table(record),
        "simulation_run_id": record.simulation_run_id,
        "scenario_id": record.scenario_id,
        "intersection_id": record.intersection_id,
        "direction": direction,
        "simulation_time_sec": float(record.simulation_time_sec),
        "cycle_sequence": int(record.cycle_sequence),
        "source_partition": int(record.source_partition),
        "source_offset": int(record.source_offset),
        "source_bronze_event_id": record.source_bronze_event_id,
        "source_payload_hash": record.source_payload_hash,
        "migration_version": record.migration_version,
    }


def canonical_source_set_bytes(records: tuple[SilverGoldInput, ...]) -> bytes:
    rows = sorted((canonical_source_row(record) for record in records), key=lambda row: tuple(str(row[key]) for key in sorted(row)))
    payload = "\n".join(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) for row in rows)
    return payload.encode("utf-8")


def canonical_source_set_hash(records: tuple[SilverGoldInput, ...]) -> str:
    return hashlib.sha256(canonical_source_set_bytes(records)).hexdigest()


@dataclass(frozen=True)
class LineageMetadata:
    source_set_hash: str
    source_row_count: int
    source_valid_row_count: int
    source_min_simulation_time: float
    source_max_simulation_time: float
    source_min_offset: int | None
    source_max_offset: int | None
    source_tables: tuple[str, ...]
    source_latest_simulation_time: float
    source_latest_processed_at: datetime


def build_lineage(records: tuple[SilverGoldInput, ...], *, valid_records: tuple[SilverGoldInput, ...] | None = None) -> LineageMetadata:
    if not records:
        raise ValueError("lineage requires at least one source row")
    valid = valid_records if valid_records is not None else tuple(row for row in records if numeric_allowed(row.quality_status))
    return LineageMetadata(
        source_set_hash=canonical_source_set_hash(records),
        source_row_count=len(records),
        source_valid_row_count=len(valid),
        source_min_simulation_time=min(float(row.simulation_time_sec) for row in records),
        source_max_simulation_time=max(float(row.simulation_time_sec) for row in records),
        source_min_offset=min(int(row.source_offset) for row in records),
        source_max_offset=max(int(row.source_offset) for row in records),
        source_tables=tuple(sorted({source_table(row) for row in records})),
        source_latest_simulation_time=max(float(row.simulation_time_sec) for row in records),
        source_latest_processed_at=max(row.processed_at for row in records),
    )
