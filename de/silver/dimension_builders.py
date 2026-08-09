"""Silver Plan 2 — dimension builders + DimensionCandidate wrapper (pure)."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Union

from de.silver.contracts import (
    ENTITY_INTERSECTION,
    ENTITY_VEHICLE_SENSOR,
    EVENT_RUN_STARTED,
    parse_urn_id,
)
from de.silver.input_models import BronzeEntityInputRecord, BronzeRunInputRecord
from de.silver.models import (
    SilverDimApproach,
    SilverDimIntersection,
    SilverDimRun,
    SilverDimScenario,
)

DimensionRecord = Union[
    SilverDimRun,
    SilverDimIntersection,
    SilverDimApproach,
    SilverDimScenario,
]


@dataclass(frozen=True)
class DimensionCandidate:
    target_table: str
    business_key: tuple[str, ...]
    source_hash: str
    row: DimensionRecord


def _sha256(*parts: str) -> str:
    material = "|".join(parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def build_approach_dimension(
    record: BronzeEntityInputRecord,
    normalized: dict[str, Any],
) -> DimensionCandidate:
    intersection_id = parse_urn_id(str(normalized["refIntersection"]))
    direction = str(normalized["trafficDirection"])
    source_hash = _sha256(intersection_id, direction)
    row = SilverDimApproach(
        intersection_id=intersection_id,
        direction=direction,
        source_bronze_event_id=record.event_id,
        created_at=None,
        updated_at=None,
    )
    return DimensionCandidate(
        target_table="silver_dim_approach",
        business_key=(intersection_id, direction),
        source_hash=source_hash,
        row=row,
    )


def build_intersection_dimension(
    record: BronzeEntityInputRecord,
    normalized: dict[str, Any],
) -> DimensionCandidate:
    intersection_id = parse_urn_id(str(normalized["id"]))
    name = str(normalized["name"])
    lon, lat = normalized["location"]
    network_zone = ""
    connected: list[str] = []
    source_hash = _sha256(
        intersection_id,
        name,
        f"{lat:.8f}",
        f"{lon:.8f}",
        network_zone,
    )
    row = SilverDimIntersection(
        intersection_id=intersection_id,
        intersection_name=name,
        latitude=float(lat),
        longitude=float(lon),
        source_hash=source_hash,
        source_bronze_event_id=record.event_id,
        network_zone=network_zone,
        connected_intersections=connected,
        created_at=None,
        updated_at=None,
    )
    return DimensionCandidate(
        target_table="silver_dim_intersection",
        business_key=(intersection_id,),
        source_hash=source_hash,
        row=row,
    )


def build_run_dimension(record: BronzeRunInputRecord) -> DimensionCandidate:
    seed = record.producer_session_id or None
    source_hash = _sha256(
        record.simulation_run_id,
        record.scenario_id,
        record.producer_id,
        record.contract_version,
        seed or "",
    )
    row = SilverDimRun(
        simulation_run_id=record.simulation_run_id,
        scenario_id=record.scenario_id,
        producer_id=record.producer_id,
        started_at=record.started_at,
        contract_version=record.contract_version,
        source_bronze_run_id=record.raw_ingestion_id,
        seed=seed,
        ended_at=None,
        run_status="RUNNING",
        node_count=None,
        created_at=None,
        updated_at=None,
    )
    return DimensionCandidate(
        target_table="silver_dim_run",
        business_key=(record.simulation_run_id,),
        source_hash=source_hash,
        row=row,
    )


def build_scenario_dimension(record: BronzeRunInputRecord) -> DimensionCandidate:
    source_hash = _sha256(record.scenario_id)
    row = SilverDimScenario(
        scenario_id=record.scenario_id,
        description="",
        created_at=None,
    )
    return DimensionCandidate(
        target_table="silver_dim_scenario",
        business_key=(record.scenario_id,),
        source_hash=source_hash,
        row=row,
    )


def build_dimensions(
    entity_or_event_type: str,
    record: Any,
    normalized: dict[str, Any],
) -> tuple[DimensionCandidate, ...]:
    try:
        if entity_or_event_type == ENTITY_VEHICLE_SENSOR:
            return (build_approach_dimension(record, normalized),)
        if entity_or_event_type == ENTITY_INTERSECTION:
            return (build_intersection_dimension(record, normalized),)
        if entity_or_event_type == EVENT_RUN_STARTED:
            return (build_run_dimension(record), build_scenario_dimension(record))
        return ()
    except Exception:  # noqa: BLE001
        return ()
