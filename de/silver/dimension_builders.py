"""Silver Plan 2 — dimension builders + DimensionCandidate wrapper (pure)."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Union

from contracts.traffic_observation import canonical_json
from de.silver.archive_resolver import VerifiedRunContext

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


def build_v1_dimensions(context: VerifiedRunContext, artifacts_root: str) -> dict[str, dict[str, Any]]:
    """Freeze archive-derived dimension content, excluding mutable lifecycle fields."""
    base = f"{artifacts_root.rstrip('/')}/runs/{context.run_id}"
    metadata = {key: value for key, value in context.manifest.items()
                if key not in {"runStatus", "failedCycleCount"}}
    participants = canonical_json(list(context.participants))
    # The topology dimension is keyed by the immutable catalog digest, so it
    # must describe the entire catalog even when this run uses a participant
    # subset. Run scope belongs in the separate run and binding dimensions.
    node_to_tls = context.catalog["node_to_tls"]
    approaches = context.catalog["approach_edges"]
    topology_directions = {
        node: tuple(approaches[node_to_tls[node]])
        for node in sorted(node_to_tls)
    }
    return {
        "silver_run_dimension_v1": dict(
            simulation_run_id=context.run_id, run_generation=context.generation,
            topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
            participant_ids_json=participants, manifest_metadata_json=canonical_json(metadata),
            manifest_path=base + "/run_manifest.json"),
        "silver_topology_dimension_v1": dict(
            topology_digest=context.topology_digest, digest_algorithm_version="v1",
            catalog_hash=context.catalog_file_hash,
            intersection_ids_json=canonical_json(sorted(node_to_tls)),
            approaches_by_intersection_json=canonical_json(topology_directions),
            catalog_path=base + "/network_topology_catalog.json"),
        "silver_run_topology_binding_v1": dict(
            simulation_run_id=context.run_id, run_generation=context.generation,
            topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
            catalog_hash=context.catalog_file_hash, participant_ids_json=participants),
    }


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
