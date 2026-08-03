"""Silver Plan 2 — routing specifications (pure)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from de.silver.contracts import (
    DISPOSITION_PROCESSED,
    DISPOSITION_QUARANTINED,
    ENTITY_CAMERA,
    ENTITY_INTERSECTION,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_VEHICLE_SENSOR,
    EVENT_RUN_STARTED,
    ROUTING_MATRIX,
)


@dataclass(frozen=True)
class RouteSpecification:
    primary_target_table: str
    dimension_targets: tuple[str, ...]
    proposed_disposition: str


ROUTING_SPECIFICATIONS: dict[str, RouteSpecification] = {
    ENTITY_VEHICLE_SENSOR: RouteSpecification(
        "silver_fact_traffic_observation",
        ("silver_dim_approach",),
        DISPOSITION_PROCESSED,
    ),
    ENTITY_TRAFFIC_LIGHT: RouteSpecification(
        "silver_fact_signal_state",
        (),
        DISPOSITION_PROCESSED,
    ),
    ENTITY_INTERSECTION: RouteSpecification(
        "silver_fact_intersection_state",
        ("silver_dim_intersection",),
        DISPOSITION_PROCESSED,
    ),
    ENTITY_CAMERA: RouteSpecification(
        "silver_fact_camera_observation",
        (),
        DISPOSITION_PROCESSED,
    ),
    EVENT_RUN_STARTED: RouteSpecification(
        "silver_fact_run_event",
        ("silver_dim_run", "silver_dim_scenario"),
        DISPOSITION_PROCESSED,
    ),
}


def resolve_route(entity_or_event_type: str) -> Optional[RouteSpecification]:
    return ROUTING_SPECIFICATIONS.get(entity_or_event_type)


def quarantine_route(target_hint: str = "silver_quarantine") -> RouteSpecification:
    return RouteSpecification(target_hint, (), DISPOSITION_QUARANTINED)


def assert_routes_match_contract_matrix() -> None:
    for key, spec in ROUTING_SPECIFICATIONS.items():
        fact, dims, disp = ROUTING_MATRIX[key]
        assert fact == spec.primary_target_table
        assert dims == spec.dimension_targets
        assert disp == spec.proposed_disposition
