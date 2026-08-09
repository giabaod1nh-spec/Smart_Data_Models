"""Unit tests for de.silver.routers."""
from __future__ import annotations

from de.silver.contracts import (
    DISPOSITION_PROCESSED,
    ENTITY_CAMERA,
    ENTITY_INTERSECTION,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_VEHICLE_SENSOR,
    EVENT_RUN_STARTED,
)
from de.silver.routers import ROUTING_SPECIFICATIONS, assert_routes_match_contract_matrix, resolve_route


def test_routing_matrix_all_five():
    assert resolve_route(ENTITY_VEHICLE_SENSOR).primary_target_table == (
        "silver_fact_traffic_observation"
    )
    assert resolve_route(ENTITY_TRAFFIC_LIGHT).dimension_targets == ()
    assert resolve_route(ENTITY_INTERSECTION).dimension_targets == ("silver_dim_intersection",)
    assert resolve_route(ENTITY_CAMERA).primary_target_table == "silver_fact_camera_observation"
    assert resolve_route(ENTITY_CAMERA).proposed_disposition == DISPOSITION_PROCESSED
    run = resolve_route(EVENT_RUN_STARTED)
    assert run.dimension_targets == ("silver_dim_run", "silver_dim_scenario")


def test_unknown_route_is_none():
    assert resolve_route("UnknownThing") is None


def test_routes_match_plan1_contract_matrix():
    assert_routes_match_contract_matrix()
    assert set(ROUTING_SPECIFICATIONS) == {
        ENTITY_VEHICLE_SENSOR,
        ENTITY_TRAFFIC_LIGHT,
        ENTITY_INTERSECTION,
        ENTITY_CAMERA,
        EVENT_RUN_STARTED,
    }
