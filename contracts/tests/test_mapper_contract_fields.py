"""Contract tests also verify production mapper emits Contract v1 fields."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
VIS = ROOT / "Visualize"


@pytest.fixture(scope="module")
def mapper():
    sys.path.insert(0, str(VIS))
    from integration.orion import entity_mapper as m

    return m


def _snap():
    return {
        "phase": "EW_YELLOW",
        "green_duration": 42,
        "yellow_duration": 3,
        "red_duration": 42,
        "colors": {
            "North": "red",
            "South": "red",
            "East": "yellow",
            "West": "yellow",
        },
        "simulation_time_sec": 9.0,
        "simulation_run_id": "run-mapper-test",
        "scenario": "rain",
        "incidents": [],
        "directions": {
            d: {
                "vehicle_count": 1,
                "pcu_equivalent": 1.0,
                "left_count": 0,
                "straight_count": 1,
                "right_count": 0,
                "average_speed_kmh": 10.0,
                "waiting_vehicle_count": 1,
                "queue_length_m": 5.0,
                "queue_by_movement": {"straight": 5.0, "left": 0.0, "right": 0.0},
                "occupancy_pct": 5.0,
                "density": "LOW",
                "arrival_rate_pcu_per_sec": 0.0,
                "waiting_reason_counts": {"RED_PHASE": 1, "CONGESTION": 0},
                "theoretical_speed_kmh": 40.0,
            }
            for d in ["North", "South", "East", "West"]
        },
        "derived_phenomena": {
            "spillback_active": False,
            "box_blocked": False,
            "spillback_risk": False,
        },
        "operational_state": {
            "incident_active": False,
            "emergency_preemption_active": False,
            "downstream_restriction_active": False,
        },
        "probable_causes": [],
    }


def test_mapper_publishes_contract_fields(mapper):
    ents = mapper.build_all_entities("A", _snap())
    assert len(ents) == 10
    for e in ents:
        assert e["simulationRunId"]["value"] == "run-mapper-test"
        assert e["scenarioId"]["value"] == "rain"
        assert e["simulationTime"]["value"] == 9.0
    phases = {
        e["currentPhase"]["value"]
        for e in ents
        if e["type"] in ("TrafficLight", "Intersection")
    }
    assert phases == {"EW_YELLOW"}
