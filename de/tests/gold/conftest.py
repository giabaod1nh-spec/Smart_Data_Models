"""Deterministic Gold 2 source fixtures."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from de.gold.input_models import (
    GoldTransformationContext,
    SilverIntersectionStateInput,
    SilverSignalStateInput,
    SilverTrafficObservationInput,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def traffic(
    direction: str = "N", time: float = 70.0, *, offset: int = 1,
    queue: float = 50.0, speed: float = 20.0, occupancy: float = 60.0,
    vehicles: int = 10, spillback: bool = True, quality: str = "VALID",
    payload_hash: str | None = None, event_id: str | None = None,
) -> SilverTrafficObservationInput:
    event = event_id or f"traffic-{direction}-{time}-{offset}"
    return SilverTrafficObservationInput(
        simulation_run_id="run-1", scenario_id="scenario-1", intersection_id="J1",
        source_direction=direction, canonical_direction=direction,
        direction_mapping_version="direction-v1", source_entity_id=f"TrafficFlowObserved:J1:{direction}",
        cycle_sequence=int(time), simulation_time_sec=time, vehicle_count=vehicles,
        pcu_equivalent=float(vehicles), average_speed_kmh=speed, queue_length_m=queue,
        waiting_vehicle_count=max(0, vehicles // 2), occupancy_pct=occupancy,
        arrival_rate_pcu_per_sec=2.0, traffic_status="CONGESTED" if queue >= 50 else "FREE",
        spillback_risk=spillback, quality_status=quality, quality_flags=(),
        source_bronze_event_id=event, source_raw_ingestion_id=f"raw-{event}",
        source_topic="traffic.entity-events.v2", source_partition=0, source_offset=offset,
        source_payload_hash=payload_hash or f"hash-{event}", processed_at=NOW,
    )


def intersection(time: float = 70.0, *, offset: int = 100, incident: bool = True) -> SilverIntersectionStateInput:
    event = f"intersection-{time}-{offset}"
    return SilverIntersectionStateInput(
        simulation_run_id="run-1", scenario_id="scenario-1", intersection_id="J1",
        source_entity_id="TrafficIntersection:J1", cycle_sequence=int(time),
        simulation_time_sec=time, overall_traffic_status="CONGESTED",
        derived_traffic_state="CONGESTED", current_phase="NS_GREEN",
        has_active_incident=incident, has_spillback=time >= 60, is_box_blocked=False,
        total_vehicle_count=40 if time >= 60 else 20, quality_status="VALID", quality_flags=(),
        source_bronze_event_id=event, source_raw_ingestion_id=f"raw-{event}",
        source_topic="traffic.entity-events.v2", source_partition=1, source_offset=offset,
        source_payload_hash=f"hash-{event}", processed_at=NOW,
    )


def signal(direction: str = "N", time: float = 70.0, *, offset: int = 200, status: str = "GREEN") -> SilverSignalStateInput:
    event = f"signal-{direction}-{time}-{offset}"
    return SilverSignalStateInput(
        simulation_run_id="run-1", scenario_id="scenario-1", intersection_id="J1",
        source_direction=direction, canonical_direction=direction,
        direction_mapping_version="direction-v1", source_entity_id=f"TrafficLight:J1:{direction}",
        cycle_sequence=int(time), simulation_time_sec=time, signal_status=status,
        current_phase="NS_GREEN", green_duration_sec=30.0, red_duration_sec=25.0,
        yellow_duration_sec=5.0, timing_mode="FIXED", quality_status="VALID", quality_flags=(),
        source_bronze_event_id=event, source_raw_ingestion_id=f"raw-{event}",
        source_topic="traffic.entity-events.v2", source_partition=2, source_offset=offset,
        source_payload_hash=f"hash-{event}", processed_at=NOW,
    )


@pytest.fixture
def context() -> GoldTransformationContext:
    return GoldTransformationContext(namespace="smart_traffic", computed_at=NOW)


@pytest.fixture
def traffic_factory():
    return traffic


@pytest.fixture
def intersection_factory():
    return intersection


@pytest.fixture
def signal_factory():
    return signal


@pytest.fixture
def two_window_records():
    rows = []
    for index, direction in enumerate(("N", "S", "E", "W")):
        rows.append(traffic(direction, 10.0, offset=index + 1, queue=20, speed=40, occupancy=20, vehicles=5, spillback=False))
        rows.append(traffic(direction, 70.0, offset=index + 11, queue=50, speed=20, occupancy=60, vehicles=10, spillback=True))
    rows.extend((intersection(10.0, offset=101, incident=False), intersection(70.0, offset=102, incident=True)))
    rows.extend((signal("N", 10.0, offset=201, status="RED"), signal("N", 70.0, offset=202, status="GREEN")))
    return tuple(rows)
