"""Static allowlist and destination-mode guard enforcement (Plan 3 §12.2/§18)."""
from __future__ import annotations

import pytest

from de.silver.config import SilverSettings
from de.silver.models import SilverDimApproach, SilverObservationFact
from de.silver.repositories import (
    InvalidTargetTableError,
    ReplayModeGuardError,
    SilverClickHouseRepository,
)
from de.tests.silver.conftest import FakeClient


def _repo(**overrides):
    settings = SilverSettings(**overrides)
    client = FakeClient()
    return SilverClickHouseRepository(settings, client=client), client


def _fact_stub() -> SilverObservationFact:
    return SilverObservationFact(
        simulation_run_id="run-1", cycle_sequence=1, simulation_time_sec=1.0,
        intersection_id="I1", direction="N", source_entity_id="E1",
        vehicle_count=1, pcu_equivalent=1.0, average_speed_kmh=1.0, queue_length_m=1.0,
        waiting_vehicle_count=0, occupancy_pct=1.0, arrival_rate_pcu_per_sec=0.0,
        traffic_status="LIGHT", spillback_risk=0, dominant_waiting_reason="",
        scenario_id="normal", source_bronze_event_id="evt-1", source_raw_ingestion_id="raw-1",
        source_topic="t", source_partition=0, source_offset=1, source_payload_hash="hash-1",
    )


def _approach_stub() -> SilverDimApproach:
    return SilverDimApproach(intersection_id="I1", direction="N", source_bronze_event_id="evt-1")


def test_invalid_fact_target_rejected_before_sql():
    repo, client = _repo()
    with pytest.raises(InvalidTargetTableError):
        repo.insert_fact_batch("silver_fact_not_a_table", [_fact_stub()])
    assert client.queries == []
    assert client.inserted == []


def test_invalid_dimension_target_rejected_before_sql():
    repo, client = _repo()
    with pytest.raises(InvalidTargetTableError):
        repo.insert_dimension_batch("silver_dim_not_a_table", [_approach_stub()])
    assert client.inserted == []


def test_find_fact_states_rejects_invalid_target_before_sql():
    repo, client = _repo()
    with pytest.raises(InvalidTargetTableError):
        repo.find_fact_states("not_a_real_table", [])
    assert client.queries == []


def test_fetch_current_dimension_states_rejects_invalid_target():
    from de.silver.dimension_builders import DimensionCandidate

    repo, client = _repo()
    bogus = DimensionCandidate("silver_dim_bogus", ("k",), "hash", row=None)
    with pytest.raises(InvalidTargetTableError):
        repo.fetch_current_dimension_states([bogus])
    assert client.queries == []


def test_main_insert_blocked_in_replay_mode():
    repo, client = _repo(destination_mode="replay", namespace="replay:r1", replay_run_id="r1")
    with pytest.raises(ReplayModeGuardError):
        repo.insert_fact_batch("silver_fact_traffic_observation", [_fact_stub()], replay_run_id=None)
    assert client.inserted == []


def test_replay_insert_blocked_in_main_mode():
    repo, client = _repo()
    with pytest.raises(ReplayModeGuardError):
        repo.insert_fact_batch(
            "silver_fact_traffic_observation", [_fact_stub()], replay_run_id="r1"
        )
    assert client.inserted == []


def test_replay_quarantine_insert_blocked_in_main_mode():
    from de.silver.models import SilverQuarantineEntry

    repo, client = _repo()
    entry = SilverQuarantineEntry(
        silver_quarantine_id="q1", source_bronze_event_id="evt-1", raw_ingestion_id="raw-1",
        simulation_run_id=None, entity_id=None, entity_type=None,
        failure_stage="PARSE", error_code="X", error_message="",
    )
    with pytest.raises(ReplayModeGuardError):
        repo.insert_quarantine_batch([entry], replay_run_id="r1")
    assert client.inserted == []


def test_no_replay_table_for_approach_dimension():
    repo, client = _repo(destination_mode="replay", namespace="replay:r1", replay_run_id="r1")
    with pytest.raises(InvalidTargetTableError):
        repo.insert_dimension_batch("silver_dim_approach", [_approach_stub()], replay_run_id="r1")
    assert client.inserted == []


def test_empty_rows_is_noop_and_skips_guards_and_sql():
    repo, client = _repo(destination_mode="replay", namespace="replay:r1", replay_run_id="r1")
    receipt = repo.insert_fact_batch("silver_fact_traffic_observation", [], replay_run_id=None)
    assert receipt.attempted == ()
    assert client.inserted == []
