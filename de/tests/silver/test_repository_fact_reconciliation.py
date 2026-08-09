"""Fact reconciliation enum classification (Plan 3 §12.1/§12.3/§32.4)."""
from __future__ import annotations

from de.silver.config import FactReconcileResult, SilverSettings
from de.silver.repositories import FactIdentity, SilverClickHouseRepository, classify_fact_identity
from de.tests.silver.conftest import FakeClient, FakeQueryResult

COLS = [
    "source_bronze_event_id", "source_payload_hash",
    "simulation_run_id", "intersection_id", "direction", "source_entity_id",
    "simulation_time_sec",
]


def _identity(event_id="evt-1", payload_hash="hash-1", key=("run-1", "I1", "N", "E1", 1.0)):
    return FactIdentity(
        source_bronze_event_id=event_id,
        source_payload_hash=payload_hash,
        business_key=key,
        source_topic="t", source_partition=0, source_offset=1,
    )


def _repo_with_rows(rows) -> SilverClickHouseRepository:
    settings = SilverSettings()
    client = FakeClient(responses=[FakeQueryResult(rows, COLS)])
    return SilverClickHouseRepository(settings, client=client)


def test_missing_when_no_matching_rows():
    repo = _repo_with_rows([])
    out = repo.find_fact_states("silver_fact_traffic_observation", [_identity()])
    assert out["evt-1"].result == FactReconcileResult.MISSING


def test_exact_match():
    identity = _identity()
    rows = [("evt-1", "hash-1", "run-1", "I1", "N", "E1", 1.0)]
    repo = _repo_with_rows(rows)
    out = repo.find_fact_states("silver_fact_traffic_observation", [identity])
    assert out["evt-1"].result == FactReconcileResult.EXACT_MATCH


def test_source_match_payload_conflict_different_hash():
    identity = _identity(payload_hash="hash-1")
    rows = [("evt-1", "hash-DIFFERENT", "run-1", "I1", "N", "E1", 1.0)]
    repo = _repo_with_rows(rows)
    out = repo.find_fact_states("silver_fact_traffic_observation", [identity])
    assert out["evt-1"].result == FactReconcileResult.SOURCE_MATCH_PAYLOAD_CONFLICT


def test_source_match_payload_conflict_different_business_key():
    identity = _identity(key=("run-1", "I1", "N", "E1", 1.0))
    rows = [("evt-1", "hash-1", "run-1", "I1", "S", "E1", 1.0)]
    repo = _repo_with_rows(rows)
    out = repo.find_fact_states("silver_fact_traffic_observation", [identity])
    assert out["evt-1"].result == FactReconcileResult.SOURCE_MATCH_PAYLOAD_CONFLICT


def test_business_key_owned_by_other_source():
    identity = _identity(event_id="evt-2", key=("run-1", "I1", "N", "E1", 1.0))
    rows = [("evt-OTHER", "hash-1", "run-1", "I1", "N", "E1", 1.0)]
    repo = _repo_with_rows(rows)
    out = repo.find_fact_states("silver_fact_traffic_observation", [identity])
    assert out["evt-2"].result == FactReconcileResult.BUSINESS_KEY_OWNED_BY_OTHER_SOURCE


def test_physical_duplicate_exact():
    identity = _identity()
    rows = [
        ("evt-1", "hash-1", "run-1", "I1", "N", "E1", 1.0),
        ("evt-1", "hash-1", "run-1", "I1", "N", "E1", 1.0),
    ]
    repo = _repo_with_rows(rows)
    out = repo.find_fact_states("silver_fact_traffic_observation", [identity])
    assert out["evt-1"].result == FactReconcileResult.PHYSICAL_DUPLICATE_EXACT


def test_multiple_id_matches_with_inconsistent_payload_is_conflict_not_duplicate():
    identity = _identity()
    rows = [
        ("evt-1", "hash-1", "run-1", "I1", "N", "E1", 1.0),
        ("evt-1", "hash-DIFFERENT", "run-1", "I1", "N", "E1", 1.0),
    ]
    repo = _repo_with_rows(rows)
    out = repo.find_fact_states("silver_fact_traffic_observation", [identity])
    assert out["evt-1"].result == FactReconcileResult.SOURCE_MATCH_PAYLOAD_CONFLICT


def test_empty_identities_returns_empty_without_query():
    settings = SilverSettings()
    client = FakeClient(responses=[])
    repo = SilverClickHouseRepository(settings, client=client)
    out = repo.find_fact_states("silver_fact_traffic_observation", [])
    assert out == {}
    assert client.queries == []


def test_classify_fact_identity_pure_function_matches_repository_result():
    identity = _identity()
    rows = [{"source_bronze_event_id": "evt-1", "source_payload_hash": "hash-1",
             "simulation_run_id": "run-1", "intersection_id": "I1", "direction": "N",
             "source_entity_id": "E1", "simulation_time_sec": 1.0}]
    key_cols = ("simulation_run_id", "intersection_id", "direction", "source_entity_id", "simulation_time_sec")
    result = classify_fact_identity(identity, rows, key_cols)
    assert result.result == FactReconcileResult.EXACT_MATCH


def test_classify_fact_identity_decodes_fixedstring_bytes_from_clickhouse():
    """Live CH often returns FixedString(64) lineage columns as bytes; str(bytes) must not win."""
    identity = _identity()
    rows = [{
        "source_bronze_event_id": b"evt-1",
        "source_payload_hash": b"hash-1",
        "simulation_run_id": "run-1",
        "intersection_id": "I1",
        "direction": "N",
        "source_entity_id": "E1",
        "simulation_time_sec": 1.0,
    }]
    key_cols = (
        "simulation_run_id", "intersection_id", "direction", "source_entity_id", "simulation_time_sec",
    )
    result = classify_fact_identity(identity, rows, key_cols)
    assert result.result == FactReconcileResult.EXACT_MATCH
