"""Processor orchestration with fakes: one transform, persistence order, replay isolation."""
from __future__ import annotations

from fastapi.testclient import TestClient

from de.gold_runtime.config import ProcessorState, WorkUnitState
from de.gold_runtime.health_api import app, bind_processor
from de.gold_runtime.processor import (
    OUTCOME_IDEMPOTENT,
    OUTCOME_PROCESSED,
    GoldProcessor,
    filter_result_to_window,
)
from de.gold_runtime.replay import (
    ReplayManifest,
    ReplayTableWindow,
    assert_namespace_isolation,
    replay_settings_from,
)
from de.gold_runtime.repositories import PERSISTENCE_ORDER
from de.gold_runtime.window_scheduler import make_window_identity
from de.gold.engine import GoldTransformationResult
from de.tests.gold_runtime.conftest import (
    NOW,
    RUN_ID,
    SCENARIO_ID,
    CountingEngine,
    FakeGoldRepository,
    FakeSilverReader,
    SOURCE_TABLE_TRAFFIC,
    build_silver_dataset,
    make_settings,
)


def _build_processor(tmp_path, store, *, times=None, settings=None):
    settings = settings or make_settings(tmp_path)
    reader = FakeSilverReader(
        build_silver_dataset(
            times=times or tuple(float(t) for t in range(0, 180, 10))
        )
    )
    repository = FakeGoldRepository(settings)
    engine = CountingEngine()
    proc = GoldProcessor(
        settings,
        reader=reader,
        repository=repository,
        store=store,
        engine=engine,
        clock=lambda: NOW,
        lock_held=True,
    )
    proc._schema_ok = True
    proc.state = ProcessorState.READY
    return proc, engine, repository


def test_processor_calls_public_engine_once_per_window(tmp_path, store):
    proc, engine, repository = _build_processor(tmp_path, store)
    processed = proc.run_cycle()
    assert processed == 1
    assert engine.calls == 1
    assert "gold_fact_traffic_window" in repository.tables
    assert len(repository.ledger) >= 2
    dispositions = {row.disposition for row in repository.ledger}
    assert "RECEIVED" in dispositions
    assert "PERSISTED" in dispositions


def test_processor_idempotent_on_closed_window(tmp_path, store):
    proc, engine, repository = _build_processor(tmp_path, store)
    first = proc.run_cycle()
    assert first == 1
    calls_after_first = engine.calls
    facts_after_first = len(repository.tables.get("gold_fact_traffic_window", []))

    # Re-process the same closed window identity directly.
    window = make_window_identity("live", RUN_ID, SCENARIO_ID, 60, 0.0)
    result = proc.process_window(window)
    assert result.outcome == OUTCOME_IDEMPOTENT
    assert engine.calls == calls_after_first
    assert len(repository.tables.get("gold_fact_traffic_window", [])) == facts_after_first


def test_persistence_order_is_contract_v1(tmp_path, store):
    proc, engine, repository = _build_processor(tmp_path, store)
    assert proc.run_cycle() == 1
    fact_calls = [name for name in repository.insert_calls if name.startswith("gold_fact_")]
    # Dimensions may precede facts; facts themselves must follow PERSISTENCE_ORDER.
    order_index = {name: idx for idx, name in enumerate(PERSISTENCE_ORDER)}
    seen = [name for name in fact_calls if name in order_index]
    assert seen == sorted(seen, key=lambda name: order_index[name])
    assert seen[0] == "gold_fact_traffic_window"


def test_filter_result_keeps_only_target_window(tmp_path):
    target = make_window_identity("live", RUN_ID, SCENARIO_ID, 60, 0.0)
    empty = GoldTransformationResult(
        traffic_windows=(),
        intersection_windows=(),
        comparisons=(),
        signal_operation_windows=(),
        kpi_results=(),
        metric_definitions=(),
        warnings=(),
        unsupported_records=(),
        conflict_evidence=(),
        lineage_evidence=(),
    )
    filtered = filter_result_to_window(empty, target)
    assert filtered.traffic_windows == ()


def test_replay_namespace_isolation(tmp_path):
    live = make_settings(tmp_path)
    replay = replay_settings_from(live, "replay-demo-1")
    assert_namespace_isolation(live, replay)
    assert replay.namespace.startswith("replay:")
    assert replay.checkpoint_path != live.checkpoint_path


def test_replay_manifest_hash_is_stable():
    manifest = ReplayManifest(
        replay_id="demo-1",
        source_database="smart_traffic",
        destination_namespace="replay:demo-1",
        table_windows=(
            ReplayTableWindow(
                source_table=SOURCE_TABLE_TRAFFIC,
                window_start_sim_sec=0.0,
                window_end_sim_sec=60.0,
                cursor_json="{}",
            ),
        ),
        source_set_hash="a" * 64,
    ).validate()
    assert manifest.manifest_hash() == manifest.manifest_hash()
    assert len(manifest.manifest_hash()) == 64


def test_processor_state_enum_covers_contract_machine():
    names = {state.value for state in ProcessorState}
    for required in {
        "STARTING", "RECOVERING", "READY", "PROCESSING", "RETRYING",
        "DEGRADED", "FAULTED", "STOPPING", "STOPPED",
    }:
        assert required in names


def test_health_and_ready_endpoints_use_cached_snapshot(tmp_path, store):
    proc, _engine, _repository = _build_processor(tmp_path, store)
    proc._clickhouse_ok = True
    proc._sqlite_ok = True
    proc._schema_ok = True
    proc._lock_held = True
    proc.metrics.mark_checkpoint()
    proc._publish()
    bind_processor(proc, max_age_sec=30.0)
    client = TestClient(app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["lock_held"] is True
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["ready"] is True
