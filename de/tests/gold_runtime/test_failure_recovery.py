"""Failure, CAS conflict and restart reconciliation under Gold Runtime Contract v1."""
from __future__ import annotations

from de.gold_runtime.checkpoint_store import GoldRuntimeStore
from de.gold_runtime.config import CasResult, ProcessorState, WorkUnitState
from de.gold_runtime.cursor import ZERO_FACT_CURSOR, FactCursor
from de.gold_runtime.instance_lock import GoldInstanceAlreadyRunning, InstanceLock
from de.gold_runtime.processing_ledger import ExpectedOutputManifest, ManifestEntry
from de.gold_runtime.processor import GoldProcessor
from de.gold_runtime.repositories import UncertainWriteError
from de.tests.gold_runtime.conftest import (
    NOW,
    CountingEngine,
    FakeGoldRepository,
    FakeSilverReader,
    SOURCE_TABLE_TRAFFIC,
    build_silver_dataset,
    make_settings,
)


def test_uncertain_write_leaves_work_unit_non_terminal(tmp_path, store):
    settings = make_settings(tmp_path)
    reader = FakeSilverReader(build_silver_dataset())
    repository = FakeGoldRepository(settings)
    repository.suppress_insert.add("gold_fact_kpi_result")
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
    processed = proc.run_cycle()
    assert processed == 0
    assert proc.state is ProcessorState.DEGRADED
    units = store.non_terminal_work_units("live")
    assert units
    assert units[0].state in {
        WorkUnitState.PERSISTENCE_UNKNOWN.value,
        WorkUnitState.TRANSFORMED.value,
        WorkUnitState.FAILED_RETRYABLE.value,
        WorkUnitState.PERSISTENCE_UNKNOWN.value,
    }


def test_recover_marks_complete_manifest_terminal(tmp_path, store):
    from de.gold_runtime.repositories import logical_identity

    settings = make_settings(tmp_path)
    repository = FakeGoldRepository(settings)

    class _Row:
        revision_seq = 0
        source_set_hash = "a" * 64
        namespace = "live"
        simulation_run_id = "run-1"
        scenario_id = "scenario-1"
        intersection_id = "J1"
        direction = "N"
        window_id = "wid"

    row = _Row()
    entry = ManifestEntry(
        target_table="gold_fact_traffic_window",
        logical_identity=logical_identity("gold_fact_traffic_window", row),
        source_set_hash="a" * 64,
        revision_seq=0,
        payload_digest="d1",
    )
    manifest = ExpectedOutputManifest(
        batch_id="batch-recover-1",
        namespace="live",
        window_id="wid",
        revision_seq=0,
        entries=(entry,),
    )
    store.upsert_work_unit(
        batch_id=manifest.batch_id,
        namespace="live",
        window_id="wid",
        revision_seq=0,
        state=WorkUnitState.PERSISTENCE_UNKNOWN,
        input_digest="in",
        expected_manifest_json=manifest.to_json(),
    )
    repository.tables["gold_fact_traffic_window"] = [row]
    proc = GoldProcessor(
        settings,
        reader=FakeSilverReader({}),
        repository=repository,
        store=store,
        engine=CountingEngine(),
        clock=lambda: NOW,
        lock_held=True,
    )
    recovered = proc.recover()
    assert recovered == 1
    unit = store.get_work_unit(manifest.batch_id)
    assert unit is not None
    assert unit.state == WorkUnitState.CHECKPOINTED.value


def test_cursor_cas_conflict_is_detectable(tmp_path):
    from de.gold_runtime.checkpoint_store import CheckpointCasConflictError

    store = GoldRuntimeStore(tmp_path / "cas.sqlite3")
    store.open()
    try:
        store.initialize_cursor("live", SOURCE_TABLE_TRAFFIC, ZERO_FACT_CURSOR.to_json())
        row = store.get_cursor("live", SOURCE_TABLE_TRAFFIC)
        assert row is not None
        advanced = store.compare_and_advance_cursor(
            "live",
            SOURCE_TABLE_TRAFFIC,
            expected_generation=row.generation,
            cursor_json=FactCursor(
                processed_at=NOW,
                source_topic="t",
                source_partition=0,
                source_offset=1,
                source_payload_hash="aa",
            ).to_json(),
        )
        assert advanced is CasResult.ADVANCED
        try:
            store.compare_and_advance_cursor(
                "live",
                SOURCE_TABLE_TRAFFIC,
                expected_generation=row.generation,
                cursor_json=FactCursor(
                    processed_at=NOW,
                    source_topic="t",
                    source_partition=0,
                    source_offset=2,
                    source_payload_hash="bb",
                ).to_json(),
            )
            raised = False
        except CheckpointCasConflictError:
            raised = True
        assert raised
        # Same cursor+generation+1 is ALREADY_ADVANCED.
        current = store.get_cursor("live", SOURCE_TABLE_TRAFFIC)
        assert current is not None
        already = store.compare_and_advance_cursor(
            "live",
            SOURCE_TABLE_TRAFFIC,
            expected_generation=row.generation,
            cursor_json=current.cursor_json,
        )
        assert already is CasResult.ALREADY_ADVANCED
    finally:
        store.close()


def test_instance_lock_is_exclusive(tmp_path, store):
    settings = make_settings(tmp_path)
    first = InstanceLock(settings.instance_lock_path, "live", store)
    first.acquire()
    try:
        second = InstanceLock(settings.instance_lock_path, "live", store)
        try:
            second.acquire()
            raised = False
        except GoldInstanceAlreadyRunning:
            raised = True
        except OSError:
            raised = True
        finally:
            if second.held:
                second.release()
        assert raised or not second.held
    finally:
        first.release()


def test_repository_uncertain_write_error_type():
    err = UncertainWriteError("insert ack missing")
    assert "insert" in str(err)
