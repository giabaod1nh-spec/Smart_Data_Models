import pytest
import sqlite3

from de.silver.checkpoint_store import CheckpointError, SilverCheckpointStore


def test_backlog_index_excludes_reconciled_history_and_retains_faults(tmp_path):
    store = SilverCheckpointStore(tmp_path / 'backlog.sqlite3')
    store.open()
    try:
        for sequence in range(1, 4):
            digest = store.freeze_v1_obligation('live', 'changes', sequence, str(sequence), 'fact', {'x': sequence})
            if sequence == 1:
                store.reconcile_v1_obligation('live', 'changes', sequence, 'fact', digest)
        store.conn.execute("UPDATE silver_v1_obligation SET status='FAULT' WHERE source_sequence=3")
        plan = store.conn.execute("EXPLAIN QUERY PLAN SELECT * FROM silver_v1_obligation "
            "WHERE namespace=? AND status!='RECONCILED' ORDER BY source_kind,source_sequence,target_table LIMIT ?",
            ('live', 100)).fetchall()
        assert any('silver_v1_pending_obligation_idx' in row[3] for row in plan)
        assert [(row['source_sequence'], row['status']) for row in store.pending_v1_obligations('live')] == [
            (2, 'PENDING'), (3, 'FAULT')]
    finally:
        store.close()


def test_simultaneous_cursor_cas_has_one_winner(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from de.silver.checkpoint_store import CheckpointCasConflictError
    path = tmp_path / 'race.sqlite3'
    setup = SilverCheckpointStore(path)
    setup.open()
    setup.bind_v1_source('live', dict(namespace='live', source_path='bronze',
        schema_fingerprint='s', bronze_version='v1', file_identity=[1, 2]))
    digest = setup.freeze_v1_obligation('live', 'changes', 1, 'e', 'fact', {'x': 0})
    setup.reconcile_v1_obligation('live', 'changes', 1, 'fact', digest)
    setup.close()
    barrier = Barrier(2)
    def attempt():
        store = SilverCheckpointStore(path)
        store.open()
        try:
            generation = store.v1_cursors('live')['generation']
            barrier.wait(timeout=10)
            try:
                store.advance_v1_cursor('live', 'changes', 1, generation)
                return 'winner'
            except CheckpointCasConflictError:
                return 'stale'
        finally:
            store.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt(), range(2)))
    assert sorted(results) == ['stale', 'winner']
    check = SilverCheckpointStore(path)
    check.open()
    try:
        saved = check.v1_cursors('live')
        assert saved['generation'] == 1 and saved['change_cursor'] == 1
    finally:
        check.close()


def test_finalization_rolls_back_validity_when_cursor_update_fails(tmp_path):
    path = tmp_path / 'atomic.sqlite3'
    store = SilverCheckpointStore(path)
    store.open()
    try:
        store.bind_v1_source('live', dict(namespace='live', source_path='bronze',
            schema_fingerprint='schema', bronze_version='v1', file_identity=[1, 2]))
        row = dict(event_id='e', simulation_run_id='r', topology_digest='digest',
                   simulation_time_seconds=12.0)
        store.freeze_v1_bundle('live', 'changes', 1, 'e', 1,
                              [('silver_intersection_state_fact_v1', row)])
        with pytest.raises(CheckpointError, match='unresolved'):
            store.finalize_v1_event('live', 'e', 1, 1, 0)
        store.reconcile_v1_obligations(store.pending_v1_obligations('live'))
        store.conn.executescript("CREATE TRIGGER fail_cursor BEFORE UPDATE ON silver_v1_source_binding "
                                 "BEGIN SELECT RAISE(ABORT,'injected cursor failure'); END;")
        with pytest.raises(sqlite3.IntegrityError, match='injected cursor failure'):
            store.finalize_v1_event('live', 'e', 1, 1, 0)
        assert store.conn.execute('SELECT state FROM silver_v1_event_validity').fetchone()[0] == 'PENDING'
        assert store.v1_cursors('live')['change_cursor'] == 0
        store.close()
        store = SilverCheckpointStore(path)
        store.open()
        assert store.conn.execute('SELECT state FROM silver_v1_event_validity').fetchone()[0] == 'PENDING'
        store.conn.execute('DROP TRIGGER fail_cursor')
        assert store.finalize_v1_event('live', 'e', 1, 1, 0) == 1
        assert store.conn.execute('SELECT state FROM silver_v1_event_validity').fetchone()[0] == 'VALID'
        assert store.v1_cursors('live')['change_cursor'] == 1
        with pytest.raises(CheckpointError, match='generation/sequence'):
            store.finalize_v1_event('live', 'e', 1, 1, 0)
    finally:
        store.close()


def test_finalization_never_restores_invalidated_event(tmp_path):
    store = SilverCheckpointStore(tmp_path / 'invalidated.sqlite3')
    store.open()
    try:
        store.bind_v1_source('live', dict(namespace='live', source_path='bronze',
            schema_fingerprint='schema', bronze_version='v1', file_identity=[1, 2]))
        row = dict(event_id='e', simulation_run_id='r', topology_digest='digest',
                   simulation_time_seconds=12.0)
        store.freeze_v1_bundle('live', 'changes', 1, 'e', 1,
                              [('silver_intersection_state_fact_v1', row)])
        store.reconcile_v1_obligations(store.pending_v1_obligations('live'))
        store.suppress_v1_event('live', 'e', 2, 2, 1, 'silver_invalidation_v1',
                               dict(row, invalidation_id='i'))
        with pytest.raises(CheckpointError, match='not publishable'):
            store.finalize_v1_event('live', 'e', 1, 1, 0)
        assert store.conn.execute('SELECT state FROM silver_v1_event_validity').fetchone()[0] == 'INVALIDATED'
        assert store.v1_cursors('live')['change_cursor'] == 0
    finally:
        store.close()


def test_upgrade_empty_earlier_v1_tables_without_losing_checkpoint(tmp_path):
    path = tmp_path / "older.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE silver_v1_event_validity (namespace TEXT,event_id TEXT,state TEXT)")
        db.execute("CREATE TABLE silver_v1_replay_state (replay_run_id TEXT,status TEXT)")
    store = SilverCheckpointStore(path)
    try:
        store.open()
        validity = {row[1] for row in store.conn.execute(
            "PRAGMA table_info(silver_v1_event_validity)")}
        replay = {row[1] for row in store.conn.execute(
            "PRAGMA table_info(silver_v1_replay_state)")}
        assert {"simulation_run_id", "topology_digest", "simulation_time_seconds"} <= validity
        assert "logical_hash" in replay
    finally:
        store.close()


def test_upgrade_refuses_unlocated_existing_validity_rows(tmp_path):
    path = tmp_path / "older.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE silver_v1_event_validity (namespace TEXT,event_id TEXT,state TEXT)")
        db.execute("INSERT INTO silver_v1_event_validity VALUES ('live','e','VALID')")
    store = SilverCheckpointStore(path)
    try:
        with pytest.raises(CheckpointError, match="lack canonical coordinates"):
            store.open()
    finally:
        store.close()


def test_v1_cursor_waits_for_frozen_reconciled_obligation(tmp_path):
    store = SilverCheckpointStore(tmp_path / "silver.sqlite3")
    store.open()
    try:
        store.bind_v1_source("live", dict(namespace="live", source_path="bronze.sqlite3",
            schema_fingerprint="schema", bronze_version="v1", file_identity=[1, 2]))
        with pytest.raises(CheckpointError, match="no durable disposition"):
            store.advance_v1_cursor("live", "changes", 1, 0)
        digest = store.freeze_v1_obligation("live", "changes", 1, "event", "fact", {"x": 0})
        assert len(digest) == 64
        store.freeze_v1_obligation("live", "changes", 1, "event", "traffic",
            {"event_id": "event", "source_direction": "North", "vehicle_count": 0})
        store.freeze_v1_obligation("live", "changes", 1, "event", "traffic",
            {"event_id": "event", "source_direction": "South", "vehicle_count": 1})
        assert len(store.pending_v1_obligations("live")) == 3
        with pytest.raises(CheckpointError, match="unresolved"):
            store.advance_v1_cursor("live", "changes", 1, 0)
        with pytest.raises(CheckpointError, match="changed"):
            store.freeze_v1_obligation("live", "changes", 1, "event", "fact", {"x": 1})
        store.reconcile_v1_obligation("live", "changes", 1, "fact", digest)
        for pending in store.pending_v1_obligations("live"):
            store.reconcile_v1_obligation("live", pending["source_kind"],
                pending["source_sequence"], pending["target_table"], pending["expected_hash"])
        assert store.advance_v1_cursor("live", "changes", 1, 0) == 1
        tombstone = store.suppress_v1_event("live", "event", 2, 1, 1,
            "silver_invalidation_v1", {"event_id": "event", "invalid": True,
            "simulation_run_id": "r", "topology_digest": "digest",
            "simulation_time_seconds": 12.0})
        state = store.conn.execute("SELECT state FROM silver_v1_event_validity WHERE event_id='event'").fetchone()
        assert state[0] == "INVALIDATED"
        with pytest.raises(CheckpointError, match="unresolved"):
            store.advance_v1_cursor("live", "invalidations", 1, 1)
        store.reconcile_v1_obligation("live", "invalidations", 1,
                                      "silver_invalidation_v1", tombstone)
        assert store.advance_v1_cursor("live", "invalidations", 1, 1) == 2
    finally:
        store.close()


def test_independent_connections_reject_stale_generation(tmp_path):
    from de.silver.checkpoint_store import CheckpointCasConflictError
    path = tmp_path / 'shared.sqlite3'
    first, second = SilverCheckpointStore(path), SilverCheckpointStore(path)
    first.open()
    second.open()
    try:
        first.bind_v1_source('live', dict(namespace='live', source_path='bronze',
            schema_fingerprint='s', bronze_version='v1', file_identity=[1, 2]))
        observed = second.v1_cursors('live')['generation']
        digest = first.freeze_v1_obligation('live', 'changes', 1, 'e', 'fact', {'x': 0})
        first.reconcile_v1_obligation('live', 'changes', 1, 'fact', digest)
        assert first.advance_v1_cursor('live', 'changes', 1, observed) == observed + 1
        with pytest.raises(CheckpointCasConflictError):
            second.advance_v1_cursor('live', 'changes', 1, observed)
        saved = second.v1_cursors('live')
        assert saved['change_cursor'] == 1 and saved['generation'] == observed + 1
        assert saved['invalidation_cursor'] == 0
    finally:
        second.close()
        first.close()


def test_v1_bundle_freezes_all_directions_before_validity(tmp_path):
    store = SilverCheckpointStore(tmp_path / "silver.sqlite3")
    store.open()
    try:
        rows = [("silver_traffic_observation_fact_v1",
                 {"event_id": "e", "source_direction": direction, "vehicle_count": count,
                  "simulation_run_id": "r", "topology_digest": "digest",
                  "simulation_time_seconds": 12.0})
                for direction, count in (("North", 0), ("South", 2))]
        store.freeze_v1_bundle("live", "changes", 1, "e", 1, rows)
        assert len(store.pending_v1_obligations("live")) == 2
        with pytest.raises(CheckpointError, match="unresolved"):
            store.publish_v1_valid("live", "e", 1, 1)
        for obligation in store.pending_v1_obligations("live"):
            store.reconcile_v1_obligation("live", "changes", 1,
                obligation["target_table"], obligation["expected_hash"])
        store.publish_v1_valid("live", "e", 1, 1)
        assert store.conn.execute("SELECT state FROM silver_v1_event_validity").fetchone()[0] == "VALID"
        with pytest.raises(CheckpointError, match="already frozen"):
            store.freeze_v1_bundle("live", "changes", 1, "e", 1, rows)
    finally:
        store.close()
