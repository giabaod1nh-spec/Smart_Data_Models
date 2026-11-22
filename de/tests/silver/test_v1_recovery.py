from pathlib import Path
from types import SimpleNamespace
import pytest

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.processor import SilverProcessor
from de.silver.checkpoint_store import CheckpointError
from de.silver.repositories import FactBusinessKeyConflictError


def test_acknowledgement_batch_rolls_back_on_unknown_frozen_hash(tmp_path):
    store = SilverCheckpointStore(tmp_path / 'batch.sqlite3')
    store.open()
    try:
        store.freeze_v1_obligation('live', 'changes', 1, 'event',
                                  'silver_run_dimension_v1', {'simulation_run_id': 'r'})
        pending = store.pending_v1_obligations('live')[0]
        with pytest.raises(CheckpointError, match='identity/content mismatch'):
            store.reconcile_v1_obligations([pending, dict(pending, expected_hash='unknown')])
        after = store.pending_v1_obligations('live')[0]
        assert after['status'] == 'PENDING' and after['attempts'] == 0
        store.reconcile_v1_obligations([pending])
        assert not store.pending_v1_obligations('live')
    finally:
        store.close()


def test_recovery_acknowledges_only_verified_prefix_on_later_failure(tmp_path):
    store = SilverCheckpointStore(tmp_path / 'partial.sqlite3')
    store.open()
    for table in ('silver_run_dimension_v1', 'silver_topology_dimension_v1'):
        store.freeze_v1_obligation('live', 'changes', 1, 'event', table, {'id': table})
    def reconcile(table, *args, **kwargs):
        if table == 'silver_topology_dimension_v1':
            raise ConnectionError('second output unavailable')
        return True
    worker = SimpleNamespace(checkpoint=store, settings=SimpleNamespace(
        namespace='live', destination_mode='main', replay_run_id=''),
        repo=SimpleNamespace(reconcile_v1_row=reconcile))
    try:
        with pytest.raises(ConnectionError):
            SilverProcessor.recover_v1_obligations(worker)
        pending = store.pending_v1_obligations('live')
        assert len(pending) == 1 and pending[0]['target_table'] == 'silver_topology_dimension_v1'
        assert pending[0]['attempts'] == 1 and pending[0]['status'] == 'PENDING'
    finally:
        store.close()


def test_permanent_content_fault_is_durable_and_not_retried_after_restart(tmp_path):
    path = tmp_path / 'fault.sqlite3'
    store = SilverCheckpointStore(path)
    store.open()
    store.freeze_v1_obligation('live', 'changes', 1, 'event',
                              'silver_run_dimension_v1', {'simulation_run_id': 'r', 'run_generation': 1})
    calls = []
    def conflict(*args, **kwargs):
        calls.append(True)
        raise FactBusinessKeyConflictError('immutable content differs')
    worker = SimpleNamespace(checkpoint=store, settings=SimpleNamespace(
        namespace='live', destination_mode='main', replay_run_id=''),
        repo=SimpleNamespace(reconcile_v1_row=conflict))
    try:
        with pytest.raises(FactBusinessKeyConflictError):
            SilverProcessor.recover_v1_obligations(worker)
        failed = store.pending_v1_obligations('live')[0]
        assert failed['status'] == 'FAULT' and failed['attempts'] == 1
        assert failed['last_error'] == 'immutable content differs'
        store.close()
        store = SilverCheckpointStore(path)
        store.open()
        worker.checkpoint = store
        with pytest.raises(CheckpointError, match='permanent fault'):
            SilverProcessor.recover_v1_obligations(worker)
        assert len(calls) == 1
    finally:
        store.close()


def test_recovery_reconciles_lost_response_without_second_insert(tmp_path):
    store = SilverCheckpointStore(Path(tmp_path) / "silver.sqlite3")
    store.open()
    try:
        row = {"simulation_run_id": "r", "run_generation": 1}
        store.freeze_v1_obligation("live", "changes", 1, "event",
                                   "silver_run_dimension_v1", row)

        class Repo:
            def __init__(self):
                self.visible = True  # CH committed, insert response was lost.
                self.inserts = 0

            def reconcile_v1_row(self, table, expected, **kwargs):
                assert expected == row
                return self.visible

            def insert_v1_rows(self, table, rows, **kwargs):
                self.inserts += 1
                self.visible = True

        repo = Repo()
        worker = SimpleNamespace(checkpoint=store, settings=SimpleNamespace(
            namespace="live", destination_mode="main", replay_run_id=""), repo=repo)
        assert SilverProcessor.recover_v1_obligations(worker) == 1
        assert repo.inserts == 0
        assert SilverProcessor.recover_v1_obligations(worker) == 0
    finally:
        store.close()
