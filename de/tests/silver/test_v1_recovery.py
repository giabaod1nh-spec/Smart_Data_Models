import json
from pathlib import Path
from types import SimpleNamespace
import pytest

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import SilverSettings
from de.silver.metrics import Metrics
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
    worker = SimpleNamespace(checkpoint=store, metrics=Metrics(), settings=SimpleNamespace(
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
    worker = SimpleNamespace(checkpoint=store, metrics=Metrics(), settings=SimpleNamespace(
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
        worker = SimpleNamespace(checkpoint=store, metrics=Metrics(), settings=SimpleNamespace(
            namespace="live", destination_mode="main", replay_run_id=""), repo=repo)
        assert SilverProcessor.recover_v1_obligations(worker) == 1
        assert repo.inserts == 0
        assert SilverProcessor.recover_v1_obligations(worker) == 0
    finally:
        store.close()


@pytest.mark.parametrize("lost_response", [False, True])
def test_direction_group_recovery_keeps_exact_obligation_outcomes(tmp_path, lost_response):
    table = "silver_traffic_observation_fact_v1"
    settings = SilverSettings()
    store = SilverCheckpointStore(tmp_path / "group.sqlite3")
    store.open()

    class Repo:
        def __init__(self):
            self.visible = {"east"}
            self.inserts = []

        def reconcile_v1_directions(self, target, rows, **kwargs):
            assert target == table
            return [row["source_direction"] in self.visible for row in rows]

        def insert_v1_rows(self, target, rows, **kwargs):
            assert target == table
            self.inserts.append({row["source_direction"] for row in rows})
            self.visible.add("north")
            if lost_response and len(self.inserts) == 1:
                raise ConnectionError("response lost after a partial write")
            if len(self.inserts) == 2:
                self.visible.add("south")

        def reconcile_v1_row(self, target, row, **kwargs):
            return row["source_direction"] in self.visible

    worker = SilverProcessor(settings, repo=Repo(), checkpoint=store)
    try:
        for direction in ("east", "north", "south"):
            store.freeze_v1_obligation("live", "changes", 1, "event", table, dict(
                simulation_run_id="run", topology_digest="digest", event_id="event",
                intersection_id="A", source_direction=direction, bronze_semantic_revision=1))
        with pytest.raises(ConnectionError if lost_response else RuntimeError):
            worker.recover_v1_obligations()
        pending = store.pending_v1_obligations("live")
        assert {json.loads(item["expected_json"])["source_direction"] for item in pending} == (
            {"north", "south"} if lost_response else {"south"})
        assert all(item["status"] == "PENDING" and item["attempts"] == 1 and item["last_error"]
                   for item in pending)
        assert worker.recover_v1_obligations() == len(pending)
        assert worker.repo.inserts == [{"north", "south"}, {"south"}]
        assert store.pending_v1_obligations("live") == []
    finally:
        store.close()


def test_group_conflict_fallback_faults_only_the_divergent_key(tmp_path):
    table = "silver_signal_state_fact_v1"
    store = SilverCheckpointStore(tmp_path / "conflict.sqlite3")
    store.open()

    class Repo:
        def reconcile_v1_directions(self, _target, _rows, **_kwargs):
            raise FactBusinessKeyConflictError("Group contents differ")

        def reconcile_v1_row(self, _target, row, **_kwargs):
            if row["source_direction"] == "south":
                raise FactBusinessKeyConflictError("South contains different content")
            return True

    worker = SilverProcessor(SilverSettings(), repo=Repo(), checkpoint=store)
    try:
        for direction in ("east", "south"):
            store.freeze_v1_obligation("live", "changes", 1, "event", table, dict(
                simulation_run_id="run", topology_digest="digest", event_id="event",
                intersection_id="A", source_direction=direction, bronze_semantic_revision=1))
        with pytest.raises(FactBusinessKeyConflictError, match="South"):
            worker.recover_v1_obligations()
        pending = store.pending_v1_obligations("live")
        assert len(pending) == 1
        assert json.loads(pending[0]["expected_json"])["source_direction"] == "south"
        assert pending[0]["status"] == "FAULT"
        assert pending[0]["attempts"] == 1
    finally:
        store.close()
