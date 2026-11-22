"""Synthetic fault boundaries with real CH replay tables and durable SQLite."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import SilverSettings
from de.silver.processor import SilverProcessor
from de.silver.repositories import SilverClickHouseRepository

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='writes isolated replay fixture rows to real ClickHouse')


@pytest.mark.parametrize('failure_boundary', ['before-insert', 'lost-response'])
def test_real_invalidation_survives_restart_and_reconciles(failure_boundary):
    base = SilverSettings()
    replay_id = 'p3-invalidation-' + uuid4().hex[:16]
    path = Path(base.checkpoint_path).parent / 'acceptance' / replay_id / 'checkpoint.sqlite3'
    settings = base.model_copy(update=dict(namespace='replay:' + replay_id,
        destination_mode='replay', replay_run_id=replay_id, checkpoint_path=str(path)))
    store = SilverCheckpointStore(path)
    store.open()
    store.bind_v1_source(settings.namespace, dict(namespace='live', source_path='synthetic-source',
        schema_fingerprint='fixture', bronze_version='fixture', file_identity=[0, 0]))
    entry = dict(invalidation_id='a' * 64, invalidation_sequence=1, event_id='b' * 64,
        revision=2, previous_hash='c' * 64, conflicting_hash='d' * 64,
        first_raw_id='e' * 64, conflicting_raw_id='f' * 64)
    provenance = dict(event_id=entry['event_id'], simulation_run_id=replay_id, topology_digest='v1:sha256:' + '1' * 64,
        intersection_id='A', cycle_sequence=4, simulation_time_seconds=12.0,
        change_sequence=2, original_delivery={'raw': entry['first_raw_id']},
        conflicting_delivery={'raw': entry['conflicting_raw_id']})
    source = SimpleNamespace(read_invalidation_provenance=lambda *args: provenance)
    archive = SimpleNamespace(resolve=lambda *args: SimpleNamespace(generation=1))
    store.conn.execute('INSERT INTO silver_v1_event_validity VALUES (?,?,?,?,?,?,?,?,?,?)',
        (settings.namespace, entry['event_id'], replay_id, provenance['topology_digest'],
         12.0, 1, 'VALID', 1, None, 'before-conflict'))

    class FaultRepository(SilverClickHouseRepository):
        armed = True

        def insert_v1_rows(self, table, rows, **kwargs):
            state = worker.checkpoint.conn.execute('SELECT state FROM silver_v1_event_validity').fetchone()[0]
            assert state == 'INVALIDATED', 'local suppression must precede CH write'
            if self.armed:
                self.armed = False
                if failure_boundary == 'lost-response':
                    super().insert_v1_rows(table, rows, **kwargs)
                raise ConnectionError(failure_boundary)
            super().insert_v1_rows(table, rows, **kwargs)

    repo = FaultRepository(settings)
    repo.connect()
    worker = SimpleNamespace(settings=settings, checkpoint=store, repo=repo)
    worker.recover_v1_obligations = lambda limit: SilverProcessor.recover_v1_obligations(worker, limit)
    try:
        with pytest.raises(ConnectionError, match=failure_boundary):
            SilverProcessor.process_v1_invalidation(worker, entry, source, archive)
        assert store.v1_cursors(settings.namespace)['invalidation_cursor'] == 0
        frozen = store.pending_v1_obligations(settings.namespace)
        assert len(frozen) == 1
        assert frozen[0]['attempts'] == 1
        assert frozen[0]['last_error'] == failure_boundary
        store.close()
        store = SilverCheckpointStore(path)
        store.open()
        worker.checkpoint = store
        assert store.conn.execute('SELECT state FROM silver_v1_event_validity').fetchone()[0] == 'INVALIDATED'
        assert store.pending_v1_obligations(settings.namespace)[0]['expected_json'] == frozen[0]['expected_json']
        SilverProcessor.process_v1_invalidation(worker, entry, source, archive)
        assert store.v1_cursors(settings.namespace)['invalidation_cursor'] == 1
        assert not store.pending_v1_obligations(settings.namespace)
        count = repo.client.query('SELECT count() FROM ' + repo.database +
            '.silver_invalidation_replay_v1 WHERE replay_run_id={id:String}',
            parameters={'id': replay_id}).result_rows[0][0]
        assert count == 1, 'lost response must reconcile without another physical insert'
        report = dict(replay_id=replay_id, boundary=failure_boundary,
            local_state='INVALIDATED', invalidation_cursor=1, physical_rows=count,
            frozen_hash=frozen[0]['expected_hash'])
        (path.parent / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    finally:
        store.close()
        repo.close()
