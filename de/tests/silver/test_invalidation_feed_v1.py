from types import SimpleNamespace

import pytest

from de.tests.silver.v1_fixtures import snapshot_v1 as _snapshot
from contracts.traffic_observation import build_observation
from de.bronze.checkpoint_store import V1SemanticStore, V1SemanticReader
from de.silver.canonical_reader import (CanonicalSilverReader, InvalidationLagError,
    SourceBindingChangedError, ValidityUnknownError)
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import SilverSettings
from de.silver.metrics import Metrics
from de.silver.processor import SilverProcessor
from de.silver.source_binding import verify_bronze_source


def test_incremental_feed_exposes_durable_suppression_before_ch_mirror(tmp_path):
    bronze_path, silver_path = tmp_path / 'bronze.sqlite3', tmp_path / 'silver.sqlite3'
    bronze = V1SemanticStore(bronze_path)
    for cycle in (4, 5):
        snapshot = _snapshot()
        for attempt in (0, 1):
            event = build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 100)
            raw_id = f'raw-{cycle}-{attempt}'
            intent = bronze.prepare('live', dict(raw_ingestion_id=raw_id, topic='traffic.observations.v1',
                partition=0, offset=cycle * 2 + attempt, topic_identity='topic'), event,
                'envelope-' + str(attempt), 'content-' + str(attempt), lambda *args: [])
            bronze.complete('live', raw_id, intent['expected_hash'])
            next(iter(snapshot['directions'].values()))['vehicle_count'] += 1
    silver = SilverCheckpointStore(silver_path)
    silver.open()
    silver.bind_v1_source('live', verify_bronze_source(bronze_path, 'live'))
    source = V1SemanticReader(bronze_path)
    entries = source.read_page('live', 'invalidations')['rows']
    assert len(entries) == 2
    def offline(*args, **kwargs):
        raise ConnectionError('CH mirror offline')
    worker = SimpleNamespace(settings=SilverSettings(v1_bronze_path=str(bronze_path)),
        checkpoint=silver, repo=SimpleNamespace(reconcile_v1_row=offline), metrics=Metrics())
    worker.recover_v1_obligations = lambda limit: SilverProcessor.recover_v1_obligations(worker, limit)
    archive = SimpleNamespace(resolve=lambda *args: SimpleNamespace(generation=1))
    def suppress(entry):
        with pytest.raises(ConnectionError):
            SilverProcessor.process_v1_invalidation(worker, entry, source, archive)
    try:
        with CanonicalSilverReader(bronze_path, silver_path, None, None, None) as reader:
            for limit in (0, 101, True, 1.5):
                with pytest.raises(ValueError):
                    reader.read_invalidation_page(limit=limit)
            with pytest.raises(InvalidationLagError):
                reader.read_invalidation_page()
            suppress(entries[0])
            first = reader.read_invalidation_page(limit=1)
            assert first.next_sequence == 1 and first.has_more and first.mirror_pending == 1
            assert first.rows[0]['invalidation_id'] == entries[0]['invalidation_id']
            assert first.rows[0]['simulation_run_id'] == _snapshot()['simulation_run_id']
            assert first.rows[0]['mirror_status'] == 'PENDING'
            assert silver.v1_cursors('live')['invalidation_cursor'] == 0
            with pytest.raises(InvalidationLagError):
                reader.read_invalidation_page(limit=2)  # Never return the first row as a partial page.
            with pytest.raises(SourceBindingChangedError):
                reader.read_invalidation_page(after_sequence=1)
            worker.repo.reconcile_v1_row = lambda *args, **kwargs: True
            SilverProcessor.process_v1_invalidation(worker, entries[0], source, archive)
            assert reader.read_invalidation_page(limit=1).mirror_pending == 0
            worker.repo.reconcile_v1_row = offline
            suppress(entries[1])
            second = reader.read_invalidation_page(1, 1, first.source_binding)
            assert second.next_sequence == 2 and not second.has_more and second.mirror_pending == 1
            assert second.rows[0]['event_id'] != first.rows[0]['event_id']
            assert reader.read_invalidation_page(2, 1, second.source_binding).rows == ()
            with pytest.raises(SourceBindingChangedError):
                reader.read_invalidation_page(1, 1, dict(first.source_binding, source_path='other'))
            silver.conn.execute("UPDATE silver_v1_obligation SET expected_hash='corrupt' WHERE source_sequence=1")
            with pytest.raises(ValidityUnknownError):
                reader.read_invalidation_page(limit=1)
    finally:
        source.close()
        silver.close()
        bronze.close()
