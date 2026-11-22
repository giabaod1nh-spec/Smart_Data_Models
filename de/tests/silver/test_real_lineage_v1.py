"""Bronze-owner duplicate delivery against isolated real CH and Silver state."""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from contracts.traffic_observation import build_observation, canonical_json
from de.bronze.checkpoint_store import V1SemanticStore, V1SemanticReader
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.bronze.processor import BronzeProcessor
from de.kafka_raw.clickhouse_repository import _split
from de.silver.archive_resolver import ArchiveResolver
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.canonical_reader import (
    CanonicalSilverReader, CanonicalReadQuery, CanonicalContentConflict, SnapshotStaleError,
    CanonicalReadSession,
)
from de.silver.replay import run_replay, build_replay_settings
from de.silver.config import SilverSettings
from de.silver.processor import SilverProcessor
from de.silver.repositories import SilverClickHouseRepository, V1_TABLES
from de.silver.source_binding import verify_bronze_source, SourceBoundaryChangedError
from de.tests.silver.test_archive_lifecycle_v1 import _fixture
from de.tests.silver.v1_fixtures import snapshot_v1

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='creates retained isolated real CH lineage database')


@pytest.mark.parametrize('conflict_before_consumption', [False, True])
@pytest.mark.parametrize('conflict_stage', ['completion', 'materialization'])
def test_real_benign_duplicate_preserves_silver_and_grows_lineage(monkeypatch, conflict_before_consumption, conflict_stage,
                                                               crash_boundary=None):
    base = SilverSettings()
    database = 'p3_lineage_' + uuid4().hex[:16]
    root = Path(base.checkpoint_path).parent / 'acceptance' / database
    manifest, _, registry = _fixture(root / 'archive')
    with sqlite3.connect(registry) as db:
        db.execute('UPDATE runs SET activated_at=?,updated_at=?',
                   ('2026-11-22T00:00:00Z', '2026-11-22T00:00:00Z'))
    archive = ArchiveResolver(root / 'archive')
    context = archive.resolve(manifest['simulationRunId'], manifest['topologyDigest'])
    bootstrap = SilverClickHouseRepository(base)
    bootstrap.connect()
    bootstrap.client.command(f'CREATE DATABASE {database}')
    migrations = Path(__file__).resolve().parents[2] / 'migrations'
    for prefix in ('006_', '007_', '008_'):
        sql = next(migrations.glob(prefix + '*.sql')).read_text().replace('smart_traffic', database)
        for statement in _split(sql):
            bootstrap.client.command(statement)
    settings = base.model_copy(update=dict(clickhouse_database=database,
        checkpoint_path=str(root / 'silver_v1/state.sqlite3'),
        v1_bronze_path=str(root / 'bronze_v1/state.sqlite3'),
        v1_archive_root=str(root / 'archive'), namespace='live', destination_mode='main', replay_run_id=''))
    bronze = BronzeClickHouseRepository(ObservationSettings(
        checkpoint_path=settings.v1_bronze_path, clickhouse_database=database,
        clickhouse_host=base.clickhouse_host, clickhouse_port=base.clickhouse_port,
        clickhouse_user=base.clickhouse_user, clickhouse_password=base.clickhouse_password))
    bronze.connect()
    source = V1SemanticStore(Path(settings.v1_bronze_path))
    silver = SilverCheckpointStore(Path(settings.checkpoint_path))
    silver.open()
    repo = SilverClickHouseRepository(settings)
    repo.connect()
    reader = None
    try:
        snapshot = snapshot_v1()
        measured = next(iter(snapshot['directions'].values()))
        snapshot.update(simulation_run_id=context.run_id, topology_digest=context.topology_digest,
                        directions={name: dict(measured) for name in context.directions['A']},
                        colors={name: 'green' for name in context.directions['A']})
        event = build_observation(snapshot, cycle_sequence=7, simulation_step=700)
        raw_ids = {name: hashlib.sha256((database + name).encode()).hexdigest() for name in ('R1', 'R2', 'R3', 'R4', 'R5')}
        def deliver(raw_id, offset):
            raw = dict(raw_ingestion_id=raw_ids[raw_id], topic='traffic.observations.v1', topic_identity=database,
                partition=0, offset=offset, physical_record_hash=hashlib.sha256(raw_id.encode()).hexdigest(),
                payload_bytes_hash=hashlib.sha256(canonical_json(event).encode()).hexdigest())
            return BronzeProcessor.materialize_v1_delivery(source, bronze, 'live', raw, event)
        assert deliver('R1', 1) == 'ACCEPTED_NEW'
        first_event = event
        event = build_observation(snapshot, cycle_sequence=8, simulation_step=800)
        other_event_id = event['eventId']
        assert deliver('R4', 4) == 'ACCEPTED_NEW'
        event = first_event
        before = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
        crashed_cursor = 0
        crash_snapshot = None
        if crash_boundary:
            silver.close()
            result = subprocess.run([sys.executable, '-m', 'de.tests.silver.crash_worker_v1', crash_boundary],
                input=settings.model_dump_json(), text=True, capture_output=True, timeout=60)
            assert result.returncode == 73, result.stderr
            silver.open()
            crashed_cursor = silver.v1_cursors('live')['change_cursor']
            assert crashed_cursor == (1 if crash_boundary == 'finalize' else 0)
            crash_state = silver.conn.execute('SELECT state FROM silver_v1_event_validity WHERE event_id=?',
                                               (event['eventId'],)).fetchone()[0]
            assert crash_state == ('VALID' if crash_boundary == 'finalize' else 'PENDING')
            crash_snapshot = dict(cursor=crashed_cursor, local_state=crash_state,
                obligations=[list(row) for row in silver.conn.execute(
                    'SELECT status,count(*) FROM silver_v1_obligation GROUP BY status')])
        worker = SilverProcessor(settings, repo=repo, checkpoint=silver)
        reader = V1SemanticReader(settings.v1_bronze_path)
        lifecycle = SimpleNamespace(register=lambda *args: None)
        if conflict_before_consumption:
            snapshot['directions'][next(iter(snapshot['directions']))]['vehicle_count'] = 1
            event = build_observation(snapshot, cycle_sequence=7, simulation_step=700)
            assert deliver('R3', 3) == 'SEMANTIC_CONFLICT'
            assert worker.run_v1_journal_pass(reader, bronze, archive, lifecycle) == 4
            for table in ('silver_traffic_observation_fact_v1', 'silver_signal_state_fact_v1',
                          'silver_intersection_state_fact_v1'):
                assert repo.fetch_v1_event_rows(table, context.run_id, context.topology_digest,
                                                event['eventId']) == []
                assert repo.fetch_v1_event_rows(table, context.run_id, context.topology_digest,
                                                other_event_id)
            assert silver.conn.execute('SELECT state FROM silver_v1_event_validity WHERE event_id=?',
                                       (event['eventId'],)).fetchone()[0] == 'INVALIDATED'
            cursors = silver.v1_cursors('live')
            assert cursors['change_cursor'] == 3 and cursors['invalidation_cursor'] == 1
            (root / 'report.json').write_text(json.dumps(dict(database=database,
                conflict_before_consumption=True, zero_conflicted_event_facts=True,
                other_event_materialized=True, cursors=cursors), default=str, indent=2))
            return
        assert worker.run_v1_journal_pass(reader, bronze, archive, lifecycle) == 2 - crashed_cursor
        def facts():
            return {table: sorted(repo.fetch_v1_event_rows(table, context.run_id, context.topology_digest,
                                                   event['eventId']), key=lambda row: row.get('source_direction', '')) for table in (
                'silver_traffic_observation_fact_v1', 'silver_signal_state_fact_v1',
                'silver_intersection_state_fact_v1')}
        frozen = facts()
        assert all(row['source_raw_ingestion_id'] == raw_ids['R1'] for rows in frozen.values() for row in rows)
        query = CanonicalReadQuery('live', context.run_id, context.topology_digest, 0, 100)
        with CanonicalSilverReader(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                                   repo, bronze, archive) as canonical:
            bundles = canonical.read_complete_scan(query, page_size=1)
            assert [bundle.event_id for bundle in bundles] == sorted([event['eventId'], other_event_id])
        reader_user = database + '_reader'
        reader_password = uuid4().hex
        bootstrap.client.command(f'CREATE USER {reader_user} IDENTIFIED WITH sha256_password BY '
                                 '{password:String} SETTINGS readonly=1', parameters={'password': reader_password})
        bootstrap.client.command(f'GRANT SELECT ON {database}.* TO {reader_user}')
        readonly_settings = settings.model_copy(update=dict(clickhouse_user=reader_user,
                                                            clickhouse_password=reader_password))
        with CanonicalReadSession(readonly_settings) as canonical:
            assert [bundle.event_id for bundle in canonical.read_complete_scan(query, page_size=1)] == [
                bundle.event_id for bundle in bundles]
            from clickhouse_connect.driver.exceptions import DatabaseError
            with pytest.raises(DatabaseError):
                canonical.silver_repo.client.command(
                    f'INSERT INTO {database}.silver_traffic_observation_fact_v1 '
                    f'SELECT * FROM {database}.silver_traffic_observation_fact_v1 WHERE 0')
        replay_id = database.replace('_', '-')
        doc = dict(manifest_version='silver-replay-v1', replay_run_id=replay_id,
            source_namespace='live', source_binding=before,
            change_high_water=before['change_high_water'], invalidation_high_water=before['invalidation_high_water'],
            simulation_run_id=context.run_id, run_generation=context.generation,
            topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
            manifest_file_hash=context.manifest_file_hash, catalog_file_hash=context.catalog_file_hash,
            reader_version='1', processor_version='1', schema_version='008')
        manifest_path = root / 'replay.manifest.json'
        manifest_path.write_text(json.dumps(doc))
        def live_inventory():
            tables = {}
            for table in sorted(V1_TABLES):
                rows = repo.client.query(f'SELECT * FROM {database}.{table}').result_rows
                material = json.dumps(sorted(json.dumps(row, default=str) for row in rows))
                tables[table] = dict(rows=len(rows), sha256=hashlib.sha256(material.encode()).hexdigest())
            local = {}
            for (table,) in silver.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'silver_%'"):
                rows = silver.conn.execute(f'SELECT * FROM "{table}"').fetchall()
                material = json.dumps(sorted(json.dumps(list(row), default=str) for row in rows))
                local[table] = dict(rows=len(rows), sha256=hashlib.sha256(material.encode()).hexdigest())
            return dict(main_clickhouse=tables, main_sqlite=local)
        live_before_replays = live_inventory()
        replay_before = run_replay(manifest_path, replay_id, base_settings=settings)
        disposition = deliver('R2', 2)
        assert disposition == 'SEMANTIC_DUPLICATE'
        assert verify_bronze_source(Path(settings.v1_bronze_path), 'live') == before
        assert worker.run_v1_journal_pass(reader, bronze, archive, lifecycle) == 0
        assert facts() == frozen
        lineage = reader.read_delivery_lineage('live', event['eventId'])
        assert {row['raw_id'] for row in lineage} == {raw_ids['R1'], raw_ids['R2']}
        replay_after = run_replay(manifest_path, replay_id, resume=True, base_settings=settings)
        assert replay_after['idempotent'] and replay_after['logical_hash'] == replay_before['logical_hash']
        second_id = replay_id + '-second'
        doc['replay_run_id'] = second_id
        second_manifest = root / 'second.manifest.json'
        second_manifest.write_text(json.dumps(doc))
        replay_second = run_replay(second_manifest, second_id, base_settings=settings)
        assert replay_second['logical_hash'] == replay_before['logical_hash']
        live_after_replays = live_inventory()
        assert live_after_replays == live_before_replays
        report = dict(database=database, event_id=event['eventId'], source_boundary=before,
                      lineage=lineage, fact_counts={key: len(rows) for key, rows in frozen.items()},
                      exact_facts_unchanged=True, duplicate_disposition=disposition,
                      replay_before=replay_before, replay_after=replay_after, replay_second=replay_second,
                      live_inventory_before_replays=live_before_replays,
                      live_inventory_after_replays=live_after_replays,
                      canonical_complete_scan_events=len(bundles),
                      select_only_reader_user=reader_user, readonly_insert_rejected=True)
        if crash_boundary:
            report.update(abrupt_process_exit=73, crash_boundary=crash_boundary,
                          cursor_after_crash=crashed_cursor, crash_snapshot=crash_snapshot,
                          recovered_cursor=silver.v1_cursors('live')['change_cursor'])
        (root / 'report.json').write_text(json.dumps(report, default=str, indent=2))
        # Same semantic identity, changed payload: Bronze owns classification;
        # Silver suppresses it while preserving the old physical facts.
        snapshot['directions'][next(iter(snapshot['directions']))]['vehicle_count'] = 1
        event = build_observation(snapshot, cycle_sequence=7, simulation_step=700)
        original_fetch = repo.fetch_v1_event_rows
        injected = []
        def conflict_during_page(*args):
            rows = original_fetch(*args)
            if not injected:
                injected.append(deliver('R3', 3))
            return rows
        with monkeypatch.context() as patch:
            patch.setattr(repo, 'fetch_v1_event_rows', conflict_during_page)
            with CanonicalSilverReader(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                                       repo, bronze, archive) as canonical:
                with pytest.raises(SnapshotStaleError):
                    canonical.read_valid_page(query, limit=1)
        assert injected == ['SEMANTIC_CONFLICT']
        assert worker.run_v1_journal_pass(reader, bronze, archive, lifecycle) == 2
        assert facts() == frozen
        assert silver.conn.execute('SELECT state FROM silver_v1_event_validity WHERE event_id=?',
                                   (event['eventId'],)).fetchone()[0] == 'INVALIDATED'
        with CanonicalSilverReader(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                                   None, None, None) as canonical:
            feed = canonical.read_invalidation_page()
            assert len(feed.rows) == 1 and feed.next_sequence == 1 and not feed.has_more
            assert feed.rows[0]['event_id'] == event['eventId']
        with CanonicalSilverReader(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                                   repo, bronze, archive) as canonical:
            remaining = canonical.read_complete_scan(query, page_size=1)
            assert [bundle.event_id for bundle in remaining] == [other_event_id]
        report.update(conflict_disposition='SEMANTIC_CONFLICT', local_state='INVALIDATED',
                      invalidation_feed_sequence=feed.next_sequence, physical_facts_preserved=True,
                      source_conflict_during_page_rejected=True)
        table = 'silver_traffic_observation_fact_v1'
        duplicate_rows = repo.fetch_v1_event_rows(table, context.run_id, context.topology_digest, other_event_id)
        repo.insert_v1_rows(table, duplicate_rows)
        with CanonicalSilverReader(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                                   repo, bronze, archive) as canonical:
            deduped = canonical.read_complete_scan(query, page_size=1)
            assert len(deduped) == 1 and len(deduped[0].traffic) == len(context.directions['A'])
            conflicting = dict(duplicate_rows[0], vehicle_count=99)
            repo.insert_v1_rows(table, [conflicting])
            with pytest.raises(CanonicalContentConflict):
                canonical.read_valid_page(query, limit=1)
        report.update(identical_physical_duplicates_collapsed=True, conflicting_physical_row_rejected=True)
        abort_id = replay_id + '-conflict-abort'
        cutoff = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
        abort_doc = dict(doc, replay_run_id=abort_id, source_binding=cutoff,
                         change_high_water=cutoff['change_high_water'],
                         invalidation_high_water=cutoff['invalidation_high_water'])
        abort_manifest = root / 'abort.manifest.json'
        abort_manifest.write_text(json.dumps(abort_doc))
        event = build_observation(snapshot, cycle_sequence=8, simulation_step=800)
        original_pass = SilverProcessor.run_v1_journal_pass
        def conflict_before_replay_completion(processor, *args, **kwargs):
            if conflict_stage == 'materialization':
                original_insert = SilverClickHouseRepository.insert_v1_rows
                triggered = []
                def insert_then_conflict(repository, *insert_args, **insert_kwargs):
                    result = original_insert(repository, *insert_args, **insert_kwargs)
                    if not triggered:
                        triggered.append(deliver('R5', 5))
                        assert triggered == ['SEMANTIC_CONFLICT']
                    return result
                with monkeypatch.context() as patch:
                    patch.setattr(SilverClickHouseRepository, 'insert_v1_rows', insert_then_conflict)
                    return original_pass(processor, *args, **kwargs)
            result = original_pass(processor, *args, **kwargs)
            assert deliver('R5', 5) == 'SEMANTIC_CONFLICT'
            return result
        with monkeypatch.context() as patch:
            patch.setattr(SilverProcessor, 'run_v1_journal_pass', conflict_before_replay_completion)
            with pytest.raises(SourceBoundaryChangedError):
                run_replay(abort_manifest, abort_id, base_settings=settings)
        aborted_settings = build_replay_settings(settings, abort_id)
        with sqlite3.connect(aborted_settings.checkpoint_path) as db:
            assert db.execute('SELECT status FROM silver_v1_replay_state').fetchone()[0] == 'ABORTED'
        report.update(conflict_during_replay_stage=conflict_stage,
                      replay_failure='SOURCE_BOUNDARY_CHANGED', replay_abort_state='ABORTED')
        (root / 'report.json').write_text(json.dumps(report, default=str, indent=2))
    finally:
        if reader:
            reader.close()
        repo.close()
        silver.close()
        source.close()
        bronze.close()
        bootstrap.close()
