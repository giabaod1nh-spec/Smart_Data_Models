"""Lifecycle projection on real CH with isolated archived-run fixtures."""
import json
import os
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest

from de.silver.archive_resolver import ArchiveResolver, ArchiveUnavailable
from de.silver.config import SilverSettings
from de.silver.lifecycle_refresher import LifecycleRefresher
from de.silver.repositories import SilverClickHouseRepository
from de.silver.processor import SilverProcessor
from de.tests.silver.test_archive_lifecycle_v1 import _fixture

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='writes lifecycle fixtures to isolated real CH replay targets')


@pytest.mark.parametrize('terminal', ['COMPLETE', 'INCOMPLETE', 'STOPPED'])
def test_real_lifecycle_transitions_without_observations(terminal, monkeypatch):
    settings = SilverSettings()
    replay_id = 'p3-lifecycle-' + uuid4().hex[:16]
    root = Path(settings.checkpoint_path).parent / 'acceptance' / replay_id
    manifest, folder, registry = _fixture(root / 'archive')
    manifest.update(runStatus='RUNNING', failedCycleCount=1)
    manifest_path = folder / 'run_manifest.json'
    manifest_path.write_text(json.dumps(manifest))
    with sqlite3.connect(registry) as db:
        db.execute('UPDATE runs SET updated_at=?', ('2026-11-21T00:00:00Z',))
    resolver = ArchiveResolver(root / 'archive')
    poller = LifecycleRefresher(root / 'checkpoint.sqlite3', resolver)
    poller.register(manifest['simulationRunId'], manifest['topologyDigest'], manifest['runGeneration'])
    repo = SilverClickHouseRepository(settings.model_copy(update=dict(destination_mode='replay',
        namespace='replay:' + replay_id, replay_run_id=replay_id)))
    repo.connect()
    timeline = []
    def publish():
        changed = poller.poll()
        assert len(changed) == 1
        assert poller.project_pending(repo, replay_run_id=replay_id) == 1
        assert poller.poll() == []
        assert poller.project_pending(repo, replay_run_id=replay_id) == 0
        timeline.append(changed[0].__dict__)
    try:
        publish()
        with sqlite3.connect(registry) as db:
            db.execute('UPDATE runs SET status=?,updated_at=?', (terminal, '2026-11-21T00:01:00Z'))
        publish()
        manifest_path.rename(folder / 'offline')
        publish()
        (folder / 'offline').rename(manifest_path)
        manifest.update(runStatus=terminal, failedCycleCount=0 if terminal == 'COMPLETE' else 2)
        manifest_path.write_text(json.dumps(manifest))
        publish()
        with monkeypatch.context() as scoped:
            def unavailable(*args):
                raise ArchiveUnavailable('registry offline')
            scoped.setattr(resolver, 'registry_row', unavailable)
            publish()
        poller = LifecycleRefresher(root / 'checkpoint.sqlite3', resolver)
        publish()
        result = repo.client.query('SELECT registry_state,run_status,manifest_mirror_state,'
            'failed_cycle_count,lifecycle_freshness FROM ' + repo.database +
            '.silver_run_lifecycle_replay_v1 WHERE replay_run_id={id:String} ORDER BY observed_at',
            parameters={'id': replay_id}).result_rows
        assert len(result) == 6
        assert [row[2] for row in result] == ['SYNCED', 'LAGGING', 'UNAVAILABLE', 'SYNCED', 'UNAVAILABLE', 'SYNCED']
        assert [row[3] for row in result] == [1, 1, 1] + [manifest['failedCycleCount']] * 3
        assert result[4][0] == 'UNAVAILABLE' and result[4][4] == 'STALE'
        assert result[4][1] == terminal and result[5][4] == 'CURRENT'
        (root / 'report.json').write_text(json.dumps(dict(replay_id=replay_id,
            terminal=terminal, snapshots=timeline, physical_rows=result), indent=2), encoding='utf-8')
    finally:
        repo.close()


def test_real_worker_readiness_tracks_registry_outage(monkeypatch):
    base = SilverSettings()
    replay_id = 'p3-lifecycle-worker-' + uuid4().hex[:12]
    root = Path(base.checkpoint_path).parent / 'acceptance' / replay_id
    manifest, folder, registry = _fixture(root / 'archive')
    manifest.update(runStatus='RUNNING', failedCycleCount=3)
    (folder / 'run_manifest.json').write_text(json.dumps(manifest))
    with sqlite3.connect(registry) as db:
        db.execute('UPDATE runs SET updated_at=?', ('2026-11-21T00:00:00Z',))
    settings = base.model_copy(update=dict(checkpoint_path=str(root / 'checkpoint.sqlite3'),
        v1_archive_root=str(root / 'archive'), namespace='replay:' + replay_id,
        destination_mode='replay', replay_run_id=replay_id,
        v1_replay_run_filter='no-events-for-this-fixture', v1_lifecycle_interval_sec=1,
        poll_interval_sec=0.05))
    poller = LifecycleRefresher(root / 'checkpoint.sqlite3', ArchiveResolver(root / 'archive'))
    poller.register(manifest['simulationRunId'], manifest['topologyDigest'], manifest['runGeneration'])
    worker = SilverProcessor(settings, lock_held=True)
    def wait_for(predicate, seconds=10):
        deadline = time.monotonic() + seconds
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert predicate()
    try:
        worker.start()
        wait_for(lambda: worker.health_snapshot().ready)
        before = asdict(worker.health_snapshot())
        with monkeypatch.context() as scoped:
            def offline(*args):
                raise ArchiveUnavailable('fixture registry unavailable')
            scoped.setattr(worker.archive_resolver, 'registry_row', offline)
            wait_for(lambda: not worker.health_snapshot().lifecycle_ok)
            during = asdict(worker.health_snapshot())
            assert not during['ready']
            cached = worker.lifecycle.latest(manifest['simulationRunId'])
            assert cached.status == 'RUNNING' and cached.failed_cycle_count == 3
            assert cached.registry_state == 'UNAVAILABLE' and cached.freshness == 'STALE'
            assert worker.lifecycle.poll_state()['retry_count'] > 0
        wait_for(lambda: worker.health_snapshot().ready)
        after = asdict(worker.health_snapshot())
        assert after['lifecycle_ok'] and not after['lifecycle_error']
        assert worker.lifecycle.poll_state()['retry_count'] == 0
        (root / 'worker-report.json').write_text(json.dumps(dict(replay_id=replay_id,
            before=before, during=during, after=after), indent=2), encoding='utf-8')
    finally:
        worker.stop()
