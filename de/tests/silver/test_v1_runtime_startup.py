import sqlite3
import time
import threading
import pytest
from pathlib import Path
from types import SimpleNamespace

import de.silver.processor as processor_module
from de.bronze.checkpoint_store import V1SemanticStore
from de.silver.config import SilverSettings
from de.silver.processor import SilverProcessor
from de.silver.source_binding import SourceVerificationError
from de.silver.repositories import FactBusinessKeyConflictError
from de.silver.checkpoint_store import CheckpointError


class FakeClickHouse:
    def connect(self):
        pass

    def close(self):
        pass

    def ping(self):
        return True

    def verify_v1_schema(self, **kwargs):
        if getattr(self, 'offline', False):
            raise ConnectionError('ClickHouse temporarily offline')
        pass


@pytest.mark.parametrize(("verified_backlog", "wait_on_first_pass"), [(11, False), (10, True)])
def test_worker_waits_only_after_the_verified_prefix_is_consumed(monkeypatch,
                                                                 verified_backlog, wait_on_first_pass):
    settings = SilverSettings(poll_interval_sec=0.02)
    worker = SilverProcessor(settings, repo=FakeClickHouse(),
                             checkpoint=SimpleNamespace(is_readable=lambda: True))
    worker._schema_ok = True
    worker.bronze_repo = FakeClickHouse()
    monkeypatch.setattr(processor_module, "V1SemanticReader",
                        lambda path: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(worker, "_publish_snapshot", lambda: None)
    seen = []

    def journal_pass(*args, max_entries, **kwargs):
        seen.append("pass")
        if len(seen) == 1:
            worker.metrics.source_lag = {"changes": verified_backlog, "invalidations": 0}
            return max_entries
        worker.metrics.source_lag = {"changes": 0, "invalidations": 0}
        return 0

    def wait(delay):
        assert delay == settings.poll_interval_sec
        seen.append("wait")
        worker._stop.set()

    monkeypatch.setattr(worker, "run_v1_journal_pass", journal_pass)
    monkeypatch.setattr(worker._stop, "wait", wait)
    worker._run_v1()
    assert seen == (["pass", "wait"] if wait_on_first_pass else ["pass", "pass", "wait"])


@pytest.mark.parametrize('fault,expected_code', [
    (None, None),
    (SourceVerificationError('CURSOR_GAP', 'gap'), 'CURSOR_GAP'),
    (SourceVerificationError('SOURCE_BINDING_CHANGED', 'replaced'), 'SOURCE_BINDING_CHANGED'),
    (FactBusinessKeyConflictError('changed row'), 'CANONICAL_CONTENT_CONFLICT'),
    (CheckpointError('SOURCE_BINDING_CHANGED'), 'SOURCE_BINDING_CHANGED'),
    (SourceVerificationError('AUTHORITY_UNAVAILABLE', 'temporarily offline'), 'AUTHORITY_UNAVAILABLE'),
])
def test_existing_silver_processor_starts_v1_source_and_poll_loop(tmp_path, monkeypatch, fault, expected_code):
    bronze_path = tmp_path / "bronze_v1" / "checkpoint.sqlite3"
    bronze = V1SemanticStore(bronze_path)
    bronze.close()
    archive_root = tmp_path / "archive"
    registry = archive_root / "runs" / "active_run.sqlite3"
    registry.parent.mkdir(parents=True)
    with sqlite3.connect(registry) as db:
        db.execute("CREATE TABLE runs (generation INTEGER, run_id TEXT, manifest_identity TEXT,"
                   "status TEXT, activated_at TEXT, updated_at TEXT)")
    monkeypatch.setattr(processor_module, "BronzeClickHouseRepository", lambda settings: FakeClickHouse())
    settings = SilverSettings(checkpoint_path=str(tmp_path / "silver_v1" / "checkpoint.sqlite3"),
        v1_bronze_path=str(bronze_path), v1_archive_root=str(archive_root),
        poll_interval_sec=0.02)
    worker = SilverProcessor(settings, repo=FakeClickHouse(), lock_held=True)
    try:
        worker.start()
        deadline = time.monotonic() + 2
        while not worker.health_snapshot().ready and time.monotonic() < deadline:
            time.sleep(0.02)
        assert worker.health_snapshot().ready
        assert worker.checkpoint.v1_cursors("live")["change_cursor"] == 0
        assert worker.health_snapshot().reader_initialized
        if fault is None:
            last_poll = worker._lifecycle_last_poll
            worker._lifecycle_last_poll = time.monotonic() - settings.v1_lifecycle_interval_sec * 4
            assert not worker.health_snapshot().ready and not worker.health_snapshot().lifecycle_ok
            worker._lifecycle_last_poll = last_poll
        if fault is not None:
            original_pass = worker.run_v1_journal_pass
            def fail(*args, **kwargs):
                raise fault
            monkeypatch.setattr(worker, 'run_v1_journal_pass', fail)
            expected_state = 'RETRYING' if expected_code == 'AUTHORITY_UNAVAILABLE' else 'FAULTED'
            deadline = time.monotonic() + 2
            while worker.health_snapshot().state != expected_state and time.monotonic() < deadline:
                time.sleep(0.02)
            assert worker.health_snapshot().state == expected_state
            assert not worker.health_snapshot().ready
            assert worker.health_snapshot().fault_code == expected_code
            assert worker.checkpoint.v1_cursors('live')['change_cursor'] == 0
            if expected_state == 'RETRYING':
                monkeypatch.setattr(worker, 'run_v1_journal_pass', original_pass)
                deadline = time.monotonic() + 8
                while not worker.health_snapshot().ready and time.monotonic() < deadline:
                    time.sleep(0.02)
                assert worker.health_snapshot().ready
                assert not worker.health_snapshot().fault_code
    finally:
        worker.stop()


def test_startup_outage_keeps_health_available_and_recovers(tmp_path, monkeypatch):
    bronze_path = tmp_path / 'bronze.sqlite3'
    V1SemanticStore(bronze_path).close()
    registry = tmp_path / 'archive' / 'runs' / 'active_run.sqlite3'
    registry.parent.mkdir(parents=True)
    with sqlite3.connect(registry) as db:
        db.execute('CREATE TABLE runs (generation INTEGER,run_id TEXT,manifest_identity TEXT,'
                   'status TEXT,activated_at TEXT,updated_at TEXT)')
    repo = FakeClickHouse()
    repo.offline = True
    monkeypatch.setattr(processor_module, 'BronzeClickHouseRepository', lambda settings: FakeClickHouse())
    settings = SilverSettings(checkpoint_path=str(tmp_path / 'silver.sqlite3'),
        v1_bronze_path=str(bronze_path), v1_archive_root=str(tmp_path / 'archive'),
        poll_interval_sec=0.02)
    worker = SilverProcessor(settings, repo=repo, lock_held=True)
    try:
        worker.start()
        deadline = time.monotonic() + 2
        while worker.health_snapshot().state != 'RETRYING' and time.monotonic() < deadline:
            time.sleep(0.02)
        snapshot = worker.health_snapshot()
        assert snapshot.state == 'RETRYING' and snapshot.worker_alive and not snapshot.ready
        assert not snapshot.schema_ok
        assert worker.checkpoint.conn.execute('SELECT count(*) FROM silver_v1_source_binding').fetchone()[0] == 0
        repo.offline = False
        deadline = time.monotonic() + 8
        while not worker.health_snapshot().ready and time.monotonic() < deadline:
            time.sleep(0.02)
        assert worker.health_snapshot().ready
        assert worker.checkpoint.v1_cursors('live')['change_cursor'] == 0
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        def slow_batch(*args, **kwargs):
            entered.set()
            release.wait(15)
            finished.set()
            return 0
        monkeypatch.setattr(worker, 'run_v1_journal_pass', slow_batch)
        worker.lifecycle.interval_seconds = 0.05
        assert entered.wait(2)
        last_poll = worker._lifecycle_last_poll
        deadline = time.monotonic() + 6
        while worker._lifecycle_last_poll <= last_poll and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not finished.is_set(), 'event worker must still be blocked during independent lifecycle poll'
        release.set()
        assert worker._lifecycle_last_poll > last_poll, 'lifecycle must poll while event worker is blocked'
    finally:
        if 'release' in locals():
            release.set()
        worker.stop()
