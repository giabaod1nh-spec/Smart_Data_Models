"""Test-only subprocess that exits abruptly after a selected durable action."""
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from de.bronze.checkpoint_store import V1SemanticReader
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.silver.archive_resolver import ArchiveResolver
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import SilverSettings
from de.silver.processor import SilverProcessor
from de.silver.repositories import SilverClickHouseRepository


def main():
    settings = SilverSettings.model_validate_json(sys.stdin.read())
    boundary = sys.argv[1]
    store = SilverCheckpointStore(Path(settings.checkpoint_path))
    store.open()
    repo = SilverClickHouseRepository(settings)
    repo.connect()
    bronze = BronzeClickHouseRepository(ObservationSettings(
        checkpoint_path=settings.v1_bronze_path, clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port, clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password, clickhouse_database=settings.clickhouse_database))
    bronze.connect()
    target, method = {
        'freeze': (store, 'freeze_v1_bundle'),
        'insert': (repo, 'insert_v1_rows'),
        'insert-fact': (repo, 'insert_v1_rows'),
        'acknowledge': (store, 'reconcile_v1_obligations'),
        'finalize': (store, 'finalize_v1_event'),
    }[boundary]
    original = getattr(target, method)
    def exit_after_commit(*args, **kwargs):
        result = original(*args, **kwargs)
        if boundary == 'insert-fact' and args[0] != 'silver_traffic_observation_fact_v1':
            return result
        os._exit(73)  # Deliberately bypass finally/close/flush handlers.
    setattr(target, method, exit_after_commit)
    worker = SilverProcessor(settings, repo=repo, checkpoint=store)
    worker.run_v1_journal_pass(V1SemanticReader(settings.v1_bronze_path), bronze,
        ArchiveResolver(Path(settings.v1_archive_root)), SimpleNamespace(register_verified=lambda _context: None))
    raise AssertionError('crash boundary was not reached')


if __name__ == '__main__':
    main()
