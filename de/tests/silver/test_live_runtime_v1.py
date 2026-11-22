"""Opt-in, read-only integration checks against the running V1 stack."""
import os
import sqlite3
from pathlib import Path

import pytest

from de.silver.canonical_reader import CanonicalReadQuery, CanonicalReadSession, CanonicalSilverReader, SourceLagError
from de.silver.config import SilverSettings
from de.silver.repositories import SilverClickHouseRepository
from de.silver.source_binding import verify_bronze_source

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='requires real ClickHouse and live RO Bronze/Silver state')


def test_real_clickhouse_schema():
    repo = SilverClickHouseRepository(SilverSettings())
    repo.connect()
    try:
        repo.verify_v1_schema()
    finally:
        repo.close()


def test_real_empty_invalidation_feed_needs_no_clickhouse_client():
    settings = SilverSettings()
    source = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
    assert source['invalidation_high_water'] == 0, 'this case records the no-conflict runtime boundary'
    with CanonicalSilverReader(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                               None, None, None) as reader:
        page = reader.read_invalidation_page()
        assert page.rows == () and page.next_sequence == 0
        assert not page.has_more and page.mirror_pending == 0


def test_real_reader_returns_source_lag_before_partial_results():
    settings = SilverSettings()
    source = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
    path = Path(settings.checkpoint_path)
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as db:
        cursor = db.execute('SELECT change_cursor FROM silver_v1_source_binding WHERE namespace=?',
                            ('live',)).fetchone()[0]
        run_id, digest = db.execute('SELECT simulation_run_id,topology_digest '
                                    'FROM silver_v1_event_validity LIMIT 1').fetchone()
    assert source['change_high_water'] - cursor > 10, 'this fault check requires a live backlog'
    query = CanonicalReadQuery('live', run_id, digest, 0, 1000000)
    with CanonicalReadSession(settings) as reader:
        with pytest.raises(SourceLagError) as result:
            reader.read_valid_page(query, limit=1)
        assert result.value.code == 'SOURCE_LAG'
