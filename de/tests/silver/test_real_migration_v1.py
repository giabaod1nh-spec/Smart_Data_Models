"""Clean install/upgrade acceptance in unique CH databases; retain all evidence."""
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from de.kafka_raw.clickhouse_repository import _split
from de.scripts.migrate_clickhouse import select_migrations
from de.silver.config import SilverSettings
from de.silver.repositories import SilverClickHouseRepository

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='creates isolated migration acceptance databases on real CH')


@pytest.mark.parametrize('populated_v2', [False, True])
def test_real_clean_upgrade_and_repeated_008_preserve_v2(populated_v2):
    settings = SilverSettings()
    database = 'p3_migration_' + uuid4().hex[:16]
    migrations = Path(__file__).resolve().parents[2] / 'migrations'
    repo = SilverClickHouseRepository(settings)
    repo.connect()
    client = repo.client
    def apply(path):
        sql = path.read_text(encoding='utf-8').replace('smart_traffic', database)
        for statement in _split(sql):
            client.command(statement)
    def inventory(names):
        output = {}
        for table, engine, ddl in names:
            rows = [] if engine == 'View' else client.query(f'SELECT * FROM {database}.{table}').result_rows
            encoded = json.dumps(sorted(json.dumps(row, default=str, sort_keys=True) for row in rows))
            output[table] = dict(ddl=ddl, rows=len(rows),
                content_hash=hashlib.sha256(encoded.encode()).hexdigest())
        return output
    try:
        client.command(f'CREATE DATABASE {database}')
        old_objects = []
        sentinel_counts = {}
        if populated_v2:
            for prefix in ('001_', '002_', '003_', '004_', '005_'):
                apply(next(migrations.glob(prefix + '*.sql')))
            old_objects = client.query('SELECT name,engine,create_table_query FROM system.tables '
                'WHERE database={db:String} ORDER BY name', parameters={'db': database}).result_rows
            for table, engine, _ in old_objects:
                if engine == 'View':
                    continue
                column = client.query('SELECT name FROM system.columns WHERE database={db:String} '
                    'AND table={table:String} AND type IN (\'String\',\'LowCardinality(String)\',\'Nullable(String)\') '
                    'ORDER BY position LIMIT 1', parameters={'db': database, 'table': table}).result_rows
                assert column, table + ' needs a textual sentinel column'
                prior_count = client.query(f'SELECT count() FROM {database}.{table}').result_rows[0][0]
                client.command(f'INSERT INTO {database}.{table} ({column[0][0]}) VALUES ({{marker:String}})',
                               parameters={'marker': 'preserve-v2-' + database})
                sentinel_counts[table] = prior_count + 1
        before = inventory(old_objects)
        for prefix in ('006_', '007_'):
            apply(next(migrations.glob(prefix + '*.sql')))
        selected = select_migrations(migrations, v1_silver=True)
        assert [path.name for path in selected] == ['008_create_silver_v1.sql']
        for _ in range(2):
            apply(selected[0])
        target = SilverClickHouseRepository(settings.model_copy(update={'clickhouse_database': database}), client=client)
        target.verify_v1_schema()
        after_objects = client.query('SELECT name,engine,create_table_query FROM system.tables '
            'WHERE database={db:String} AND name IN {names:Array(String)} ORDER BY name',
            parameters={'db': database, 'names': [row[0] for row in old_objects]}).result_rows if old_objects else []
        after = inventory(after_objects)
        assert before == after, 'V2 definitions or sentinel contents changed'
        if populated_v2:
            assert all(before[name]['rows'] == count for name, count in sentinel_counts.items())
        report = dict(database=database, populated_v2=populated_v2,
            silver_tables_verified=20, silver_migration_repeats=2, v2_before=before, v2_after=after)
        path = Path(settings.checkpoint_path).parent / 'acceptance' / (database + '.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    finally:
        repo.close()
