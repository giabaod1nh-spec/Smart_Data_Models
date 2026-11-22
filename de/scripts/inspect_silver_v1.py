"""Capture read-only Silver V1 runtime evidence; not an acceptance verdict."""
from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from de.silver.config import SilverSettings
from de.silver.repositories import SilverClickHouseRepository, V1_TABLES
from de.silver.source_binding import verify_bronze_source


def capture(settings: SilverSettings) -> dict:
    repo = SilverClickHouseRepository(settings)
    repo.connect()
    try:
        repo.verify_v1_schema()
        source = verify_bronze_source(Path(settings.v1_bronze_path), settings.v1_source_namespace)
        db = sqlite3.connect(Path(settings.checkpoint_path).resolve().as_uri() + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA query_only=ON')
            cursors = [dict(row) for row in db.execute('SELECT * FROM silver_v1_source_binding')]
            validity = [list(row) for row in db.execute(
                'SELECT simulation_run_id,state,count(*) FROM silver_v1_event_validity GROUP BY 1,2')]
            obligations = [list(row) for row in db.execute(
                'SELECT status,count(*) FROM silver_v1_obligation GROUP BY status')]
            lifecycle_runs = [dict(row) for row in db.execute(
                'SELECT run_id,last_polled_at FROM silver_known_runs_v1 ORDER BY run_id')]
            poll_state = db.execute('SELECT * FROM silver_lifecycle_poll_state_v1 WHERE singleton=1').fetchone()
            lifecycle_poll = dict(poll_state) if poll_state else None
        finally:
            db.close()
        counts = {}
        for table in sorted(V1_TABLES):
            counts[table] = repo.client.command(f'SELECT count() FROM {repo.database}.{table}')
        runs = repo.client.query(f'SELECT simulation_run_id,count(),uniqExact(event_id) '
            f'FROM {repo.database}.silver_intersection_state_fact_v1 GROUP BY simulation_run_id').result_rows
        with urllib.request.urlopen(f'http://localhost:{settings.health_port}/health', timeout=5) as response:
            health = json.load(response)
        return dict(observed_at=datetime.now(timezone.utc).isoformat(), source=source,
                    cursors=cursors, validity=validity, obligations=obligations,
                    lifecycle_runs=lifecycle_runs, lifecycle_poll=lifecycle_poll,
                    physical_counts=counts, intersection_runs=runs, health=health)
    finally:
        repo.close()


if __name__ == '__main__':
    report = capture(SilverSettings())
    target = Path(SilverSettings().checkpoint_path).parent / 'acceptance' / 'runtime-latest.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
    with (target.parent / 'runtime-samples.jsonl').open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(report, default=str) + '\n')
    print(json.dumps(report, default=str))
