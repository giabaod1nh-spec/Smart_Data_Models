"""Gold M1 migration-profile isolation."""
from __future__ import annotations

from pathlib import Path

from de.scripts.migrate_clickhouse import select_migrations

REPO = Path(__file__).resolve().parents[3]
MIGRATIONS = REPO / "de" / "migrations"


def test_historical_v2_stops_at_silver():
    files = select_migrations(MIGRATIONS, historical_v2=True)
    assert [path.name for path in files] == [
        "002_create_kafka_raw_events.sql",
        "003_create_bronze_v2.sql",
        "004_create_silver.sql",
    ]


def test_gold_m1_selects_explicit_chain():
    files = select_migrations(MIGRATIONS, gold_m1=True)
    assert [path.name for path in files] == [
        "002_create_kafka_raw_events.sql",
        "003_create_bronze_v2.sql",
        "004_create_silver.sql",
        "005_create_gold_m1.sql",
    ]


def test_default_profile_excludes_gold_m1():
    assert "005_create_gold_m1.sql" not in {
        path.name for path in select_migrations(MIGRATIONS)
    }
