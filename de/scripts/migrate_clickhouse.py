"""Dedicated ClickHouse migrate for K-4 (run before consumer)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.kafka_raw.clickhouse_repository import run_migration_file  # noqa: E402
from de.kafka_raw.config import get_settings  # noqa: E402

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("de.migrate")


def select_migrations(
    migrations_dir: Path,
    *,
    apply_all: bool = False,
    historical_v2: bool = False,
    migration: str | None = None,
) -> list[Path]:
    """Resolve the migration set without connecting to ClickHouse."""
    if apply_all:
        return sorted(migrations_dir.glob("*.sql"))
    if historical_v2:
        return sorted(
            f
            for f in migrations_dir.glob("*.sql")
            if f.name[:3].isdigit() and int(f.name[:3]) >= 2
        )
    if migration:
        return [Path(migration)]
    files: list[Path] = []
    f001 = next(iter(sorted(migrations_dir.glob("001_*.sql"))), None)
    f002 = migrations_dir / "002_create_kafka_raw_events.sql"
    f003 = migrations_dir / "003_create_bronze_v2.sql"
    if f001 is not None and f001.is_file():
        files.append(f001)
    files.append(f002)
    if f003.is_file():
        files.append(f003)
    return files


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--migration", default=None, help="Path to SQL migration file")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--all",
        action="store_true",
        help="Also apply 001 webhook raw if present",
    )
    mode.add_argument(
        "--historical-v2",
        action="store_true",
        help="Apply only Kafka Raw v2 and Bronze migrations (002+); never migration 001",
    )
    args = p.parse_args()
    settings = get_settings()
    migrations_dir = _REPO / "de" / "migrations"
    files = select_migrations(
        migrations_dir,
        apply_all=args.all,
        historical_v2=args.historical_v2,
        migration=args.migration,
    )
    for f in files:
        log.info("applying %s", f)
        run_migration_file(settings, f)
    log.info("migrate done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
