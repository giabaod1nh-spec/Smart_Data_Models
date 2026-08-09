from pathlib import Path


REPO = Path(__file__).resolve().parents[2]


def test_canonical_gold_startup_migration_is_nondestructive() -> None:
    sql = (REPO / "de" / "migrations" / "005_create_gold_m1.sql").read_text(
        encoding="utf-8"
    ).upper()

    assert "DROP TABLE" not in sql
    assert "TRUNCATE TABLE" not in sql
    assert "DROP VIEW" not in sql
