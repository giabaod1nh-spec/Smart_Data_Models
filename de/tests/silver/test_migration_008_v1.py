from pathlib import Path

from de.scripts.migrate_clickhouse import select_migrations


def test_silver_migration_is_additive_and_profile_is_exclusive():
    migrations = Path(__file__).resolve().parents[2] / "migrations"
    selected = select_migrations(migrations, v1_silver=True)
    assert [path.name for path in selected] == ["008_create_silver_v1.sql"]
    sql = selected[0].read_text()
    assert sql.count("CREATE TABLE IF NOT EXISTS smart_traffic.silver_") == 20
    for name in ("traffic_observation_fact", "signal_state_fact", "intersection_state_fact",
                 "run_dimension", "topology_dimension", "run_topology_binding",
                 "run_lifecycle", "event_validity", "invalidation", "quarantine"):
        assert f"silver_{name}_v1" in sql
        assert f"silver_{name}_replay_v1" in sql
    assert "DROP " not in sql and "TRUNCATE " not in sql and "ALTER TABLE" not in sql
