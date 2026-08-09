"""Gold 1 schema, contract, and model parity."""
from __future__ import annotations

import re
from pathlib import Path

from de.gold.contracts import (
    ALL_GOLD_TABLES,
    FORBIDDEN_GOLD_MEASURE_NAMES,
    FORBIDDEN_MART_NAME_FRAGMENTS,
    MAIN_FACT_TABLES,
    MART_VIEWS,
    TABLE_COLUMNS,
    assert_no_realtime_mart_names,
)
from de.gold.models import FACT_MODEL_BY_TABLE, model_field_names

REPO = Path(__file__).resolve().parents[3]
DDL_PATH = REPO / "de" / "migrations" / "005_create_gold_m1.sql"


def _parse_tables(sql: str) -> dict[str, str]:
    return {
        match.group(1): match.group(2)
        for match in re.finditer(
            r"CREATE TABLE IF NOT EXISTS smart_traffic\.(\w+)\s*\((.*?)\)\s*ENGINE",
            sql, re.DOTALL | re.IGNORECASE,
        )
    }


def _columns(body: str, expected: tuple[str, ...]) -> set[str]:
    return {
        name for name in expected
        if re.search(rf"(?:\A|[,(])\s*{re.escape(name)}\s+", body)
    }


def test_ddl_inventory_and_contract_columns_match():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl = _parse_tables(sql)
    assert set(ddl) == set(ALL_GOLD_TABLES)
    for table, expected in TABLE_COLUMNS.items():
        assert _columns(ddl[table], expected) == set(expected), table


def test_fact_models_match_contract_and_ddl():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl = _parse_tables(sql)
    assert set(FACT_MODEL_BY_TABLE) == set(MAIN_FACT_TABLES)
    for table, model in FACT_MODEL_BY_TABLE.items():
        fields = model_field_names(model)
        assert fields == set(TABLE_COLUMNS[table])
        assert fields == _columns(ddl[table], TABLE_COLUMNS[table])


def test_marts_are_windowed_and_not_realtime_named():
    sql = DDL_PATH.read_text(encoding="utf-8")
    assert len(MART_VIEWS) == 6
    assert_no_realtime_mart_names()
    for mart in MART_VIEWS:
        assert f"CREATE VIEW IF NOT EXISTS smart_traffic.{mart}" in sql
        assert not any(fragment in mart for fragment in FORBIDDEN_MART_NAME_FRAGMENTS)
    assert "definition_major" in sql
    assert "revision_seq" in sql
    assert "source_direction" in sql
    assert "direction_mapping_version" in sql


def test_traffic_window_uses_explicit_vehicle_aggregates():
    traffic = TABLE_COLUMNS["gold_fact_traffic_window"]
    assert "avg_vehicle_count" in traffic
    assert "vehicle_count" not in traffic
    assert "vehicle_count" in FORBIDDEN_GOLD_MEASURE_NAMES
