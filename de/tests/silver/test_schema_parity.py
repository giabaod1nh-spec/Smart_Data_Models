"""Schema parity: contract ↔ DDL ↔ models (Plan 1)."""
from __future__ import annotations

import re
from pathlib import Path

from de.silver.contracts import (
    ALL_SILVER_TABLES,
    CONTROL_TABLES,
    FORBIDDEN_SILVER_DERIVATIONS,
    LINEAGE_COLUMNS,
    MAIN_DIM_TABLES,
    MAIN_FACT_TABLES,
    REPLAY_TABLES,
    TABLE_COLUMNS,
    assert_no_forbidden_derivations,
)
from de.silver.models import (
    FACT_MODEL_BY_TABLE,
    LINEAGE_COLUMNS as MODEL_LINEAGE,
    model_field_names,
)

REPO = Path(__file__).resolve().parents[3]
DDL_PATH = REPO / "de" / "migrations" / "004_create_silver.sql"
CONTRACT_MD = REPO / "docs" / "shared" / "BRONZE_TO_SILVER_CONTRACT.md"


def _parse_ddl_tables(sql: str) -> dict[str, set[str]]:
    tables: dict[str, set[str]] = {}
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS smart_traffic\.(\w+)\s*\((.*?)\)\s*ENGINE",
        re.DOTALL | re.IGNORECASE,
    )
    for match in pattern.finditer(sql):
        name = match.group(1)
        body = match.group(2)
        cols: set[str] = set()
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            # first token is column name
            col = line.split()[0].strip("`")
            if col.upper() in {"PRIMARY", "INDEX", "CONSTRAINT"}:
                continue
            cols.add(col)
        tables[name] = cols
    return tables


def test_ddl_file_exists():
    assert DDL_PATH.is_file()
    assert CONTRACT_MD.is_file()


def test_exact_table_inventory_19():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl_tables = _parse_ddl_tables(sql)
    assert len(ALL_SILVER_TABLES) == 19
    assert len(MAIN_FACT_TABLES) == 5
    assert len(MAIN_DIM_TABLES) == 4
    assert len(CONTROL_TABLES) == 2
    assert len(REPLAY_TABLES) == 8
    assert set(ALL_SILVER_TABLES) == set(ddl_tables.keys())
    assert "silver_fact_camera_observation" in ddl_tables
    assert "silver_fact_camera_observation_replay" in ddl_tables


def test_contract_columns_exist_in_ddl():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl_tables = _parse_ddl_tables(sql)
    for table, cols in TABLE_COLUMNS.items():
        assert table in ddl_tables, table
        missing = cols - ddl_tables[table]
        assert not missing, f"{table} missing columns: {sorted(missing)}"


def test_no_forbidden_derivation_columns_in_ddl():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl_tables = _parse_ddl_tables(sql)
    for table, cols in ddl_tables.items():
        bad = FORBIDDEN_SILVER_DERIVATIONS.intersection(cols)
        assert not bad, f"{table} has forbidden columns: {sorted(bad)}"
        assert_no_forbidden_derivations(cols)


def test_lineage_on_all_fact_tables():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl_tables = _parse_ddl_tables(sql)
    for fact in MAIN_FACT_TABLES:
        for col in LINEAGE_COLUMNS:
            assert col in ddl_tables[fact], f"{fact}.{col}"
    assert tuple(MODEL_LINEAGE) == LINEAGE_COLUMNS


def test_replay_tables_have_replay_run_id():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl_tables = _parse_ddl_tables(sql)
    for table in REPLAY_TABLES:
        assert "replay_run_id" in ddl_tables[table], table


def test_fact_model_fields_subset_of_ddl_and_contract():
    sql = DDL_PATH.read_text(encoding="utf-8")
    ddl_tables = _parse_ddl_tables(sql)
    for table, model in FACT_MODEL_BY_TABLE.items():
        model_cols = model_field_names(model)
        assert model_cols <= ddl_tables[table], (
            f"{table} model extras: {sorted(model_cols - ddl_tables[table])}"
        )
        assert model_cols <= TABLE_COLUMNS[table], (
            f"{table} model not in contract map: "
            f"{sorted(model_cols - TABLE_COLUMNS[table])}"
        )


def test_contract_md_mentions_camera_and_no_weather_derivation():
    text = CONTRACT_MD.read_text(encoding="utf-8")
    assert "silver_fact_camera_observation" in text
    assert "no** `scenario_type` / `weather`" in text or "no `scenario_type`" in text.lower() or "Forbidden in Silver" in text
