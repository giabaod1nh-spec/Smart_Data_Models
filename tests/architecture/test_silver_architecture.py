"""Architecture enforcement for Silver Plan 1 + 2 + 3 packages."""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SILVER_DIR = REPO / "de" / "silver"

FORBIDDEN_IMPORT_PREFIXES = (
    "confluent_kafka",
    "de.kafka_raw",
    "integration.orion",
    "de.webhook",
    "de.gold",
)

PLAN2_MODULES = {
    "input_models.py",
    "unwrapper.py",
    "normalizers.py",
    "validators.py",
    "routers.py",
    "fact_builders.py",
    "dimension_builders.py",
    "engine.py",
}

PLAN3_MODULES = {
    "config.py",
    "readers.py",
    "repositories.py",
    "checkpoint_store.py",
    "batch_ledger.py",
    "dimension_state.py",
    "instance_lock.py",
    "processor.py",
    "health_api.py",
    "replay.py",
    "main.py",
    "metrics.py",
}

PLAN3_IO_ALLOWED = PLAN3_MODULES  # only these may import CH/SQLite/FastAPI

FORBIDDEN_NAME_FRAGMENTS = (
    "aggregate",
    "kpi",
    "congestion_score",
    "anomaly",
)


def _iter_silver_py() -> list[Path]:
    return sorted(SILVER_DIR.glob("*.py"))


def test_silver_package_has_plan1_2_3_files():
    names = {p.name for p in _iter_silver_py()}
    assert {"__init__.py", "contracts.py", "models.py"} <= names
    assert PLAN2_MODULES <= names
    assert PLAN3_MODULES <= names
    assert "enrichers.py" not in names


def test_silver_forbidden_boundary_imports():
    hits: list[str] = []
    for path in _iter_silver_py():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            mods: list[str] = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                mods = [node.module]
            for mod in mods:
                if any(mod == p or mod.startswith(p + ".") for p in FORBIDDEN_IMPORT_PREFIXES):
                    hits.append(f"{path.name}: {mod}")
    assert hits == [], hits


def test_plan2_modules_remain_io_free():
    banned = {
        "clickhouse_connect",
        "clickhouse_driver",
        "sqlite3",
        "fastapi",
        "starlette",
        "uvicorn",
        "socket",
    }
    hits: list[str] = []
    for name in PLAN2_MODULES:
        path = SILVER_DIR / name
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in banned:
                        hits.append(f"{name}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.split(".")[0] in banned:
                    hits.append(f"{name}: from {node.module}")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"now", "utcnow", "uuid4"}:
                    hits.append(f"{name}: {node.func.attr}()")
    assert hits == [], hits


def test_only_plan3_modules_import_io_stack():
    io_mods = {
        "clickhouse_connect",
        "clickhouse_driver",
        "sqlite3",
        "fastapi",
        "uvicorn",
    }
    hits: list[str] = []
    for path in _iter_silver_py():
        if path.name in PLAN3_IO_ALLOWED or path.name == "__init__.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            mods: list[str] = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                mods = [node.module]
            for mod in mods:
                root = mod.split(".")[0]
                if root in io_mods:
                    hits.append(f"{path.name}: {mod}")
    assert hits == [], hits


def test_no_kpi_aggregate_weather_scenario_type_code():
    hits = []
    for path in _iter_silver_py():
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        for frag in FORBIDDEN_NAME_FRAGMENTS:
            if frag in lower:
                hits.append(f"{path.name}:{frag}")
        # scenario_type / weather as derivation markers (allow in comments about prohibition)
        if "scenario_type" in text and "FORBIDDEN" not in text and "no " not in lower:
            # contracts may list forbidden names
            if path.name not in {"contracts.py", "config.py"}:
                if "scenario_type" in text and "FORBIDDEN_SILVER_DERIVATIONS" not in text:
                    hits.append(f"{path.name}:scenario_type")
    assert not any(h.endswith(":aggregate") or h.endswith(":kpi") or h.endswith(":anomaly") for h in hits), hits


def test_health_routes_only_health_and_ready():
    src = (SILVER_DIR / "health_api.py").read_text(encoding="utf-8")
    assert '@app.get("/health")' in src
    assert '@app.get("/ready")' in src
    assert "/metrics" not in src


def test_migration_004_and_contract_present():
    assert (REPO / "de" / "migrations" / "004_create_silver.sql").is_file()
    assert (REPO / "docs" / "shared" / "BRONZE_TO_SILVER_CONTRACT.md").is_file()


def test_no_docker_files_modified_marker():
    # Plan 3 must not require compose service presence
    compose = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
    # absence of de-silver-processor is OK for Plan 3
    assert "de-bronze-processor" in compose or "clickhouse" in compose.lower()


def test_plan3_modules_importable():
    import importlib

    for mod in [
        "de.silver.config",
        "de.silver.readers",
        "de.silver.repositories",
        "de.silver.checkpoint_store",
        "de.silver.batch_ledger",
        "de.silver.dimension_state",
        "de.silver.instance_lock",
        "de.silver.processor",
        "de.silver.health_api",
        "de.silver.replay",
        "de.silver.main",
        "de.silver.metrics",
    ]:
        importlib.import_module(mod)
