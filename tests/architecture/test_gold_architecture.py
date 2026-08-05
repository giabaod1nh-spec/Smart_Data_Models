"""Architecture enforcement for Gold 1 contracts and pure Gold 2 computation."""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GOLD_DIR = REPO / "de" / "gold"
REQUIRED_FILES = {
    "__init__.py", "contracts.py", "models.py", "input_models.py",
    "validation.py", "canonicalization.py", "deduplication.py", "windowing.py",
    "latest_selector.py", "quality.py", "lineage.py", "engine.py",
    "aggregators/__init__.py", "aggregators/traffic_window.py",
    "aggregators/intersection_window.py", "aggregators/signal_operation_window.py",
    "calculators/__init__.py", "calculators/comparison.py",
    "calculators/congestion.py", "calculators/priority.py",
    "calculators/explanation.py", "builders/__init__.py", "builders/facts.py",
    "builders/network.py",
}
FORBIDDEN_IMPORT_PREFIXES = (
    "confluent_kafka", "de.kafka_raw", "integration.orion", "de.webhook",
    "fastapi", "clickhouse_connect", "requests", "httpx", "sqlite3", "socket",
)
FORBIDDEN_MODULE_FRAGMENTS = ("processor", "reader", "repository", "runtime")


def _gold_files() -> list[Path]:
    return sorted(GOLD_DIR.rglob("*.py"))


def _relative(path: Path) -> str:
    return path.relative_to(GOLD_DIR).as_posix()


def test_gold_module_inventory_is_exact_and_execute_ready():
    assert GOLD_DIR.is_dir()
    assert {_relative(path) for path in _gold_files()} == REQUIRED_FILES


def test_gold_required_contracts_and_schema_exist():
    assert (REPO / "de" / "migrations" / "005_create_gold_m1.sql").is_file()
    assert (REPO / "docs" / "shared" / "SILVER_TO_GOLD_CONTRACT.md").is_file()


def test_gold_has_no_runtime_imports_modules_or_io_calls():
    hits: list[str] = []
    for path in _gold_files():
        assert not any(fragment in path.stem.lower() for fragment in FORBIDDEN_MODULE_FRAGMENTS)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                if any(module == banned or module.startswith(banned + ".") for banned in FORBIDDEN_IMPORT_PREFIXES):
                    hits.append(f"{_relative(path)}: import {module}")
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in {"open", "print", "input"}:
                    hits.append(f"{_relative(path)}: call {node.func.id}")
                if isinstance(node.func, ast.Attribute) and node.func.attr in {
                    "read_text", "write_text", "read_bytes", "write_bytes",
                    "connect", "execute", "executemany", "now", "utcnow",
                }:
                    hits.append(f"{_relative(path)}: call {node.func.attr}")
    assert hits == [], hits

