"""Architecture enforcement for the isolated Gold 3 runtime package."""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNTIME_DIR = REPO / "de" / "gold_runtime"
REQUIRED_MODULES = {
    "__init__.py",
    "config.py",
    "silver_readers.py",
    "cursor.py",
    "window_scheduler.py",
    "context_builder.py",
    "dimensions.py",
    "repositories.py",
    "processing_ledger.py",
    "checkpoint_store.py",
    "revisions.py",
    "replay.py",
    "instance_lock.py",
    "processor.py",
    "health_api.py",
    "metrics.py",
    "main.py",
}
FORBIDDEN_IMPORT_PREFIXES = (
    "confluent_kafka",
    "integration.orion",
    "de.webhook",
    "de.kafka_raw",
)
# Gold2 private modules must not be imported by runtime.
FORBIDDEN_GOLD_PRIVATES = (
    "de.gold.calculators",
    "de.gold.aggregators",
    "de.gold.builders",
)


def _runtime_files() -> list[Path]:
    return sorted(RUNTIME_DIR.glob("*.py"))


def test_gold_runtime_module_inventory_matches_plan3():
    assert RUNTIME_DIR.is_dir()
    assert {path.name for path in _runtime_files()} == REQUIRED_MODULES


def test_gold_runtime_forbids_kafka_orion_webhook_and_private_gold2():
    hits: list[str] = []
    for path in _runtime_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                if any(
                    module == banned or module.startswith(banned + ".")
                    for banned in FORBIDDEN_IMPORT_PREFIXES + FORBIDDEN_GOLD_PRIVATES
                ):
                    hits.append(f"{path.name}: import {module}")
    assert hits == [], hits


def test_processor_only_references_public_engine_api():
    text = (RUNTIME_DIR / "processor.py").read_text(encoding="utf-8")
    assert "GoldTransformationEngine" in text
    assert ".transform(" in text
    assert "from de.gold.calculators" not in text
    assert "from de.gold.aggregators" not in text
