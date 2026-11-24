"""Gold4 Compose / package guards for de-gold-runtime (Model A platform)."""
from __future__ import annotations

import ast
from pathlib import Path

from arch_utils import read_text
from ownership_matrix import COMPOSE_BASE, REPO_ROOT

RUNTIME_DIR = REPO_ROOT / "de" / "gold_runtime"


def test_compose_defines_de_gold_runtime_contract():
    text = read_text(COMPOSE_BASE)
    assert "de-gold-runtime:" in text
    assert "python -m de.gold_runtime.main" in text or '"de.gold_runtime.main"' in text
    assert "8096:8096" in text
    gold = text.split("  de-gold-runtime:", 1)[1].split("  kafka:", 1)[0]
    assert 'profiles: ["v1-gold"]' in gold
    assert 'restart: "no"' in gold
    assert "GOLD_CHECKPOINT_PATH: /app/de/artifacts/gold_v1/checkpoint.sqlite3" in gold
    assert "GOLD_INSTANCE_LOCK_PATH: /app/de/artifacts/gold_v1/instance.lock" in gold
    assert "GOLD_SOURCE_CLICKHOUSE_USER" in gold
    assert "/app/de/artifacts/silver_v1:ro" in gold
    assert "/app/de/artifacts/bronze_v1:ro" in gold
    assert "GOLD_TRAFFIC_EXPECTED_CADENCE_SEC" not in gold
    assert "GOLD_ALLOW" not in gold
    assert "localhost:8096/ready" in text
    assert "de-silver-processor:" in text
    # Preserve the existing selector. Core depends only on Silver for source DDL.
    assert '"--gold-m1"' in text
    assert "de-migrate:" not in gold
    assert "migrate_clickhouse" in text


def test_dockerfile_exposes_gold_health_port():
    text = read_text(REPO_ROOT / "de" / "Dockerfile")
    assert "8096" in text
    assert "COPY de/ de/" in text


def test_gold_runtime_package_does_not_import_kafka_or_orion():
    hits: list[str] = []
    banned = ("confluent_kafka", "integration.orion", "de.webhook", "de.kafka_raw")
    for path in sorted(RUNTIME_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                if any(module == b or module.startswith(b + ".") for b in banned):
                    hits.append(f"{path.name}:{module}")
    assert hits == []
