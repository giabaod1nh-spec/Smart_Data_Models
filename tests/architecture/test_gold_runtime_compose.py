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
    assert "GOLD_NAMESPACE: live" in text
    assert "GOLD_CHECKPOINT_PATH: /app/de/artifacts/gold/checkpoint.sqlite3" in text
    assert "GOLD_INSTANCE_LOCK_PATH: /app/de/artifacts/gold/instance.lock" in text
    assert "GOLD_TRAFFIC_EXPECTED_CADENCE_SEC" in text
    assert "GOLD_INTERSECTION_EXPECTED_CADENCE_SEC" in text
    assert "GOLD_SIGNAL_EXPECTED_CADENCE_SEC" in text
    assert "localhost:8096/ready" in text
    assert "de-silver-processor:" in text
    # Migration owner is --gold-m1; Gold never migrates at startup.
    assert '"--gold-m1"' in text
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
