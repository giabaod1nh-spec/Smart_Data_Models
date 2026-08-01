from __future__ import annotations

import json
import shutil
import subprocess

import pytest

from arch_utils import read_text
from ownership_matrix import COMPOSE_BASE, REPO_ROOT


EXPECTED_DEFAULT_SERVICES = {
    "mongo-db",
    "orion",
    "context-server",
    "clickhouse",
    "de-migrate",
    "de-kafka-raw-consumer",
    "de-bronze-processor",
    "kafka",
    "kafka-init",
    "orion-projector",
}


def test_default_source_has_no_raw_v1_runtime_path():
    text = read_text(COMPOSE_BASE)
    assert "raw_ngsi_notifications" not in text
    assert '"--all"' not in text
    assert '"--historical-v2"' in text
    assert "profiles: [\"rollback\"]" in text


def test_operator_tools_have_no_raw_v1_replay_entrypoint():
    tools = REPO_ROOT / "de" / "tools"
    hits = []
    for path in tools.glob("*.py"):
        text = read_text(path)
        if "raw_ngsi_notifications" in text or "de.webhook.scripts.replay_raw" in text:
            hits.append(str(path.relative_to(REPO_ROOT)))
    assert hits == []


def test_final_manifest_json_matches_static_service_contract():
    path = REPO_ROOT / "docs" / "architecture" / "final_runtime_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    assert set(manifest["canonical_containerized_services"]) == EXPECTED_DEFAULT_SERVICES
    assert manifest["default_webhook_created"] is False
    assert manifest["default_migration_mode"] == "historical-v2"
    assert manifest["k6b_full_pass"] is False


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI unavailable")
def test_default_compose_render_is_exact_kafka_runtime():
    result = subprocess.run(
        ["docker", "compose", "config", "--services"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    services = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    assert services == EXPECTED_DEFAULT_SERVICES
    assert "de-webhook" not in services


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI unavailable")
def test_rollback_compose_render_has_full_activation_contract():
    services_result = subprocess.run(
        ["docker", "compose", "--profile", "rollback", "config", "--services"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    services = {
        line.strip() for line in services_result.stdout.splitlines() if line.strip()
    }
    assert services == EXPECTED_DEFAULT_SERVICES | {"de-webhook"}

    render = subprocess.run(
        ["docker", "compose", "--profile", "rollback", "config"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    assert "DE_WEBHOOK_ENABLED: \"true\"" in render or "DE_WEBHOOK_ENABLED: 'true'" in render
    assert "DE_WEBHOOK_MODE: ROLLBACK_ONLY" in render
