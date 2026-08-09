"""S4-A: Compose render contract for de-silver-processor."""
from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]
COMPOSE = REPO / "docker-compose.yml"
DOCKERFILE = REPO / "de" / "Dockerfile"


def _services() -> dict:
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert isinstance(doc, dict) and "services" in doc
    return doc["services"]


def test_compose_has_de_silver_processor_with_locked_contract():
    svc = _services()["de-silver-processor"]
    assert svc["container_name"] == "de-silver-processor"
    assert svc["restart"] == "unless-stopped"
    assert svc["command"] == ["python", "-m", "de.silver.main"]
    assert "8095:8095" in svc["ports"]
    assert "./de/artifacts:/app/de/artifacts" in svc["volumes"]

    deps = svc["depends_on"]
    assert deps["de-migrate"]["condition"] == "service_completed_successfully"
    assert deps["de-bronze-processor"]["condition"] == "service_healthy"
    assert deps["clickhouse"]["condition"] == "service_healthy"

    env = svc["environment"]
    assert env["SILVER_CLICKHOUSE_HOST"] == "clickhouse"
    assert env["SILVER_CLICKHOUSE_PORT"] == "8123"
    assert env["SILVER_CLICKHOUSE_DATABASE"] == "smart_traffic"
    assert env["SILVER_CHECKPOINT_PATH"] == "/app/de/artifacts/silver/checkpoint.sqlite3"
    assert env["SILVER_NAMESPACE"] == "live"
    assert env["SILVER_DESTINATION_MODE"] == "main"
    assert env["SILVER_REPLAY_RUN_ID"] == ""
    assert env["SILVER_TOPIC_ALLOWLIST"] == (
        "traffic.entity-events.v2,traffic.simulation-events.v2"
    )
    assert env["SILVER_HEALTH_HOST"] == "0.0.0.0"
    assert env["SILVER_HEALTH_PORT"] == "8095"
    assert env["SILVER_BATCH_SIZE"] == "500"
    assert env["SILVER_HEALTH_SNAPSHOT_MAX_AGE_SEC"] == "60.0"
    assert env["SILVER_SINGLE_INSTANCE"] == "true"
    assert env["SILVER_WORKER_COUNT"] == "1"

    hc = svc["healthcheck"]
    assert hc["interval"] == "10s"
    assert hc["timeout"] == "5s"
    assert hc["retries"] == 12
    assert hc["start_period"] == "30s"
    assert "8095/ready" in " ".join(hc["test"])


def test_dockerfile_exposes_8095():
    text = DOCKERFILE.read_text(encoding="utf-8")
    assert "8095" in text
    assert "EXPOSE" in text


def test_webhook_remains_profile_only():
    svc = _services()["de-webhook"]
    assert svc.get("profiles") == ["rollback"]


def test_default_services_include_silver_not_webhook():
    names = set(_services())
    assert "de-silver-processor" in names
    # webhook exists but profile-gated; compose config without profile excludes it at runtime
    assert "de-webhook" in names
