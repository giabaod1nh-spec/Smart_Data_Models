"""Ownership matrix — single source for architecture conformance tests."""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Package roots (relative to repo)
PROJECTOR_PACKAGES = [
    REPO_ROOT / "Visualize" / "integration" / "projector",
]
KAFKA_PRODUCER_PACKAGES = [
    REPO_ROOT / "Visualize" / "integration" / "kafka",
]
RAW_CONSUMER_PACKAGES = [
    REPO_ROOT / "de" / "kafka_raw",
]
BRONZE_PACKAGES = [
    REPO_ROOT / "de" / "bronze",
]
WEBHOOK_PACKAGES = [
    REPO_ROOT / "de" / "webhook",
]
SERVER_POM = REPO_ROOT / "server" / "pom.xml"

PROJECTOR_FORBIDDEN_IMPORTS = (
    "de.kafka_raw",
    "de.bronze",
    "de.webhook",
    "clickhouse_connect",
    "clickhouse_driver",
)
RAW_FORBIDDEN_IMPORTS = (
    "integration.orion",
    "integration.projector",
    "de.webhook",
    "de.bronze",
    "com.traffic.server",
)
BRONZE_FORBIDDEN_IMPORTS = (
    "integration.orion",
    "integration.projector",
    "integration.kafka",
    "de.webhook",
    "de.kafka_raw",
    "confluent_kafka",
    "com.traffic.server",
)
KAFKA_PRODUCER_FORBIDDEN_IMPORTS = (
    "de.kafka_raw",
    "de.webhook",
    "de.bronze",
    "clickhouse_connect",
)
SERVER_FORBIDDEN_ARTIFACTS = (
    "kafka",
    "confluent",
    "clickhouse",
)

RAW_ALLOWED_TABLES = frozenset(
    {
        "kafka_raw_events",
        "kafka_quarantine_events",
        "kafka_raw_events_replay",
        "kafka_quarantine_events_replay",
    }
)
RAW_FORBIDDEN_TABLES = frozenset({"raw_ngsi_notifications"})

BRONZE_ALLOWED_TABLES = frozenset(
    {
        "kafka_raw_events",
        "kafka_quarantine_events",
        "bronze_entity_events",
        "bronze_run_events",
        "bronze_quarantine",
        "bronze_entity_events_replay",
        "bronze_run_events_replay",
        "bronze_quarantine_replay",
    }
)
BRONZE_FORBIDDEN_TABLES = frozenset(
    {
        "raw_ngsi_notifications",
        "kafka_raw_events_replay",
        "kafka_quarantine_events_replay",
    }
)

MAIN_TOPIC = "traffic.entity-events.v2"
EVENT_CONTRACT_VERSION = "2.0.0"
ENTITY_CONTRACT_VERSION = "1.0.0"

COMPOSE_BASE = REPO_ROOT / "docker-compose.yml"
COMPOSE_MIGRATION = REPO_ROOT / "deploy" / "migration" / "docker-compose.migration.yml"
COMPOSE_FINAL = REPO_ROOT / "deploy" / "archive" / "docker-compose.final.yml"
COMPOSE_K5_CUTOVER = REPO_ROOT / "deploy" / "archive" / "docker-compose.k5-cutover.yml"
COMPOSE_K6_DUAL = REPO_ROOT / "deploy" / "archive" / "docker-compose.k6-dual.yml"
PROFILE_MIGRATION_ENV = REPO_ROOT / "deploy" / "profiles" / "migration.env"
PROFILE_FINAL_ENV = REPO_ROOT / "deploy" / "profiles" / "final.env"
PROFILE_K5_CUTOVER_ENV = REPO_ROOT / "deploy" / "profiles" / "k5-cutover.env"
PROFILE_K6_DUAL_ENV = REPO_ROOT / "deploy" / "profiles" / "k6-dual.env"
PROFILE_K6_FINAL_ENV = REPO_ROOT / "deploy" / "profiles" / "k6-final.env"

K6_FINAL_ROOT_COMPOSE_ALLOWLIST = frozenset({"docker-compose.yml"})
