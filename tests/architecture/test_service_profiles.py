from __future__ import annotations

from arch_utils import read_text, service_has_excluding_profile, yaml_service_names
from ownership_matrix import (
    COMPOSE_BASE,
    COMPOSE_FINAL,
    COMPOSE_K5_CUTOVER,
    COMPOSE_K6_DUAL,
    COMPOSE_MIGRATION,
    K6_FINAL_ROOT_COMPOSE_ALLOWLIST,
    PROFILE_K6_FINAL_ENV,
    REPO_ROOT,
)


def test_canonical_base_is_kafka_only_default_runtime():
    text = read_text(COMPOSE_BASE)
    names = yaml_service_names(text)
    assert {
        "kafka",
        "kafka-init",
        "orion",
        "orion-projector",
        "de-kafka-raw-consumer",
        "de-bronze-processor",
    } <= names
    assert service_has_excluding_profile(text, "de-webhook")
    assert "profiles: [\"rollback\"]" in text
    assert "PROJECTOR_TARGET_NAMESPACE: production" in text
    assert "PROJECTOR_GROUP_ID: projector-k5-production" in text
    assert "PROJECTOR_WRITE_MODE: active" in text
    assert "ARCHITECTURE_PROFILE: k6-final" in text


def test_default_migration_is_gold_m1_chain():
    text = read_text(COMPOSE_BASE)
    assert '"--gold-m1"' in text
    assert '"--all"' not in text



def test_rollback_webhook_render_contract_is_complete_in_source():
    text = read_text(COMPOSE_BASE)
    assert "DE_WEBHOOK_ENABLED: \"true\"" in text
    assert "DE_WEBHOOK_MODE: ROLLBACK_ONLY" in text
    assert "ROLLBACK_RAW_V1_CLICKHOUSE_USER" in text
    assert "ROLLBACK_RAW_V1_CLICKHOUSE_PASSWORD" in text


def test_k6_final_env_locks_default_webhook_off():
    env = read_text(PROFILE_K6_FINAL_ENV)
    assert "ARCHITECTURE_PROFILE=k6-final" in env
    assert "DE_WEBHOOK_ENABLED=false" in env
    assert "RAW_CONSUMER_ENABLED=true" in env
    assert "BRONZE_ENABLED=true" in env
    assert "PROJECTOR_TARGET_NAMESPACE=production" in env
    assert "PROJECTOR_GROUP_ID=projector-k5-production" in env


def test_top_level_compose_allowlist_is_exactly_one():
    actual = {path.name for path in REPO_ROOT.glob("docker-compose*.yml")}
    assert actual == K6_FINAL_ROOT_COMPOSE_ALLOWLIST


def test_non_default_compose_files_are_outside_root():
    assert COMPOSE_MIGRATION.is_file()
    assert COMPOSE_FINAL.is_file()
    assert COMPOSE_K5_CUTOVER.is_file()
    assert COMPOSE_K6_DUAL.is_file()
    for path in (COMPOSE_MIGRATION, COMPOSE_FINAL, COMPOSE_K5_CUTOVER, COMPOSE_K6_DUAL):
        assert path.parent != REPO_ROOT


def test_archived_k5_and_k6_profiles_preserve_evidence_topology():
    k5 = read_text(COMPOSE_K5_CUTOVER)
    dual = read_text(COMPOSE_K6_DUAL)
    assert "PROJECTOR_TARGET_NAMESPACE: production" in k5
    assert "DE_WEBHOOK_ENABLED: \"true\"" in k5
    assert "ARCHITECTURE_PROFILE: k6-dual" in dual
    assert "DE_WEBHOOK_ENABLED: \"true\"" in dual
