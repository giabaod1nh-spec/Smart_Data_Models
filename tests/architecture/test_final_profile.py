from __future__ import annotations

import pytest

from contracts.architecture_profiles import (
    COMPONENT_PRODUCER,
    COMPONENT_PROJECTOR,
    ProfileValidationError,
    validate_env,
)
from arch_utils import load_dotenv
from ownership_matrix import (
    PROFILE_FINAL_ENV,
    PROFILE_K6_DUAL_ENV,
    PROFILE_K6_FINAL_ENV,
    PROFILE_MIGRATION_ENV,
)


def test_migration_profile_env_valid():
    env = load_dotenv(PROFILE_MIGRATION_ENV)
    flags = validate_env("migration", env)
    assert flags.orion_publish_enabled is True
    assert flags.kafka_outbox_enabled is True
    assert flags.de_webhook_enabled is True
    assert flags.projector_shadow_mode is True
    assert flags.orion_sync_publish is False


def test_final_profile_env_valid_for_lock_smoke():
    env = load_dotenv(PROFILE_FINAL_ENV)
    flags = validate_env("final", env)
    assert flags.orion_publish_enabled is False
    assert flags.kafka_outbox_enabled is True
    assert flags.de_webhook_enabled is False
    assert flags.projector_target_namespace == "test"
    assert flags.architecture_lock_smoke is True
    assert flags.orion_sync_publish is False


def test_final_rejects_webhook_enabled():
    env = load_dotenv(PROFILE_FINAL_ENV)
    env["DE_WEBHOOK_ENABLED"] = "true"
    with pytest.raises(ProfileValidationError, match="DE_WEBHOOK_ENABLED"):
        validate_env("final", env)


def test_final_lock_smoke_rejects_production_namespace():
    env = load_dotenv(PROFILE_FINAL_ENV)
    env["PROJECTOR_TARGET_NAMESPACE"] = "production"
    with pytest.raises(ProfileValidationError, match="production"):
        validate_env("final", env)


def test_migration_rejects_sync_publish():
    env = load_dotenv(PROFILE_MIGRATION_ENV)
    env["ORION_SYNC_PUBLISH"] = "true"
    with pytest.raises(ProfileValidationError, match="SYNC"):
        validate_env("migration", env)


def test_final_rejects_sync_publish():
    env = load_dotenv(PROFILE_FINAL_ENV)
    env["ORION_SYNC_PUBLISH"] = "true"
    with pytest.raises(ProfileValidationError, match="SYNC"):
        validate_env("final", env)


def test_final_rejects_outbox_off():
    env = load_dotenv(PROFILE_FINAL_ENV)
    env["KAFKA_OUTBOX_ENABLED"] = "false"
    with pytest.raises(ProfileValidationError, match="KAFKA_OUTBOX"):
        validate_env("final", env)


def test_projector_component_ignores_producer_and_webhook_flags():
    """Projector container env has no outbox/webhook flags — must not fail on them."""
    env = {
        "PROJECTOR_SHADOW_MODE": "true",
        "PROJECTOR_TARGET_NAMESPACE": "shadow",
    }
    flags = validate_env("migration", env, COMPONENT_PROJECTOR)
    assert flags.projector_target_namespace == "shadow"


def test_projector_component_still_enforces_namespace_under_lock_smoke():
    env = {
        "PROJECTOR_SHADOW_MODE": "true",
        "PROJECTOR_TARGET_NAMESPACE": "production",
        "ARCHITECTURE_LOCK_SMOKE": "true",
    }
    with pytest.raises(ProfileValidationError, match="production"):
        validate_env("final", env, COMPONENT_PROJECTOR)


def test_producer_component_ignores_webhook_flag():
    env = {
        "ORION_PUBLISH_ENABLED": "false",
        "KAFKA_OUTBOX_ENABLED": "true",
        "ORION_SYNC_PUBLISH": "false",
    }
    flags = validate_env("final", env, COMPONENT_PRODUCER)
    assert flags.kafka_outbox_enabled is True


def test_producer_component_still_enforces_sync_ban():
    env = {
        "ORION_PUBLISH_ENABLED": "true",
        "KAFKA_OUTBOX_ENABLED": "true",
        "ORION_SYNC_PUBLISH": "true",
    }
    with pytest.raises(ProfileValidationError, match="SYNC"):
        validate_env("migration", env, COMPONENT_PRODUCER)


def test_k6_dual_profile_env_valid():
    env = load_dotenv(PROFILE_K6_DUAL_ENV)
    flags = validate_env("k6-dual", env)
    assert flags.orion_publish_enabled is False
    assert flags.kafka_outbox_enabled is True
    assert flags.projector_shadow_mode is False
    assert flags.projector_target_namespace == "production"
    assert flags.de_webhook_enabled is True
    assert flags.raw_consumer_enabled is True
    assert flags.bronze_enabled is True


def test_k6_final_profile_env_valid():
    env = load_dotenv(PROFILE_K6_FINAL_ENV)
    flags = validate_env("k6-final", env)
    assert flags.orion_publish_enabled is False
    assert flags.kafka_outbox_enabled is True
    assert flags.projector_shadow_mode is False
    assert flags.projector_target_namespace == "production"
    assert flags.de_webhook_enabled is False
    assert flags.raw_consumer_enabled is True
    assert flags.bronze_enabled is True


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("DE_WEBHOOK_ENABLED", "true"),
        ("RAW_CONSUMER_ENABLED", "false"),
        ("BRONZE_ENABLED", "false"),
        ("PROJECTOR_SHADOW_MODE", "true"),
        ("PROJECTOR_TARGET_NAMESPACE", "shadow"),
        ("ORION_PUBLISH_ENABLED", "true"),
    ],
)
def test_k6_final_rejects_unsafe_flags(key, value):
    env = load_dotenv(PROFILE_K6_FINAL_ENV)
    env[key] = value
    with pytest.raises(ProfileValidationError):
        validate_env("k6-final", env)


@pytest.mark.parametrize(
    "key", ["DE_WEBHOOK_ENABLED", "RAW_CONSUMER_ENABLED", "BRONZE_ENABLED"]
)
def test_k6_dual_rejects_required_historical_service_off(key):
    env = load_dotenv(PROFILE_K6_DUAL_ENV)
    env[key] = "false"
    with pytest.raises(ProfileValidationError, match=key):
        validate_env("k6-dual", env)
