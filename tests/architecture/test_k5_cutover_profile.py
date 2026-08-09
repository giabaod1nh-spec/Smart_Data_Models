from __future__ import annotations

import json
from pathlib import Path

import pytest

from contracts.architecture_profiles import (
    COMPONENT_PRODUCER,
    COMPONENT_PROJECTOR,
    COMPONENT_STACK,
    ProfileValidationError,
    validate_env,
)


def _load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def test_k5_cutover_profile_env_valid():
    env = _load_env(Path("deploy/profiles/k5-cutover.env"))
    flags = validate_env("k5-cutover", env, COMPONENT_STACK)
    assert flags.orion_publish_enabled is False
    assert flags.kafka_outbox_enabled is True
    assert flags.de_webhook_enabled is True
    assert flags.projector_shadow_mode is False
    assert flags.projector_target_namespace == "production"


def test_k5_cutover_rejects_direct_orion():
    env = _load_env(Path("deploy/profiles/k5-cutover.env"))
    env["ORION_PUBLISH_ENABLED"] = "true"
    with pytest.raises(ProfileValidationError, match="ORION_PUBLISH_ENABLED"):
        validate_env("k5-cutover", env, COMPONENT_PRODUCER)


def test_k5_cutover_rejects_webhook_off():
    env = _load_env(Path("deploy/profiles/k5-cutover.env"))
    env["DE_WEBHOOK_ENABLED"] = "false"
    with pytest.raises(ProfileValidationError, match="DE_WEBHOOK_ENABLED"):
        validate_env("k5-cutover", env, COMPONENT_STACK)


def test_k5_cutover_projector_requires_production_no_shadow():
    env = {
        "PROJECTOR_SHADOW_MODE": "true",
        "PROJECTOR_TARGET_NAMESPACE": "production",
    }
    with pytest.raises(ProfileValidationError, match="SHADOW_MODE"):
        validate_env("k5-cutover", env, COMPONENT_PROJECTOR)
