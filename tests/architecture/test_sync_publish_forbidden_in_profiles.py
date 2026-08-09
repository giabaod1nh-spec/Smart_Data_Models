from __future__ import annotations

import pytest

from contracts.architecture_profiles import ProfileValidationError, validate_env
from arch_utils import load_dotenv
from ownership_matrix import PROFILE_FINAL_ENV, PROFILE_MIGRATION_ENV


@pytest.mark.parametrize("profile_env", [PROFILE_MIGRATION_ENV, PROFILE_FINAL_ENV])
def test_locked_profiles_forbid_sync_publish(profile_env):
    env = load_dotenv(profile_env)
    env["ORION_SYNC_PUBLISH"] = "true"
    with pytest.raises(ProfileValidationError):
        validate_env(env["ARCHITECTURE_PROFILE"], env)


def test_debug_profile_allows_sync():
    # unlocked profile name skips sync ban
    flags = validate_env(
        "debug",
        {
            "ORION_SYNC_PUBLISH": "true",
            "KAFKA_OUTBOX_ENABLED": "false",
            "PROJECTOR_TARGET_NAMESPACE": "shadow",
        },
    )
    assert flags.orion_sync_publish is True
