"""Architecture Lock deployment profile validation (migration vs final).

Shared by TraCI startup, projector entrypoint, and tests/architecture.
Does not flip runtime defaults — callers opt into a named profile.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

ALLOWED_NAMESPACES = frozenset({"shadow", "test", "production"})
LOCKED_PROFILES = frozenset(
    {"migration", "final", "k5-cutover", "k6-dual", "k6-final"}
)

# Validation scope: a single container only owns a subset of the stack flags,
# so component-scoped calls must not assert flags owned by other services.
COMPONENT_STACK = "stack"
COMPONENT_PRODUCER = "producer"
COMPONENT_PROJECTOR = "projector"
COMPONENTS = frozenset({COMPONENT_STACK, COMPONENT_PRODUCER, COMPONENT_PROJECTOR})


class ProfileValidationError(ValueError):
    """Unsafe or forbidden profile combination."""


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class ProfileFlags:
    profile: str
    orion_publish_enabled: bool
    kafka_outbox_enabled: bool
    projector_shadow_mode: bool
    projector_target_namespace: str
    de_webhook_enabled: bool
    raw_consumer_enabled: bool
    bronze_enabled: bool
    orion_sync_publish: bool
    architecture_lock_smoke: bool = False

    @classmethod
    def from_mapping(cls, profile: str, env: Mapping[str, Any]) -> "ProfileFlags":
        ns = str(env.get("PROJECTOR_TARGET_NAMESPACE", "shadow")).strip().lower() or "shadow"
        return cls(
            profile=profile.strip().lower(),
            orion_publish_enabled=_as_bool(env.get("ORION_PUBLISH_ENABLED"), True),
            kafka_outbox_enabled=_as_bool(env.get("KAFKA_OUTBOX_ENABLED"), False),
            projector_shadow_mode=_as_bool(env.get("PROJECTOR_SHADOW_MODE"), True),
            projector_target_namespace=ns,
            de_webhook_enabled=_as_bool(env.get("DE_WEBHOOK_ENABLED"), True),
            raw_consumer_enabled=_as_bool(env.get("RAW_CONSUMER_ENABLED"), True),
            bronze_enabled=_as_bool(env.get("BRONZE_ENABLED"), True),
            orion_sync_publish=_as_bool(
                env.get("ORION_SYNC_PUBLISH", env.get("SYNC_PUBLISH")), False
            ),
            architecture_lock_smoke=_as_bool(env.get("ARCHITECTURE_LOCK_SMOKE"), False),
        )


def validate_namespace(namespace: str, *, architecture_lock_smoke: bool = False) -> str:
    ns = (namespace or "").strip().lower()
    if ns not in ALLOWED_NAMESPACES:
        raise ProfileValidationError(
            f"PROJECTOR_TARGET_NAMESPACE must be one of {sorted(ALLOWED_NAMESPACES)}, got {namespace!r}"
        )
    if architecture_lock_smoke and ns == "production":
        raise ProfileValidationError(
            "Architecture Lock smoke forbids PROJECTOR_TARGET_NAMESPACE=production "
            "(use test or shadow; production cutover is K-5)"
        )
    return ns


def validate_profile_flags(
    flags: ProfileFlags, component: str = COMPONENT_STACK
) -> None:
    if component not in COMPONENTS:
        raise ProfileValidationError(f"unknown validation component: {component!r}")

    profile = flags.profile
    if profile not in LOCKED_PROFILES and profile not in ("", "none", "debug"):
        raise ProfileValidationError(f"unknown architecture profile: {profile!r}")

    owns_producer = component in (COMPONENT_STACK, COMPONENT_PRODUCER)
    owns_projector = component in (COMPONENT_STACK, COMPONENT_PROJECTOR)

    if flags.projector_target_namespace not in ALLOWED_NAMESPACES:
        raise ProfileValidationError(
            f"invalid PROJECTOR_TARGET_NAMESPACE={flags.projector_target_namespace!r}"
        )

    if flags.architecture_lock_smoke and owns_projector:
        validate_namespace(
            flags.projector_target_namespace,
            architecture_lock_smoke=True,
        )

    if profile not in LOCKED_PROFILES:
        return

    if owns_producer:
        # Sync publish is debug-only — forbidden in migration/final
        if flags.orion_sync_publish:
            raise ProfileValidationError(
                f"profile={profile}: ORION_SYNC_PUBLISH/--sync-publish is forbidden "
                "(debug-only; Kafka fanout requires async path)"
            )
        if not flags.kafka_outbox_enabled:
            raise ProfileValidationError(
                f"profile={profile}: KAFKA_OUTBOX_ENABLED must be true"
            )

    if component == COMPONENT_STACK and not flags.raw_consumer_enabled:
        raise ProfileValidationError(
            f"profile={profile}: RAW_CONSUMER_ENABLED must be true"
        )
    if component == COMPONENT_STACK and profile in {"k6-dual", "k6-final"} and not flags.bronze_enabled:
        raise ProfileValidationError(
            f"profile={profile}: BRONZE_ENABLED must be true"
        )

    if profile == "migration":
        if owns_producer and not flags.orion_publish_enabled:
            raise ProfileValidationError(
                "profile=migration: ORION_PUBLISH_ENABLED must be true"
            )
        if owns_projector:
            if not flags.projector_shadow_mode:
                raise ProfileValidationError(
                    "profile=migration: PROJECTOR_SHADOW_MODE must be true"
                )
            if flags.projector_target_namespace not in ("shadow", "test"):
                raise ProfileValidationError(
                    "profile=migration: PROJECTOR_TARGET_NAMESPACE must be shadow|test"
                )
        if component == COMPONENT_STACK and not flags.de_webhook_enabled:
            raise ProfileValidationError(
                "profile=migration: DE_WEBHOOK_ENABLED must be true "
                "(compose must also deploy de-webhook)"
            )

    if profile == "final":
        if owns_producer and flags.orion_publish_enabled:
            raise ProfileValidationError(
                "profile=final: ORION_PUBLISH_ENABLED must be false"
            )
        if component == COMPONENT_STACK and flags.de_webhook_enabled:
            raise ProfileValidationError(
                "profile=final: DE_WEBHOOK_ENABLED must be false "
                "(compose must also omit de-webhook service)"
            )
        # Final *target* after K-5 may use production + shadow off; Architecture Lock
        # smoke must still use test/shadow and never production.
        if flags.architecture_lock_smoke and owns_projector:
            if flags.projector_target_namespace != "test":
                raise ProfileValidationError(
                    "profile=final + ARCHITECTURE_LOCK_SMOKE: "
                    "PROJECTOR_TARGET_NAMESPACE must be 'test'"
                )
            if not flags.projector_shadow_mode and flags.projector_target_namespace == "production":
                raise ProfileValidationError(
                    "Architecture Lock smoke must not write production Orion IDs"
                )

    if profile == "k5-cutover":
        if owns_producer and flags.orion_publish_enabled:
            raise ProfileValidationError(
                "profile=k5-cutover: ORION_PUBLISH_ENABLED must be false"
            )
        if owns_producer and not flags.kafka_outbox_enabled:
            raise ProfileValidationError(
                "profile=k5-cutover: KAFKA_OUTBOX_ENABLED must be true"
            )
        if owns_projector:
            if flags.projector_shadow_mode:
                raise ProfileValidationError(
                    "profile=k5-cutover: PROJECTOR_SHADOW_MODE must be false"
                )
            if flags.projector_target_namespace != "production":
                raise ProfileValidationError(
                    "profile=k5-cutover: PROJECTOR_TARGET_NAMESPACE must be production"
                )
        if component == COMPONENT_STACK and not flags.de_webhook_enabled:
            raise ProfileValidationError(
                "profile=k5-cutover: DE_WEBHOOK_ENABLED must be true "
                "(K-5 does not retire webhook until K-6b)"
            )

    if profile == "k6-dual":
        if owns_producer and flags.orion_publish_enabled:
            raise ProfileValidationError(
                "profile=k6-dual: ORION_PUBLISH_ENABLED must be false"
            )
        if owns_projector:
            if flags.projector_shadow_mode:
                raise ProfileValidationError(
                    "profile=k6-dual: PROJECTOR_SHADOW_MODE must be false"
                )
            if flags.projector_target_namespace != "production":
                raise ProfileValidationError(
                    "profile=k6-dual: PROJECTOR_TARGET_NAMESPACE must be production"
                )
        if component == COMPONENT_STACK and not flags.de_webhook_enabled:
            raise ProfileValidationError(
                "profile=k6-dual: DE_WEBHOOK_ENABLED must be true "
                "(K-6a retains webhook/subscription for parity and rollback)"
            )

    if profile == "k6-final":
        if owns_producer and flags.orion_publish_enabled:
            raise ProfileValidationError(
                "profile=k6-final: ORION_PUBLISH_ENABLED must be false"
            )
        if owns_projector:
            if flags.projector_shadow_mode:
                raise ProfileValidationError(
                    "profile=k6-final: PROJECTOR_SHADOW_MODE must be false"
                )
            if flags.projector_target_namespace != "production":
                raise ProfileValidationError(
                    "profile=k6-final: PROJECTOR_TARGET_NAMESPACE must be production"
                )
        if component == COMPONENT_STACK and flags.de_webhook_enabled:
            raise ProfileValidationError(
                "profile=k6-final: DE_WEBHOOK_ENABLED must be false "
                "(webhook is an operator-only rollback asset)"
            )


def validate_env(
    profile: Optional[str],
    env: Mapping[str, Any],
    component: str = COMPONENT_STACK,
) -> ProfileFlags:
    """Validate env against a named profile.

    ``component`` scopes the assertions to the flags that runtime actually owns:
    a projector container has no say over KAFKA_OUTBOX_ENABLED or DE_WEBHOOK_ENABLED.
    Empty/None profile skips locked rules.
    """
    name = (profile or env.get("ARCHITECTURE_PROFILE") or "none")
    if isinstance(name, str):
        name = name.strip().lower()
    else:
        name = "none"
    flags = ProfileFlags.from_mapping(name, env)
    validate_profile_flags(flags, component)
    return flags
