"""Fail-fast selection of the single V1 durable publishing path."""
from __future__ import annotations

from pathlib import Path

from contracts.traffic_observation import TOPIC


def validate_v1_publish_mode(*, args, cfg) -> bool:
    enabled = bool(getattr(args, "v1_publish", False) or cfg.V1_PUBLISH_ENABLED)
    if not enabled:
        return False
    conflicts = []
    if not getattr(args, "v1_observations", False):
        conflicts.append("--v1-observations is required")
    if cfg.KAFKA_PUBLISH_ENABLED:
        conflicts.append("KAFKA_PUBLISH_ENABLED must be false")
    if cfg.KAFKA_OUTBOX_ENABLED:
        conflicts.append("KAFKA_OUTBOX_ENABLED must be false")
    if not getattr(args, "no_orion", False) and cfg.ORION_PUBLISH_ENABLED:
        conflicts.append("direct Orion publishing must be disabled (--no-orion)")
    if getattr(args, "sync_publish", False) or getattr(args, "async_publish", False):
        conflicts.append("legacy Orion publish flags cannot be used")
    if cfg.V1_KAFKA_TOPIC != TOPIC:
        conflicts.append(f"V1_KAFKA_TOPIC must be {TOPIC}")
    if not getattr(cfg, "KAFKA_BOOTSTRAP_SERVERS", "").strip():
        conflicts.append("KAFKA_BOOTSTRAP_SERVERS must be set")
    if cfg.V1_OUTBOX_DB is None or Path(cfg.V1_OUTBOX_DB).is_dir():
        conflicts.append("V1_OUTBOX_DB must be set")
    elif Path(cfg.V1_OUTBOX_DB).resolve() == Path(cfg.KAFKA_OUTBOX_DB).resolve():
        conflicts.append("V1_OUTBOX_DB must differ from the V2 Outbox path")
    if conflicts:
        raise ValueError("V1 startup configuration: " + "; ".join(conflicts))
    return True
