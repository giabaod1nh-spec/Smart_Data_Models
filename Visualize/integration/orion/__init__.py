"""integration.orion — NGSI-LD client and entity mapping."""
from integration.orion.client import (
    BatchEntityError,
    BatchUpsertResult,
    OrionBatchProtocolError,
    OrionPermanentError,
    OrionPublishError,
    OrionTransientError,
    batch_upsert_entities,
    created_cache_snapshot,
    is_in_created_cache,
    reset_created_cache,
    upsert_entity,
    wait_orion_ready,
)
from integration.orion.entity_mapper import (
    CONTEXT,
    build_all_entities,
    build_camera,
    build_intersection,
    build_traffic_light,
    build_vehicle_sensor,
)
from integration.orion.publish_cycle import CaptureValidationError, PublishCycle

__all__ = [
    "upsert_entity",
    "batch_upsert_entities",
    "wait_orion_ready",
    "reset_created_cache",
    "is_in_created_cache",
    "created_cache_snapshot",
    "OrionPublishError",
    "OrionTransientError",
    "OrionPermanentError",
    "OrionBatchProtocolError",
    "BatchUpsertResult",
    "BatchEntityError",
    "PublishCycle",
    "CaptureValidationError",
    "CONTEXT",
    "build_all_entities",
    "build_intersection",
    "build_vehicle_sensor",
    "build_camera",
    "build_traffic_light",
]
