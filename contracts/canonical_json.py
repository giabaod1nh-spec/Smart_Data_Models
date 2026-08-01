"""Shared canonical JSON + SHA-256 helpers for Contract hashing.

Algorithm (locked for Kafka Event Delivery Contract 2.0.0 and DE):
  json.dumps(obj, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
  → UTF-8 encode → SHA-256 hex (64 lowercase chars)

Producer, DVT, DE, and Projector MUST import this module — no divergent serializers.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    """Canonical JSON string: sorted object keys, array order preserved, compact."""
    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )


def canonical_hash(obj: Any) -> str:
    """SHA-256 hex (64 chars) of canonical JSON."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def compute_event_id(
    *,
    contract_version: str,
    simulation_run_id: str,
    cycle_sequence: int,
    entity_id: str,
) -> str:
    """Deterministic eventId for Kafka Event Delivery Contract 2.0.0."""
    material = "|".join(
        [
            str(contract_version),
            str(simulation_run_id),
            str(cycle_sequence),
            str(entity_id),
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def entity_payload_hash(entity: dict[str, Any]) -> str:
    """Hash of the inner NGSI-LD entity object only (not the delivery envelope)."""
    return canonical_hash(entity)


def node_id_from_entity_id(entity_id: str) -> str:
    """Derive node letter from urn:ngsi-ld:{Type}:{node...} convention."""
    if not entity_id.startswith("urn:ngsi-ld:"):
        raise ValueError(f"not an NGSI-LD urn: {entity_id}")
    rest = entity_id[len("urn:ngsi-ld:") :]
    parts = rest.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"unexpected entity id shape: {entity_id}")
    local = parts[1]
    # Intersection:A | Camera:A | TrafficLight:A-North | VehicleSensor:A:NORTHBOUND
    for prior in ("shadow:", "test:"):
        if local.startswith(prior):
            local = local[len(prior) :]
            break
    return local.split(":")[0].split("-")[0]


def to_shadow_entity_id(entity_id: str) -> str:
    """urn:ngsi-ld:{Type}:{rest} → urn:ngsi-ld:{Type}:shadow:{rest}."""
    return to_namespaced_entity_id(entity_id, "shadow")


def to_namespaced_entity_id(entity_id: str, namespace: str) -> str:
    """urn:ngsi-ld:{Type}:{rest} → urn:ngsi-ld:{Type}:{namespace}:{rest}.

    ``namespace`` must be a non-empty token without ':' (e.g. shadow, test).
    ``production`` returns the id unchanged. Idempotent if already namespaced.
    """
    ns = (namespace or "").strip().lower()
    if not ns or ns == "production":
        return entity_id
    if ":" in ns:
        raise ValueError(f"invalid namespace token: {namespace!r}")
    marker = f":{ns}:"
    if marker in entity_id:
        return entity_id
    prefix = "urn:ngsi-ld:"
    if not entity_id.startswith(prefix):
        raise ValueError(f"not an NGSI-LD urn: {entity_id}")
    rest = entity_id[len(prefix) :]
    type_name, _, local = rest.partition(":")
    if not local:
        raise ValueError(f"unexpected entity id shape: {entity_id}")
    # Strip a known prior isolation namespace so shadow↔test switches stay clean.
    for prior in ("shadow:", "test:"):
        if local.startswith(prior):
            local = local[len(prior) :]
            break
    return f"{prefix}{type_name}:{ns}:{local}"
