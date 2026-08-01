"""Shadow entity mapper — collision-safe Relationship rewrite (K-3)."""
from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from contracts.canonical_json import (  # noqa: E402
    to_namespaced_entity_id,
    to_shadow_entity_id,
)

# Smart Traffic entity types whose URNs we rewrite
_SMART_TYPES = frozenset(
    {"Intersection", "TrafficLight", "VehicleSensor", "Camera"}
)

# Relationship attribute names on NGSI-LD entities in this project
_REL_ATTRS = frozenset(
    {
        "refIntersection",
        "refCamera",
        "refTrafficLight",
        "refTrafficLights",
        "refCameras",
        "refVehicleSensors",
        "refVehicleSensor",
        "affectedBy",
    }
)


def is_smart_traffic_urn(urn: str) -> bool:
    if not isinstance(urn, str) or not urn.startswith("urn:ngsi-ld:"):
        return False
    rest = urn[len("urn:ngsi-ld:") :]
    type_name, _, _local = rest.partition(":")
    return type_name in _SMART_TYPES


def rewrite_relationship_object(obj: Any, namespace: str = "shadow") -> Any:
    if isinstance(obj, str):
        if is_smart_traffic_urn(obj):
            return to_namespaced_entity_id(obj, namespace)
        return obj
    if isinstance(obj, list):
        return [rewrite_relationship_object(x, namespace) for x in obj]
    return obj


def to_namespaced_entity(entity: dict[str, Any], namespace: str = "shadow") -> dict[str, Any]:
    """Deep-copy entity → namespaced IDs + Relationship objects; Properties unchanged."""
    ns = (namespace or "shadow").strip().lower()
    if ns in ("", "production"):
        return copy.deepcopy(entity)
    out = copy.deepcopy(entity)
    eid = str(out.get("id") or "")
    if is_smart_traffic_urn(eid):
        out["id"] = to_namespaced_entity_id(eid, ns)
    for key, val in list(out.items()):
        if key in ("id", "type", "@context"):
            continue
        if not isinstance(val, dict):
            continue
        if val.get("type") == "Relationship" and "object" in val:
            if key in _REL_ATTRS or is_smart_traffic_urn(
                val["object"] if isinstance(val["object"], str) else ""
            ) or (
                isinstance(val["object"], list)
                and any(is_smart_traffic_urn(x) for x in val["object"] if isinstance(x, str))
            ):
                val["object"] = rewrite_relationship_object(val["object"], ns)
            elif key.startswith("ref") or key in _REL_ATTRS:
                val["object"] = rewrite_relationship_object(val["object"], ns)
    return out


def to_shadow_entity(entity: dict[str, Any]) -> dict[str, Any]:
    """Deep-copy entity → shadow IDs + Relationship objects; Properties unchanged."""
    _ = to_shadow_entity_id  # keep import used for re-exports / clarity
    return to_namespaced_entity(entity, "shadow")
