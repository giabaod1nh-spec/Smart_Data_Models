"""Project a domain observation into the current NGSI-LD entity model."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from contracts.traffic_observation import validate_observation
from integration.orion.entity_mapper import build_all_entities


def observation_to_entities(event: Mapping[str, Any]) -> list[dict]:
    validate_observation(event)
    node_id = str(event["intersectionId"])
    snapshot = deepcopy(dict(event["payload"]))
    snapshot["captured_at"] = str(event["capturedAt"])
    entities = build_all_entities(node_id, snapshot)
    ids = [str(e["id"]) for e in entities]
    if len(ids) != len(set(ids)):
        raise ValueError("observation produced duplicate Orion entity IDs")
    for entity in entities:
        if entity["simulationRunId"]["value"] != event["simulationRunId"]:
            raise ValueError("projection run identity mismatch")
        if float(entity["simulationTime"]["value"]) != float(event["simulationTimeSeconds"]):
            raise ValueError("projection simulation time mismatch")
    return entities
