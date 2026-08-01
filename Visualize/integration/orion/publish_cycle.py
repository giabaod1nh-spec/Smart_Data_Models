"""Immutable full-cycle publish unit for the async Orion publisher."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

EXPECTED_ENTITIES_PER_NODE = 10  # Contract v1 soft expectation (WARNING only)


class CaptureValidationError(ValueError):
    """Raised when a captured cycle is inconsistent and must not be published."""


def _entity_prop_value(entity: dict, key: str) -> Any:
    attr = entity.get(key)
    if isinstance(attr, dict) and "value" in attr:
        return attr["value"]
    return attr


def serialize_entity(entity: dict) -> bytes:
    return json.dumps(
        entity, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    ).encode("utf-8")


def deserialize_entity(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


def validate_entities_for_capture(
    entities: Sequence[dict],
    *,
    expected_node_count: Optional[int] = None,
) -> Tuple[str, float, str]:
    """Validate entity list before enqueue.

    Returns (simulation_run_id, simulation_time, scenario_id).
    Raises CaptureValidationError for empty / duplicate IDs / metadata mismatch.
    Logs WARNING only for unexpected cardinality.
    """
    if not entities:
        raise CaptureValidationError("entities list is empty")

    ids = [str(e.get("id") or "") for e in entities]
    if any(not i for i in ids):
        raise CaptureValidationError("one or more entities missing id")
    if len(ids) != len(set(ids)):
        raise CaptureValidationError(f"duplicate entity ids in cycle: {ids}")

    run_ids = {_entity_prop_value(e, "simulationRunId") for e in entities}
    sim_times = {_entity_prop_value(e, "simulationTime") for e in entities}
    scenarios = {_entity_prop_value(e, "scenarioId") for e in entities}

    if len(run_ids) != 1 or None in run_ids:
        raise CaptureValidationError(
            f"simulationRunId mismatch across entities: {run_ids}"
        )
    if len(sim_times) != 1 or None in sim_times:
        raise CaptureValidationError(
            f"simulationTime mismatch across entities: {sim_times}"
        )
    if len(scenarios) != 1 or None in scenarios:
        raise CaptureValidationError(
            f"scenarioId mismatch across entities: {scenarios}"
        )

    if expected_node_count is not None:
        expected = EXPECTED_ENTITIES_PER_NODE * expected_node_count
        if len(entities) != expected:
            log.warning(
                "Unexpected entity count=%d expected=%d (nodes=%d); enqueueing anyway",
                len(entities),
                expected,
                expected_node_count,
            )

    return str(next(iter(run_ids))), float(next(iter(sim_times))), str(next(iter(scenarios)))


@dataclass(frozen=True)
class PublishCycle:
    sequence_number: int
    simulation_run_id: str
    simulation_time: float
    scenario_id: str
    nodes: tuple[str, ...]
    entities_json: tuple[bytes, ...]
    entity_ids: tuple[str, ...]
    captured_at_monotonic: float

    @classmethod
    def from_entities(
        cls,
        *,
        sequence_number: int,
        nodes: Sequence[str],
        entities: Iterable[dict],
        captured_at_monotonic: Optional[float] = None,
    ) -> "PublishCycle":
        entity_list: List[dict] = list(entities)
        run_id, sim_t, scenario = validate_entities_for_capture(
            entity_list, expected_node_count=len(nodes)
        )
        entities_json = tuple(serialize_entity(e) for e in entity_list)
        entity_ids = tuple(str(e["id"]) for e in entity_list)
        return cls(
            sequence_number=int(sequence_number),
            simulation_run_id=run_id,
            simulation_time=sim_t,
            scenario_id=scenario,
            nodes=tuple(str(n) for n in nodes),
            entities_json=entities_json,
            entity_ids=entity_ids,
            captured_at_monotonic=(
                float(captured_at_monotonic)
                if captured_at_monotonic is not None
                else time.monotonic()
            ),
        )

    def entity_at(self, index: int) -> dict:
        return deserialize_entity(self.entities_json[index])
