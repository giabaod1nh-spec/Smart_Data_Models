"""Map NGSI-LD entities → Kafka Event Delivery Contract 2.0.0 envelopes."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from contracts.canonical_json import (  # noqa: E402
    compute_event_id,
    entity_payload_hash,
    node_id_from_entity_id,
)

EVENT_VERSION = "2.0.0"
CONTRACT_VERSION = "2.0.0"
EVENT_TYPE = "TrafficEntityObserved"
SOURCE = "sumo"
DEFAULT_PRODUCER_ID = "visualize-traci"


def _prop_value(entity: Mapping[str, Any], key: str) -> Any:
    node = entity.get(key)
    if isinstance(node, dict) and "value" in node:
        return node["value"]
    return node


def partition_key(simulation_run_id: str, node_id: str) -> str:
    return f"{simulation_run_id}:{node_id}"


def build_entity_event(
    entity: dict[str, Any],
    *,
    cycle_sequence: int,
    entity_sequence: int,
    cycle_entity_count: int,
    node_entity_count: Optional[int] = None,
    captured_at: Optional[str] = None,
    producer_id: str = DEFAULT_PRODUCER_ID,
    producer_session_id: str = "",
    trace_id: Optional[str] = None,
) -> dict[str, Any]:
    """Build one TrafficEntityObserved event from a frozen NGSI-LD entity dict."""
    entity_id = str(entity.get("id") or "")
    if not entity_id:
        raise ValueError("entity missing id")

    run_id = str(_prop_value(entity, "simulationRunId") or "")
    sim_t = _prop_value(entity, "simulationTime")
    scenario_id = str(_prop_value(entity, "scenarioId") or "")
    if not run_id:
        raise ValueError(f"entity {entity_id} missing simulationRunId")
    if sim_t is None:
        raise ValueError(f"entity {entity_id} missing simulationTime")
    if not scenario_id:
        raise ValueError(f"entity {entity_id} missing scenarioId")

    node_id = node_id_from_entity_id(entity_id)
    # Millisecond SLA measurements require sub-second precision.  Truncating to
    # whole seconds injects 0..999 ms of artificial latency before any I/O.
    captured = captured_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload_hash = entity_payload_hash(entity)
    event_id = compute_event_id(
        contract_version=CONTRACT_VERSION,
        simulation_run_id=run_id,
        cycle_sequence=int(cycle_sequence),
        entity_id=entity_id,
    )
    correlation_id = f"{run_id}:{int(cycle_sequence)}"
    nec = int(node_entity_count) if node_entity_count is not None else None

    event: dict[str, Any] = {
        "eventId": event_id,
        "eventVersion": EVENT_VERSION,
        "contractVersion": CONTRACT_VERSION,
        "eventType": EVENT_TYPE,
        "source": SOURCE,
        "simulationRunId": run_id,
        "simulationTime": float(sim_t),
        "scenarioId": scenario_id,
        "nodeId": node_id,
        "cycleSequence": int(cycle_sequence),
        "entitySequence": int(entity_sequence),
        "cycleEntityCount": int(cycle_entity_count),
        "capturedAt": captured,
        "producerId": producer_id,
        "producerSessionId": producer_session_id or "unknown-session",
        "correlationId": correlation_id,
        "entityPayloadHash": payload_hash,
        "entity": entity,
    }
    if nec is not None:
        event["nodeEntityCount"] = nec
    if trace_id:
        event["traceId"] = trace_id
    return event


def build_run_started_event(
    *,
    simulation_run_id: str,
    producer_session_id: str,
    producer_id: str = DEFAULT_PRODUCER_ID,
    started_at: Optional[str] = None,
    scenario_id: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> dict[str, Any]:
    """Control event — sole activator of projector_active_runs."""
    started = started_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    event: dict[str, Any] = {
        "eventType": "TrafficSimulationRunStarted",
        "eventVersion": EVENT_VERSION,
        "contractVersion": CONTRACT_VERSION,
        "source": SOURCE,
        "producerId": producer_id,
        "producerSessionId": producer_session_id,
        "simulationRunId": simulation_run_id,
        "startedAt": started,
    }
    if scenario_id:
        event["scenarioId"] = scenario_id
    if trace_id:
        event["traceId"] = trace_id
    return event


def run_started_partition_key(simulation_run_id: str) -> str:
    return f"{simulation_run_id}:__run__"


def build_cycle_events(
    entities: Sequence[dict[str, Any]],
    *,
    cycle_sequence: int,
    captured_at: Optional[str] = None,
    producer_id: str = DEFAULT_PRODUCER_ID,
    producer_session_id: str = "",
    trace_id: Optional[str] = None,
) -> List[dict[str, Any]]:
    """Map a full cycle entity list → ordered Event v2 envelopes (entitySequence 0..N-1).

    Sets `nodeEntityCount` per node from the count of entities sharing that nodeId.
    """
    n = len(entities)
    if n < 1:
        raise ValueError("cycle entities must be non-empty")
    node_counts: dict[str, int] = {}
    for e in entities:
        nid = node_id_from_entity_id(str(e.get("id") or ""))
        node_counts[nid] = node_counts.get(nid, 0) + 1
    return [
        build_entity_event(
            entities[i],
            cycle_sequence=cycle_sequence,
            entity_sequence=i,
            cycle_entity_count=n,
            node_entity_count=node_counts[
                node_id_from_entity_id(str(entities[i].get("id") or ""))
            ],
            captured_at=captured_at,
            producer_id=producer_id,
            producer_session_id=producer_session_id,
            trace_id=trace_id,
        )
        for i in range(n)
    ]


def events_partition_keys(events: Iterable[dict[str, Any]]) -> List[str]:
    return [partition_key(e["simulationRunId"], e["nodeId"]) for e in events]
