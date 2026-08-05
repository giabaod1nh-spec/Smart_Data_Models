"""Poison record disposition — parse/validate → DLQ → terminal ledger (RT-B)."""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional, Tuple

from integration.kafka.event_validator import validate_entity_event
from integration.projector.dlq import DlqPublishError, build_dlq_envelope
from integration.projector.schema import (
    STATUS_DLQ_FAILED,
    STATUS_INVALID_JSON,
    STATUS_INVALID_PROTOCOL,
    STATUS_INVALID_SCHEMA,
)

log = logging.getLogger(__name__)


def classify_raw_record(
    raw: Optional[bytes],
) -> Tuple[str, Optional[dict], Optional[str]]:
    """Return (error_type, parsed_dict_or_none, simulation_run_id_or_none)."""
    if raw is None or len(raw) == 0:
        return "INVALID_PROTOCOL", None, None
    try:
        text = raw.decode("utf-8")
    except Exception:
        return "INVALID_JSON", None, None
    try:
        body = json.loads(text)
    except Exception:
        return "INVALID_JSON", None, None
    if not isinstance(body, dict):
        return "INVALID_PROTOCOL", None, None
    event_type = body.get("eventType")
    if event_type == "TrafficEntityObserved":
        vr = validate_entity_event(body)
        if not vr.ok:
            return "INVALID_SCHEMA", body, str(body.get("simulationRunId") or "") or None
    elif event_type == "TrafficSimulationRunStarted":
        for key in ("simulationRunId", "producerId", "producerSessionId"):
            if key not in body:
                return "INVALID_SCHEMA", body, str(body.get("simulationRunId") or "") or None
        # eventId is optional — core derives run-started:{session}:{run}
    elif event_type is None:
        return "INVALID_PROTOCOL", body, None
    return "", body, str(body.get("simulationRunId") or "") or None


def disposition_poison(
    *,
    store,
    offsets,
    dlq_publisher,
    topic: str,
    partition: int,
    offset: int,
    raw: Optional[bytes],
    error_type: str,
    error_message: str,
    simulation_run_id: Optional[str],
    can_commit: Callable[[], bool],
    maybe_commit: Callable[[str, int], None],
    mark_completed: Callable[[str, int, int], None],
) -> str:
    status_map = {
        "INVALID_JSON": STATUS_INVALID_JSON,
        "INVALID_SCHEMA": STATUS_INVALID_SCHEMA,
        "INVALID_PROTOCOL": STATUS_INVALID_PROTOCOL,
    }
    terminal_status = status_map.get(error_type, STATUS_INVALID_PROTOCOL)
    event_id = f"poison:{topic}:{partition}:{offset}"
    try:
        envelope = build_dlq_envelope(
            original_topic=topic,
            partition=partition,
            offset=offset,
            raw=raw,
            error_type=error_type,
            error_message=error_message,
            simulation_run_id=simulation_run_id,
        )
        dlq_publisher.publish_sync(envelope)
    except DlqPublishError as e:
        log.error("DLQ publish failed p=%s o=%s: %s", partition, offset, e)
        from integration.projector.dlq import payload_hash

        store.apply_batch_tx(
            ledger_rows=[
                {
                    "event_id": event_id,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "simulation_run_id": simulation_run_id or "",
                    "cycle_sequence": 0,
                    "entity_id": "",
                    "status": STATUS_DLQ_FAILED,
                    "payload_hash": payload_hash(raw),
                }
            ],
            entity_updates=[],
        )
        return "dlq_failed"
    store.apply_batch_tx(
        ledger_rows=[
            {
                "event_id": event_id,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "simulation_run_id": simulation_run_id or "",
                "cycle_sequence": 0,
                "entity_id": "",
                "status": terminal_status,
                "payload_hash": envelope["payloadHash"],
            }
        ],
        entity_updates=[],
    )
    if can_commit():
        mark_completed(topic, partition, offset)
        maybe_commit(topic, partition)
    return "poison_terminal"
