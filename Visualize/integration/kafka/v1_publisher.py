"""V1-only durable boundary: successful Phase 2 cycles to frozen Outbox rows."""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path

from contracts.topology.v1_digest import topology_digest
from contracts.traffic_observation import (
    TOPIC, canonical_json, observation_key, validate_observation,
)
from integration.kafka.durable_publisher import DurableKafkaPublisher
from integration.kafka.outbox_schema import EVENT_KIND_OBSERVATION_V1
from integration.kafka.outbox_store import OutboxRow, V1AppendResult
from runtime.active_run_registry import ActiveRunRegistry
from runtime.run_manifest import read_verified_v1_manifest


class InvalidV1CycleError(ValueError):
    """A sampling result is incomplete or inconsistent before persistence."""


@dataclass(frozen=True)
class FrozenV1Cycle:
    run_id: str
    cycle_sequence: int
    rows: tuple[OutboxRow, ...]


class V1DurablePublisher(DurableKafkaPublisher):
    """Reuses the existing Outbox worker without any V2 event construction."""

    def __init__(self, *, db_path: Path, **kwargs) -> None:
        if kwargs.pop("topic", TOPIC) != TOPIC:
            raise ValueError(f"V1 publisher requires topic {TOPIC}")
        super().__init__(db_path=db_path, topic=TOPIC, **kwargs)
        self._last_consumed_sequence = 0
        self.v1_inserted_total = 0
        self.v1_reused_total = 0

    def append_cycle(self, *args, **kwargs) -> int:
        raise RuntimeError("V2 entity publishing is unavailable in V1 mode")

    def append_observation_cycle(self, *args, **kwargs) -> int:
        raise RuntimeError("use freeze_cycle and persist_frozen for V1 observations")

    def freeze_cycle(self, result, *, manifest_path: Path, context, registry: ActiveRunRegistry) -> FrozenV1Cycle:
        """Validate run package and serialize each complete envelope exactly once."""
        if result is None or not result.success or result.snapshot is None:
            raise InvalidV1CycleError("V1 result is not a successful sampling cycle")
        snap = result.snapshot
        if result.cycle_sequence <= self._last_consumed_sequence:
            raise InvalidV1CycleError("V1 sampling sequence already consumed")
        if snap.cycle_sequence != result.cycle_sequence or snap.cycle_sequence < 1:
            raise InvalidV1CycleError("V1 cycle sequence mismatch")
        if context is None or not context.archive_ready or snap.simulation_run_id != context.simulation_run_id:
            raise InvalidV1CycleError("V1 run context is unavailable or inconsistent")
        try:
            archived = json.loads(context.archive_path.read_text(encoding="utf-8"))
            if topology_digest(archived) != context.topology_digest or archived != context.catalog:
                raise ValueError("archived V1 catalog changed after startup")
            manifest = read_verified_v1_manifest(
                manifest_path, run_id=snap.simulation_run_id,
                topology_digest=context.topology_digest,
                participants=tuple(item.intersection_id for item in snap.intersections),
                generation=int(json.loads(manifest_path.read_text(encoding="utf-8"))["runGeneration"]),
            )
            registry.assert_publishable(manifest)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise InvalidV1CycleError(f"V1 run package is not ready: {exc}") from exc
        participants = tuple(manifest["participatingIntersectionIds"])
        if not participants or len(set(participants)) != len(participants):
            raise InvalidV1CycleError("invalid V1 participant set")
        if snap.topology_digest != context.topology_digest:
            raise InvalidV1CycleError("snapshot topology digest mismatch")
        if len(result.observations) != len(participants):
            raise InvalidV1CycleError("incomplete V1 observation cycle")
        events = [copy.deepcopy(event) for event in result.observations]
        nodes = [event.get("intersectionId") for event in events]
        if len(nodes) != len(set(nodes)) or set(nodes) != set(participants):
            raise InvalidV1CycleError("V1 observations do not cover participants exactly once")
        if tuple(item.intersection_id for item in snap.intersections) != participants:
            raise InvalidV1CycleError("snapshot participants differ from manifest")
        rows = []
        for index, event in enumerate(events):
            try:
                validate_observation(event)
                if (
                    event["simulationRunId"] != snap.simulation_run_id
                    or event["cycleSequence"] != snap.cycle_sequence
                    or event["simulationStep"] != snap.simulation_step
                    or event["simulationTimeSeconds"] != snap.simulation_time_seconds
                    or event["capturedAt"] != snap.captured_at
                    or event["topologyDigest"] != snap.topology_digest
                ):
                    raise ValueError("event envelope differs from canonical cycle")
                tls = archived["node_to_tls"][event["intersectionId"]]
                expected_directions = set(archived["approach_edges"][tls])
                if set(event["payload"]["directions"]) != expected_directions:
                    raise ValueError("event approaches differ from archived catalog")
                serialized = canonical_json(event)
            except (ValueError, KeyError, TypeError) as exc:
                raise InvalidV1CycleError(f"invalid V1 observation: {exc}") from exc
            rows.append(OutboxRow(
                event_id=event["eventId"],
                simulation_run_id=snap.simulation_run_id,
                cycle_sequence=snap.cycle_sequence,
                entity_sequence=index,
                event_key=observation_key(event),
                topic=TOPIC,
                payload_json=serialized,
                payload_hash=event["payloadHash"],
                event_kind=EVENT_KIND_OBSERVATION_V1,
            ))
        if len({row.event_id for row in rows}) != len(rows):
            raise InvalidV1CycleError("duplicate event IDs within V1 cycle")
        self._last_consumed_sequence = result.cycle_sequence
        return FrozenV1Cycle(snap.simulation_run_id, snap.cycle_sequence, tuple(rows))

    def persist_frozen(self, cycle: FrozenV1Cycle) -> V1AppendResult:
        if self.is_faulted:
            from integration.kafka.outbox_store import OutboxAppendError

            raise OutboxAppendError("V1 publisher is faulted")
        result = self.store.append_v1_cycle(cycle.rows)
        self.cycles_outboxed += 1
        self.last_append_ms = result.duration_ms
        self.v1_inserted_total += result.inserted
        self.v1_reused_total += result.reused_pending + result.already_delivered
        return result

    def health(self) -> dict:
        result = super().health()
        result.update({
            "v1_last_consumed_sequence": self._last_consumed_sequence,
            "v1_inserted_total": self.v1_inserted_total,
            "v1_reused_total": self.v1_reused_total,
        })
        return result
