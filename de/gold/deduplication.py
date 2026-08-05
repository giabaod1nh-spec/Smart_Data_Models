"""Deterministic semantic duplicate collapse and conflict detection."""
from __future__ import annotations

from dataclasses import dataclass

from de.gold.input_models import SilverGoldInput, SilverSignalStateInput, SilverTrafficObservationInput


def semantic_identity(record: SilverGoldInput) -> tuple:
    direction = record.canonical_direction if isinstance(record, (SilverTrafficObservationInput, SilverSignalStateInput)) else ""
    return (
        type(record).__name__, record.simulation_run_id, record.scenario_id,
        record.intersection_id, direction, record.simulation_time_sec,
        record.cycle_sequence, record.source_entity_id,
        record.source_bronze_event_id, record.source_partition, record.source_offset,
    )


@dataclass(frozen=True)
class DeduplicationResult:
    records: tuple[SilverGoldInput, ...]
    conflicts: tuple[tuple, ...]
    conflicted_records: tuple[SilverGoldInput, ...]


def deduplicate(records: tuple[SilverGoldInput, ...]) -> DeduplicationResult:
    grouped: dict[tuple, dict[str, SilverGoldInput]] = {}
    for record in records:
        grouped.setdefault(semantic_identity(record), {})[record.source_payload_hash] = record
    accepted: list[SilverGoldInput] = []
    conflicts: list[tuple] = []
    conflicted_records: list[SilverGoldInput] = []
    for identity in sorted(grouped, key=repr):
        hashes = grouped[identity]
        if len(hashes) > 1:
            conflicts.append(identity)
            conflicted_records.extend(hashes.values())
            continue
        accepted.append(next(iter(hashes.values())))
    accepted.sort(key=lambda r: (r.simulation_run_id, r.scenario_id, r.intersection_id, r.simulation_time_sec, r.source_partition, r.source_offset, r.source_bronze_event_id))
    conflicted_records.sort(key=lambda r: (repr(semantic_identity(r)), r.source_payload_hash))
    return DeduplicationResult(tuple(accepted), tuple(conflicts), tuple(conflicted_records))
