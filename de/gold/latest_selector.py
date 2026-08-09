"""Latest-state selection using the frozen Gold 1 ordering."""
from __future__ import annotations

from dataclasses import dataclass

from de.gold.input_models import SilverGoldInput


def latest_order(record: SilverGoldInput) -> tuple:
    return (
        float(record.simulation_time_sec), int(record.cycle_sequence),
        int(record.source_partition), int(record.source_offset),
        tuple(-ord(char) for char in record.source_bronze_event_id),
    )


@dataclass(frozen=True)
class LatestResult:
    record: SilverGoldInput | None
    conflicted: bool


def select_latest(records: tuple[SilverGoldInput, ...]) -> LatestResult:
    if not records:
        return LatestResult(None, False)
    ordered = sorted(records, key=latest_order, reverse=True)
    winner = ordered[0]
    position = (winner.simulation_time_sec, winner.cycle_sequence, winner.source_partition, winner.source_offset, winner.source_bronze_event_id)
    hashes = {
        row.source_payload_hash for row in ordered
        if (row.simulation_time_sec, row.cycle_sequence, row.source_partition, row.source_offset, row.source_bronze_event_id) == position
    }
    return LatestResult(None if len(hashes) > 1 else winner, len(hashes) > 1)

