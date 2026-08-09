"""Gold direction-v1 canonicalization."""
from __future__ import annotations

from dataclasses import replace

from de.gold.contracts import DIRECTION_MAPPING_VERSION, canonicalize_direction
from de.gold.input_models import SilverGoldInput, SilverSignalStateInput, SilverTrafficObservationInput


def canonicalize_record(record: SilverGoldInput) -> SilverGoldInput:
    if not isinstance(record, (SilverTrafficObservationInput, SilverSignalStateInput)):
        return record
    direction, source, flags = canonicalize_direction(record.source_direction)
    return replace(
        record,
        canonical_direction=direction,
        source_direction=source,
        direction_mapping_version=DIRECTION_MAPPING_VERSION,
        quality_flags=tuple(sorted(set(record.quality_flags).union(flags))),
    )

