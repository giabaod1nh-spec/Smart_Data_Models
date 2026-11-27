"""Public Silver contract for verified Stage A archive metadata, version 1."""

from dataclasses import dataclass
from typing import Any, Literal

from de.silver.incremental_contract import canonical_hash


@dataclass(frozen=True, kw_only=True)
class ObservationMetadataCursor:
    stream: Literal["SCHEDULE", "MEASUREMENT"]
    sequence: int = 0
    digest: str | None = None
    settled_ordinal: int = -1
    pending_digest: str | None = None


@dataclass(frozen=True, kw_only=True)
class ObservationMetadataRequest:
    source_binding_id: str
    run_id: str
    run_generation: int
    topology_digest: str
    cursor: ObservationMetadataCursor
    max_slots: int = 4
    max_processing_ms: int = 10_000
    contract_version: int = 1


@dataclass(frozen=True, kw_only=True)
class VerifiedObservationMetadata:
    source_binding_id: str
    run_id: str
    run_generation: int
    topology_digest: str
    manifest_identity: str
    schedule_id: str
    archive_head_digest: str
    captured_through_sequence: int
    before: ObservationMetadataCursor
    after: ObservationMetadataCursor
    required_change_sequence: int
    required_invalidation_sequence: int
    payload: Any
    status: Literal["VERIFIED", "EMPTY", "WAITING"]
    contract_version: int = 1

    @property
    def receipt_digest(self):
        return canonical_hash(self)
