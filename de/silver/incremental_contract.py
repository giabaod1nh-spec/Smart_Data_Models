"""Public DTOs and receipt serialization for the Silver sequence feeds."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass, replace
from types import MappingProxyType
from typing import Any, Literal

import rfc8785

CONTRACT_VERSION = "1"
MAX_SEQUENCE = (1 << 63) - 1
MIB = 1024 * 1024
RETRYABLE_ERRORS = frozenset({
    "SOURCE_LAG", "PREFIX_BLOCKED", "INVALIDATION_LAG", "BOUNDARY_STALE",
    "DEADLINE_EXCEEDED", "AUTHORITY_UNAVAILABLE", "CLICKHOUSE_UNAVAILABLE",
})


def freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item) for item in value)
    return value


class CanonicalFeedError(RuntimeError):
    def __init__(self, code: str, message: str, **details):
        super().__init__(message)
        self.code = self.error_code = code
        self.retryable = code in RETRYABLE_ERRORS
        self.details = freeze(details)
        self.receipt = None
        self.cursor_advance = False


@dataclass(frozen=True, kw_only=True)
class FeedLimits:
    max_entries_cap: int = 100
    max_encoded_bytes_cap: int = 8 * MIB
    max_single_entry_bytes: int = 4 * MIB
    max_processing_ms_cap: int = 60_000
    max_entry_processing_ms_cap: int = 15_000

    def __post_init__(self):
        defaults = (100, 8 * MIB, 4 * MIB, 60_000, 15_000)
        for field, maximum in zip(fields(self), defaults):
            value = getattr(self, field.name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{field.name} must be in 1..{maximum}")


@dataclass(frozen=True)
class SourceDiscoveryRequest:
    namespace: str
    expected_source_binding_id: str | None = None
    contract_version: str = CONTRACT_VERSION


@dataclass(frozen=True, kw_only=True)
class _PageRequest:
    namespace: str
    expected_source_binding_id: str
    max_entries: int = 50
    max_encoded_bytes: int = 4 * MIB
    max_processing_ms: int = 30_000
    max_entry_processing_ms: int = 10_000
    contract_version: str = CONTRACT_VERSION


@dataclass(frozen=True, kw_only=True)
class CanonicalChangePageRequest(_PageRequest):
    after_change_sequence: int


@dataclass(frozen=True, kw_only=True)
class CanonicalInvalidationPageRequest(_PageRequest):
    after_invalidation_sequence: int
    through_invalidation_sequence: int


@dataclass(frozen=True, kw_only=True)
class SourceDiscovery(FeedLimits):
    namespace: str
    source_binding_id: str
    source_binding: Mapping[str, Any]
    bronze_change_high_water: int
    bronze_invalidation_high_water: int
    silver_change_applied_sequence: int
    silver_invalidation_applied_sequence: int
    verified_change_prefix: int
    verified_invalidation_prefix: int
    bootstrap_required: bool
    next_change_sequence: int = 0
    next_invalidation_sequence: int = 0
    run_discovery_scope: str = "journaled-runs-only"
    contract_version: str = CONTRACT_VERSION


@dataclass(frozen=True, kw_only=True)
class RunIdentity:
    run_id: str
    run_generation: int
    topology_digest: str
    manifest_identity: str


Disposition = Literal["BUNDLE_VALID", "SUPERSEDED_BY_CONFLICT", "CONFLICTED"]


@dataclass(frozen=True, kw_only=True)
class CanonicalChangeEntry(RunIdentity):
    change_sequence: int
    event_id: str
    semantic_revision: int
    change_type: str
    previous_state: str
    new_state: str
    source_transition_hash: str
    source_invalidation_id: str | None
    effective_disposition: Disposition
    superseding_change_sequence: int | None
    invalidation_sequence: int | None
    canonical_bundle: Any
    bundle_hash: str | None
    evidence_status: str = "VERIFIED"


@dataclass(frozen=True, kw_only=True)
class CanonicalInvalidationEntry(RunIdentity):
    invalidation_sequence: int
    invalidation_id: str
    event_id: str
    semantic_revision: int
    previous_immutable_envelope_hash: str
    conflicting_immutable_envelope_hash: str
    source_transition_hash: str
    mirror_status: str
    intersection_id: str
    cycle_sequence: int
    simulation_time_seconds: float


@dataclass(frozen=True, kw_only=True)
class CapturedBoundary:
    captured_bronze_change_high_water: int
    captured_bronze_invalidation_high_water: int
    captured_silver_change_applied_sequence: int
    captured_silver_invalidation_applied_sequence: int


@dataclass(frozen=True, kw_only=True)
class CanonicalBatchReceipt(CapturedBoundary):
    receipt_kind: Literal["CHANGE", "INVALIDATION"]
    namespace: str
    source_binding_id: str
    requested_after_sequence: int
    returned_after_sequence: int
    returned_through_sequence: int
    safe_invalidation_through_sequence: int | None
    entry_count: int
    contract_version: str = CONTRACT_VERSION
    encoded_bytes: int = 0
    batch_digest: str = ""
    receipt_id: str = ""


@dataclass(frozen=True, kw_only=True)
class CanonicalInvalidationReceipt(CanonicalBatchReceipt):
    requested_through_invalidation_sequence: int


@dataclass(frozen=True, kw_only=True)
class _Page:
    namespace: str
    source_binding_id: str
    entry_count: int
    boundary_id: str
    has_more: bool
    status: Literal["OK", "EMPTY"]
    receipt: CanonicalBatchReceipt | None
    contract_version: str = CONTRACT_VERSION
    encoded_bytes: int = 0


@dataclass(frozen=True, kw_only=True)
class CanonicalChangePage(_Page, CapturedBoundary):
    after_change_sequence: int
    through_change_sequence: int
    verified_change_prefix: int
    safe_invalidation_through_sequence: int
    entries: tuple[CanonicalChangeEntry, ...]


@dataclass(frozen=True, kw_only=True)
class CanonicalInvalidationPage(_Page):
    after_invalidation_sequence: int
    through_invalidation_sequence: int
    requested_through_invalidation_sequence: int
    captured_bronze_invalidation_high_water: int
    captured_silver_invalidation_applied_sequence: int
    verified_invalidation_prefix: int
    entries: tuple[CanonicalInvalidationEntry, ...]


def _project(value, key=""):
    if is_dataclass(value):
        value = {field.name: getattr(value, field.name) for field in fields(value)}
    if isinstance(value, Mapping):
        return {name: _project(item, name) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_project(item, key) for item in value]
    if type(value) is int and (
        key.endswith(("_sequence", "_high_water", "_prefix")) or abs(value) >= (1 << 53)
    ):
        return str(value)
    return value


def canonical_bytes(value) -> bytes:
    """JCS of the v1 DTO projection. Also usable on the saved JSON projection."""
    return rfc8785.dumps(_project(value))


def canonical_hash(value) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def batch_digest(receipt: CanonicalBatchReceipt, entries) -> str:
    excluded = {"encoded_bytes", "batch_digest", "receipt_id"}
    header = {f.name: getattr(receipt, f.name) for f in fields(receipt) if f.name not in excluded}
    return canonical_hash(dict(header, entries=entries))


def size_page(page: CanonicalChangePage | CanonicalInvalidationPage, entry_bytes: int):
    """Size the envelope using cached entry byte lengths and fixed-width hashes."""
    receipt = page.receipt
    skeleton = replace(page, entries=(), encoded_bytes=0)
    if receipt is not None:
        skeleton = replace(skeleton, receipt=replace(
            receipt, encoded_bytes=0, batch_digest="0" * 64, receipt_id="sha256:" + "0" * 64))
    base = len(canonical_bytes(skeleton)) + entry_bytes + max(0, page.entry_count - 1)
    size = base
    while True:
        exact = base + (2 if receipt else 1) * (len(str(size)) - 1)
        if exact == size:
            return size
        size = exact
