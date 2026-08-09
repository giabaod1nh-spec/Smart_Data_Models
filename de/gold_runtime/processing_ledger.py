"""Batch identity, expected-output manifest and terminal ledger disposition.

The ``ExpectedOutputManifest`` is built directly from the complete, validated
``GoldTransformationResult`` before any ClickHouse write. Reconciliation is set
equality over ``(target_table, logical_identity, source_set_hash, revision_seq,
payload_digest)``, never row counts.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from de.gold.contracts import TABLE_COLUMNS
from de.gold.models import GoldProcessingLedger
from de.gold_runtime.config import (
    DEFINITION_MAJOR,
    DEFINITION_MINOR,
    WorkUnitState,
)
from de.gold_runtime.repositories import (
    PERSISTENCE_ORDER,
    RESULT_FIELD_BY_TABLE,
    ExistingState,
    logical_identity,
)

DISPOSITION_RECEIVED = "RECEIVED"
DISPOSITION_TRANSFORMED = "TRANSFORMED"
DISPOSITION_PERSISTED = "PERSISTED"
DISPOSITION_CHECKPOINTED = "CHECKPOINTED"
DISPOSITION_REPLAYED = "REPLAYED"
DISPOSITION_QUARANTINED = "QUARANTINED"
DISPOSITION_FAILED_RETRYABLE = "FAILED_RETRYABLE"

LEDGER_DISPOSITIONS: tuple[str, ...] = (
    DISPOSITION_RECEIVED,
    DISPOSITION_TRANSFORMED,
    DISPOSITION_PERSISTED,
    DISPOSITION_CHECKPOINTED,
    DISPOSITION_REPLAYED,
    DISPOSITION_QUARANTINED,
    DISPOSITION_FAILED_RETRYABLE,
)
TERMINAL_DISPOSITIONS: frozenset[str] = frozenset(
    {DISPOSITION_CHECKPOINTED, DISPOSITION_REPLAYED, DISPOSITION_QUARANTINED}
)

LEDGER_SEQUENCE: dict[str, frozenset[str]] = {
    DISPOSITION_RECEIVED: frozenset(
        {DISPOSITION_TRANSFORMED, DISPOSITION_QUARANTINED, DISPOSITION_FAILED_RETRYABLE}
    ),
    DISPOSITION_TRANSFORMED: frozenset(
        {DISPOSITION_PERSISTED, DISPOSITION_QUARANTINED, DISPOSITION_FAILED_RETRYABLE}
    ),
    DISPOSITION_PERSISTED: frozenset({DISPOSITION_CHECKPOINTED, DISPOSITION_REPLAYED}),
    DISPOSITION_FAILED_RETRYABLE: frozenset(
        {DISPOSITION_RECEIVED, DISPOSITION_TRANSFORMED, DISPOSITION_QUARANTINED}
    ),
    DISPOSITION_CHECKPOINTED: frozenset(),
    DISPOSITION_REPLAYED: frozenset(),
    DISPOSITION_QUARANTINED: frozenset(),
}


class LedgerTransitionError(ValueError):
    """Ambiguous or illegal ledger disposition transition."""


class ManifestError(ValueError):
    """Malformed expected-output manifest."""


class ReconcileStatus(str, Enum):
    DURABLE = "DURABLE"
    MISSING = "MISSING"
    CONFLICTED = "CONFLICTED"


def assert_ledger_transition(current: Optional[str], new: str) -> str:
    if new not in LEDGER_DISPOSITIONS:
        raise LedgerTransitionError(f"unknown disposition {new!r}")
    if current is None:
        if new != DISPOSITION_RECEIVED:
            raise LedgerTransitionError(f"first disposition must be RECEIVED, got {new!r}")
        return new
    if new not in LEDGER_SEQUENCE[current]:
        raise LedgerTransitionError(f"illegal ledger transition {current} -> {new}")
    return new


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_safe(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (memoryview, bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace").rstrip("\x00")
    if isinstance(value, float):
        return float(value)
    return value


def canonical_json(payload: Any) -> str:
    return json.dumps(
        _json_safe(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def batch_identity(
    *,
    namespace: str,
    simulation_run_id: str,
    scenario_id: str,
    window_id: str,
    source_set_hash: str,
    definition_version: str,
    gold_schema_version: str,
    revision_seq: int,
) -> dict:
    return {
        "namespace": namespace,
        "simulation_run_id": simulation_run_id,
        "scenario_id": scenario_id,
        "window_id": window_id,
        "source_set_hash": source_set_hash,
        "definition_version": definition_version,
        "gold_schema_version": gold_schema_version,
        "revision_seq": int(revision_seq),
    }


def batch_id_for(identity: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()


def payload_digest(target_table: str, row: Any) -> str:
    """Deterministic digest of the persisted column projection for one row."""
    columns = TABLE_COLUMNS[target_table]
    payload = [[column, _json_safe(getattr(row, column))] for column in columns]
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ManifestEntry:
    target_table: str
    logical_identity: tuple
    source_set_hash: str
    revision_seq: int
    payload_digest: str

    def to_dict(self) -> dict:
        return {
            "target_table": self.target_table,
            "logical_identity": list(self.logical_identity),
            "source_set_hash": self.source_set_hash,
            "revision_seq": int(self.revision_seq),
            "payload_digest": self.payload_digest,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ManifestEntry":
        return cls(
            target_table=str(payload["target_table"]),
            logical_identity=tuple(payload["logical_identity"]),
            source_set_hash=str(payload["source_set_hash"]),
            revision_seq=int(payload["revision_seq"]),
            payload_digest=str(payload["payload_digest"]),
        )


@dataclass(frozen=True)
class ReconcileReport:
    durable: tuple[ManifestEntry, ...]
    missing: tuple[ManifestEntry, ...]
    conflicted: tuple[ManifestEntry, ...]

    @property
    def status(self) -> ReconcileStatus:
        if self.conflicted:
            return ReconcileStatus.CONFLICTED
        if self.missing:
            return ReconcileStatus.MISSING
        return ReconcileStatus.DURABLE

    @property
    def complete(self) -> bool:
        return self.status is ReconcileStatus.DURABLE


@dataclass(frozen=True)
class ExpectedOutputManifest:
    batch_id: str
    namespace: str
    window_id: str
    revision_seq: int
    entries: tuple[ManifestEntry, ...]

    def identity_set(self) -> frozenset[tuple]:
        return frozenset(entry.logical_identity for entry in self.entries)

    def by_identity(self) -> dict[tuple, ManifestEntry]:
        return {entry.logical_identity: entry for entry in self.entries}

    def to_json(self) -> str:
        return canonical_json(
            {
                "batch_id": self.batch_id,
                "namespace": self.namespace,
                "window_id": self.window_id,
                "revision_seq": int(self.revision_seq),
                "entries": [entry.to_dict() for entry in self.entries],
            }
        )

    @classmethod
    def from_json(cls, payload: str) -> "ExpectedOutputManifest":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ManifestError(f"invalid manifest json: {exc}") from exc
        return cls(
            batch_id=str(data["batch_id"]),
            namespace=str(data["namespace"]),
            window_id=str(data["window_id"]),
            revision_seq=int(data["revision_seq"]),
            entries=tuple(ManifestEntry.from_dict(item) for item in data["entries"]),
        )

    def reconcile(self, existing: ExistingState) -> ReconcileReport:
        """Set equality over manifest identities; hash/revision drift is CONFLICTED."""
        found = existing.by_identity()
        durable: list[ManifestEntry] = []
        missing: list[ManifestEntry] = []
        conflicted: list[ManifestEntry] = []
        for entry in self.entries:
            row = found.get(entry.logical_identity)
            if row is None:
                missing.append(entry)
            elif (
                row.source_set_hash == entry.source_set_hash
                and int(row.revision_seq) == int(entry.revision_seq)
            ):
                durable.append(entry)
            else:
                conflicted.append(entry)
        return ReconcileReport(tuple(durable), tuple(missing), tuple(conflicted))


def build_manifest(
    result: Any,
    *,
    batch_id: str,
    namespace: str,
    window_id: str,
    revision_seq: int,
) -> ExpectedOutputManifest:
    """Built from the complete validated engine result, before any persistence."""
    entries: list[ManifestEntry] = []
    for table in PERSISTENCE_ORDER:
        rows = getattr(result, RESULT_FIELD_BY_TABLE[table])
        for row in rows:
            entries.append(
                ManifestEntry(
                    target_table=table,
                    logical_identity=logical_identity(table, row),
                    source_set_hash=str(row.source_set_hash),
                    revision_seq=int(row.revision_seq),
                    payload_digest=payload_digest(table, row),
                )
            )
    identities = [entry.logical_identity for entry in entries]
    if len(set(identities)) != len(identities):
        raise ManifestError("duplicate logical identity in expected output manifest")
    return ExpectedOutputManifest(
        batch_id=batch_id,
        namespace=namespace,
        window_id=window_id,
        revision_seq=int(revision_seq),
        entries=tuple(entries),
    )


def output_digest(manifest: ExpectedOutputManifest) -> str:
    """Deterministic digest retained for non-target Gold 2 outputs and evidence."""
    payload = sorted(
        (entry.target_table, list(entry.logical_identity), entry.payload_digest)
        for entry in manifest.entries
    )
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def build_ledger_row(
    *,
    namespace: str,
    source_set_hash: str,
    definition_version: str,
    revision_seq: int,
    disposition: str,
    computed_at: datetime,
    gold_schema_version: str,
    error_message: str = "",
) -> GoldProcessingLedger:
    if disposition not in LEDGER_DISPOSITIONS:
        raise LedgerTransitionError(f"unknown disposition {disposition!r}")
    return GoldProcessingLedger(
        namespace=namespace,
        source_set_hash=source_set_hash,
        definition_version=definition_version,
        definition_major=DEFINITION_MAJOR,
        definition_minor=DEFINITION_MINOR,
        revision_seq=int(revision_seq),
        disposition=disposition,
        computed_at=computed_at.astimezone(timezone.utc),
        error_message=error_message,
        gold_schema_version=gold_schema_version,
    )


def work_unit_terminal_disposition(state: WorkUnitState) -> Optional[str]:
    return {
        WorkUnitState.CHECKPOINTED: DISPOSITION_CHECKPOINTED,
        WorkUnitState.REPLAYED: DISPOSITION_REPLAYED,
        WorkUnitState.QUARANTINED: DISPOSITION_QUARANTINED,
    }.get(state)


def input_digest(records: Sequence[Any]) -> str:
    """Digest of the exact engine input tuple, for restart evidence."""
    payload = [
        canonical_json(asdict(record) if is_dataclass(record) else record)
        for record in records
    ]
    return hashlib.sha256("\n".join(payload).encode("utf-8")).hexdigest()


def model_columns(model: type) -> tuple[str, ...]:
    return tuple(field.name for field in fields(model))
