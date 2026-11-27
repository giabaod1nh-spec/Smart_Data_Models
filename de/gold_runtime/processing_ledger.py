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


# Core metadata differs from the legacy ClickHouse output manifest above.

import time

from de.gold.windowing import TUMBLING_SPEC, tumbling_bounds
from de.gold_runtime.checkpoint_store import CoreStateFault
from de.silver.incremental_contract import canonical_bytes, canonical_hash


def canonical_run_identity(run_id, generation, topology_digest, manifest_identity):
    return dict(source_namespace="live", simulation_run_id=run_id,
                run_generation=generation, topology_digest=topology_digest,
                manifest_identity=manifest_identity)


def core_run_identity(entry):
    return canonical_run_identity(entry.run_id, entry.run_generation, entry.topology_digest,
                                  entry.manifest_identity)


def core_window_identity(entry, simulation_time, duration):
    start, end = tumbling_bounds(simulation_time, duration)
    return dict(core_run_identity(entry), window_spec_version=TUMBLING_SPEC,
                window_duration_sec=duration, start_simulation_time=start, end_simulation_time=end)


def core_bundle_coordinates(entry):
    fact = entry.canonical_bundle.intersection
    run = core_run_identity(entry)
    if (any(fact.get(field) != value for field, value in run.items() if field != "source_namespace")
            or fact.get("event_id") != entry.event_id
            or fact.get("bronze_semantic_revision") != entry.semantic_revision
            or fact.get("silver_schema_version") != "v1"):
        raise CoreStateFault("Bundle event, revision, schema or archive identity differs")
    simulation_time = fact["simulation_time_seconds"]
    tumbling_bounds(simulation_time, 300)
    return (simulation_time, fact["scenario_id"], fact["intersection_id"], fact["cycle_sequence"])


class CoreWorkLedger:
    """Apply verified source entries and maintain a pending work generation."""

    def __init__(self, store, namespace, token, replay=None):
        self.store, self.namespace, self.token, self.replay = store, namespace, token, replay

    def acknowledge(self, page, barrier=None):
        receipt = page.receipt
        if receipt is None:
            return False
        kind = receipt.receipt_kind
        binding = self.store.binding()
        if binding is None or binding["binding_id"] != receipt.source_binding_id:
            raise CoreStateFault("Receipt does not match persisted Gold source binding")
        after = self.store.cursors()[kind]
        if receipt.returned_after_sequence < after:
            for entry in page.entries:
                sequence = entry.change_sequence if kind == "CHANGE" else entry.invalidation_sequence
                if sequence > after:
                    raise CoreStateFault("Overlapping receipt crosses committed cursor")
                row = self.store.conn.execute(
                    "SELECT transition_hash,bundle_hash FROM core_transition WHERE kind=? AND sequence=?",
                    (kind, sequence)).fetchone()
                if row is None or row[0] != entry.source_transition_hash:
                    raise CoreStateFault("An immutable acknowledged transition changed")
                if (kind == "CHANGE" and row["bundle_hash"] is not None and entry.bundle_hash is not None
                        and row["bundle_hash"] != entry.bundle_hash):
                    raise CoreStateFault("An acknowledged canonical bundle changed")
            # The independent invalidation stream applies any newer correction.
            return False

        def apply(db):
            for entry in page.entries:
                self._apply(db, kind, entry, receipt.receipt_id)

        return self.store.acknowledge(self.namespace, self.token, receipt,
                                      canonical_bytes(receipt), apply, barrier)

    def _apply(self, db, kind, entry, receipt_id):
        run = core_run_identity(entry)
        run_key = canonical_hash(run)
        db.execute("INSERT OR IGNORE INTO core_run(id,identity) VALUES (?,?)",
                   (run_key, canonical_bytes(run)))
        disposition = entry.effective_disposition if kind == "CHANGE" else "SUPPRESS"
        sequence = entry.change_sequence if kind == "CHANGE" else entry.invalidation_sequence
        bundle_hash = entry.bundle_hash if kind == "CHANGE" else None
        provenance = dict(run, semantic_revision=entry.semantic_revision)
        if kind == "CHANGE":
            provenance.update(change_type=entry.change_type, previous_state=entry.previous_state,
                              new_state=entry.new_state, invalidation_sequence=entry.invalidation_sequence,
                              source_invalidation_id=entry.source_invalidation_id,
                              superseding_change_sequence=entry.superseding_change_sequence)
            if disposition == "BUNDLE_VALID":
                fact = entry.canonical_bundle.intersection
                provenance.update({key: fact[key] for key in (
                    "scenario_id", "simulation_time_seconds", "intersection_id", "cycle_sequence",
                    "captured_at", "source_payload_hash", "source_raw_ingestion_id", "source_topic",
                    "source_partition", "source_offset", "source_physical_record_hash",
                    "source_byte_hash", "bronze_canonical_hash")})
        else:
            provenance.update(invalidation_id=entry.invalidation_id, mirror_status=entry.mirror_status,
                              previous_hash=entry.previous_immutable_envelope_hash,
                              conflicting_hash=entry.conflicting_immutable_envelope_hash)
        db.execute("INSERT INTO core_transition VALUES (?,?,?,?,?,?,?,?)", (
            kind, sequence, entry.event_id, entry.source_transition_hash, disposition,
            bundle_hash, receipt_id, canonical_bytes(provenance)))
        if kind == "INVALIDATION":
            self._suppress(db, entry, run_key, receipt_id)
        elif disposition == "BUNDLE_VALID":
            self._accept(db, entry, run_key, receipt_id)
        else:
            suppression = db.execute("SELECT * FROM core_suppression WHERE event_id=?",
                                     (entry.event_id,)).fetchone()
            if (suppression is None or suppression["sequence"] != entry.invalidation_sequence
                    or suppression["run_key"] != run_key):
                raise CoreStateFault("Change conflict lacks its durable linked invalidation")
            if disposition == "CONFLICTED" and (
                suppression["transition_hash"] != entry.source_transition_hash
                or suppression["invalidation_id"] != entry.source_invalidation_id
                or suppression["revision"] != entry.semantic_revision
            ):
                raise CoreStateFault("Conflict transition differs from its durable invalidation")

    def _accept(self, db, entry, run_key, receipt_id):
        simulation_time, scenario, node, cycle = core_bundle_coordinates(entry)
        suppressed = db.execute("SELECT * FROM core_suppression WHERE event_id=?",
                                (entry.event_id,)).fetchone()
        if suppressed is not None:
            if (suppressed["run_key"], suppressed["simulation_time"], suppressed["intersection_id"],
                suppressed["cycle"]) != (run_key, simulation_time, node, cycle):
                raise CoreStateFault("Suppressed event has different input coordinates")
            return
        if self.replay is not None and not self.replay.includes(entry, simulation_time):
            return
        db.execute("INSERT INTO core_event VALUES (?,?,?,?,?,?,?,?,?,?)", (
            entry.event_id, run_key, entry.change_sequence, entry.semantic_revision,
            entry.source_transition_hash, entry.bundle_hash, scenario, node, cycle, simulation_time))
        db.execute("UPDATE core_run SET observed_time=max(observed_time,?) WHERE id=?",
                   (int(simulation_time), run_key))
        db.execute("INSERT OR IGNORE INTO core_promotion VALUES (?,?)", (run_key, time.time()))
        for duration in (60, 300):
            identity = core_window_identity(entry, simulation_time, duration)
            window_id = canonical_hash(identity)
            db.execute("""INSERT OR IGNORE INTO core_window(id,run_key,spec,duration,start,end)
                          VALUES (?,?,?,?,?,?)""", (
                window_id, run_key, TUMBLING_SPEC, duration, identity["start_simulation_time"],
                identity["end_simulation_time"]))
            db.execute("INSERT INTO core_membership VALUES (?,?)", (window_id, entry.event_id))
            self._advance_work(db, window_id, receipt_id)

    def _suppress(self, db, entry, run_key, receipt_id):
        tumbling_bounds(entry.simulation_time_seconds, 300)
        coords = (run_key, entry.simulation_time_seconds, entry.intersection_id, entry.cycle_sequence)
        existing = db.execute("SELECT * FROM core_event WHERE id=?", (entry.event_id,)).fetchone()
        if existing and (existing["run_key"], existing["simulation_time"], existing["intersection_id"],
                         existing["cycle"]) != coords:
            raise CoreStateFault("Invalidation does not match the accepted event coordinates")
        db.execute("INSERT INTO core_suppression VALUES (?,?,?,?,?,?,?,?,?)", (
            entry.event_id, entry.invalidation_sequence, entry.invalidation_id, *coords,
            entry.semantic_revision, entry.source_transition_hash))
        # At most two memberships under the frozen window specification.
        impacted = db.execute("SELECT window_id FROM core_membership WHERE event_id=?",
                              (entry.event_id,)).fetchall()
        db.execute("DELETE FROM core_membership WHERE event_id=?", (entry.event_id,))
        for (window_id,) in impacted:
            self._advance_work(db, window_id, receipt_id)

    @staticmethod
    def _advance_work(db, window_id, receipt_id):
        eligible, generation = db.execute("SELECT eligible,generation FROM core_window WHERE id=?",
                                          (window_id,)).fetchone()
        db.execute("""UPDATE core_work SET state='SUPERSEDED' WHERE window_id=? AND generation=?
                      AND state IN ('OPEN','PENDING','RUNNING')""", (window_id, generation))
        db.execute("UPDATE core_window SET generation=generation+1 WHERE id=?", (window_id,))
        now = time.time()
        db.execute("""INSERT INTO core_work(window_id,generation,state,source_receipt,created_at,eligible_at)
                      VALUES (?,?,?,?,?,?)""", (window_id, generation + 1,
                      "PENDING" if eligible else "OPEN", receipt_id, now, now if eligible else None))

    def promote(self, limit):
        if self.store.conn.execute("SELECT 1 FROM core_promotion LIMIT 1").fetchone() is None:
            return 0
        with self.store.transaction(self.namespace, self.token) as db:
            run_key, observed = db.execute("""SELECT p.run_key,r.observed_time FROM core_promotion p
                JOIN core_run r ON r.id=p.run_key ORDER BY p.queued_at,p.run_key LIMIT 1""").fetchone()
            candidates = db.execute("""SELECT id,generation FROM core_window
                WHERE run_key=? AND eligible=0 AND end<=? ORDER BY end LIMIT ?""",
                (run_key, observed, limit)).fetchall()
            for window_id, generation in candidates:
                db.execute("UPDATE core_window SET eligible=1 WHERE id=?", (window_id,))
                db.execute("""UPDATE core_work SET state='PENDING', eligible_at=?
                              WHERE window_id=? AND generation=? AND state='OPEN'""",
                           (time.time(), window_id, generation))
            if len(candidates) < limit:
                db.execute("DELETE FROM core_promotion WHERE run_key=?", (run_key,))
            else:
                db.execute("UPDATE core_promotion SET queued_at=? WHERE run_key=?", (time.time(), run_key))
            return len(candidates) + 1

    def claim(self):
        db = self.store.conn
        row = db.execute("""SELECT * FROM core_work WHERE state='RUNNING'
                            ORDER BY eligible_at,window_id,generation LIMIT 1""").fetchone()
        if row is not None:
            return row
        row = db.execute("""SELECT * FROM core_work WHERE state='PENDING'
                            ORDER BY eligible_at,window_id,generation LIMIT 1""").fetchone()
        if row is not None:
            with self.store.transaction(self.namespace, self.token) as tx:
                tx.execute("UPDATE core_work SET state='RUNNING' WHERE window_id=? AND generation=?",
                           (row["window_id"], row["generation"]))
        return row

    def members(self, window_id, after_event="", limit=1):
        return self.store.conn.execute("""SELECT e.* FROM core_membership m
            JOIN core_event e ON e.id=m.event_id WHERE m.window_id=? AND m.event_id>?
            ORDER BY m.event_id LIMIT ?""", (window_id, after_event, limit)).fetchall()

    def current_generation(self, window_id):
        row = self.store.conn.execute("SELECT generation FROM core_window WHERE id=?", (window_id,)).fetchone()
        return None if row is None else row[0]

    def finish(self, work, input_hash, count, *, seal=None):
        with self.store.transaction(self.namespace, self.token) as db:
            contract, schema, processor = db.execute(
                "SELECT contract_version,schema_version,processor_version FROM core_binding").fetchone()
            work_id = canonical_hash([self.namespace, work["window_id"], work["generation"], input_hash,
                                      contract, schema, processor])
            changed = db.execute("""UPDATE core_work SET state='COMPLETE', input_hash=?,input_count=?,
                completed_at=?,work_id=? WHERE window_id=? AND generation=? AND state='RUNNING'
                AND generation=(SELECT generation FROM core_window WHERE id=?)""", (
                input_hash, count, time.time(), work_id, work["window_id"],
                work["generation"], work["window_id"]))
            if changed.rowcount != 1:
                return False
            if seal is not None:
                seal(db, work_id)
            return True
