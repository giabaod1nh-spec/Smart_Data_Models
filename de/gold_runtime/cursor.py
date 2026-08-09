"""Per-source cursor codec and read receipts (Plan §10, Appendix R).

The fact cursor is the complete tuple
``(processed_at, source_topic, source_partition, source_offset, source_payload_hash)``
compared lexicographically with a strict ``>`` predicate against a per-poll
upper-bound snapshot. ``processed_at``-only pagination and OFFSET are forbidden.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

FACT_CURSOR_COLUMNS: tuple[str, ...] = (
    "processed_at",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_payload_hash",
)
DIMENSION_CURSOR_COLUMNS: tuple[str, ...] = (
    "effective_from",
    "approved_source_hash",
    "stable_id",
)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class CursorError(ValueError):
    """Malformed cursor payload or ordering violation."""


class SourceIdentityConflict(CursorError):
    """One logical source identity carries two different payload hashes."""


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def normalize_hash(value: Any) -> str:
    """FixedString(64) values arrive as bytes/memoryview; compare normalized hex."""
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", errors="replace")
    return str("" if value is None else value).rstrip("\x00").strip()


@dataclass(frozen=True, order=False)
class FactCursor:
    processed_at: datetime
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str

    def key(self) -> tuple:
        return (
            self.processed_at.astimezone(timezone.utc),
            self.source_topic,
            int(self.source_partition),
            int(self.source_offset),
            self.source_payload_hash,
        )

    def is_after(self, other: "FactCursor") -> bool:
        return self.key() > other.key()

    def to_dict(self) -> dict:
        return {
            "processed_at": _iso(self.processed_at),
            "source_topic": self.source_topic,
            "source_partition": int(self.source_partition),
            "source_offset": int(self.source_offset),
            "source_payload_hash": self.source_payload_hash,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactCursor":
        missing = [name for name in FACT_CURSOR_COLUMNS if name not in payload]
        if missing:
            raise CursorError(f"cursor payload missing {missing}")
        return cls(
            processed_at=_as_utc(payload["processed_at"]),
            source_topic=str(payload["source_topic"]),
            source_partition=int(payload["source_partition"]),
            source_offset=int(payload["source_offset"]),
            source_payload_hash=normalize_hash(payload["source_payload_hash"]),
        )

    @classmethod
    def from_json(cls, payload: str) -> "FactCursor":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CursorError(f"invalid cursor json: {exc}") from exc
        return cls.from_dict(data)

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "FactCursor":
        if row.get("source_topic") in (None, ""):
            raise CursorError("source_topic is null/empty; rejected at schema validation")
        return cls.from_dict({name: row[name] for name in FACT_CURSOR_COLUMNS})


ZERO_FACT_CURSOR = FactCursor(EPOCH, "", -1, -1, "")


@dataclass(frozen=True)
class DimensionCursor:
    """Silver dimensions expose no offset tuple; ordering uses effective time.

    ``approved_source_hash`` is the ``gold-lineage-hash-v1`` value computed for the
    row. It participates in conflict detection (same identity/effective time with a
    different hash) but not in the SQL predicate, because Silver dimension DDL has no
    physical hash column for run/scenario/approach.
    """

    effective_from: datetime
    approved_source_hash: str
    stable_id: str

    def key(self) -> tuple:
        return (self.effective_from.astimezone(timezone.utc), self.stable_id)

    def is_after(self, other: "DimensionCursor") -> bool:
        return self.key() > other.key()

    def to_dict(self) -> dict:
        return {
            "effective_from": _iso(self.effective_from),
            "approved_source_hash": self.approved_source_hash,
            "stable_id": self.stable_id,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "DimensionCursor":
        missing = [name for name in DIMENSION_CURSOR_COLUMNS if name not in payload]
        if missing:
            raise CursorError(f"dimension cursor payload missing {missing}")
        return cls(
            effective_from=_as_utc(payload["effective_from"]),
            approved_source_hash=normalize_hash(payload["approved_source_hash"]),
            stable_id=str(payload["stable_id"]),
        )

    @classmethod
    def from_json(cls, payload: str) -> "DimensionCursor":
        return cls.from_dict(json.loads(payload))


ZERO_DIMENSION_CURSOR = DimensionCursor(EPOCH, "", "")


def fact_cursor_predicate(lower: str = "p", upper: str = "u") -> str:
    """Exact Appendix R lexicographic predicate with a bounded upper snapshot."""
    return (
        f"(     (processed_at > {{{lower}_at:DateTime64(3)}})\n"
        f"   OR (processed_at = {{{lower}_at:DateTime64(3)}} AND source_topic > {{{lower}_topic:String}})\n"
        f"   OR (processed_at = {{{lower}_at:DateTime64(3)}} AND source_topic = {{{lower}_topic:String}}"
        f" AND source_partition > {{{lower}_partition:Int32}})\n"
        f"   OR (processed_at = {{{lower}_at:DateTime64(3)}} AND source_topic = {{{lower}_topic:String}}"
        f" AND source_partition = {{{lower}_partition:Int32}} AND source_offset > {{{lower}_offset:Int64}})\n"
        f"   OR (processed_at = {{{lower}_at:DateTime64(3)}} AND source_topic = {{{lower}_topic:String}}"
        f" AND source_partition = {{{lower}_partition:Int32}} AND source_offset = {{{lower}_offset:Int64}}"
        f" AND toString(source_payload_hash) > {{{lower}_hash:String}}))\n"
        f"  AND (     (processed_at < {{{upper}_at:DateTime64(3)}})\n"
        f"        OR (processed_at = {{{upper}_at:DateTime64(3)}} AND source_topic < {{{upper}_topic:String}})\n"
        f"        OR (processed_at = {{{upper}_at:DateTime64(3)}} AND source_topic = {{{upper}_topic:String}}"
        f" AND source_partition < {{{upper}_partition:Int32}})\n"
        f"        OR (processed_at = {{{upper}_at:DateTime64(3)}} AND source_topic = {{{upper}_topic:String}}"
        f" AND source_partition = {{{upper}_partition:Int32}} AND source_offset < {{{upper}_offset:Int64}})\n"
        f"        OR (processed_at = {{{upper}_at:DateTime64(3)}} AND source_topic = {{{upper}_topic:String}}"
        f" AND source_partition = {{{upper}_partition:Int32}} AND source_offset = {{{upper}_offset:Int64}}"
        f" AND toString(source_payload_hash) <= {{{upper}_hash:String}}))"
    )


def fact_cursor_order_by() -> str:
    return "ORDER BY " + ", ".join(FACT_CURSOR_COLUMNS)


def fact_cursor_order_by_desc() -> str:
    """True lexicographic DESC — every cursor component must be DESC, not only the last."""
    return "ORDER BY " + ", ".join(f"{column} DESC" for column in FACT_CURSOR_COLUMNS)


def cursor_parameters(cursor: FactCursor, prefix: str) -> dict[str, Any]:
    # Keep tz-aware UTC: naive datetimes break clickhouse_connect binding on Windows
    # when the local offset would push the instant before 1970-01-01.
    return {
        f"{prefix}_at": cursor.processed_at.astimezone(timezone.utc),
        f"{prefix}_topic": cursor.source_topic,
        f"{prefix}_partition": int(cursor.source_partition),
        f"{prefix}_offset": int(cursor.source_offset),
        f"{prefix}_hash": cursor.source_payload_hash,
    }


# ── logical identity and source-set hashing ─────────────────────────────────

IDENTITY_COLUMNS: dict[str, tuple[str, ...]] = {
    "silver_fact_traffic_observation": (
        "simulation_run_id", "intersection_id", "direction", "source_entity_id",
        "simulation_time_sec",
    ),
    "silver_fact_signal_state": (
        "simulation_run_id", "intersection_id", "direction", "source_entity_id",
        "simulation_time_sec",
    ),
    "silver_fact_intersection_state": (
        "simulation_run_id", "intersection_id", "source_entity_id", "simulation_time_sec",
    ),
    "silver_fact_camera_observation": (
        "simulation_run_id", "intersection_id", "source_entity_id", "simulation_time_sec",
    ),
    "silver_fact_run_event": ("simulation_run_id", "event_simulation_time"),
}


def _scalar(value: Any) -> str:
    value = normalize_hash(value) if isinstance(value, (bytes, bytearray, memoryview)) else value
    if isinstance(value, float):
        return repr(float(value))
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "\x00NULL"
    if isinstance(value, datetime):
        return _iso(value)
    return str(value)


def row_identity(source_name: str, row: Mapping[str, Any]) -> tuple:
    columns = IDENTITY_COLUMNS[source_name]
    return (source_name,) + tuple(_scalar(row.get(column)) for column in columns)


def source_set_hash(entries: Iterable[tuple]) -> str:
    """Canonical SHA-256 over sorted identity+hash entries; order independent."""
    payload = "\n".join(
        "\x1f".join(_scalar(part) for part in entry) for entry in sorted(entries, key=lambda e: tuple(_scalar(p) for p in e))
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def rows_source_set_hash(source_name: str, rows: Sequence[Mapping[str, Any]]) -> str:
    return source_set_hash(
        row_identity(source_name, row) + (normalize_hash(row.get("source_payload_hash")),)
        for row in rows
    )


@dataclass(frozen=True)
class ReadReceipt:
    source_name: str
    first_cursor: Optional[FactCursor]
    last_cursor: Optional[FactCursor]
    physical_count: int
    logical_count: int
    duplicate_count: int
    source_set_hash: str
    conflicts: tuple[tuple, ...] = ()


@dataclass(frozen=True)
class DeduplicationResult:
    rows: tuple[dict, ...]
    duplicates: int
    conflicts: tuple[tuple, ...]


def deduplicate_rows(source_name: str, rows: Sequence[Mapping[str, Any]]) -> DeduplicationResult:
    """Identical identity + payload hash is idempotent; a differing hash is a conflict."""
    seen: dict[tuple, str] = {}
    unique: list[dict] = []
    duplicates = 0
    conflicts: list[tuple] = []
    for row in rows:
        identity = row_identity(source_name, row)
        payload_hash = normalize_hash(row.get("source_payload_hash"))
        previous = seen.get(identity)
        if previous is None:
            seen[identity] = payload_hash
            unique.append(dict(row))
        elif previous == payload_hash:
            duplicates += 1
        else:
            conflicts.append(identity)
    return DeduplicationResult(tuple(unique), duplicates, tuple(sorted(set(conflicts))))


def build_receipt(source_name: str, rows: Sequence[Mapping[str, Any]]) -> ReadReceipt:
    if not rows:
        return ReadReceipt(source_name, None, None, 0, 0, 0, source_set_hash(()))
    cursors = [FactCursor.from_row(row) for row in rows]
    keys = [cursor.key() for cursor in cursors]
    if keys != sorted(keys):
        raise CursorError(f"{source_name}: batch is not ordered by the approved cursor")
    result = deduplicate_rows(source_name, rows)
    return ReadReceipt(
        source_name=source_name,
        first_cursor=cursors[0],
        last_cursor=cursors[-1],
        physical_count=len(rows),
        logical_count=len(result.rows),
        duplicate_count=result.duplicates,
        source_set_hash=rows_source_set_hash(source_name, result.rows),
        conflicts=result.conflicts,
    )
