"""Silver Plan 3 — BronzeReader: partition-aware, deterministic ClickHouse Bronze reads.

Implements Plan 3 §6/§7 (reader design, source allowlist, duplicate collapse, discovery).
Only ``bronze_entity_events`` and ``bronze_run_events`` are ever read; no other table name
is accepted, and physical column names follow §32.9 (``raw_ingestion_id`` vs
``bronze_ingestion_id``).
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import groupby
from typing import Any, Dict, List, Optional, Sequence, Tuple

import clickhouse_connect

from de.silver.config import (
    SOURCE_TABLE_ENTITY,
    SOURCE_TABLE_RUN,
    ReadReceipt,
    SilverSettings,
    SourceStream,
)
from de.silver.input_models import BronzeEntityInputRecord, BronzeInputRecord, BronzeRunInputRecord
from de.silver.repositories import SchemaMismatchError, SourceOffsetConflictError

# Re-exported so callers may import either from readers or repositories (Plan 3 instructions).
__all__ = [
    "BronzeReader",
    "SourceSchemaReport",
    "SourceOffsetConflictError",
    "SchemaMismatchError",
]

ENTITY_READ_COLUMNS: Tuple[str, ...] = (
    "topic", "partition", "offset", "raw_ingestion_id", "event_id", "event_type",
    "contract_version", "simulation_run_id", "simulation_time", "cycle_sequence",
    "captured_at", "entity_id", "entity_type", "entity_payload_hash",
    "entity_payload_json", "bronze_canonical_hash", "processed_at", "scenario_id",
    "bronze_ingestion_id",
)

RUN_READ_COLUMNS: Tuple[str, ...] = (
    "topic", "partition", "offset", "raw_ingestion_id", "event_type", "contract_version",
    "source", "producer_id", "producer_session_id", "simulation_run_id", "started_at",
    "scenario_id", "event_payload_json", "bronze_canonical_hash", "processed_at",
    "bronze_ingestion_id",
)

# Over-fetch multiplier for the physical row limit so a bounded number of duplicate
# physical rows per offset does not silently truncate the logical batch below `limit`.
_OVER_FETCH_FACTOR = 4


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _ts_key(value: Any) -> Any:
    # datetimes compare directly; fall back to string comparison for str timestamps.
    return value


def _rows_to_dicts(result: Any, columns: Sequence[str]) -> List[Dict[str, Any]]:
    cols = list(columns)
    return [dict(zip(cols, row)) for row in result.result_rows]


def _map_entity_row(d: Dict[str, Any]) -> BronzeEntityInputRecord:
    scenario_id = d.get("scenario_id")
    return BronzeEntityInputRecord(
        topic=str(d["topic"]),
        partition=int(d["partition"]),
        offset=int(d["offset"]),
        raw_ingestion_id=_to_str(d["raw_ingestion_id"]),
        event_id=_to_str(d["event_id"]),
        event_type=str(d["event_type"]),
        contract_version=str(d["contract_version"]),
        simulation_run_id=str(d["simulation_run_id"]),
        simulation_time=float(d["simulation_time"]),
        cycle_sequence=int(d["cycle_sequence"]),
        captured_at=d["captured_at"],
        entity_id=str(d["entity_id"]),
        entity_type=str(d["entity_type"]),
        entity_payload_hash=_to_str(d["entity_payload_hash"]),
        entity_payload_json=str(d["entity_payload_json"]),
        bronze_canonical_hash=_to_str(d["bronze_canonical_hash"]),
        processed_at=d["processed_at"],
        scenario_id="" if scenario_id is None else str(scenario_id),
    )


def _map_run_row(d: Dict[str, Any]) -> BronzeRunInputRecord:
    scenario_id = d.get("scenario_id")
    return BronzeRunInputRecord(
        topic=str(d["topic"]),
        partition=int(d["partition"]),
        offset=int(d["offset"]),
        raw_ingestion_id=_to_str(d["raw_ingestion_id"]),
        event_type=str(d["event_type"]),
        contract_version=str(d["contract_version"]),
        source=str(d["source"]),
        producer_id=str(d["producer_id"]),
        producer_session_id=str(d["producer_session_id"]),
        simulation_run_id=str(d["simulation_run_id"]),
        started_at=d["started_at"],
        scenario_id="" if scenario_id is None else str(scenario_id),
        event_payload_json=str(d["event_payload_json"]),
        bronze_canonical_hash=_to_str(d["bronze_canonical_hash"]),
        processed_at=d["processed_at"],
    )


def _collapse_rows(dicts: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], int]]:
    """Group physical rows by offset, verify hash consistency, pick the winner per §6.4.

    Returns an ascending-offset list of (selected_row, physical_group_size) tuples.
    Raises SourceOffsetConflictError if two different canonical hashes share one offset.
    """
    dicts_sorted = sorted(dicts, key=lambda d: int(d["offset"]))
    groups: List[Tuple[Dict[str, Any], int]] = []
    for offset, group_iter in groupby(dicts_sorted, key=lambda d: int(d["offset"])):
        group = list(group_iter)
        hashes = {_to_str(g["bronze_canonical_hash"]) for g in group}
        if len(hashes) > 1:
            raise SourceOffsetConflictError(
                f"Conflicting bronze_canonical_hash at offset={offset}: {sorted(hashes)}"
            )
        # Latest physical processed_at wins; tie-break on greatest bronze_ingestion_id.
        group.sort(
            key=lambda d: (_ts_key(d["processed_at"]), _to_str(d["bronze_ingestion_id"])),
            reverse=True,
        )
        groups.append((group[0], len(group)))
    return groups


def _stream_sort_key(stream: SourceStream) -> Tuple[str, int, int]:
    priority = 0 if stream.source_table == SOURCE_TABLE_RUN else 1
    return (stream.topic, stream.partition, priority)


@dataclass(frozen=True)
class SourceSchemaReport:
    tables_ok: Dict[str, bool]


class BronzeReader:
    """Partition-aware, deterministic Bronze reader (Plan 3 §6/§7)."""

    def __init__(self, settings: SilverSettings, client: Any = None) -> None:
        self.settings = settings
        self.database = settings.clickhouse_database
        self._client = client

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._client is not None:
            return
        self._client = clickhouse_connect.get_client(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_user,
            password=self.settings.clickhouse_password,
            database=self.database,
            secure=self.settings.clickhouse_secure,
            connect_timeout=self.settings.clickhouse_connect_timeout,
            send_receive_timeout=self.settings.clickhouse_query_timeout,
            settings={"async_insert": 0, "wait_end_of_query": 1},
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def initialized(self) -> bool:
        """True once a ClickHouse client is bound (injected or via connect())."""
        return self._client is not None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Bronze reader not connected")
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    # -- schema ---------------------------------------------------------------

    def verify_source_schema(self) -> SourceSchemaReport:
        required = {
            SOURCE_TABLE_ENTITY: set(ENTITY_READ_COLUMNS),
            SOURCE_TABLE_RUN: set(RUN_READ_COLUMNS),
        }
        report: Dict[str, bool] = {}
        for table, cols in required.items():
            r = self.client.query(
                "SELECT name FROM system.columns WHERE database={db:String} AND table={t:String}",
                parameters={"db": self.database, "t": table},
            )
            present = {str(row[0]) for row in r.result_rows}
            missing = cols - present
            if missing:
                raise SchemaMismatchError(f"{table} missing columns: {sorted(missing)}")
            report[table] = True
        return SourceSchemaReport(tables_ok=report)

    # -- discovery --------------------------------------------------------------

    def discover_streams(self, topic_allowlist: Sequence[str]) -> Tuple[SourceStream, ...]:
        allow = tuple(topic_allowlist)
        if not allow:
            raise SchemaMismatchError("topic_allowlist must not be empty")
        streams: List[SourceStream] = []
        for source_table in (SOURCE_TABLE_RUN, SOURCE_TABLE_ENTITY):
            sql = f"""
                SELECT DISTINCT topic, partition FROM {self.database}.{source_table}
                WHERE topic IN {{topics:Array(String)}}
            """
            r = self.client.query(sql, parameters={"topics": list(allow)})
            for row in r.result_rows:
                topic, partition = str(row[0]), int(row[1])
                if topic not in allow:
                    raise SchemaMismatchError(f"Discovered topic outside allowlist: {topic!r}")
                if partition < 0:
                    raise SchemaMismatchError(f"Discovered negative partition: {partition}")
                streams.append(SourceStream(source_table, topic, partition))
        return tuple(sorted(streams, key=_stream_sort_key))

    # -- offsets ------------------------------------------------------------

    def min_offset(self, stream: SourceStream) -> Optional[int]:
        return self._offset_query("min", stream)

    def max_offset(self, stream: SourceStream) -> Optional[int]:
        return self._offset_query("max", stream)

    def _offset_query(self, agg: str, stream: SourceStream) -> Optional[int]:
        sql = f"""
            SELECT {agg}(offset) FROM {self.database}.{stream.source_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
        """
        r = self.client.query(
            sql, parameters={"topic": stream.topic, "part": int(stream.partition)}
        )
        if not r.result_rows or r.result_rows[0][0] is None:
            return None
        return int(r.result_rows[0][0])

    # -- batch reads ------------------------------------------------------------

    def fetch_batch(
        self,
        stream: SourceStream,
        after_offset: int,
        limit: int,
        end_offset_exclusive: Optional[int] = None,
    ) -> Tuple[Tuple[BronzeInputRecord, ...], ReadReceipt]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be within 1..500")
        is_entity = stream.source_table == SOURCE_TABLE_ENTITY
        cols = ENTITY_READ_COLUMNS if is_entity else RUN_READ_COLUMNS
        physical_limit = limit * _OVER_FETCH_FACTOR

        predicate = "offset > {after:Int64}"
        params: Dict[str, Any] = {
            "topic": stream.topic,
            "part": int(stream.partition),
            "after": int(after_offset),
            "lim": int(physical_limit),
        }
        if end_offset_exclusive is not None:
            predicate += " AND offset < {end:Int64}"
            params["end"] = int(end_offset_exclusive)

        sql = f"""
            SELECT {", ".join(cols)}
            FROM {self.database}.{stream.source_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}} AND {predicate}
            ORDER BY offset
            LIMIT {{lim:UInt32}}
        """
        r = self.client.query(sql, parameters=params)
        dicts = _rows_to_dicts(r, cols)
        physical_fetched = len(dicts)

        groups = _collapse_rows(dicts)
        taken = groups[:limit]
        selected = [g[0] for g in taken]
        physical_count = sum(g[1] for g in taken)
        logical_count = len(taken)
        duplicate_count = physical_count - logical_count

        mapper = _map_entity_row if is_entity else _map_run_row
        records = tuple(mapper(d) for d in selected)

        receipt = ReadReceipt(
            first_offset=int(selected[0]["offset"]) if selected else None,
            last_offset=int(selected[-1]["offset"]) if selected else None,
            logical_count=logical_count,
            physical_count=physical_count,
            duplicate_count=duplicate_count,
        )
        # physical_fetched is intentionally unused beyond documentation of the over-fetch factor.
        del physical_fetched
        return records, receipt
