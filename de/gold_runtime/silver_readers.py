"""Bounded, ordered Silver reads (Plan §9 column map, §10 reader contract).

Every SELECT lists its columns explicitly, binds parameters, is bounded by a
per-poll upper-bound cursor snapshot and a row limit, and orders by the approved
cursor. Bronze, Raw, quarantine, Silver ledger and replay mirrors are never read.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

from de.gold_runtime.config import (
    ALLOWED_SOURCE_TABLES,
    DIM_SOURCE_TABLES,
    FACT_SOURCE_TABLES,
    SOURCE_TABLE_CAMERA,
    SOURCE_TABLE_DIM_APPROACH,
    SOURCE_TABLE_DIM_INTERSECTION,
    SOURCE_TABLE_DIM_RUN,
    SOURCE_TABLE_DIM_SCENARIO,
    SOURCE_TABLE_INTERSECTION,
    SOURCE_TABLE_RUN_EVENT,
    SOURCE_TABLE_SIGNAL,
    SOURCE_TABLE_TRAFFIC,
    GoldSettings,
)
from de.gold_runtime.cursor import (
    DimensionCursor,
    FactCursor,
    ReadReceipt,
    build_receipt,
    cursor_parameters,
    fact_cursor_order_by,
    fact_cursor_order_by_desc,
    fact_cursor_predicate,
    normalize_hash,
)

LINEAGE_COLUMNS: tuple[str, ...] = (
    "source_bronze_event_id", "source_raw_ingestion_id", "source_topic",
    "source_partition", "source_offset", "source_payload_hash",
)
CURSOR_TAIL: tuple[str, ...] = ("quality_flags", "processed_at", "migration_version")

SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    SOURCE_TABLE_TRAFFIC: (
        "simulation_run_id", "cycle_sequence", "simulation_time_sec", "intersection_id",
        "direction", "source_entity_id", "vehicle_count", "pcu_equivalent",
        "average_speed_kmh", "queue_length_m", "waiting_vehicle_count", "occupancy_pct",
        "arrival_rate_pcu_per_sec", "traffic_status", "spillback_risk",
        "dominant_waiting_reason", "scenario_id", *LINEAGE_COLUMNS, "quality_status",
        *CURSOR_TAIL,
    ),
    SOURCE_TABLE_INTERSECTION: (
        "simulation_run_id", "cycle_sequence", "simulation_time_sec", "intersection_id",
        "source_entity_id", "overall_traffic_status", "derived_traffic_state",
        "current_phase", "has_active_incident", "has_spillback", "is_box_blocked",
        "total_vehicle_count", "scenario_id", *LINEAGE_COLUMNS, *CURSOR_TAIL,
    ),
    SOURCE_TABLE_SIGNAL: (
        "simulation_run_id", "cycle_sequence", "simulation_time_sec", "intersection_id",
        "direction", "source_entity_id", "signal_status", "current_phase",
        "green_duration_sec", "red_duration_sec", "yellow_duration_sec", "timing_mode",
        "scenario_id", *LINEAGE_COLUMNS, *CURSOR_TAIL,
    ),
    SOURCE_TABLE_CAMERA: (
        "simulation_run_id", "cycle_sequence", "simulation_time_sec", "intersection_id",
        "source_entity_id", "vehicle_count", "average_speed_kmh", "occupancy_pct",
        "traffic_status", "incident_detected", "confidence", "recommended_signal_action",
        "incident_type", "incident_severity", "scenario_id", *LINEAGE_COLUMNS,
        *CURSOR_TAIL,
    ),
    SOURCE_TABLE_RUN_EVENT: (
        "simulation_run_id", "event_name", "event_simulation_time", "scenario_id",
        "producer_id", *LINEAGE_COLUMNS, "processed_at", "migration_version",
    ),
    SOURCE_TABLE_DIM_RUN: (
        "simulation_run_id", "scenario_id", "seed", "producer_id", "started_at",
        "ended_at", "run_status", "contract_version", "node_count",
        "source_bronze_run_id", "created_at", "updated_at",
    ),
    SOURCE_TABLE_DIM_SCENARIO: ("scenario_id", "description", "created_at"),
    SOURCE_TABLE_DIM_INTERSECTION: (
        "intersection_id", "intersection_name", "latitude", "longitude", "network_zone",
        "connected_intersections", "source_hash", "source_bronze_event_id", "created_at",
        "updated_at",
    ),
    SOURCE_TABLE_DIM_APPROACH: (
        "intersection_id", "direction", "source_bronze_event_id", "created_at",
        "updated_at",
    ),
}

# Physical evidence used as the dimension cursor's effective time / stable id.
DIMENSION_CURSOR_MAP: dict[str, tuple[str, tuple[str, ...]]] = {
    SOURCE_TABLE_DIM_RUN: ("updated_at", ("simulation_run_id",)),
    SOURCE_TABLE_DIM_SCENARIO: ("created_at", ("scenario_id",)),
    SOURCE_TABLE_DIM_INTERSECTION: ("updated_at", ("intersection_id",)),
    SOURCE_TABLE_DIM_APPROACH: ("updated_at", ("intersection_id", "direction")),
}

# Streams whose maxima feed the watermark (Gold Runtime Contract v1).
WATERMARK_STREAMS: dict[str, str] = {
    SOURCE_TABLE_TRAFFIC: "traffic",
    SOURCE_TABLE_INTERSECTION: "intersection",
    SOURCE_TABLE_SIGNAL: "signal",
    SOURCE_TABLE_CAMERA: "camera",
}


class SilverReadError(Exception):
    """Base Silver read failure."""


class RetryableReadError(SilverReadError):
    """Transient connection/timeout failure; cursor is retained."""


class SourceSchemaError(SilverReadError):
    """Missing table/column or null cursor component; permanent."""


class ForbiddenSourceError(SilverReadError):
    """A non-allowlisted Silver source was requested; permanent."""


@dataclass(frozen=True)
class SchemaReport:
    tables: tuple[str, ...]
    ok: bool
    missing: tuple[str, ...] = ()


def assert_allowed_source(source_name: str) -> str:
    if source_name not in ALLOWED_SOURCE_TABLES:
        raise ForbiddenSourceError(f"Source outside the Gold3 allowlist: {source_name!r}")
    return source_name


def _cell(value: Any) -> Any:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace").rstrip("\x00")
    return value


def rows_to_dicts(columns: Sequence[str], result_rows: Sequence[Sequence[Any]]) -> list[dict]:
    return [{name: _cell(value) for name, value in zip(columns, row)} for row in result_rows]


def _looks_retryable(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        token in text
        for token in ("timeout", "reset", "broken pipe", "connection", "unavailable", "refused")
    )


class SilverReader:
    """Ordered, bounded reads over the approved Silver sources."""

    def __init__(self, settings: GoldSettings, client: Any = None) -> None:
        self.settings = settings
        self.database = settings.clickhouse_database
        self._client = client
        self._initialized = client is not None

    # -- lifecycle ----------------------------------------------------------

    def connect(self) -> None:
        if self._client is not None:
            return
        import clickhouse_connect

        self._client = clickhouse_connect.get_client(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_user,
            password=self.settings.clickhouse_password,
            database=self.database,
            secure=self.settings.clickhouse_secure,
            connect_timeout=self.settings.clickhouse_connect_timeout,
            send_receive_timeout=self.settings.clickhouse_query_timeout,
        )
        self._initialized = True

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None
                self._initialized = False

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def client(self) -> Any:
        if self._client is None:
            raise SilverReadError("Silver reader not connected")
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    # -- query helper --------------------------------------------------------

    def _query(self, sql: str, parameters: Mapping[str, Any], columns: Sequence[str]) -> list[dict]:
        try:
            result = self.client.query(sql, parameters=dict(parameters))
        except Exception as exc:  # noqa: BLE001 — reclassified into the read taxonomy
            if _looks_retryable(exc):
                raise RetryableReadError(str(exc)) from exc
            raise SilverReadError(str(exc)) from exc
        return rows_to_dicts(columns, result.result_rows)

    # -- schema --------------------------------------------------------------

    def verify_source_schema(self, tables: Optional[Sequence[str]] = None) -> SchemaReport:
        wanted = tuple(tables) if tables is not None else self.settings.source_table_list()
        missing: list[str] = []
        for table in wanted:
            assert_allowed_source(table)
            rows = self._query(
                "SELECT name FROM system.columns "
                "WHERE database={db:String} AND table={tbl:String}",
                {"db": self.database, "tbl": table},
                ("name",),
            )
            present = {str(row["name"]) for row in rows}
            required = set(SOURCE_COLUMNS[table])
            if not present or not required.issubset(present):
                missing.append(table)
        if missing:
            raise SourceSchemaError(f"Missing Silver tables/columns: {missing}")
        return SchemaReport(tuple(wanted), True, ())

    # -- upper-bound snapshot -------------------------------------------------

    def snapshot_upper_bound(self, source_name: str) -> Optional[FactCursor]:
        """One consistent upper bound per source, taken before any batch read."""
        assert_allowed_source(source_name)
        if source_name not in FACT_SOURCE_TABLES:
            raise ForbiddenSourceError(f"{source_name} has no fact cursor")
        sql = (
            "SELECT processed_at, source_topic, source_partition, source_offset, "
            "toString(source_payload_hash) AS source_payload_hash "
            f"FROM {self.database}.{source_name} "
            f"{fact_cursor_order_by_desc()} LIMIT 1"
        )
        rows = self._query(sql, {}, (
            "processed_at", "source_topic", "source_partition", "source_offset",
            "source_payload_hash",
        ))
        if not rows:
            return None
        return FactCursor.from_row(rows[0])

    def snapshot_upper_bounds(
        self, sources: Optional[Sequence[str]] = None
    ) -> dict[str, Optional[FactCursor]]:
        wanted = tuple(sources) if sources is not None else tuple(
            table for table in self.settings.source_table_list() if table in FACT_SOURCE_TABLES
        )
        return {name: self.snapshot_upper_bound(name) for name in wanted}

    # -- incremental fact reads ------------------------------------------------

    def read_fact_batch(
        self,
        source_name: str,
        cursor: FactCursor,
        upper_bound: FactCursor,
        limit: Optional[int] = None,
    ) -> tuple[tuple[dict, ...], ReadReceipt]:
        assert_allowed_source(source_name)
        if source_name not in FACT_SOURCE_TABLES:
            raise ForbiddenSourceError(f"{source_name} is not a fact source")
        columns = SOURCE_COLUMNS[source_name]
        batch = int(limit if limit is not None else self.settings.silver_fetch_batch_size)
        sql = (
            f"SELECT {self._projection(columns)} "
            f"FROM {self.database}.{source_name} "
            f"WHERE {fact_cursor_predicate()} "
            f"{fact_cursor_order_by()} LIMIT {{lim:UInt32}}"
        )
        parameters = {
            **cursor_parameters(cursor, "p"),
            **cursor_parameters(upper_bound, "u"),
            "lim": max(1, min(batch, self.settings.silver_fetch_batch_size)),
        }
        rows = self._query(sql, parameters, columns)
        receipt = build_receipt(source_name, rows)
        return tuple(rows), receipt

    def read_window_rows(
        self,
        source_name: str,
        *,
        simulation_run_id: str,
        window_start_sim_sec: float,
        window_end_sim_sec: float,
        upper_bound: FactCursor,
        limit: Optional[int] = None,
    ) -> tuple[tuple[dict, ...], ReadReceipt]:
        """Bounded window read against the same upper-bound snapshot as the poll."""
        assert_allowed_source(source_name)
        if source_name not in WATERMARK_STREAMS:
            raise ForbiddenSourceError(f"{source_name} is not a windowed fact source")
        columns = SOURCE_COLUMNS[source_name]
        batch = int(limit if limit is not None else self.settings.silver_fetch_batch_size)
        sql = (
            f"SELECT {self._projection(columns)} "
            f"FROM {self.database}.{source_name} "
            "WHERE simulation_run_id = {run:String} "
            "  AND simulation_time_sec >= {w_start:Float64} "
            "  AND simulation_time_sec < {w_end:Float64} "
            f"  AND {self._upper_bound_only()} "
            f"{fact_cursor_order_by()} LIMIT {{lim:UInt32}}"
        )
        parameters = {
            "run": simulation_run_id,
            "w_start": float(window_start_sim_sec),
            "w_end": float(window_end_sim_sec),
            **cursor_parameters(upper_bound, "u"),
            "lim": max(1, batch),
        }
        rows = self._query(sql, parameters, columns)
        receipt = build_receipt(source_name, rows)
        return tuple(rows), receipt

    def max_simulation_time(
        self, source_name: str, *, simulation_run_id: str, upper_bound: FactCursor
    ) -> Optional[float]:
        assert_allowed_source(source_name)
        if source_name not in WATERMARK_STREAMS:
            raise ForbiddenSourceError(f"{source_name} does not contribute to the watermark")
        sql = (
            "SELECT max(simulation_time_sec) AS max_sim "
            f"FROM {self.database}.{source_name} "
            "WHERE simulation_run_id = {run:String} "
            f"  AND {self._upper_bound_only()}"
        )
        rows = self._query(
            sql,
            {"run": simulation_run_id, **cursor_parameters(upper_bound, "u")},
            ("max_sim",),
        )
        if not rows or rows[0]["max_sim"] is None:
            return None
        return float(rows[0]["max_sim"])

    def discover_runs(
        self, upper_bounds: Mapping[str, Optional[FactCursor]], *, limit: int = 100
    ) -> tuple[tuple[str, str], ...]:
        """Bounded (run, scenario) discovery from the traffic stream."""
        bound = upper_bounds.get(SOURCE_TABLE_TRAFFIC)
        if bound is None:
            return ()
        sql = (
            "SELECT DISTINCT simulation_run_id, scenario_id "
            f"FROM {self.database}.{SOURCE_TABLE_TRAFFIC} "
            f"WHERE {self._upper_bound_only()} "
            "ORDER BY simulation_run_id, scenario_id LIMIT {lim:UInt32}"
        )
        rows = self._query(
            sql,
            {**cursor_parameters(bound, "u"), "lim": int(limit)},
            ("simulation_run_id", "scenario_id"),
        )
        return tuple((str(row["simulation_run_id"]), str(row["scenario_id"])) for row in rows)

    def read_run_events(
        self, *, simulation_run_id: str, upper_bound: FactCursor, limit: int = 500
    ) -> tuple[dict, ...]:
        columns = SOURCE_COLUMNS[SOURCE_TABLE_RUN_EVENT]
        sql = (
            f"SELECT {self._projection(columns)} "
            f"FROM {self.database}.{SOURCE_TABLE_RUN_EVENT} "
            "WHERE simulation_run_id = {run:String} "
            f"  AND {self._upper_bound_only()} "
            f"{fact_cursor_order_by()} LIMIT {{lim:UInt32}}"
        )
        rows = self._query(
            sql,
            {"run": simulation_run_id, **cursor_parameters(upper_bound, "u"), "lim": int(limit)},
            columns,
        )
        return tuple(rows)

    # -- dimensions -------------------------------------------------------------

    def read_dimension_rows(
        self, source_name: str, cursor: DimensionCursor, limit: Optional[int] = None
    ) -> tuple[tuple[dict, ...], DimensionCursor]:
        assert_allowed_source(source_name)
        if source_name not in DIM_SOURCE_TABLES:
            raise ForbiddenSourceError(f"{source_name} is not a dimension source")
        columns = SOURCE_COLUMNS[source_name]
        effective_col, key_cols = DIMENSION_CURSOR_MAP[source_name]
        batch = int(limit if limit is not None else self.settings.silver_fetch_batch_size)
        stable_expr = self._stable_id_expression(key_cols)
        sql = (
            f"SELECT {self._projection(columns)} "
            f"FROM {self.database}.{source_name} FINAL "
            f"WHERE ({effective_col} > {{d_from:DateTime64(3)}}) "
            f"   OR ({effective_col} = {{d_from:DateTime64(3)}} AND {stable_expr} > {{d_id:String}}) "
            f"ORDER BY {effective_col}, {stable_expr} LIMIT {{lim:UInt32}}"
        )
        rows = self._query(
            sql,
            {
                "d_from": cursor.effective_from.astimezone(timezone.utc),
                "d_id": cursor.stable_id,
                "lim": max(1, batch),
            },
            columns,
        )
        if not rows:
            return (), cursor
        last = rows[-1]
        next_cursor = DimensionCursor(
            effective_from=self._as_dt(last[effective_col]),
            approved_source_hash=normalize_hash(last.get("source_hash", "")),
            stable_id="\x1f".join(str(last[column]) for column in key_cols),
        )
        return tuple(rows), next_cursor

    # -- SQL fragments ------------------------------------------------------------

    @staticmethod
    def _projection(columns: Sequence[str]) -> str:
        rendered = []
        for column in columns:
            if column in {"source_payload_hash", "source_bronze_event_id",
                          "source_raw_ingestion_id", "source_hash"}:
                rendered.append(f"toString({column}) AS {column}")
            else:
                rendered.append(column)
        return ", ".join(rendered)

    @staticmethod
    def _upper_bound_only() -> str:
        return (
            "(     (processed_at < {u_at:DateTime64(3)})\n"
            "   OR (processed_at = {u_at:DateTime64(3)} AND source_topic < {u_topic:String})\n"
            "   OR (processed_at = {u_at:DateTime64(3)} AND source_topic = {u_topic:String}"
            " AND source_partition < {u_partition:Int32})\n"
            "   OR (processed_at = {u_at:DateTime64(3)} AND source_topic = {u_topic:String}"
            " AND source_partition = {u_partition:Int32} AND source_offset < {u_offset:Int64})\n"
            "   OR (processed_at = {u_at:DateTime64(3)} AND source_topic = {u_topic:String}"
            " AND source_partition = {u_partition:Int32} AND source_offset = {u_offset:Int64}"
            " AND toString(source_payload_hash) <= {u_hash:String}))"
        )

    @staticmethod
    def _stable_id_expression(key_cols: Sequence[str]) -> str:
        if len(key_cols) == 1:
            return f"toString({key_cols[0]})"
        pieces: list[str] = []
        for index, column in enumerate(key_cols):
            if index:
                pieces.append("'\\x1f'")
            pieces.append(f"toString({column})")
        return "concat(" + ", ".join(pieces) + ")"

    @staticmethod
    def _as_dt(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
