"""Bounded, ordered Silver reads (Plan §9 column map, §10 reader contract).

Every SELECT lists its columns explicitly, binds parameters, is bounded by a
per-poll upper-bound cursor snapshot and a row limit, and orders by the approved
cursor. Bronze, Raw, quarantine, Silver ledger and replay mirrors are never read.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence
from de.silver.incremental_contract import (
    CONTRACT_VERSION, MAX_SEQUENCE, CanonicalBatchReceipt, CanonicalChangePage,
    CanonicalChangePageRequest, CanonicalInvalidationPage, CanonicalInvalidationPageRequest,
    CanonicalInvalidationReceipt, SourceDiscovery, SourceDiscoveryRequest,
    batch_digest, canonical_bytes, canonical_hash,
)

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


class CoreReceiptFault(RuntimeError):
    """Gold rejected a Silver response or its durable evidence linkage."""


def _require(condition, message):
    if not condition:
        raise CoreReceiptFault(message)


def _sequence(value):
    return type(value) is int and 0 <= value <= MAX_SEQUENCE


def _hex_digest(value):
    return isinstance(value, str) and len(value) == 64 and not (set(value) - set("0123456789abcdef"))


def discovery_boundary(discovery):
    return (discovery.bronze_change_high_water, discovery.bronze_invalidation_high_water,
            discovery.silver_change_applied_sequence, discovery.silver_invalidation_applied_sequence)


def receipt_boundary(receipt):
    return (receipt.captured_bronze_change_high_water, receipt.captured_bronze_invalidation_high_water,
            receipt.captured_silver_change_applied_sequence, receipt.captured_silver_invalidation_applied_sequence)


def _bounds(values):
    hc, hi, sc, si = values
    _require(all(map(_sequence, values)) and sc <= hc and si <= hi, "Invalid captured boundaries")
    return hc, hi, sc, si


def verify_discovery(request, discovery):
    _require(type(discovery) is SourceDiscovery, "Unexpected source discovery type")
    _require(discovery.namespace == request.namespace and discovery.contract_version == request.contract_version,
             "Unexpected discovery namespace/version")
    binding = dict(discovery.source_binding)
    _require(set(binding) == {"namespace", "source_path", "file_identity", "schema_fingerprint", "bronze_version"}
             and binding["namespace"] == request.namespace, "Unexpected source binding projection")
    binding["canonical_source_path"] = binding.pop("source_path")
    _require(discovery.source_binding_id == "sha256:" + canonical_hash(binding)
             and request.expected_source_binding_id in (None, discovery.source_binding_id),
             "Discovery source binding mismatch")
    _, _, sc, si = _bounds(discovery_boundary(discovery))
    _require(discovery.bootstrap_required is (request.expected_source_binding_id is None)
             and discovery.run_discovery_scope == "journaled-runs-only"
             and discovery.verified_change_prefix == sc and discovery.verified_invalidation_prefix == si
             and discovery.next_change_sequence == discovery.next_invalidation_sequence == 0,
             "Unexpected discovery prefix/bootstrap state")
    return discovery


def _verify_entry(entry, change, pi, hc):
    _require(isinstance(entry.event_id, str) and bool(entry.event_id)
             and _sequence(entry.semantic_revision) and entry.semantic_revision > 0
             and _hex_digest(entry.source_transition_hash) and _sequence(entry.run_generation)
             and all(isinstance(value, str) and value for value in
                     (entry.run_id, entry.topology_digest, entry.manifest_identity)),
             "Malformed canonical identity")
    if not change:
        _require(entry.mirror_status in ("PENDING", "RECONCILED") and bool(entry.invalidation_id)
                 and _hex_digest(entry.previous_immutable_envelope_hash)
                 and _hex_digest(entry.conflicting_immutable_envelope_hash),
                 "Malformed canonical invalidation")
        return
    disposition = entry.effective_disposition
    _require(entry.evidence_status == "VERIFIED", "Unverified entry")
    if disposition == "BUNDLE_VALID":
        _require(entry.change_type == "ACCEPTED" and entry.new_state == "ACCEPTED"
                 and entry.invalidation_sequence is None and entry.source_invalidation_id is None
                 and entry.superseding_change_sequence is None and entry.canonical_bundle is not None
                 and entry.canonical_bundle.event_id == entry.event_id
                 and entry.bundle_hash == canonical_hash(entry.canonical_bundle), "Invalid canonical bundle")
        return
    _require(_sequence(entry.invalidation_sequence) and 0 < entry.invalidation_sequence <= pi
             and entry.bundle_hash is None and entry.canonical_bundle is None,
             "Invalid conflict linkage")
    if disposition == "SUPERSEDED_BY_CONFLICT":
        _require(entry.change_type == "ACCEPTED" and entry.new_state == "ACCEPTED"
                 and entry.source_invalidation_id is None and _sequence(entry.superseding_change_sequence)
                 and entry.change_sequence < entry.superseding_change_sequence <= hc, "Invalid supersession")
    else:
        _require(disposition == "CONFLICTED" and entry.change_type == "CONFLICTED"
                 and entry.previous_state == "ACCEPTED" and entry.new_state == "CONFLICTED"
                 and bool(entry.source_invalidation_id) and entry.superseding_change_sequence is None,
                 "Unknown or malformed disposition")


def _verify_core_page(request, page):
    change = type(request) is CanonicalChangePageRequest
    _require(type(page) is (CanonicalChangePage if change else CanonicalInvalidationPage),
             "Unexpected page type")
    _require(page.contract_version == request.contract_version == CONTRACT_VERSION
             and page.namespace == request.namespace == "live"
             and page.source_binding_id == request.expected_source_binding_id, "Page scope mismatch")
    after = request.after_change_sequence if change else request.after_invalidation_sequence
    returned_after = page.after_change_sequence if change else page.after_invalidation_sequence
    through = page.through_change_sequence if change else page.through_invalidation_sequence
    _require(_sequence(returned_after) and _sequence(through) and after == returned_after <= through,
             "Returned range differs from request")
    _require(type(page.entries) is tuple, "Expected a frozen tuple of entries")
    count = len(page.entries)
    _require(type(page.entry_count) is int and page.entry_count == count == through - after
             and count <= request.max_entries, "Page length or range mismatch")
    _require(type(page.encoded_bytes) is int and 0 < page.encoded_bytes <= request.max_encoded_bytes
             and len(canonical_bytes(page)) == page.encoded_bytes, "Canonical envelope byte count differs")
    receipt = page.receipt
    if not count:
        _require(page.status == "EMPTY" and receipt is None and page.has_more is False,
                 "Malformed EMPTY response")
        if change:
            hc, hi, sc, si = _bounds(receipt_boundary(page))
            _require(after == hc == sc == page.verified_change_prefix
                     and page.safe_invalidation_through_sequence == si, "Invalid empty change bounds")
            _verify_boundary_id(page, hc, hi, sc, si)
        else:
            _require(page.requested_through_invalidation_sequence == request.through_invalidation_sequence
                     == after == page.verified_invalidation_prefix
                     and after <= page.captured_silver_invalidation_applied_sequence
                     <= page.captured_bronze_invalidation_high_water, "Invalid empty invalidation bounds")
            # The EMPTY invalidation DTO omits Hc/Sc. It carries no ack authority.
        return page
    _require(page.status == "OK"
             and type(receipt) is (CanonicalBatchReceipt if change else CanonicalInvalidationReceipt),
             "Nonempty page has no matching receipt")
    hc, hi, sc, si = _bounds(receipt_boundary(receipt))
    ceiling = sc if change else min(request.through_invalidation_sequence, hi, si)
    _require(through <= ceiling and page.has_more is (through < ceiling), "Invalid captured prefix")
    if change:
        _require(receipt_boundary(page) == (hc, hi, sc, si) and page.verified_change_prefix == sc
                 and page.safe_invalidation_through_sequence == si
                 and receipt.safe_invalidation_through_sequence == si, "Change boundary or safe Pi differs")
    else:
        _require(page.captured_bronze_invalidation_high_water == hi
                 and page.captured_silver_invalidation_applied_sequence == si
                 and page.verified_invalidation_prefix == ceiling
                 and page.requested_through_invalidation_sequence == receipt.requested_through_invalidation_sequence
                 == request.through_invalidation_sequence and receipt.safe_invalidation_through_sequence is None,
                 "Invalidation boundary mismatch")
    _verify_boundary_id(page, hc, hi, sc, si)
    _require(receipt.contract_version == page.contract_version and receipt.namespace == page.namespace
             and receipt.source_binding_id == page.source_binding_id
             and receipt.receipt_kind == ("CHANGE" if change else "INVALIDATION")
             and receipt.requested_after_sequence == receipt.returned_after_sequence == after
             and receipt.returned_through_sequence == through
             and receipt.entry_count == count and receipt.encoded_bytes == page.encoded_bytes,
             "Receipt and response differ")
    for expected, entry in enumerate(page.entries, after + 1):
        sequence = entry.change_sequence if change else entry.invalidation_sequence
        _require(_sequence(sequence) and sequence == expected, "Receipt coverage is not exact and contiguous")
        _verify_entry(entry, change, si, hc)
    digest = batch_digest(receipt, page.entries)
    _require(receipt.batch_digest == digest and receipt.receipt_id == "sha256:" + digest,
             "Receipt digest or ID differs")
    return page


def _verify_boundary_id(page, hc, hi, sc, si):
    expected = canonical_hash(dict(contract_version=page.contract_version, source_binding_id=page.source_binding_id,
        captured_bronze_change_high_water=hc, captured_bronze_invalidation_high_water=hi,
        captured_silver_change_applied_sequence=sc, captured_silver_invalidation_applied_sequence=si))
    _require(page.boundary_id == "sha256:" + expected, "Boundary ID differs")


def verify_core_page(request, page):
    try:
        return _verify_core_page(request, page)
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise CoreReceiptFault("Malformed canonical page or JCS projection") from exc


class CanonicalGoldSource:
    """Only the public in-process Silver API is used by the Core path."""

    def __init__(self, session, *, max_entries=50, max_encoded_bytes=4 * 1024 * 1024,
                 max_processing_ms=30_000, max_entry_processing_ms=10_000):
        self.session = session
        self.limits = dict(max_entries=max_entries, max_encoded_bytes=max_encoded_bytes,
                           max_processing_ms=max_processing_ms, max_entry_processing_ms=max_entry_processing_ms)
        self.effective_limits = self.limits.copy()
        self.request_counts = dict(discovery=0, change=0, invalidation=0, schedule=0, measurement=0)

    def discover(self, expected):
        self.request_counts["discovery"] += 1
        request = SourceDiscoveryRequest("live", expected)
        result = verify_discovery(request, self.session.discover_canonical_source(request))
        self.effective_limits = {key: min(value, getattr(result, key + "_cap"))
                                 for key, value in self.limits.items()}
        return result

    def changes(self, binding, after, *, entries=None):
        self.request_counts["change"] += 1
        options = self.effective_limits.copy()
        if entries is not None:
            options["max_entries"] = min(options["max_entries"], entries)
        request = CanonicalChangePageRequest(namespace="live", expected_source_binding_id=binding,
                                             after_change_sequence=after, **options)
        return verify_core_page(request, self.session.read_canonical_changes_page(request))

    def invalidations(self, binding, after, ceiling):
        self.request_counts["invalidation"] += 1
        request = CanonicalInvalidationPageRequest(namespace="live", expected_source_binding_id=binding,
            after_invalidation_sequence=after, through_invalidation_sequence=ceiling, **self.effective_limits)
        return verify_core_page(request, self.session.read_canonical_invalidations_page(request))

    def observation_metadata(self, binding, run_id, generation, topology_digest, cursor):
        from de.silver.observation_contract import ObservationMetadataRequest, VerifiedObservationMetadata

        request = ObservationMetadataRequest(source_binding_id=binding, run_id=run_id,
            run_generation=generation, topology_digest=topology_digest, cursor=cursor,
            max_processing_ms=self.effective_limits["max_processing_ms"])
        self.request_counts[cursor.stream.lower()] += 1
        receipt = self.session.read_observation_metadata(request)
        _require(type(receipt) is VerifiedObservationMetadata and receipt.contract_version == request.contract_version
                 and (receipt.source_binding_id, receipt.run_id, receipt.run_generation, receipt.topology_digest,
                      receipt.before) == (binding, run_id, generation, topology_digest, cursor),
                 "Observation metadata response differs from requested Silver source and cursor")
        return receipt

    def close(self):
        self.session.close()
