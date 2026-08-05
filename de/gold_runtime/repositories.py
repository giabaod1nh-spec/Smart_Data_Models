"""Gold ClickHouse repository: bounded reads, identity reconciliation and writes.

Targets are dispatched through a frozen migration-005 allowlist, column lists come
from the executable Gold 1 contract, every statement binds parameters, and no
``SELECT *``/unbounded mutation is issued. Replay writes the same physical
namespace-bearing tables with ``namespace='replay:<id>'`` and never touches
dimensions.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from de.gold.contracts import (
    ALL_GOLD_TABLES,
    CONTROL_TABLES,
    MAIN_DIM_TABLES,
    MAIN_FACT_TABLES,
    TABLE_COLUMNS,
)
from de.gold.models import FACT_MODEL_BY_TABLE, GoldProcessingLedger
from de.gold_runtime.config import GoldSettings
from de.gold_runtime.cursor import FactCursor, ReadReceipt, normalize_hash
from de.gold_runtime.dimensions import DIM_BUSINESS_KEY, DimensionCandidate
from de.gold_runtime.silver_readers import SilverReader

LEDGER_TABLE = "gold_processing_ledger"

TARGET_IDENTITY_COLUMNS: Dict[str, tuple[str, ...]] = {
    "gold_fact_traffic_window": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "direction",
        "window_id",
    ),
    "gold_fact_intersection_window": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "window_id",
    ),
    "gold_fact_traffic_comparison": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "direction",
        "metric_code", "current_window_id",
    ),
    "gold_fact_signal_operation_window": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "direction",
        "window_id",
    ),
    "gold_fact_kpi_result": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "direction",
        "window_id", "metric_code",
    ),
}

RESULT_FIELD_BY_TABLE: Dict[str, str] = {
    "gold_fact_traffic_window": "traffic_windows",
    "gold_fact_intersection_window": "intersection_windows",
    "gold_fact_traffic_comparison": "comparisons",
    "gold_fact_signal_operation_window": "signal_operation_windows",
    "gold_fact_kpi_result": "kpi_results",
}

# Gold Runtime Contract v1 persistence order.
PERSISTENCE_ORDER: tuple[str, ...] = (
    "gold_fact_traffic_window",
    "gold_fact_intersection_window",
    "gold_fact_traffic_comparison",
    "gold_fact_signal_operation_window",
    "gold_fact_kpi_result",
)


class GoldRepositoryError(Exception):
    """Base Gold repository failure."""


class RetryableRepositoryError(GoldRepositoryError):
    """Connection reset, timeout or temporary unavailability."""


class UncertainWriteError(GoldRepositoryError):
    """The client cannot determine whether a synchronous insert committed."""


class SchemaMismatchError(GoldRepositoryError):
    """Missing Gold table/column; permanent."""


class InvalidTargetTableError(GoldRepositoryError):
    """Target outside the migration-005 allowlist; raised before SQL is built."""


class IdentityConflictError(GoldRepositoryError):
    """An existing identity carries a different source-set hash or revision."""


class NamespaceGuardError(GoldRepositoryError):
    """A row's namespace does not match the configured runtime namespace."""


class DimensionWriteForbiddenError(GoldRepositoryError):
    """Replay runs verify dimensions read-only and never write them."""


@dataclass(frozen=True)
class SchemaReport:
    tables: tuple[str, ...]
    ok: bool
    missing: tuple[str, ...] = ()


@dataclass(frozen=True)
class WriteReceipt:
    target_table: str
    attempted: int
    confirmed: int
    uncertain: int = 0


@dataclass(frozen=True)
class ExistingRow:
    identity: tuple
    source_set_hash: str
    revision_seq: int


@dataclass(frozen=True)
class ExistingState:
    batch_id: str
    rows: tuple[ExistingRow, ...]

    def by_identity(self) -> dict[tuple, ExistingRow]:
        return {row.identity: row for row in self.rows}


def logical_identity(target_table: str, row: Any) -> tuple:
    try:
        columns = TARGET_IDENTITY_COLUMNS[target_table]
    except KeyError as exc:
        raise InvalidTargetTableError(f"Unknown Gold target: {target_table!r}") from exc
    return (target_table,) + tuple(str(getattr(row, column)) for column in columns)


def _looks_uncertain(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(token in text for token in ("timeout", "reset", "broken pipe", "connection aborted"))


def _cell(value: Any) -> Any:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace").rstrip("\x00")
    return value


def _key_expr(columns: Sequence[str]) -> str:
    if len(columns) == 1:
        return f"toString({columns[0]})"
    pieces: list[str] = []
    for index, column in enumerate(columns):
        if index:
            pieces.append("'\\x1f'")
        pieces.append(f"toString({column})")
    return "concat(" + ", ".join(pieces) + ")"


def _key_str(values: Sequence[Any]) -> str:
    return "\x1f".join(str(value) for value in values)


class GoldClickHouseRepository:
    """Repository API fixed by Plan §15."""

    def __init__(
        self,
        settings: GoldSettings,
        client: Any = None,
        reader: Optional[SilverReader] = None,
    ) -> None:
        self.settings = settings
        self.database = settings.clickhouse_database
        self._client = client
        self._reader = reader

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._client is None:
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
                settings={"async_insert": 0, "wait_end_of_query": 1},
            )
        if self._reader is None:
            self._reader = SilverReader(self.settings, self._client)

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise GoldRepositoryError("Gold repository not connected")
        return self._client

    @property
    def reader(self) -> SilverReader:
        if self._reader is None:
            self._reader = SilverReader(self.settings, self._client)
        return self._reader

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    # -- schema ---------------------------------------------------------------

    def verify_schema(self) -> SchemaReport:
        missing: list[str] = []
        for table in ALL_GOLD_TABLES:
            result = self.client.query(
                "SELECT count() FROM system.tables "
                "WHERE database={db:String} AND name={n:String}",
                parameters={"db": self.database, "n": table},
            )
            if not result.result_rows or int(result.result_rows[0][0]) < 1:
                missing.append(table)
        if missing:
            raise SchemaMismatchError(f"Missing Gold tables: {missing}")
        return SchemaReport(tuple(ALL_GOLD_TABLES), True, ())

    # -- Silver reads (delegated, kept on the Plan §15 API surface) ------------

    def read_silver_batch(
        self, source: str, cursor: FactCursor, upper_bound: FactCursor
    ) -> tuple[tuple[dict, ...], ReadReceipt]:
        return self.reader.read_fact_batch(source, cursor, upper_bound)

    # -- writes ----------------------------------------------------------------

    def _guard_namespace(self, rows: Sequence[Any]) -> None:
        for row in rows:
            namespace = getattr(row, "namespace", None)
            if namespace is not None and namespace != self.settings.namespace:
                raise NamespaceGuardError(
                    f"row namespace {namespace!r} != runtime namespace "
                    f"{self.settings.namespace!r}"
                )

    def _insert(self, table: str, rows: Sequence[Any], columns: Sequence[str]) -> WriteReceipt:
        if not rows:
            return WriteReceipt(table, 0, 0, 0)
        if self.settings.dry_run:
            return WriteReceipt(table, len(rows), 0, 0)
        data = [[getattr(row, column) for column in columns] for row in rows]
        try:
            self.client.insert(f"{self.database}.{table}", data, column_names=list(columns))
        except Exception as exc:  # noqa: BLE001 — reclassified into the exception taxonomy
            if _looks_uncertain(exc):
                raise UncertainWriteError(f"{table}: {exc}") from exc
            raise RetryableRepositoryError(f"{table}: {exc}") from exc
        return WriteReceipt(table, len(rows), len(rows), 0)

    def _insert_target(self, table: str, rows: Sequence[Any]) -> WriteReceipt:
        if table not in MAIN_FACT_TABLES:
            raise InvalidTargetTableError(f"Invalid Gold fact target: {table!r}")
        self._guard_namespace(rows)
        model = FACT_MODEL_BY_TABLE[table]
        columns = [field.name for field in fields(model)]
        return self._insert(table, rows, columns)

    def insert_facts(self, rows: Sequence[Any]) -> tuple[WriteReceipt, ...]:
        """Traffic and intersection window facts, in the contract order."""
        grouped = _group_by_model(rows)
        return tuple(
            self._insert_target(table, grouped.get(table, ()))
            for table in ("gold_fact_traffic_window", "gold_fact_intersection_window")
        )

    def insert_comparisons(self, rows: Sequence[Any]) -> WriteReceipt:
        return self._insert_target("gold_fact_traffic_comparison", rows)

    def insert_signal_windows(self, rows: Sequence[Any]) -> WriteReceipt:
        return self._insert_target("gold_fact_signal_operation_window", rows)

    def insert_kpis(self, rows: Sequence[Any]) -> WriteReceipt:
        return self._insert_target("gold_fact_kpi_result", rows)

    # -- dimensions --------------------------------------------------------------

    def find_dimension_versions(
        self, candidates: Sequence[DimensionCandidate]
    ) -> dict[tuple, str]:
        """Return identity → stored ``source_hash`` for the exact definition version."""
        found: dict[tuple, str] = {}
        by_table: dict[str, list[DimensionCandidate]] = {}
        for candidate in candidates:
            if candidate.target_table not in MAIN_DIM_TABLES:
                raise InvalidTargetTableError(
                    f"Invalid Gold dimension target: {candidate.target_table!r}"
                )
            by_table.setdefault(candidate.target_table, []).append(candidate)
        for table, group in by_table.items():
            key_columns = DIM_BUSINESS_KEY[table]
            columns = list(key_columns)
            has_hash = "source_hash" in TABLE_COLUMNS[table]
            if has_hash:
                columns.append("source_hash")
            keys = [_key_str(candidate.business_key) for candidate in group]
            sql = (
                f"SELECT {', '.join(columns)} "
                f"FROM {self.database}.{table} FINAL "
                f"WHERE {_key_expr(key_columns)} IN {{keys:Array(String)}}"
            )
            result = self.client.query(sql, parameters={"keys": keys})
            for row in result.result_rows:
                values = [_cell(value) for value in row]
                identity = (table,) + tuple(str(value) for value in values[: len(key_columns)])
                found[identity] = normalize_hash(values[-1]) if has_hash else ""
        return found

    def upsert_dimensions(
        self, candidates: Sequence[DimensionCandidate]
    ) -> tuple[WriteReceipt, ...]:
        """Insert only absent versions; a different payload for one identity conflicts."""
        if not candidates:
            return ()
        if self.settings.is_replay():
            raise DimensionWriteForbiddenError(
                "replay verifies dimensions read-only; it never writes them"
            )
        existing = self.find_dimension_versions(candidates)
        conflicts = [
            candidate for candidate in candidates
            if candidate.source_hash
            and candidate.identity in existing
            and existing[candidate.identity] != candidate.source_hash
        ]
        if conflicts:
            raise IdentityConflictError(
                f"dimension version conflict for {[c.identity for c in conflicts]}"
            )
        receipts: list[WriteReceipt] = []
        by_table: dict[str, list[DimensionCandidate]] = {}
        for candidate in candidates:
            if candidate.identity in existing:
                continue
            by_table.setdefault(candidate.target_table, []).append(candidate)
        for table in MAIN_DIM_TABLES:
            group = by_table.get(table, [])
            if not group:
                continue
            columns = list(TABLE_COLUMNS[table])
            receipts.append(self._insert(table, [c.row for c in group], columns))
        return tuple(receipts)

    def verify_dimension_hashes(self, expected: Mapping[tuple, str]) -> tuple[tuple, ...]:
        """Replay guard: return identities whose stored hash differs from the manifest."""
        candidates = [
            DimensionCandidate(identity[0], tuple(identity[1:]), source_hash, None)
            for identity, source_hash in expected.items()
        ]
        stored = self.find_dimension_versions(candidates)
        return tuple(
            sorted(
                identity for identity, source_hash in expected.items()
                if stored.get(identity) != source_hash
            )
        )

    # -- reconciliation ----------------------------------------------------------

    def find_existing(
        self, batch_id: str, identities: Sequence[tuple], *, revision_seq: int = 0
    ) -> ExistingState:
        """Identity-set reconciliation; row counts are never sufficient evidence."""
        by_table: dict[str, list[tuple]] = {}
        for identity in identities:
            table = identity[0]
            if table not in TARGET_IDENTITY_COLUMNS:
                raise InvalidTargetTableError(f"Unknown Gold target: {table!r}")
            by_table.setdefault(table, []).append(identity)
        found: list[ExistingRow] = []
        for table, group in by_table.items():
            key_columns = TARGET_IDENTITY_COLUMNS[table]
            keys = [_key_str(identity[1:]) for identity in group]
            sql = (
                f"SELECT {', '.join(key_columns)}, source_set_hash, revision_seq "
                f"FROM {self.database}.{table} "
                f"WHERE {_key_expr(key_columns)} IN {{keys:Array(String)}} "
                "  AND revision_seq = {rev:UInt32}"
            )
            result = self.client.query(
                sql, parameters={"keys": keys, "rev": int(revision_seq)}
            )
            for row in result.result_rows:
                values = [_cell(value) for value in row]
                identity = (table,) + tuple(str(value) for value in values[: len(key_columns)])
                found.append(
                    ExistingRow(
                        identity=identity,
                        source_set_hash=normalize_hash(values[-2]),
                        revision_seq=int(values[-1]),
                    )
                )
        return ExistingState(batch_id, tuple(found))

    # -- ledger -------------------------------------------------------------------

    def record_ledger(self, row: GoldProcessingLedger) -> WriteReceipt:
        if row.namespace != self.settings.namespace:
            raise NamespaceGuardError(
                f"ledger namespace {row.namespace!r} != {self.settings.namespace!r}"
            )
        columns = list(TABLE_COLUMNS[LEDGER_TABLE])
        return self._insert(LEDGER_TABLE, [row], columns)

    def find_ledger_dispositions(
        self, namespace: str, source_set_hashes: Sequence[str]
    ) -> dict[tuple[str, int], str]:
        if not source_set_hashes:
            return {}
        sql = (
            "SELECT source_set_hash, revision_seq, disposition "
            f"FROM {self.database}.{LEDGER_TABLE} "
            "WHERE namespace = {ns:String} "
            "  AND source_set_hash IN {hashes:Array(String)}"
        )
        result = self.client.query(
            sql, parameters={"ns": namespace, "hashes": list(source_set_hashes)}
        )
        return {
            (normalize_hash(_cell(row[0])), int(row[1])): str(_cell(row[2]))
            for row in result.result_rows
        }


def _group_by_model(rows: Sequence[Any]) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = {}
    reverse = {model.__name__: table for table, model in FACT_MODEL_BY_TABLE.items()}
    for row in rows:
        table = reverse.get(type(row).__name__)
        if table is None:
            raise InvalidTargetTableError(f"Unmapped Gold fact model: {type(row).__name__}")
        grouped.setdefault(table, []).append(row)
    return grouped


def result_rows_for(result: Any, table: str) -> tuple[Any, ...]:
    return tuple(getattr(result, RESULT_FIELD_BY_TABLE[table]))


def control_tables() -> tuple[str, ...]:
    return tuple(CONTROL_TABLES)


def iter_target_tables() -> Iterable[str]:
    return PERSISTENCE_ORDER
