"""Three append-only ClickHouse tables for the isolated A-I03 publication probe."""

from re import fullmatch

from clickhouse_connect.driver.exceptions import OperationalError

from de.gold_runtime.business_publication import DivergentResult, PhysicalRow, _MAX_ROWS
from de.gold_runtime.business_state import _RESULT_LIMIT, _digest


_TABLES = {grain: f"gold_v1_{grain}_window" for grain in ("direction", "intersection", "network")}
_COLUMNS = ("result_id", "row_key", "row_hash", "payload_json")


class IsolatedClickHouseRows:
    """A narrowly scoped ResultRows port with a caller-owned timed client."""

    def __init__(self, client, database, *, max_query_seconds=3):
        if not isinstance(database, str) or not fullmatch(r"stage_a_probe_[0-9a-f]{16}", database):
            raise ValueError("expected a dedicated Stage A ClickHouse probe database")
        if type(max_query_seconds) is not int or not 1 <= max_query_seconds <= 5:
            raise ValueError("invalid isolated ClickHouse query budget")
        self.client = client
        self.database = database
        self.max_query_seconds = max_query_seconds

    def _name(self, grain):
        return f"{self.database}.{_TABLES[grain]}"

    def initialize(self):
        existing = self.client.query("SELECT name FROM system.tables WHERE database={db:String} LIMIT 4",
                                     parameters={"db": self.database},
                                     settings={"max_execution_time": self.max_query_seconds}).result_rows
        if any(name not in _TABLES.values() for (name,) in existing):
            raise ValueError("disposable database contains unrelated tables")
        for grain in _TABLES:
            self.client.command(f"""
                CREATE TABLE IF NOT EXISTS {self._name(grain)} (
                    result_id FixedString(64), row_key FixedString(64), row_hash FixedString(64),
                    payload_json String
                ) ENGINE=MergeTree ORDER BY (result_id,row_key,row_hash)
            """, settings={"max_execution_time": self.max_query_seconds})

    @staticmethod
    def _identity(value):
        if isinstance(value, bytes):
            value = value.decode("ascii")
        if not isinstance(value, str) or not fullmatch(r"[0-9a-f]{64}", value):
            raise DivergentResult("Invalid isolated ClickHouse row identity")
        return value

    def read(self, result_id, *, limit):
        self._identity(result_id)
        if type(limit) is not int or not 1 <= limit <= 2 * _MAX_ROWS + 1:
            raise ValueError("invalid isolated ClickHouse row cap")
        rows = []
        for grain in _TABLES:
            if len(rows) == limit:
                break
            try:
                result = self.client.query(f"""
                    SELECT row_key,row_hash,length(payload_json),
                           if(length(payload_json)<={{bytes:UInt32}},payload_json,'')
                    FROM {self._name(grain)} WHERE result_id={{rid:FixedString(64)}} LIMIT {{n:UInt16}}
                """, parameters={"rid": result_id, "n": limit - len(rows), "bytes": _RESULT_LIMIT},
                    settings={"max_execution_time": self.max_query_seconds, "max_threads": 1})
            except OperationalError as exc:
                raise ConnectionError("ClickHouse read outcome unavailable") from exc
            for key, row_hash, length, payload in result.result_rows:
                key, row_hash = self._identity(key), self._identity(row_hash)
                body = payload.encode() if isinstance(payload, str) else payload
                if not isinstance(body, bytes) or not 0 < length <= _RESULT_LIMIT or len(body) != length or _digest(body) != row_hash:
                    raise DivergentResult("ClickHouse payload length or hash differs")
                rows.append(PhysicalRow(grain, key, body))
        return tuple(rows)

    def insert(self, result_id, rows):
        result_id = self._identity(result_id)
        if len(rows) > _MAX_ROWS or sum(len(row.payload) for row in rows) > _RESULT_LIMIT:
            raise DivergentResult("ClickHouse insert exceeds result row or byte budget")
        grouped = {grain: [] for grain in _TABLES}
        for row in rows:
            if row.grain not in grouped or not isinstance(row.payload, bytes) or not row.payload:
                raise DivergentResult("Invalid isolated ClickHouse payload")
            grouped[row.grain].append((result_id, self._identity(row.key), _digest(row.payload), row.payload.decode()))
        for grain, batch in grouped.items():
            if batch:
                try:
                    self.client.insert(self._name(grain), batch, column_names=_COLUMNS,
                        settings={"async_insert": 0, "wait_end_of_query": 1,
                                  "max_execution_time": self.max_query_seconds, "max_threads": 1})
                except OperationalError as exc:
                    raise ConnectionError("ClickHouse insert delivery is uncertain") from exc
