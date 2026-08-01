"""ClickHouse batch repository for K-4 Raw/Quarantine (durable insert)."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError

from de.kafka_raw import MIGRATION_VERSION
from de.kafka_raw.config import KafkaRawSettings

log = logging.getLogger(__name__)

RAW_COLS = [
    "topic",
    "partition",
    "offset",
    "raw_ingestion_id",
    "kafka_key",
    "kafka_headers_json",
    "broker_timestamp",
    "broker_timestamp_type",
    "consumed_at",
    "captured_at",
    "event_id",
    "event_type",
    "event_version",
    "contract_version",
    "source",
    "producer_id",
    "producer_session_id",
    "simulation_run_id",
    "scenario_id",
    "simulation_time",
    "node_id",
    "cycle_sequence",
    "entity_sequence",
    "cycle_entity_count",
    "node_entity_count",
    "entity_id",
    "entity_type",
    "payload_encoding",
    "payload_stored",
    "payload_size_bytes",
    "payload_bytes_hash",
    "canonical_payload_hash",
    "migration_version",
]

QUAR_COLS = [
    "topic",
    "partition",
    "offset",
    "raw_ingestion_id",
    "kafka_key",
    "kafka_headers_json",
    "broker_timestamp",
    "broker_timestamp_type",
    "consumed_at",
    "failed_at",
    "error_code",
    "error_detail",
    "failure_stage",
    "validator_version",
    "schema_version_attempted",
    "event_id",
    "event_type",
    "payload_encoding",
    "payload_stored",
    "payload_size_bytes",
    "payload_bytes_hash",
    "canonical_payload_hash",
    "migration_version",
]


class ClickHouseRawRepository:
    def __init__(self, settings: KafkaRawSettings) -> None:
        self.settings = settings
        self._client: Any = None
        self.database = settings.clickhouse_database

    def connect(self) -> None:
        common = dict(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_user,
            password=self.settings.clickhouse_password,
            secure=self.settings.clickhouse_secure,
            connect_timeout=self.settings.clickhouse_connect_timeout,
            send_receive_timeout=self.settings.clickhouse_query_timeout,
            settings={"async_insert": 0, "wait_end_of_query": 1},
        )
        self._client = clickhouse_connect.get_client(
            database=self.database, **common
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("ClickHouse not connected")
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    def verify_tables(self) -> bool:
        for table in (
            "kafka_raw_events",
            "kafka_quarantine_events",
            "kafka_raw_events_replay",
            "kafka_quarantine_events_replay",
        ):
            r = self.client.query(
                "SELECT count() FROM system.tables WHERE database={db:String} AND name={n:String}",
                parameters={"db": self.database, "n": table},
            )
            if not r.result_rows or int(r.result_rows[0][0]) < 1:
                return False
        return True

    def insert_raw(self, rows: Sequence[Dict[str, Any]], *, replay_run_id: Optional[str] = None) -> None:
        if not rows:
            return
        table = f"{self.database}.kafka_raw_events_replay" if replay_run_id else f"{self.database}.kafka_raw_events"
        cols = list(RAW_COLS)
        data = []
        for r in rows:
            data.append([_cell(r, c) for c in RAW_COLS] + ([replay_run_id] if replay_run_id else []))
        if replay_run_id:
            cols = cols + ["replay_run_id"]
        self.client.insert(table, data, column_names=cols)

    def insert_quarantine(
        self, rows: Sequence[Dict[str, Any]], *, replay_run_id: Optional[str] = None
    ) -> None:
        if not rows:
            return
        table = (
            f"{self.database}.kafka_quarantine_events_replay"
            if replay_run_id
            else f"{self.database}.kafka_quarantine_events"
        )
        cols = list(QUAR_COLS)
        data = []
        for r in rows:
            data.append([_cell(r, c) for c in QUAR_COLS] + ([replay_run_id] if replay_run_id else []))
        if replay_run_id:
            cols = cols + ["replay_run_id"]
        self.client.insert(table, data, column_names=cols)

    def find_existing_ingestion_ids(self, ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        """Return map id → {destination, payload_bytes_hash} for ids found in Raw or Quarantine."""
        out: Dict[str, Dict[str, Any]] = {}
        if not ids:
            return out
        # ClickHouse IN list — batch
        uniq = list(dict.fromkeys(ids))
        # chunk to avoid huge queries
        for i in range(0, len(uniq), 500):
            chunk = uniq[i : i + 500]
            placeholders = ", ".join(f"'{x}'" for x in chunk)
            for dest, table in (
                ("RAW", "kafka_raw_events"),
                ("QUARANTINE", "kafka_quarantine_events"),
            ):
                sql = f"""
                    SELECT raw_ingestion_id, payload_bytes_hash
                    FROM {self.database}.{table}
                    WHERE raw_ingestion_id IN ({placeholders})
                """
                try:
                    result = self.client.query(sql)
                except Exception:
                    log.exception("find_existing failed table=%s", table)
                    raise
                for row in result.result_rows:
                    out[str(row[0])] = {
                        "destination": dest,
                        "payload_bytes_hash": str(row[1]),
                    }
        return out


def run_migration_file(settings: KafkaRawSettings, path: Optional[Path] = None) -> None:
    """Dedicated migrate: apply SQL file against ClickHouse."""
    migration = Path(path or settings.migration_path)
    sql = migration.read_text(encoding="utf-8")
    common = dict(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        secure=settings.clickhouse_secure,
        connect_timeout=settings.clickhouse_connect_timeout,
        send_receive_timeout=settings.clickhouse_query_timeout,
    )
    client = clickhouse_connect.get_client(database="default", **common)
    try:
        for statement in _split(sql):
            client.command(statement)
    finally:
        client.close()


def _cell(row: Dict[str, Any], col: str) -> Any:
    if col == "migration_version":
        return row.get(col) or MIGRATION_VERSION
    v = row.get(col)
    if isinstance(v, datetime):
        return v
    return v


def _split(sql: str) -> List[str]:
    cleaned_lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]
