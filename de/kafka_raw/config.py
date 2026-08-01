"""K-4 Raw consumer configuration."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO = Path(__file__).resolve().parents[2]


class KafkaRawSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="K4_", extra="ignore")

    bootstrap_servers: str = "localhost:29092"
    topic: str = "traffic.entity-events.v2"
    group_id: str = "de-kafka-raw-v2"
    client_id: str = "raw-consumer-node1"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "smart_traffic"
    clickhouse_secure: bool = False
    clickhouse_connect_timeout: float = 5.0
    clickhouse_query_timeout: float = 30.0

    ledger_path: str = str(_REPO / "de" / "artifacts" / "kafka_raw" / "ledger.sqlite3")
    batch_size: int = 500
    flush_ms: int = 500
    max_buffered_records: int = 5000
    max_buffered_bytes: int = 32 * 1024 * 1024
    max_batches_in_memory: int = 8
    rebalance_flush_timeout_ms: int = 2000
    commit_stale_sec: float = 300.0
    cutover_max_lag: int = 0
    watermark_sample_interval_sec: float = 5.0

    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 15_000
    max_poll_records: int = 500

    health_host: str = "0.0.0.0"
    health_port: int = 8091

    entity_schema_path: str = str(
        _REPO / "contracts" / "events" / "traffic-entity-event-v2.schema.json"
    )
    run_started_schema_path: str = str(
        _REPO / "contracts" / "events" / "traffic-simulation-run-started-v2.schema.json"
    )
    migration_path: str = str(
        _REPO / "de" / "migrations" / "002_create_kafka_raw_events.sql"
    )


@lru_cache
def get_settings() -> KafkaRawSettings:
    return KafkaRawSettings()
