"""K-7 Bronze processor configuration."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO = Path(__file__).resolve().parents[2]


class BronzeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="K7_", extra="ignore")

    topic: str = "traffic.entity-events.v2"
    partitions: str = "0,1,2"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "smart_traffic"
    clickhouse_secure: bool = False
    clickhouse_connect_timeout: float = 5.0
    clickhouse_query_timeout: float = 30.0

    checkpoint_path: str = str(_REPO / "de" / "artifacts" / "bronze" / "checkpoint.sqlite3")
    checkpoint_namespace: str = "live"
    start_mode: str = "earliest"

    worker_count: int = 1
    single_instance: bool = True

    batch_size: int = 500
    poll_interval_sec: float = 1.0
    query_timeout_sec: float = 30.0
    readiness_stale_sec: float = 120.0
    gap_wait_max_sec: float = 300.0

    health_host: str = "0.0.0.0"
    health_port: int = 8092

    entity_schema_path: str = str(
        _REPO / "contracts" / "events" / "traffic-entity-event-v2.schema.json"
    )
    run_started_schema_path: str = str(
        _REPO / "contracts" / "events" / "traffic-simulation-run-started-v2.schema.json"
    )
    migration_path: str = str(_REPO / "de" / "migrations" / "003_create_bronze_v2.sql")

    processor_name: str = "kafka-bronze-v2"
    processor_version: str = "1.0.0"
    bronze_schema_version: str = "1.0.0"
    source_contract_version: str = "2.0.0"

    def partition_list(self) -> list[int]:
        return [int(p.strip()) for p in self.partitions.split(",") if p.strip()]


@lru_cache
def get_settings() -> BronzeSettings:
    return BronzeSettings()
