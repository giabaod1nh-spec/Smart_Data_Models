"""Environment-based configuration for DE-1 webhook."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT_VERSION_FILE = _REPO_ROOT / "contracts" / "VERSION"


def _read_contract_version() -> str:
    if _CONTRACT_VERSION_FILE.is_file():
        return _CONTRACT_VERSION_FILE.read_text(encoding="utf-8").strip()
    return "1.0.0"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DE_", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"

    max_body_bytes: int = 2_097_152
    contract_version: str = _read_contract_version()
    source_type: str = "ORION_NOTIFICATION"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "smart_traffic"
    clickhouse_secure: bool = False
    clickhouse_connect_timeout: float = 5.0
    clickhouse_query_timeout: float = 10.0

    migration_path: str = str(
        Path(__file__).resolve().parents[1] / "migrations" / "001_create_raw_ngsi_notifications.sql"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
