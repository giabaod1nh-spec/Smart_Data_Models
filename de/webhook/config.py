"""Environment-based configuration for DE-1 webhook."""

from __future__ import annotations

from enum import Enum
from functools import lru_cache
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT_VERSION_FILE = _REPO_ROOT / "contracts" / "VERSION"


def _read_contract_version() -> str:
    if _CONTRACT_VERSION_FILE.is_file():
        return _CONTRACT_VERSION_FILE.read_text(encoding="utf-8").strip()
    return "1.0.0"


class WebhookMode(str, Enum):
    """Explicit K-6b lifecycle modes for the retained webhook asset."""

    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"
    ROLLBACK_ONLY = "ROLLBACK_ONLY"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DE_", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"

    # K-6b fail-closed activation contract. The legacy standalone default stays
    # ACTIVE for backwards-compatible unit tests; canonical Compose never starts
    # this service unless the explicit rollback profile is selected.
    webhook_enabled: bool = True
    webhook_mode: WebhookMode = WebhookMode.ACTIVE

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

    @model_validator(mode="after")
    def validate_webhook_activation(self) -> "Settings":
        if self.webhook_mode in (WebhookMode.ACTIVE, WebhookMode.ROLLBACK_ONLY):
            if not self.webhook_enabled:
                raise ValueError(
                    f"webhook mode {self.webhook_mode.value} requires DE_WEBHOOK_ENABLED=true"
                )
        if self.webhook_mode is WebhookMode.DISABLED and self.webhook_enabled:
            raise ValueError(
                "webhook mode DISABLED requires DE_WEBHOOK_ENABLED=false"
            )
        return self

    @property
    def accepts_writes(self) -> bool:
        return self.webhook_enabled and self.webhook_mode in (
            WebhookMode.ACTIVE,
            WebhookMode.ROLLBACK_ONLY,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
