"""ClickHouse client wrapper with timeout and error mapping."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError

from de.webhook.config import Settings
from de.webhook.domain.exceptions import ClickHouseTimeoutError, ClickHouseUnavailableError

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Any = None

    def connect(self) -> None:
        common = dict(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            username=self._settings.clickhouse_user,
            password=self._settings.clickhouse_password,
            secure=self._settings.clickhouse_secure,
            connect_timeout=self._settings.clickhouse_connect_timeout,
            send_receive_timeout=self._settings.clickhouse_query_timeout,
        )
        bootstrap = clickhouse_connect.get_client(database="default", **common)
        self._client = bootstrap
        self.run_migration()
        target_db = self._settings.clickhouse_database
        if target_db != "default":
            bootstrap.close()
            self._client = clickhouse_connect.get_client(database=target_db, **common)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise ClickHouseUnavailableError("ClickHouse client not connected")
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    def run_migration(self) -> None:
        migration_file = Path(self._settings.migration_path)
        if not migration_file.is_file():
            logger.warning("migration file not found: %s", migration_file)
            return
        sql = migration_file.read_text(encoding="utf-8")
        for statement in _split_statements(sql):
            self.client.command(statement)

    def command(self, sql: str, parameters: Optional[dict[str, Any]] = None) -> Any:
        try:
            return self.client.command(sql, parameters=parameters or {})
        except (OperationalError, ConnectionError, OSError) as exc:
            raise ClickHouseUnavailableError(str(exc)) from exc
        except TimeoutError as exc:
            raise ClickHouseTimeoutError(str(exc)) from exc
        except ClickHouseError as exc:
            message = str(exc).lower()
            if "timeout" in message or "timed out" in message:
                raise ClickHouseTimeoutError(str(exc)) from exc
            raise ClickHouseUnavailableError(str(exc)) from exc

    def query(self, sql: str, parameters: Optional[dict[str, Any]] = None) -> Any:
        try:
            return self.client.query(sql, parameters=parameters or {})
        except (OperationalError, ConnectionError, OSError) as exc:
            raise ClickHouseUnavailableError(str(exc)) from exc
        except TimeoutError as exc:
            raise ClickHouseTimeoutError(str(exc)) from exc
        except ClickHouseError as exc:
            message = str(exc).lower()
            if "timeout" in message or "timed out" in message:
                raise ClickHouseTimeoutError(str(exc)) from exc
            raise ClickHouseUnavailableError(str(exc)) from exc

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)


def _split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    for part in sql.split(";"):
        stmt = part.strip()
        if stmt:
            statements.append(stmt)
    return statements
