"""In-memory RawRepository for unit and contract tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

from de.webhook.domain.exceptions import ClickHouseTimeoutError, ClickHouseUnavailableError
from de.webhook.domain.models import RawNotificationRecord


@dataclass
class InMemoryRawRepository:
    rows: list[tuple[RawNotificationRecord, str]] = field(default_factory=list)
    fail_with: Optional[Exception] = None
    insert_delay_sec: float = 0.0

    async def exists(self, notification_id: str, subscription_id: str) -> bool:
        if self.fail_with and isinstance(self.fail_with, ClickHouseUnavailableError):
            raise self.fail_with
        return any(
            r.notification_id == notification_id and r.subscription_id == subscription_id
            for r, _ in self.rows
        )

    async def insert(self, record: RawNotificationRecord, payload_raw: str) -> None:
        if self.insert_delay_sec:
            await asyncio.sleep(self.insert_delay_sec)
        if self.fail_with:
            raise self.fail_with
        self.rows.append((record, payload_raw))

    async def count_rows(self) -> int:
        return len(self.rows)

    async def get_payload_raw(
        self, notification_id: str, subscription_id: str
    ) -> Optional[str]:
        for record, payload_raw in reversed(self.rows):
            if (
                record.notification_id == notification_id
                and record.subscription_id == subscription_id
            ):
                return payload_raw
        return None
