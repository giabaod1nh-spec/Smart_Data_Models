"""Keyed asyncio lock idempotency for notification delivery dedup."""

from __future__ import annotations

import asyncio
from typing import Literal, Protocol

from de.webhook.domain.models import RawNotificationRecord

IngestOutcome = Literal["STORED", "DUPLICATE"]


class RawRepository(Protocol):
    async def exists(self, notification_id: str, subscription_id: str) -> bool: ...

    async def insert(self, record: RawNotificationRecord, payload_raw: str) -> None: ...


class IdempotencyService:
    """Per-key asyncio.Lock + repository duplicate check (single-instance)."""

    def __init__(self, repository: RawRepository) -> None:
        self._repository = repository
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def _lock_for(self, key: tuple[str, str]) -> asyncio.Lock:
        async with self._locks_guard:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    async def ingest(
        self,
        record: RawNotificationRecord,
        payload_raw: str,
    ) -> IngestOutcome:
        key = (record.notification_id, record.subscription_id)
        lock = await self._lock_for(key)
        async with lock:
            if await self._repository.exists(record.notification_id, record.subscription_id):
                return "DUPLICATE"
            await self._repository.insert(record, payload_raw)
            return "STORED"
