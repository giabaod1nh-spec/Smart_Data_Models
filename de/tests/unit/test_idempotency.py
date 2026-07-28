"""Unit tests for idempotency keyed lock."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from de.tests.support.memory_repository import InMemoryRawRepository
from de.webhook.domain.idempotency import IdempotencyService
from de.webhook.domain.models import RawNotificationRecord


def _record(nid: str, sid: str = "sub-1") -> RawNotificationRecord:
    return RawNotificationRecord(
        ingestion_id=str(uuid.uuid4()),
        notification_id=nid,
        subscription_id=sid,
        payload_hash="abc",
        contract_version="1.0.0",
        source_type="ORION_NOTIFICATION",
        received_at=datetime.now(timezone.utc),
        notified_at=None,
        entity_count=1,
        payload_size_bytes=10,
        ingestion_status="STORED",
        source_ip="127.0.0.1",
        request_id="req-1",
    )


@pytest.mark.asyncio
async def test_sequential_duplicate():
    repo = InMemoryRawRepository()
    service = IdempotencyService(repo)
    record = _record("n-1")
    assert await service.ingest(record, '{"a":1}') == "STORED"
    assert await service.ingest(record, '{"a":1}') == "DUPLICATE"
    assert await repo.count_rows() == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate():
    repo = InMemoryRawRepository()
    repo.insert_delay_sec = 0.05
    service = IdempotencyService(repo)
    record = _record("n-concurrent")

    async def _ingest():
        return await service.ingest(record, '{"x":1}')

    results = await asyncio.gather(_ingest(), _ingest())
    assert sorted(results) == ["DUPLICATE", "STORED"]
    assert await repo.count_rows() == 1
