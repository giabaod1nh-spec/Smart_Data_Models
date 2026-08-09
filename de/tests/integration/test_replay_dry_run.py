"""Replay CLI dry-run integrity check."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from de.webhook.config import Settings
from de.webhook.domain.canonical_hash import canonical_hash
from de.webhook.infrastructure.clickhouse_client import ClickHouseClient
from de.webhook.infrastructure.raw_repository import ClickHouseRawRepository
from de.webhook.scripts import replay_raw


@pytest.fixture(scope="module")
def replay_ch():
    settings = Settings(
        clickhouse_host="localhost",
        clickhouse_port=8123,
        clickhouse_database="smart_traffic",
    )
    ch = ClickHouseClient(settings)
    try:
        ch.connect()
        ch.run_migration()
    except Exception as exc:
        pytest.skip(f"ClickHouse not available: {exc}")
    repo = ClickHouseRawRepository(ch, settings.clickhouse_database)
    yield ch, repo, settings
    ch.close()


@pytest.mark.asyncio
async def test_replay_dry_run_integrity(replay_ch):
    ch, repo, settings = replay_ch
    now = datetime.now(timezone.utc)
    payload = {
        "id": f"urn:ngsi-ld:Notification:replay-{uuid.uuid4()}",
        "type": "Notification",
        "subscriptionId": "urn:ngsi-ld:Subscription:replay",
        "data": [{"id": "urn:ngsi-ld:Intersection:R"}],
    }
    raw = json.dumps(payload, separators=(",", ":"))
    from de.webhook.domain.models import RawNotificationRecord

    record = RawNotificationRecord(
        ingestion_id=str(uuid.uuid4()),
        notification_id=payload["id"],
        subscription_id=payload["subscriptionId"],
        payload_hash=canonical_hash(payload),
        contract_version=settings.contract_version,
        source_type=settings.source_type,
        received_at=now,
        notified_at=None,
        entity_count=1,
        payload_size_bytes=len(raw.encode("utf-8")),
        ingestion_status="STORED",
        source_ip="test",
        request_id="replay-test",
    )
    await repo.insert(record, raw)

    summary = await replay_raw.run_replay(
        repo,
        now - timedelta(minutes=5),
        now + timedelta(minutes=5),
        dry_run=True,
    )
    assert summary.exit_code == 0
    assert summary.integrity_fail == 0
    assert summary.records >= 1
