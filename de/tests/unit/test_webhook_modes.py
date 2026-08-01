from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from de.tests.conftest import build_test_app
from de.tests.support.memory_repository import InMemoryRawRepository
from de.webhook.config import Settings, WebhookMode


@pytest.mark.asyncio
async def test_disabled_mode_is_not_ready_and_rejects_before_write():
    repo = InMemoryRawRepository()
    settings = Settings(webhook_enabled=False, webhook_mode=WebhookMode.DISABLED)
    app = build_test_app(repo, settings=settings)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        health = await client.get("/health")
        ready = await client.get("/ready")
        write = await client.post(
            "/webhook/ngsi", content=b"not-even-json", headers={"Content-Type": "text/plain"}
        )
    assert health.json() == {"status": "ok", "mode": "DISABLED", "enabled": False}
    assert ready.status_code == 503
    assert ready.json()["reason"] == "webhook_fail_closed"
    assert write.status_code == 503
    assert write.json()["mode"] == "DISABLED"
    assert await repo.count_rows() == 0


@pytest.mark.asyncio
async def test_rollback_only_mode_is_ready_when_clickhouse_is_ready():
    repo = InMemoryRawRepository()
    settings = Settings(webhook_enabled=True, webhook_mode=WebhookMode.ROLLBACK_ONLY)
    app = build_test_app(repo, settings=settings)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        ready = await client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["mode"] == "ROLLBACK_ONLY"


@pytest.mark.parametrize(
    ("enabled", "mode"),
    [
        (False, WebhookMode.ACTIVE),
        (False, WebhookMode.ROLLBACK_ONLY),
        (True, WebhookMode.DISABLED),
    ],
)
def test_inconsistent_activation_contract_is_rejected(enabled, mode):
    with pytest.raises(ValidationError, match="requires DE_WEBHOOK_ENABLED"):
        Settings(webhook_enabled=enabled, webhook_mode=mode)
