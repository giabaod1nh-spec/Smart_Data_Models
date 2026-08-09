"""Integration — POST webhook stores byte-identical payload_raw."""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from de.tests.conftest import build_test_app
from de.tests.support.memory_repository import InMemoryRawRepository
from de.webhook.config import Settings
from de.webhook.infrastructure.clickhouse_client import ClickHouseClient
from de.webhook.infrastructure.raw_repository import ClickHouseRawRepository


GOLDEN_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "delivery"
    / "notification.example.json"
)


@pytest.mark.asyncio
async def test_post_stores_byte_identical_payload():
    repo = InMemoryRawRepository()
    app = build_test_app(repo)
    golden = GOLDEN_PATH.read_bytes()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=golden,
            headers={"Content-Type": "application/json", "X-Request-ID": "it-1"},
        )
    assert response.status_code == 204
    parsed = json.loads(golden.decode("utf-8"))
    stored = await repo.get_payload_raw(parsed["id"], parsed.get("subscriptionId") or "")
    assert stored.encode("utf-8") == golden


@pytest.mark.asyncio
async def test_sequential_duplicate_single_row():
    repo = InMemoryRawRepository()
    app = build_test_app(repo)
    body = json.dumps(
        {
            "id": "urn:ngsi-ld:Notification:dup-seq",
            "type": "Notification",
            "data": [{"id": "e1"}],
        }
    ).encode("utf-8")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.post("/webhook/ngsi", content=body, headers={"Content-Type": "application/json"})).status_code == 204
        assert (await client.post("/webhook/ngsi", content=body, headers={"Content-Type": "application/json"})).status_code == 204
    assert await repo.count_rows() == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_single_row():
    repo = InMemoryRawRepository()
    repo.insert_delay_sec = 0.05
    app = build_test_app(repo)
    body = json.dumps(
        {
            "id": "urn:ngsi-ld:Notification:dup-concurrent",
            "type": "Notification",
            "data": [{"id": "e1"}],
        }
    ).encode("utf-8")
    transport = ASGITransport(app=app)

    async def _post(client: AsyncClient):
        return await client.post(
            "/webhook/ngsi",
            content=body,
            headers={"Content-Type": "application/json"},
        )

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        responses = await asyncio.gather(_post(client), _post(client))
    assert all(r.status_code == 204 for r in responses)
    assert await repo.count_rows() == 1


@pytest.mark.asyncio
async def test_restart_idempotency_persists_across_app_instances():
    repo = InMemoryRawRepository()
    body = json.dumps(
        {
            "id": "urn:ngsi-ld:Notification:restart",
            "type": "Notification",
            "subscriptionId": "sub-r",
            "data": [{"id": "e1"}],
        }
    ).encode("utf-8")
    app1 = build_test_app(repo)
    transport1 = ASGITransport(app=app1)
    async with AsyncClient(transport=transport1, base_url="http://test") as client:
        assert (await client.post("/webhook/ngsi", content=body, headers={"Content-Type": "application/json"})).status_code == 204

    app2 = build_test_app(repo)
    transport2 = ASGITransport(app=app2)
    async with AsyncClient(transport=transport2, base_url="http://test") as client:
        assert (await client.post("/webhook/ngsi", content=body, headers={"Content-Type": "application/json"})).status_code == 204
    assert await repo.count_rows() == 1


@pytest.fixture(scope="module")
def clickhouse_repo():
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
async def test_clickhouse_integration_byte_identical(clickhouse_repo):
    ch, repo, settings = clickhouse_repo
    nid = f"urn:ngsi-ld:Notification:ch-it-{uuid.uuid4()}"
    body = json.dumps(
        {
            "id": nid,
            "type": "Notification",
            "subscriptionId": "urn:ngsi-ld:Subscription:ch-it",
            "data": [{"id": "urn:ngsi-ld:Intersection:CH"}],
        },
        separators=(",", ":"),
    ).encode("utf-8")

    from de.tests.conftest import build_test_app

    app = build_test_app(repo, settings=settings, ping_ok=True)
    app.state.clickhouse = ch

    transport = ASGITransport(app=app, raise_app_exceptions=True)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=body,
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 204
    stored = await repo.get_payload_raw(nid, "urn:ngsi-ld:Subscription:ch-it")
    assert stored is not None
    assert stored.encode("utf-8") == body
