"""Failure path tests — HTTP status matrix."""

from __future__ import annotations

import json

import pytest
from httpx import ASGITransport, AsyncClient

from de.tests.conftest import build_test_app
from de.tests.support.memory_repository import InMemoryRawRepository
from de.webhook.config import Settings
from de.webhook.domain.exceptions import ClickHouseTimeoutError, ClickHouseUnavailableError


@pytest.mark.asyncio
async def test_invalid_json_returns_400():
    repo = InMemoryRawRepository()
    app = build_test_app(repo)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=b"{not-json",
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 400
    assert await repo.count_rows() == 0


@pytest.mark.asyncio
async def test_invalid_envelope_returns_400():
    repo = InMemoryRawRepository()
    app = build_test_app(repo)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=json.dumps({"type": "Notification", "data": []}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 400
    assert await repo.count_rows() == 0


@pytest.mark.asyncio
async def test_wrong_content_type_returns_415():
    repo = InMemoryRawRepository()
    app = build_test_app(repo)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
    assert response.status_code == 415


@pytest.mark.asyncio
async def test_body_too_large_returns_413():
    repo = InMemoryRawRepository()
    app = build_test_app(repo, settings=Settings(max_body_bytes=32))
    transport = ASGITransport(app=app)
    body = json.dumps(
        {
            "id": "urn:ngsi-ld:Notification:big",
            "type": "Notification",
            "data": [{"id": "x", "pad": "x" * 100}],
        }
    ).encode("utf-8")
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=body,
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 413


@pytest.mark.asyncio
async def test_clickhouse_unavailable_returns_503():
    repo = InMemoryRawRepository()
    repo.fail_with = ClickHouseUnavailableError("connection refused")
    app = build_test_app(repo)
    transport = ASGITransport(app=app)
    body = json.dumps(
        {
            "id": "urn:ngsi-ld:Notification:503",
            "type": "Notification",
            "data": [{"id": "e1"}],
        }
    ).encode("utf-8")
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=body,
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 503
    assert await repo.count_rows() == 0


@pytest.mark.asyncio
async def test_clickhouse_timeout_returns_504():
    repo = InMemoryRawRepository()
    repo.fail_with = ClickHouseTimeoutError("timed out")
    app = build_test_app(repo)
    transport = ASGITransport(app=app)
    body = json.dumps(
        {
            "id": "urn:ngsi-ld:Notification:504",
            "type": "Notification",
            "data": [{"id": "e1"}],
        }
    ).encode("utf-8")
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhook/ngsi",
            content=body,
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 504
    assert await repo.count_rows() == 0


@pytest.mark.asyncio
async def test_ready_fails_when_clickhouse_down():
    repo = InMemoryRawRepository()
    app = build_test_app(repo, ping_ok=False)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ready")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
