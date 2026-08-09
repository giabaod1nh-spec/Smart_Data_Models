"""Contract tests — golden notification envelope."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from httpx import AsyncClient


@pytest.fixture
def golden_bytes() -> bytes:
    path = (
        Path(__file__).resolve().parents[3]
        / "contracts"
        / "delivery"
        / "notification.example.json"
    )
    return path.read_bytes()


@pytest.mark.asyncio
async def test_golden_notification_stored(client: AsyncClient, golden_bytes: bytes, memory_repo):
    response = await client.post(
        "/webhook/ngsi",
        content=golden_bytes,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 204
    assert await memory_repo.count_rows() == 1
    parsed = json.loads(golden_bytes.decode("utf-8"))
    stored = await memory_repo.get_payload_raw(parsed["id"], parsed.get("subscriptionId") or "")
    assert stored == golden_bytes.decode("utf-8")
