"""Contract test — malformed entity still stored with 204."""

from __future__ import annotations

import json

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_malformed_entity_stored(client: AsyncClient, memory_repo):
    payload = {
        "id": "urn:ngsi-ld:Notification:malformed-entity",
        "type": "Notification",
        "subscriptionId": "urn:ngsi-ld:Subscription:test",
        "data": [{"type": "Intersection"}],
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    response = await client.post(
        "/webhook/ngsi",
        content=body,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 204
    assert await memory_repo.count_rows() == 1
    stored = await memory_repo.get_payload_raw(payload["id"], payload["subscriptionId"])
    assert stored == body.decode("utf-8")
