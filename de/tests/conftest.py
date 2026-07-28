"""Shared pytest fixtures for DE-1 tests."""

from __future__ import annotations

from typing import Iterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from de.tests.support.memory_repository import InMemoryRawRepository
from de.webhook.api.routes_health import router as health_router
from de.webhook.api.routes_webhook import router as webhook_router
from de.webhook.config import Settings
from de.webhook.domain.idempotency import IdempotencyService
from de.webhook.infrastructure.clickhouse_client import ClickHouseClient


class FakeClickHouse:
    def __init__(self, ping_ok: bool = True) -> None:
        self._ping_ok = ping_ok

    def ping(self) -> bool:
        return self._ping_ok

    async def run_sync(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


def build_test_app(
    repository,
    *,
    settings: Settings | None = None,
    ping_ok: bool = True,
) -> FastAPI:
    resolved = settings or Settings(max_body_bytes=2_097_152)
    app = FastAPI()
    app.state.settings = resolved
    app.state.repository = repository
    app.state.idempotency = IdempotencyService(repository)
    app.state.clickhouse = FakeClickHouse(ping_ok=ping_ok)
    app.include_router(health_router)
    app.include_router(webhook_router)
    return app


@pytest.fixture
def memory_repo() -> InMemoryRawRepository:
    return InMemoryRawRepository()


@pytest.fixture
def test_app(memory_repo: InMemoryRawRepository) -> FastAPI:
    return build_test_app(memory_repo)


@pytest.fixture
async def client(test_app: FastAPI) -> Iterator[AsyncClient]:
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def repo_root() -> str:
    from pathlib import Path

    return str(Path(__file__).resolve().parents[2])
