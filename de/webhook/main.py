"""FastAPI application factory for DE-1 webhook."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from de.webhook.api.routes_health import router as health_router
from de.webhook.api.routes_webhook import router as webhook_router
from de.webhook.config import Settings, get_settings
from de.webhook.domain.idempotency import IdempotencyService
from de.webhook.infrastructure.clickhouse_client import ClickHouseClient
from de.webhook.infrastructure.raw_repository import ClickHouseRawRepository
from de.webhook.logging_setup import setup_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = app.state.settings
    ch = ClickHouseClient(settings)
    ch.connect()
    await ch.run_sync(ch.run_migration)
    repository = ClickHouseRawRepository(ch, settings.clickhouse_database)
    app.state.clickhouse = ch
    app.state.repository = repository
    app.state.idempotency = IdempotencyService(repository)
    logger.info("de-webhook started")
    try:
        yield
    finally:
        ch.close()
        logger.info("de-webhook stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    setup_logging(resolved.log_level)

    app = FastAPI(title="DE-1 Webhook", version=resolved.contract_version, lifespan=lifespan)
    app.state.settings = resolved
    app.include_router(health_router)
    app.include_router(webhook_router)
    return app


app = create_app()
