"""FastAPI application factory for DE-1 webhook."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from de.webhook import (
    CUTOVER_GATE,
    LEGACY,
    LEGACY_CONTRACT,
    RETIREMENT_PHASE,
)
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
    if LEGACY:
        logger.warning(
            "LEGACY de-webhook starting contract=%s retirement=%s cutover_gate=%s "
            "— not part of final Kafka historical path",
            LEGACY_CONTRACT,
            RETIREMENT_PHASE,
            CUTOVER_GATE,
        )
    if not settings.accepts_writes:
        app.state.clickhouse = None
        app.state.repository = None
        app.state.idempotency = None
        logger.warning(
            "de-webhook fail-closed mode=%s enabled=%s; no ClickHouse connection or migration",
            settings.webhook_mode.value,
            settings.webhook_enabled,
        )
        yield
        return

    ch = ClickHouseClient(settings)
    ch.connect()
    await ch.run_sync(ch.run_migration)
    repository = ClickHouseRawRepository(ch, settings.clickhouse_database)
    app.state.clickhouse = ch
    app.state.repository = repository
    app.state.idempotency = IdempotencyService(repository)
    logger.info(
        "de-webhook started mode=%s enabled=%s",
        settings.webhook_mode.value,
        settings.webhook_enabled,
    )
    try:
        yield
    finally:
        ch.close()
        logger.info("de-webhook stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    setup_logging(resolved.log_level)

    app = FastAPI(
        title="DE-1 Webhook (LEGACY)",
        version=resolved.contract_version,
        lifespan=lifespan,
        description=(
            f"LEGACY={LEGACY} retirement={RETIREMENT_PHASE} "
            f"cutover_gate={CUTOVER_GATE} contract={LEGACY_CONTRACT}"
        ),
    )
    app.state.settings = resolved
    app.include_router(health_router)
    app.include_router(webhook_router)
    return app


app = create_app()
