"""Health and readiness endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from de.webhook.domain.exceptions import ClickHouseUnavailableError

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> dict[str, str | bool]:
    settings = request.app.state.settings
    return {
        "status": "ok",
        "mode": settings.webhook_mode.value,
        "enabled": settings.webhook_enabled,
    }


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    settings = request.app.state.settings
    if not settings.accepts_writes:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "mode": settings.webhook_mode.value,
                "enabled": settings.webhook_enabled,
                "reason": "webhook_fail_closed",
            },
        )
    ch = request.app.state.clickhouse
    try:
        ok = await ch.run_sync(ch.ping)
    except ClickHouseUnavailableError:
        ok = False
    except Exception:
        ok = False
    if ok:
        return JSONResponse(
            status_code=200,
            content={
                "status": "ready",
                "mode": settings.webhook_mode.value,
                "enabled": settings.webhook_enabled,
            },
        )
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "mode": settings.webhook_mode.value,
            "enabled": settings.webhook_enabled,
            "reason": "clickhouse_unavailable",
        },
    )
