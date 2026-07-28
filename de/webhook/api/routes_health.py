"""Health and readiness endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from de.webhook.domain.exceptions import ClickHouseUnavailableError

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    ch = request.app.state.clickhouse
    try:
        ok = await ch.run_sync(ch.ping)
    except ClickHouseUnavailableError:
        ok = False
    except Exception:
        ok = False
    if ok:
        return JSONResponse(status_code=200, content={"status": "ready"})
    return JSONResponse(status_code=503, content={"status": "not_ready"})
