"""Health HTTP API for K-7 Bronze processor."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict

from fastapi import FastAPI
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from de.bronze.processor import BronzeProcessor

app = FastAPI(title="de-bronze-processor")
_processor: "BronzeProcessor | None" = None
_started_at = time.time()


def bind_processor(p: "BronzeProcessor") -> None:
    global _processor
    _processor = p


@app.get("/health")
def health() -> Dict[str, Any]:
    from de.bronze import MIGRATION_VERSION

    body: Dict[str, Any] = {
        "status": "ok",
        "migration_version": MIGRATION_VERSION,
        "uptime_sec": time.time() - _started_at,
    }
    if _processor:
        body.update(_processor.health())
    return body


@app.get("/ready")
def ready() -> JSONResponse:
    if _processor is None:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    h = _processor.health()
    if h.get("checkpoint_stale"):
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "checkpoint_stale", **h},
        )
    if not h.get("ready"):
        return JSONResponse(status_code=503, content={"status": "not_ready", **h})
    return JSONResponse(status_code=200, content={"status": "ready", **h})


@app.get("/metrics")
def metrics() -> Dict[str, Any]:
    if _processor is None:
        return {}
    return _processor.health()
