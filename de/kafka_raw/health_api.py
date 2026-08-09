"""Health HTTP API for K-4 Raw consumer (runs beside worker thread)."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict

from fastapi import FastAPI
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from de.kafka_raw.consumer import RawKafkaConsumer

app = FastAPI(title="de-kafka-raw-consumer")
_consumer: "RawKafkaConsumer | None" = None
_started_at = time.time()


def bind_consumer(c: "RawKafkaConsumer") -> None:
    global _consumer
    _consumer = c


@app.get("/health")
def health() -> Dict[str, Any]:
    from de.kafka_raw import MIGRATION_VERSION, SCHEMA_VERSION

    body: Dict[str, Any] = {
        "status": "ok",
        "schema_version": SCHEMA_VERSION,
        "migration_version": MIGRATION_VERSION,
        "uptime_sec": time.time() - _started_at,
    }
    if _consumer:
        body.update(_consumer.health())
    return body


@app.get("/ready")
def ready() -> JSONResponse:
    if _consumer is None:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    h = _consumer.health()
    # Stale commit: only enforce after grace from start AND after first commit opportunity
    if h.get("partitions_assigned") and h.get("last_successful_commit_time"):
        if h.get("commit_stale"):
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready", "reason": "commit_stale", **h},
            )
    if not h.get("ready"):
        return JSONResponse(status_code=503, content={"status": "not_ready", **h})
    return JSONResponse(status_code=200, content={"status": "ready", **h})


@app.get("/metrics")
def metrics() -> Dict[str, Any]:
    if _consumer is None:
        return {}
    return _consumer.health()


@app.get("/cutover-ready")
def cutover_ready() -> JSONResponse:
    """K-6 readiness: runtime-ready plus bounded durable lag on every partition."""
    if _consumer is None:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    h = _consumer.health()
    if not h.get("cutover_ready"):
        return JSONResponse(
            status_code=503,
            content={"status": "not_cutover_ready", **h},
        )
    return JSONResponse(status_code=200, content={"status": "cutover_ready", **h})
