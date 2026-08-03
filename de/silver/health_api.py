"""Silver Plan 3 — cached /health and /ready only (no hot-storage queries)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Response

from de.silver.metrics import HealthSnapshot

app = FastAPI(title="de-silver-processor", docs_url=None, redoc_url=None)
_processor: Any = None
_max_age_sec: float = 5.0


def bind_processor(processor: Any, *, max_age_sec: float = 5.0) -> None:
    global _processor, _max_age_sec
    _processor = processor
    _max_age_sec = max_age_sec


def _parse_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _snapshot() -> Optional[HealthSnapshot]:
    if _processor is None:
        return None
    return _processor.health_snapshot()


@app.get("/health")
def health() -> dict[str, Any]:
    snap = _snapshot()
    if snap is None:
        return {"status": "starting", "ready": False}
    return {
        "status": snap.state,
        "ready": snap.ready,
        "worker_alive": snap.worker_alive,
        "reader_initialized": snap.reader_initialized,
        "clickhouse_ok": snap.clickhouse_ok,
        "sqlite_ok": snap.sqlite_ok,
        "schema_ok": snap.schema_ok,
        "lock_held": snap.lock_held,
        "namespace": snap.namespace,
        "mode": snap.mode,
        "shutdown_requested": snap.shutdown_requested,
        "snapshot_at": snap.snapshot_at,
        "metrics": snap.metrics,
        "fault_code": snap.fault_code,
        "fault_message": snap.fault_message,
        "streams": list(snap.streams),
    }


@app.get("/ready")
def ready(response: Response) -> dict[str, Any]:
    snap = _snapshot()
    if snap is None:
        response.status_code = 503
        return {"ready": False, "reason": "PROCESSOR_UNBOUND"}
    ts = _parse_ts(snap.snapshot_at)
    if ts is None:
        response.status_code = 503
        return {"ready": False, "reason": "HEALTH_SNAPSHOT_STALE"}
    age = (datetime.now(timezone.utc) - ts).total_seconds()
    if age > _max_age_sec:
        response.status_code = 503
        return {"ready": False, "reason": "HEALTH_SNAPSHOT_STALE", "age_sec": age}
    if not snap.ready:
        response.status_code = 503
        return {"ready": False, "state": snap.state, "reason": "NOT_READY"}
    response.status_code = 200
    return {"ready": True, "state": snap.state}
