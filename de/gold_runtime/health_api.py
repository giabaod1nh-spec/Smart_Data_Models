"""Cached ``/health`` and ``/ready``; neither endpoint queries hot storage."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Response

from de.gold_runtime.metrics import HealthSnapshot

app = FastAPI(title="de-gold-runtime", docs_url=None, redoc_url=None)
_processor: Any = None
_max_age_sec: float = 5.0


def bind_processor(processor: Any, *, max_age_sec: float = 5.0) -> None:
    global _processor, _max_age_sec
    _processor = processor
    _max_age_sec = max_age_sec


def _snapshot() -> Optional[HealthSnapshot]:
    if _processor is None:
        return None
    return _processor.health_snapshot()


def _parse(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@app.get("/health")
def health() -> dict[str, Any]:
    snapshot = _snapshot()
    if snapshot is None:
        return {"status": "starting", "ready": False}
    return {
        "status": snapshot.state,
        "ready": snapshot.ready,
        "namespace": snapshot.namespace,
        "mode": snapshot.mode,
        "worker_alive": snapshot.worker_alive,
        "reader_initialized": snapshot.reader_initialized,
        "clickhouse_ok": snapshot.clickhouse_ok,
        "sqlite_ok": snapshot.sqlite_ok,
        "schema_ok": snapshot.schema_ok,
        "lock_held": snapshot.lock_held,
        "shutdown_requested": snapshot.shutdown_requested,
        "snapshot_at": snapshot.snapshot_at,
        "last_batch_id": snapshot.last_batch_id,
        "last_window_id": snapshot.last_window_id,
        "last_checkpoint_at": snapshot.last_checkpoint_at,
        "watermark": snapshot.watermark,
        "non_terminal_work_units": snapshot.non_terminal_work_units,
        "fault_code": snapshot.fault_code,
        "fault_message": snapshot.fault_message,
        "reason": snapshot.reason,
        "metrics": snapshot.metrics,
    }


@app.get("/ready")
def ready(response: Response) -> dict[str, Any]:
    snapshot = _snapshot()
    if snapshot is None:
        response.status_code = 503
        return {"ready": False, "reason": "PROCESSOR_UNBOUND"}
    taken_at = _parse(snapshot.snapshot_at)
    if taken_at is None:
        response.status_code = 503
        return {"ready": False, "reason": "HEALTH_SNAPSHOT_STALE"}
    age = (datetime.now(timezone.utc) - taken_at).total_seconds()
    if age > _max_age_sec:
        response.status_code = 503
        return {"ready": False, "reason": "HEALTH_SNAPSHOT_STALE", "age_sec": age}
    if not snapshot.ready:
        response.status_code = 503
        return {
            "ready": False,
            "state": snapshot.state,
            "reason": snapshot.reason or "NOT_READY",
        }
    response.status_code = 200
    return {"ready": True, "state": snapshot.state}
