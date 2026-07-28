"""POST /webhook/ngsi — Persist-Then-Ack raw ingestion."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from de.webhook.domain.canonical_hash import canonical_hash
from de.webhook.domain.envelope_validator import validate_envelope
from de.webhook.domain.exceptions import (
    ClickHouseTimeoutError,
    ClickHouseUnavailableError,
    EnvelopeValidationError,
)
from de.webhook.domain.idempotency import IdempotencyService
from de.webhook.domain.models import RawNotificationRecord, extract_metadata
from de.webhook.logging_setup import log_event

router = APIRouter()
logger = logging.getLogger(__name__)


def _error_response(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


@router.post("/webhook/ngsi")
async def receive_ngsi_notification(request: Request) -> Response:
    settings = request.app.state.settings
    idempotency: IdempotencyService = request.app.state.idempotency

    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    source_ip = request.client.host if request.client else ""

    content_type = request.headers.get("content-type", "")
    if "application/json" not in content_type.lower():
        log_event(
            logger,
            "rejected notification",
            level=logging.WARNING,
            source_type=settings.source_type,
            outcome="REJECTED",
            request_id=request_id,
            source_ip=source_ip,
            error="unsupported media type",
        )
        return _error_response(415, "Content-Type must be application/json")

    raw_body = await request.body()
    payload_size = len(raw_body)

    if payload_size > settings.max_body_bytes:
        log_event(
            logger,
            "rejected notification",
            level=logging.WARNING,
            source_type=settings.source_type,
            outcome="REJECTED",
            request_id=request_id,
            source_ip=source_ip,
            payload_size_bytes=payload_size,
            error="body too large",
        )
        return _error_response(413, "Request body too large")

    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError:
        log_event(
            logger,
            "rejected notification",
            level=logging.WARNING,
            source_type=settings.source_type,
            outcome="REJECTED",
            request_id=request_id,
            source_ip=source_ip,
            error="invalid json",
        )
        return _error_response(400, "Invalid JSON body")

    try:
        validate_envelope(parsed)
    except EnvelopeValidationError as exc:
        log_event(
            logger,
            "rejected notification",
            level=logging.WARNING,
            source_type=settings.source_type,
            outcome="REJECTED",
            request_id=request_id,
            source_ip=source_ip,
            error=str(exc),
        )
        return _error_response(400, str(exc))

    notification_id, subscription_id, notified_at, entity_count = extract_metadata(parsed)
    payload_hash = canonical_hash(parsed)

    try:
        payload_raw = raw_body.decode("utf-8")
    except UnicodeDecodeError:
        log_event(
            logger,
            "rejected notification",
            level=logging.WARNING,
            source_type=settings.source_type,
            outcome="REJECTED",
            request_id=request_id,
            source_ip=source_ip,
            error="body must be utf-8",
        )
        return _error_response(400, "Request body must be UTF-8 encoded JSON")

    record = RawNotificationRecord(
        ingestion_id=str(uuid.uuid4()),
        notification_id=notification_id,
        subscription_id=subscription_id,
        payload_hash=payload_hash,
        contract_version=settings.contract_version,
        source_type=settings.source_type,
        received_at=datetime.now(timezone.utc),
        notified_at=notified_at,
        entity_count=entity_count,
        payload_size_bytes=payload_size,
        ingestion_status="STORED",
        source_ip=source_ip,
        request_id=request_id,
    )

    try:
        outcome = await idempotency.ingest(record, payload_raw)
    except ClickHouseUnavailableError as exc:
        log_event(
            logger,
            "clickhouse unavailable",
            level=logging.ERROR,
            source_type=settings.source_type,
            outcome="FAILED",
            notification_id=notification_id,
            subscription_id=subscription_id,
            request_id=request_id,
            source_ip=source_ip,
            error=str(exc),
        )
        return _error_response(503, "ClickHouse unavailable")
    except ClickHouseTimeoutError as exc:
        log_event(
            logger,
            "clickhouse timeout",
            level=logging.ERROR,
            source_type=settings.source_type,
            outcome="FAILED",
            notification_id=notification_id,
            subscription_id=subscription_id,
            request_id=request_id,
            source_ip=source_ip,
            error=str(exc),
        )
        return _error_response(504, "ClickHouse operation timed out")

    log_event(
        logger,
        "notification ingested",
        source_type=settings.source_type,
        outcome=outcome,
        notification_id=notification_id,
        subscription_id=subscription_id,
        request_id=request_id,
        payload_hash=payload_hash,
        entity_count=entity_count,
        payload_size_bytes=payload_size,
        source_ip=source_ip,
    )
    return Response(status_code=204)
