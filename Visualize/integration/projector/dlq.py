"""DLQ publisher for poison Kafka records (RT-B / §59)."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Optional

log = logging.getLogger(__name__)

DLQ_TOPIC = "traffic.entity-events.dlq.v2"
MAX_RAW_BYTES = 64 * 1024
PROJECTOR_VERSION = os.getenv("PROJECTOR_VERSION", "smart-traffic-projector-1.0")


class DlqPublishError(Exception):
    pass


class DlqEnvelopeError(Exception):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def bounded_raw_payload(raw: Optional[bytes]) -> str:
    if raw is None:
        return ""
    if len(raw) > MAX_RAW_BYTES:
        return raw[:MAX_RAW_BYTES].decode("utf-8", errors="replace") + "…[truncated]"
    return raw.decode("utf-8", errors="replace")


def payload_hash(raw: Optional[bytes]) -> str:
    return hashlib.sha256(raw or b"").hexdigest()


def build_dlq_envelope(
    *,
    original_topic: str,
    partition: int,
    offset: int,
    raw: Optional[bytes],
    error_type: str,
    error_message: str,
    simulation_run_id: Optional[str] = None,
) -> dict[str, Any]:
    msg = (error_message or "")[:512]
    return {
        "originalTopic": original_topic,
        "partition": int(partition),
        "offset": int(offset),
        "rawPayload": bounded_raw_payload(raw),
        "errorType": error_type,
        "errorMessage": msg,
        "observedAt": _utc_now(),
        "projectorVersion": PROJECTOR_VERSION,
        "simulationRunId": simulation_run_id,
        "payloadHash": payload_hash(raw),
    }


class DlqPublisher:
    """Publish bounded DLQ envelopes with idempotence key (topic, partition, offset)."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str = DLQ_TOPIC,
        producer_factory: Optional[Callable[[dict], Any]] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer_factory = producer_factory
        self._producer: Any = None
        self.metrics_dlq_total = 0

    def _ensure_producer(self) -> Any:
        if self._producer is not None:
            return self._producer
        if self._producer_factory is not None:
            self._producer = self._producer_factory(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                }
            )
        else:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                }
            )
        return self._producer

    def publish_sync(self, envelope: dict[str, Any], *, timeout_sec: float = 10.0) -> None:
        key = f"{envelope['originalTopic']}:{envelope['partition']}:{envelope['offset']}"
        payload = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
        producer = self._ensure_producer()
        delivered: dict[str, Any] = {"ok": False, "err": None}

        def _cb(err, _msg) -> None:
            if err is not None:
                delivered["err"] = err
            else:
                delivered["ok"] = True

        producer.produce(self.topic, key=key, value=payload, callback=_cb)
        producer.flush(timeout_sec)
        if not delivered["ok"]:
            raise DlqPublishError(str(delivered["err"]))
        self.metrics_dlq_total += 1

    def close(self) -> None:
        if self._producer is not None:
            try:
                self._producer.flush(5.0)
            except Exception:
                pass
            self._producer = None
