"""Domain models for raw notification ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class RawNotificationRecord:
    ingestion_id: str
    notification_id: str
    subscription_id: str
    payload_hash: str
    contract_version: str
    source_type: str
    received_at: datetime
    notified_at: Optional[datetime]
    entity_count: int
    payload_size_bytes: int
    ingestion_status: str
    source_ip: str
    request_id: str


def extract_metadata(parsed: dict[str, Any]) -> tuple[str, str, Optional[datetime], int]:
    """Extract envelope fields for raw row metadata."""
    notification_id = parsed["id"]
    subscription_id = parsed.get("subscriptionId") or ""
    if not isinstance(subscription_id, str):
        subscription_id = str(subscription_id)

    notified_at: Optional[datetime] = None
    raw_notified = parsed.get("notifiedAt")
    if isinstance(raw_notified, str) and raw_notified.strip():
        try:
            normalized = raw_notified.replace("Z", "+00:00")
            notified_at = datetime.fromisoformat(normalized)
        except ValueError:
            notified_at = None

    data = parsed.get("data")
    entity_count = len(data) if isinstance(data, list) else 0
    return notification_id, subscription_id, notified_at, entity_count
