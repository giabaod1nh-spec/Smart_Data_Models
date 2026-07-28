"""Minimal Notification envelope validation — DE-1 only (no entity fields)."""

from __future__ import annotations

from typing import Any

from de.webhook.domain.exceptions import EnvelopeValidationError


def validate_envelope(parsed: Any) -> None:
    """
    Validate Orion notification envelope per DE1_IMPLEMENTATION_PLAN §9.

    Does NOT validate entity id/type/enum/simulation fields.
    """
    if not isinstance(parsed, dict):
        raise EnvelopeValidationError("body must be a JSON object")

    if parsed.get("type") != "Notification":
        raise EnvelopeValidationError("type must be Notification")

    notification_id = parsed.get("id")
    if not isinstance(notification_id, str) or not notification_id.strip():
        raise EnvelopeValidationError("id must be a non-empty string")

    if "data" not in parsed:
        raise EnvelopeValidationError("data is required")

    data = parsed.get("data")
    if not isinstance(data, list):
        raise EnvelopeValidationError("data must be an array")

    if len(data) < 1:
        raise EnvelopeValidationError("data must be non-empty")
