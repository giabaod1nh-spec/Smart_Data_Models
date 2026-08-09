"""Unit tests for envelope validation."""

from __future__ import annotations

import pytest

from de.webhook.domain.envelope_validator import validate_envelope
from de.webhook.domain.exceptions import EnvelopeValidationError


def test_valid_minimal_envelope():
    validate_envelope(
        {
            "id": "urn:ngsi-ld:Notification:1",
            "type": "Notification",
            "data": [{"id": "x"}],
        }
    )


@pytest.mark.parametrize(
    "payload,match",
    [
        ([], "JSON object"),
        ({"type": "Notification", "data": [{}]}, "id"),
        ({"id": "n1", "type": "Entity", "data": [{}]}, "Notification"),
        ({"id": "n1", "type": "Notification", "data": []}, "non-empty"),
        ({"id": "n1", "type": "Notification"}, "data"),
        ({"id": "n1", "type": "Notification", "data": "x"}, "array"),
    ],
)
def test_invalid_envelope(payload, match: str):
    with pytest.raises(EnvelopeValidationError, match=match):
        validate_envelope(payload)


def test_entity_missing_id_not_rejected():
    validate_envelope(
        {
            "id": "urn:ngsi-ld:Notification:bad-entity",
            "type": "Notification",
            "data": [{"type": "Intersection"}],
        }
    )
