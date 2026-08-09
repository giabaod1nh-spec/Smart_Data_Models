"""Unit tests for BronzeValidator."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from de.bronze.validator import BronzeValidator

_REPO = Path(__file__).resolve().parents[3]


@pytest.fixture
def validator() -> BronzeValidator:
    v = BronzeValidator(
        _REPO / "contracts" / "events" / "traffic-entity-event-v2.schema.json",
        _REPO / "contracts" / "events" / "traffic-simulation-run-started-v2.schema.json",
    )
    v.load()
    return v


def test_valid_entity_event(validator: BronzeValidator) -> None:
    event = json.loads(
        (_REPO / "contracts" / "events" / "examples" / "camera-event.json").read_text(
            encoding="utf-8"
        )
    )
    outcome = validator.validate(event)
    assert outcome.ok
    assert outcome.kind == "ENTITY"


def test_unsupported_contract_version(validator: BronzeValidator) -> None:
    event = {"contractVersion": "1.0.0", "eventType": "TrafficEntityObserved"}
    outcome = validator.validate(event)
    assert not outcome.ok
    assert outcome.error_code == "UNSUPPORTED_CONTRACT_VERSION"


def test_unsupported_event_type(validator: BronzeValidator) -> None:
    event = {"contractVersion": "2.0.0", "eventType": "UnknownEvent"}
    outcome = validator.validate(event)
    assert not outcome.ok
    assert outcome.error_code == "UNSUPPORTED_EVENT_TYPE"
