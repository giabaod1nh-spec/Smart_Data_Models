"""Unit tests for canonical hash."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from de.webhook.domain.canonical_hash import canonical_hash

GOLDEN = Path(__file__).resolve().parents[3] / "contracts" / "delivery" / "notification.example.json"


def test_golden_hash_stable():
    parsed = json.loads(GOLDEN.read_text(encoding="utf-8"))
    first = canonical_hash(parsed)
    second = canonical_hash(parsed)
    assert first == second
    assert len(first) == 64


def test_array_order_preserved():
    a = {"type": "Notification", "data": [{"id": "1"}, {"id": "2"}]}
    b = {"type": "Notification", "data": [{"id": "2"}, {"id": "1"}]}
    assert canonical_hash(a) != canonical_hash(b)


def test_object_key_sorting():
    payload = {"z": 1, "a": {"y": 2, "b": 3}, "data": []}
    hashed = canonical_hash(payload)
    expected = '{"a":{"b":3,"y":2},"data":[],"z":1}'
    import hashlib

    assert hashed == hashlib.sha256(expected.encode("utf-8")).hexdigest()


def test_float_stability():
    payload = {"type": "Notification", "value": 1.5}
    raw = json.dumps(payload)
    parsed = json.loads(raw)
    assert canonical_hash(parsed) == canonical_hash({"type": "Notification", "value": 1.5})
