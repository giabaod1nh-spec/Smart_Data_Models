"""Shared canonical_json module tests (Contract hashing SoT)."""
from __future__ import annotations

import hashlib

from contracts.canonical_json import canonical_hash, canonical_json, compute_event_id


def test_canonical_json_compact_sorted():
    assert canonical_json({"z": 1, "a": {"y": 2, "b": 3}}) == '{"a":{"b":3,"y":2},"z":1}'


def test_canonical_hash_matches_sha256():
    payload = {"type": "X", "value": 1.5}
    expected = hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()
    assert canonical_hash(payload) == expected
    assert len(expected) == 64


def test_event_id_delimiter_stable():
    a = compute_event_id(
        contract_version="2.0.0",
        simulation_run_id="run-1",
        cycle_sequence=10,
        entity_id="urn:ngsi-ld:Intersection:A",
    )
    b = compute_event_id(
        contract_version="2.0.0",
        simulation_run_id="run-1",
        cycle_sequence=10,
        entity_id="urn:ngsi-ld:Intersection:A",
    )
    assert a == b
    # Different cycle → different id
    c = compute_event_id(
        contract_version="2.0.0",
        simulation_run_id="run-1",
        cycle_sequence=11,
        entity_id="urn:ngsi-ld:Intersection:A",
    )
    assert a != c


def test_namespaced_entity_id_shadow_and_test():
    from contracts.canonical_json import to_namespaced_entity_id, to_shadow_entity_id

    eid = "urn:ngsi-ld:Intersection:A"
    assert to_shadow_entity_id(eid) == "urn:ngsi-ld:Intersection:shadow:A"
    assert to_namespaced_entity_id(eid, "test") == "urn:ngsi-ld:Intersection:test:A"
    assert to_namespaced_entity_id(eid, "production") == eid
    assert to_namespaced_entity_id("urn:ngsi-ld:Intersection:shadow:A", "test") == (
        "urn:ngsi-ld:Intersection:test:A"
    )
