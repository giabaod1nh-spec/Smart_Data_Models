"""Unit tests for de.silver.unwrapper."""
from __future__ import annotations

import json

from de.silver.unwrapper import (
    parse_entity_payload,
    unwrap_geoproperty,
    unwrap_property,
    unwrap_relationship,
)


def test_parse_valid_object():
    result = parse_entity_payload('{"type":"VehicleSensor","id":"x"}')
    assert result.is_success
    assert result.payload_dict["type"] == "VehicleSensor"


def test_parse_empty_json():
    result = parse_entity_payload("   ")
    assert not result.is_success
    assert result.error_code == "INVALID_JSON_PAYLOAD"


def test_parse_invalid_json():
    result = parse_entity_payload("{not-json")
    assert not result.is_success
    assert result.error_code == "INVALID_JSON_PAYLOAD"


def test_parse_rejects_non_object():
    result = parse_entity_payload("[1,2,3]")
    assert not result.is_success
    assert result.error_code == "INVALID_JSON_PAYLOAD"


def test_unwrap_property_plain_and_at_value():
    assert unwrap_property({"type": "Property", "value": 5}).value == 5
    node = {"type": "Property", "value": {"@type": "DateTime", "@value": "2026-01-01T00:00:00Z"}}
    assert unwrap_property(node).value == "2026-01-01T00:00:00Z"


def test_unwrap_property_malformed():
    result = unwrap_property({"type": "Property"})  # no value key — still has type Property
    # type==Property but missing value → value None success per plan (node.get("value"))
    assert result.is_success
    assert result.value is None

    bad = unwrap_property({"foo": 1})
    assert not bad.is_success
    assert bad.error_code == "MALFORMED_PROPERTY_WRAPPER"


def test_unwrap_relationship():
    r = unwrap_relationship({"type": "Relationship", "object": "urn:ngsi-ld:Intersection:A"})
    assert r.is_success and r.value == "urn:ngsi-ld:Intersection:A"
    bad = unwrap_relationship({"type": "Something"})
    assert not bad.is_success


def test_unwrap_geoproperty():
    node = {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [106.7, 10.7]}}
    r = unwrap_geoproperty(node)
    assert r.is_success
    assert r.value == (106.7, 10.7)


def test_unwrap_geoproperty_malformed():
    assert not unwrap_geoproperty({"value": {}}).is_success
    assert not unwrap_geoproperty("nope").is_success


def test_unwrap_plain_scalar():
    assert unwrap_property(42).value == 42
