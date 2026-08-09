"""Unit tests for de.silver.normalizers."""
from __future__ import annotations

import math

from de.silver.normalizers import (
    coerce_float32,
    coerce_uint32,
    normalize_boolean,
    normalize_direction,
)


def test_normalize_direction():
    assert normalize_direction("NORTHBOUND") == "N"
    assert normalize_direction("west") == "W"
    assert normalize_direction("ALL_DIRECTIONS") is None
    assert normalize_direction(None) is None
    assert normalize_direction(1) is None


def test_normalize_boolean():
    assert normalize_boolean(True) == 1
    assert normalize_boolean(False) == 0
    assert normalize_boolean(1) == 1
    assert normalize_boolean(0) == 0
    assert normalize_boolean("true") == 1
    assert normalize_boolean("FALSE") == 0
    assert normalize_boolean("yes") is None
    assert normalize_boolean(2) is None


def test_coerce_uint32_rejects_bool():
    assert coerce_uint32(True) is None
    assert coerce_uint32(False) is None


def test_coerce_uint32_numeric_string_and_int():
    assert coerce_uint32(5) == 5
    assert coerce_uint32("12") == 12
    assert coerce_uint32(3.0) == 3
    assert coerce_uint32(-1) is None
    assert coerce_uint32(1.5) is None


def test_coerce_uint32_rejects_nan_inf():
    assert coerce_uint32(float("nan")) is None
    assert coerce_uint32(float("inf")) is None
    assert coerce_uint32("nan") is None


def test_coerce_float32():
    assert coerce_float32(1.5) == 1.5
    assert coerce_float32("2.25") == 2.25
    assert coerce_float32(True) is None
    assert coerce_float32(math.nan) is None
    assert coerce_float32(math.inf) is None
