"""Gold direction-v1 canonicalization tests."""
from __future__ import annotations

from de.gold.contracts import (
    DIRECTION_MAPPING_VERSION,
    QUALITY_FLAG_NON_CANONICAL_DIRECTION,
    canonicalize_direction,
)


def test_direction_mapping_version_locked():
    assert DIRECTION_MAPPING_VERSION == "direction-v1"


def test_cardinal_short_and_long_forms_collapse():
    cases = [
        ("N", "N"), ("NORTH", "N"), (" north ", "N"),
        ("S", "S"), ("SOUTH", "S"),
        ("E", "E"), ("EAST", "E"),
        ("W", "W"), ("WEST", "W"),
    ]
    for raw, expected in cases:
        canonical, source, flags = canonicalize_direction(raw)
        assert canonical == expected
        assert source == raw
        assert flags == ()


def test_unknown_preserves_source_and_quality_flag():
    canonical, source, flags = canonicalize_direction("NORTH-EAST")
    assert canonical == "UNKNOWN"
    assert source == "NORTH-EAST"
    assert flags == (QUALITY_FLAG_NON_CANONICAL_DIRECTION,)


def test_no_duplicate_grain_after_mapping():
    left = canonicalize_direction("N")[0]
    right = canonicalize_direction("NORTH")[0]
    assert left == right == "N"
    # Intersection-level grains do not include direction; mapping must not invent one.
    assert canonicalize_direction("N")[0] in {"N", "S", "E", "W", "UNKNOWN"}
