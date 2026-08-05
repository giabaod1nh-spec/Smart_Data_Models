from dataclasses import replace

from de.gold.canonicalization import canonicalize_record


def test_long_and_unknown_direction_canonicalization(traffic_factory):
    north = canonicalize_record(replace(traffic_factory(), source_direction=" north "))
    unknown = canonicalize_record(replace(traffic_factory(), source_direction="NE"))
    assert (north.canonical_direction, north.direction_mapping_version) == ("N", "direction-v1")
    assert unknown.canonical_direction == "UNKNOWN"
    assert unknown.source_direction == "NE"
    assert "NON_CANONICAL_DIRECTION" in unknown.quality_flags

