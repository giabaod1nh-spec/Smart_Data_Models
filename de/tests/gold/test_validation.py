from dataclasses import replace

from de.gold.validation import ANALYTICAL_INSUFFICIENT_DATA, VALID_FOR_GOLD, validate_input


def test_validation_accepts_valid_and_rejects_invalid_metric(traffic_factory):
    row = traffic_factory()
    assert validate_input(row).status == VALID_FOR_GOLD
    invalid = replace(row, occupancy_pct=101.0)
    result = validate_input(invalid)
    assert result.status == ANALYTICAL_INSUFFICIENT_DATA
    assert result.errors == ("INVALID_OCCUPANCY",)

