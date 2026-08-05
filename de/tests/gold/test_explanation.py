from de.gold.calculators.explanation import ExplanationFactor, assert_contribution_tolerance, round_half_up


def test_decimal_half_up_and_contribution_tolerance():
    assert round_half_up("2.345") == 2.35
    factors = (ExplanationFactor("A", 1, 1, 1, 10, "COUNT"),)
    assert_contribution_tolerance(10, factors)

