from de.gold.calculators.comparison import compare_values


def test_comparison_absolute_percent_zero_and_missing():
    result = compare_values(15, 10)
    assert (result.absolute_change, result.percent_change, result.change_direction, result.comparison_status) == (5, 50, "INCREASE", "COMPARABLE")
    assert compare_values(10, 0).comparison_status == "NOT_COMPARABLE"
    assert compare_values(10, None).absolute_change is None
    assert compare_values(10, 5, compatible=False).comparison_status == "NOT_COMPARABLE"

