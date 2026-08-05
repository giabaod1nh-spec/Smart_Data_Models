from de.gold.quality import analytical_freshness, combine_quality, pooled_coverage


def test_quality_precedence_coverage_and_freshness_are_locked():
    assert combine_quality(("VALID", "PARTIAL", "LOW_COVERAGE")) == "LOW_COVERAGE"
    assert pooled_coverage(8, 10).status == "VALID"
    assert pooled_coverage(79, 100).status == "LOW_COVERAGE"
    assert pooled_coverage(3, 10).status == "LOW_COVERAGE"
    assert pooled_coverage(29, 100).status == "INSUFFICIENT_DATA"
    assert pooled_coverage(0, 0).status == "INSUFFICIENT_DATA"
    assert analytical_freshness(closed=True, revision=False, age_sec=1, stale_after_sec=10, quality_status="VALID") == "CLOSED_COMPLETE"
