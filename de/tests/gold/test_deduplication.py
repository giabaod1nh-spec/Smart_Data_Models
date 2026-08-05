from dataclasses import replace

from de.gold.deduplication import deduplicate


def test_exact_duplicates_collapse_and_changed_hash_conflicts(traffic_factory):
    row = traffic_factory()
    exact = deduplicate((row, row))
    conflict = deduplicate((row, replace(row, source_payload_hash="different")))
    assert exact.records == (row,)
    assert exact.conflicts == ()
    assert conflict.records == ()
    assert len(conflict.conflicts) == 1


def test_deduplication_is_input_order_independent(traffic_factory):
    first = traffic_factory("N", offset=1)
    second = traffic_factory("S", offset=2)
    assert deduplicate((first, second)) == deduplicate((second, first))

