from dataclasses import replace

from de.gold.lineage import canonical_source_set_hash


def test_lineage_hash_is_order_independent_and_payload_sensitive(traffic_factory):
    first = traffic_factory("N", offset=1)
    second = traffic_factory("S", offset=2)
    assert canonical_source_set_hash((first, second)) == canonical_source_set_hash((second, first))
    assert canonical_source_set_hash((first,)) != canonical_source_set_hash((replace(first, source_payload_hash="changed"),))

