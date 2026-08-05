from dataclasses import replace

from de.gold.latest_selector import select_latest


def test_latest_uses_frozen_order_and_detects_position_conflict(traffic_factory):
    older = traffic_factory(time=10, offset=1)
    newer = traffic_factory(time=20, offset=2)
    assert select_latest((newer, older)).record == newer
    conflict = replace(newer, source_payload_hash="changed")
    selected = select_latest((newer, conflict))
    assert selected.record is None
    assert selected.conflicted

