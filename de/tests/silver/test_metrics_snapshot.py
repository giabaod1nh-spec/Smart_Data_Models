"""Metrics snapshot immutability."""
from __future__ import annotations

from de.silver.metrics import Metrics


def test_metrics_snapshot_is_copy():
    m = Metrics()
    m.mark_batch(3)
    m.recovered_partial_count = 2
    m.idempotent_observed_count = 1
    s1 = m.snapshot()
    s1["source_lag"]["x"] = 99
    s2 = m.snapshot()
    assert "x" not in s2["source_lag"]
    assert s2["recovered_partial_count"] == 2
    assert s2["idempotent_observed_count"] == 1
