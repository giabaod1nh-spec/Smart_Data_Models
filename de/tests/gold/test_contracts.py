"""Gold 1 contract invariants."""
from __future__ import annotations

from de.gold.contracts import (
    ANALYTICAL_FRESHNESS_VALUES,
    BD1,
    BD3,
    METRIC_SEMANTICS,
    QUALITY_STATUS_VALUES,
    REALTIME_OWNED_FIELDS,
    UNIT_CODES,
    WINDOW_SIZES_SEC,
    canonical_window_id,
)


def test_window_identity_is_stable_and_sensitive_to_components():
    first = canonical_window_id("run-1", "scenario-a", 60, 120.0, 180.0)
    assert first == canonical_window_id("run-1", "scenario-a", 60, 120.0, 180.0)
    assert first != canonical_window_id("run-1", "scenario-a", 300, 120.0, 420.0)
    assert WINDOW_SIZES_SEC == (60, 300)


def test_approved_bd_weights_sum_to_one():
    assert sum(BD1["weights"].values()) == 1.0
    assert sum(BD3["weights"].values()) == 1.0


def test_snapshot_and_gauge_semantics_forbid_sum():
    for metric in (
        "vehicle_count", "pcu_equivalent", "average_speed_kmh", "queue_length_m",
        "waiting_vehicle_count", "occupancy_pct", "arrival_rate_pcu_per_sec",
    ):
        assert "SUM" in METRIC_SEMANTICS[metric]["forbidden"]


def test_realtime_and_analytical_vocabularies_are_explicit():
    assert {"current_vehicle_count", "current_signal_phase", "remaining_green"} <= REALTIME_OWNED_FIELDS
    assert {"CLOSED_COMPLETE", "REVISED", "STALE_ANALYTICAL"} <= ANALYTICAL_FRESHNESS_VALUES
    assert {"VALID", "CONFLICTED", "INSUFFICIENT_DATA"} <= QUALITY_STATUS_VALUES
    assert {"SCORE_0_100", "ORDINAL", "COMPOSITE_SUMMARY"} <= UNIT_CODES
