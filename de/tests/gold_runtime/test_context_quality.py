"""Quality mapping, expected rows, and Gold2 context construction."""
from __future__ import annotations

from de.gold.engine import GoldTransformationEngine
from de.gold_runtime.context_builder import (
    build_context,
    build_inputs,
    build_traffic_input,
    derive_intersection_quality,
    derive_signal_quality,
    expected_rows_for,
    observation_slots,
    order_records,
)
from de.gold_runtime.window_scheduler import make_window_identity
from de.tests.gold_runtime.conftest import (
    DIRECTIONS,
    INTERSECTION_ID,
    NOW,
    RUN_ID,
    SCENARIO_ID,
    intersection_row,
    make_settings,
    signal_row,
    traffic_row,
)


def test_signal_without_phase_is_insufficient_data():
    row = signal_row()
    row["current_phase"] = ""
    assert derive_signal_quality(row) == "INSUFFICIENT_DATA"


def test_intersection_missing_required_is_insufficient():
    row = intersection_row()
    row["derived_traffic_state"] = ""
    assert derive_intersection_quality(row) == "INSUFFICIENT_DATA"


def test_direction_canonicalization_on_traffic_input():
    row = traffic_row(direction="NORTH")
    record = build_traffic_input(row)
    assert record.canonical_direction == "N"
    assert record.source_direction == "NORTH"


def test_expected_rows_matrix(tmp_path):
    settings = make_settings(tmp_path)
    window = make_window_identity("live", RUN_ID, SCENARIO_ID, 60, 0.0)
    expected = expected_rows_for(
        [window],
        intersections=(INTERSECTION_ID,),
        directions=DIRECTIONS,
        traffic_cadence_sec=settings.traffic_expected_cadence_sec,
        intersection_cadence_sec=settings.intersection_expected_cadence_sec,
        signal_cadence_sec=settings.signal_expected_cadence_sec,
    )
    slots = observation_slots(60, 10.0)
    traffic_keys = [k for k in expected if k.startswith("traffic|")]
    assert len(traffic_keys) == 4
    assert all(expected[k] == slots for k in traffic_keys)
    assert not any("camera" in k for k in expected)


def test_build_context_sets_window_closed_and_calls_engine(tmp_path):
    settings = make_settings(tmp_path)
    window = make_window_identity("live", RUN_ID, SCENARIO_ID, 60, 0.0)
    traffic = [
        traffic_row(direction=d, simulation_time_sec=float(t), offset=t * 10 + i + 1)
        for t in range(0, 60, 10)
        for i, d in enumerate(DIRECTIONS)
    ]
    intersection = [
        intersection_row(simulation_time_sec=float(t), offset=500 + t) for t in range(0, 60, 10)
    ]
    signal = [
        signal_row(direction=d, simulation_time_sec=float(t), offset=900 + t * 10 + i)
        for t in range(0, 60, 10)
        for i, d in enumerate(DIRECTIONS)
    ]
    records = order_records(
        build_inputs("silver_fact_traffic_observation", traffic)
        + build_inputs("silver_fact_intersection_state", intersection)
        + build_inputs("silver_fact_signal_state", signal)
    )
    context = build_context(
        settings,
        computed_at=NOW,
        windows=[window],
        intersections=(INTERSECTION_ID,),
        revision_seq=0,
    )
    assert context.window_closed is True
    assert context.namespace == "live"
    assert context.expected_rows is not None
    result = GoldTransformationEngine().transform(records, context)
    assert isinstance(result.traffic_windows, tuple)
