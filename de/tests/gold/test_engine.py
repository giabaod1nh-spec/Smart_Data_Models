from dataclasses import asdict

from de.gold.engine import GoldTransformationEngine
from de.gold.models import GoldFactKpiResult, GoldFactTrafficWindow


def test_engine_full_order_typed_outputs_and_shuffle_determinism(context, two_window_records):
    engine = GoldTransformationEngine()
    first = engine.transform(two_window_records, context)
    second = engine.transform(tuple(reversed(two_window_records)), context)
    assert first == second
    assert all(isinstance(item, GoldFactTrafficWindow) for item in first.traffic_windows)
    assert all(isinstance(item, GoldFactKpiResult) for item in first.kpi_results)
    assert len(first.traffic_windows) == 12
    assert len(first.intersection_windows) == 3
    assert len(first.signal_operation_windows) == 3
    assert first.conflict_evidence == ()
    assert len(first.lineage_evidence) == (
        len(first.traffic_windows) + len(first.intersection_windows)
        + len(first.comparisons) + len(first.signal_operation_windows)
        + len(first.kpi_results)
    )


def test_engine_rejects_invalid_and_keeps_camera_out_of_metrics(context, traffic_factory):
    from dataclasses import replace

    invalid = replace(traffic_factory(), occupancy_pct=200)
    result = GoldTransformationEngine().transform((invalid,), context)
    assert result.traffic_windows == ()
    assert len(result.unsupported_records) == 1


def test_engine_conflict_blocks_affected_window_kpi(context, two_window_records):
    from dataclasses import replace

    current_n = next(
        row for row in two_window_records
        if getattr(row, "source_direction", None) == "N" and row.simulation_time_sec == 70
        and hasattr(row, "vehicle_count")
    )
    result = GoldTransformationEngine().transform(
        two_window_records + (replace(current_n, source_payload_hash="conflicting-payload"),),
        context,
    )
    assert result.conflict_evidence
    assert not any(
        item.metric_code == "CONGESTION_SCORE_WINDOW"
        and item.window_size_sec == 60 and item.window_start_sim_sec == 60
        for item in result.kpi_results
    )


def test_unknown_direction_is_retained_but_excluded_from_kpi(context, two_window_records, traffic_factory):
    unknown = traffic_factory("NORTH-EAST", 70, offset=999, queue=1000, speed=0, occupancy=100)
    result = GoldTransformationEngine().transform(two_window_records + (unknown,), context)
    unknown_fact = next(
        item for item in result.traffic_windows
        if item.direction == "UNKNOWN" and item.window_size_sec == 60
    )
    congestion = next(
        item for item in result.kpi_results
        if item.metric_code == "CONGESTION_SCORE_WINDOW"
        and item.window_size_sec == 60 and item.window_start_sim_sec == 60
    )
    assert "NON_CANONICAL_DIRECTION" in unknown_fact.quality_flags
    assert congestion.numeric_value == 62.5
    assert "NON_CANONICAL_DIRECTION" in congestion.quality_flags
