from de.gold.aggregators.intersection_window import aggregate_intersection_windows, rollup_directions_to_intersections
from de.gold.aggregators.traffic_window import aggregate_traffic_windows
from de.gold.calculators.congestion import calculate_congestion
from de.gold.calculators.priority import calculate_priority
from de.gold.windowing import expand_windows


def test_priority_formula_previous_queue_and_incident(context, two_window_records):
    windowed = expand_windows(two_window_records)
    rollups = rollup_directions_to_intersections(aggregate_traffic_windows(windowed, context), context)
    current = next(item for item in rollups if item.window.window_size_sec == 60 and item.window.window_start_sim_sec == 60)
    previous = next(item for item in rollups if item.window.window_size_sec == 60 and item.window.window_start_sim_sec == 0)
    intersection = next(item for item in aggregate_intersection_windows(windowed, context) if item.window.window_id == current.window.window_id)
    result = calculate_priority(current, calculate_congestion(current), intersection, previous)
    assert result.score == 67.13
    assert "NO_PREV_QUEUE" not in result.calculation.quality_flags


def test_priority_missing_previous_is_explicit(context, two_window_records):
    windowed = expand_windows(two_window_records)
    current = next(item for item in rollup_directions_to_intersections(aggregate_traffic_windows(windowed, context), context) if item.window.window_size_sec == 60 and item.window.window_start_sim_sec == 0)
    result = calculate_priority(current, calculate_congestion(current), None, None)
    assert "NO_PREV_QUEUE" in result.calculation.quality_flags

