from de.gold.aggregators.intersection_window import aggregate_intersection_windows, rollup_directions_to_intersections
from de.gold.aggregators.traffic_window import aggregate_traffic_windows
from de.gold.windowing import expand_windows


def test_direction_rollup_is_one_intersection_per_window(context, two_window_records):
    windowed = expand_windows(two_window_records)
    traffic = aggregate_traffic_windows(windowed, context)
    rollups = rollup_directions_to_intersections(traffic, context)
    current = next(item for item in rollups if item.window.window_size_sec == 60 and item.window.window_start_sim_sec == 60)
    assert (current.avg_queue_length_m, current.max_queue_length_m) == (50, 50)
    assert current.avg_speed_kmh == 20
    intersections = aggregate_intersection_windows(windowed, context)
    assert len([item for item in intersections if item.window.window_size_sec == 60]) == 2

