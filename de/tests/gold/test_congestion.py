import json

from de.gold.aggregators.intersection_window import rollup_directions_to_intersections
from de.gold.aggregators.traffic_window import aggregate_traffic_windows
from de.gold.calculators.congestion import calculate_congestion
from de.gold.windowing import expand_windows


def test_congestion_formula_range_rounding_and_explanation(context, two_window_records):
    traffic = aggregate_traffic_windows(expand_windows(two_window_records), context)
    rollup = next(item for item in rollup_directions_to_intersections(traffic, context) if item.window.window_size_sec == 60 and item.window.window_start_sim_sec == 60)
    result = calculate_congestion(rollup)
    assert result.numeric_value == 62.5
    assert result.status == "MEDIUM"
    assert [item["factor"] for item in json.loads(result.explanation_json)["factors"]] == [
        "QUEUE_HIGH", "SPEED_LOW", "OCCUPANCY_HIGH", "SPILLBACK_PRESENT",
    ]

