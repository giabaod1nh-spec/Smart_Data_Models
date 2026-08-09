import json

from de.gold.engine import GoldTransformationEngine


def test_network_overview_is_one_composite_kpi_per_window(context, two_window_records):
    result = GoldTransformationEngine().transform(two_window_records, context)
    network = [item for item in result.kpi_results if item.metric_code == "NETWORK_OVERVIEW_WINDOW"]
    assert len(network) == 3
    current = next(item for item in network if item.window_size_sec == 60 and item.window_start_sim_sec == 60)
    payload = json.loads(current.explanation_json)
    assert payload["observed_intersection_count"] == 1
    assert payload["latest_total_vehicle_count"] == 40
    assert payload["avg_speed_kmh"] == 20
    assert current.intersection_id == ""

