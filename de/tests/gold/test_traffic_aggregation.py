from de.gold.aggregators.traffic_window import aggregate_traffic_windows
from de.gold.windowing import expand_windows


def test_snapshot_metrics_use_avg_max_latest_not_sum(context, traffic_factory):
    rows = (traffic_factory(time=10, offset=1, vehicles=10), traffic_factory(time=20, offset=2, vehicles=20))
    result = next(item for item in aggregate_traffic_windows(expand_windows(rows), context) if item.window.window_size_sec == 60)
    assert result.avg_vehicle_count == 15
    assert result.max_vehicle_count == 20
    assert result.latest_vehicle_count == 20

