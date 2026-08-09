from de.gold.aggregators.signal_operation_window import aggregate_signal_windows
from de.gold.aggregators.traffic_window import aggregate_traffic_windows
from de.gold.windowing import expand_windows


def test_signal_counts_shares_and_queue_context(context, two_window_records):
    windowed = expand_windows(two_window_records)
    traffic = aggregate_traffic_windows(windowed, context)
    signals = aggregate_signal_windows(windowed, traffic, context)
    current = next(item for item in signals if item.window.window_size_sec == 60 and item.window.window_start_sim_sec == 60)
    assert current.green_observation_count == 1
    assert current.green_share_pct == 100
    assert current.ctx_max_queue_length_m == 50
    assert not hasattr(current, "delay")

