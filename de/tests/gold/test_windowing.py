from dataclasses import replace

import pytest

from de.gold.windowing import assign_window, expand_windows


@pytest.mark.parametrize("time,start60,start300", [(0, 0, 0), (59.999, 0, 0), (60, 60, 0), (299.999, 240, 0), (300, 300, 300)])
def test_window_boundaries(traffic_factory, time, start60, start300):
    row = replace(traffic_factory(), simulation_time_sec=time)
    assert assign_window(row, 60).window_start_sim_sec == start60
    assert assign_window(row, 300).window_start_sim_sec == start300
    assert len(expand_windows((row,))) == 2


def test_invalid_window_time_fails(traffic_factory):
    with pytest.raises(ValueError):
        assign_window(replace(traffic_factory(), simulation_time_sec=-1), 60)

