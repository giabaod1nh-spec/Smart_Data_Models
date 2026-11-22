"""Portable synthetic wire fixture; independent of generated topology files."""
from copy import deepcopy


def snapshot_v1() -> dict:
    directions = ('East', 'South', 'West')
    measured_zero = dict(vehicle_count=0, pcu_equivalent=0.0, left_count=0,
        straight_count=0, right_count=0, average_speed_kmh=0.0,
        waiting_vehicle_count=0, queue_length_m=0.0, occupancy_pct=0.0,
        vehicle_class_composition={})
    return dict(node_id='A', simulation_run_id='run-contract', simulation_time_sec=12.0,
        scenario='normal', control_mode='FIXED', phase='NS_GREEN',
        captured_at='2026-01-01T00:00:12Z', topology_digest='v1:sha256:' + 'a' * 64,
        incidents=[], colors={name: 'green' for name in directions},
        directions={name: deepcopy(measured_zero) for name in directions})
