-- Bounded BS-11 acceptance fixture (live namespace, definition v1.0/1/0)
INSERT INTO smart_traffic.gold_processing_ledger
(namespace, source_set_hash, definition_version, definition_major, definition_minor,
 revision_seq, disposition, computed_at, error_message, gold_schema_version)
VALUES
('live', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'v1.0', 1, 0, 1,
 'CHECKPOINTED', now64(3, 'UTC'), '', 'k10-gold-m1-v1');

INSERT INTO smart_traffic.gold_fact_intersection_window
(simulation_run_id, scenario_id, intersection_id, window_id, window_size_sec,
 window_start_sim_sec, window_end_sim_sec, avg_total_vehicle_count,
 max_total_vehicle_count, latest_total_vehicle_count,
 latest_overall_traffic_status, latest_derived_traffic_state, latest_phase,
 incident_observation_count, incident_occurrence, spillback_observation_count, spillback_occurrence,
 box_blocked_observation_count, box_blocked_occurrence, namespace, source_set_hash,
 source_row_count, source_valid_row_count, source_min_simulation_time, source_max_simulation_time,
 source_tables, quality_status, quality_flags, analytical_freshness_status,
 source_latest_simulation_time, source_latest_processed_at, computed_at, gold_schema_version,
 definition_version, definition_major, definition_minor, revision_seq)
VALUES
('run-1', 'normal', 'int-001', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', 60,
 0, 60, 15.5, 20, 18, 'MODERATE', 'FLOWING', 'G1',
 0, 0, 2, 1, 0, 0, 'live', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
 10, 10, 0, 60, ['silver_fact_traffic_observation'], 'VALID', '', 'FRESH',
 59.5, now64(3, 'UTC'), now64(3, 'UTC'), 'k10-gold-m1-v1', 'v1.0', 1, 0, 1);

INSERT INTO smart_traffic.gold_fact_traffic_window
(simulation_run_id, scenario_id, intersection_id, direction, source_direction, direction_mapping_version,
 window_id, window_size_sec, window_start_sim_sec, window_end_sim_sec,
 avg_vehicle_count, max_vehicle_count, latest_vehicle_count, avg_pcu_equivalent,
 avg_speed_kmh, avg_queue_length_m, max_queue_length_m, latest_queue_length_m,
 avg_occupancy_pct, spillback_observation_count, spillback_ratio_pct, latest_traffic_status,
 namespace, source_set_hash, source_row_count, source_valid_row_count,
 source_min_simulation_time, source_max_simulation_time, source_tables,
 quality_status, quality_flags, analytical_freshness_status,
 source_latest_simulation_time, source_latest_processed_at, computed_at, gold_schema_version,
 definition_version, definition_major, definition_minor, revision_seq)
VALUES
('run-1', 'normal', 'int-001', 'NORTH', 'north', 'direction-v1',
 '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', 60, 0, 60,
 8.0, 12, 10, 8.5, 35.0, 12.5, 20.0, 15.0, 45.0, 1, 10.0, 'MODERATE',
 'live', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 5, 5,
 0, 60, ['silver_fact_traffic_observation'], 'VALID', '', 'FRESH',
 59.5, now64(3, 'UTC'), now64(3, 'UTC'), 'k10-gold-m1-v1', 'v1.0', 1, 0, 1);

INSERT INTO smart_traffic.gold_fact_kpi_result
(simulation_run_id, scenario_id, intersection_id, direction, source_direction, direction_mapping_version,
 window_id, window_size_sec, window_start_sim_sec, window_end_sim_sec,
 metric_code, metric_version, numeric_value, unit_code, status, explanation_json,
 namespace, source_set_hash, source_row_count, source_valid_row_count,
 source_min_simulation_time, source_max_simulation_time, source_tables,
 quality_status, quality_flags, analytical_freshness_status,
 source_latest_simulation_time, source_latest_processed_at, computed_at, gold_schema_version,
 definition_version, definition_major, definition_minor, revision_seq)
VALUES
('run-1', 'normal', 'int-001', 'NORTH', 'north', 'direction-v1',
 '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', 60, 0, 60,
 'CONGESTION_SCORE_WINDOW', 'v1.0', 62.5, 'SCORE_0_100', 'COMPUTED', '{}',
 'live', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1,
 0, 60, ['gold_fact_traffic_window'], 'VALID', '', 'FRESH',
 59.5, now64(3, 'UTC'), now64(3, 'UTC'), 'k10-gold-m1-v1', 'v1.0', 1, 0, 1),
('run-1', 'normal', 'int-001', 'NORTH', 'north', 'direction-v1',
 '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', 60, 0, 60,
 'INTERSECTION_PRIORITY_WINDOW', 'v1.0', 78.0, 'SCORE_0_100', 'COMPUTED', '{}',
 'live', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1,
 0, 60, ['gold_fact_traffic_window'], 'VALID', '', 'FRESH',
 59.5, now64(3, 'UTC'), now64(3, 'UTC'), 'k10-gold-m1-v1', 'v1.0', 1, 0, 1),
('run-1', 'normal', 'int-001', 'NORTH', 'north', 'direction-v1',
 '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', 60, 0, 60,
 'PRIORITY_RANK', 'v1.0', 2.0, 'ORDINAL', 'COMPUTED', '{}',
 'live', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 1,
 0, 60, ['gold_fact_traffic_window'], 'VALID', '', 'FRESH',
 59.5, now64(3, 'UTC'), now64(3, 'UTC'), 'k10-gold-m1-v1', 'v1.0', 1, 0, 1);
