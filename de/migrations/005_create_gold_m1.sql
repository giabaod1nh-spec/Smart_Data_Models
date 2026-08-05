-- Migration version: k10-gold-m1-v1
-- Normative source: docs/gold/GOLD_1_BUSINESS_CONTRACT_SCHEMA_PLAN.md
-- Harden: current-row views, physical definition ordering, direction-v1

CREATE DATABASE IF NOT EXISTS smart_traffic;

-- Recreate empty Gold M1 objects so additive harden columns and view semantics apply.
-- Gold runtime facts are not produced until Gold 2; drop is schema-only for M1.
DROP VIEW IF EXISTS smart_traffic.gold_mart_network_window_overview;
DROP VIEW IF EXISTS smart_traffic.gold_mart_intersection_window_summary;
DROP VIEW IF EXISTS smart_traffic.gold_mart_direction_window_summary;
DROP VIEW IF EXISTS smart_traffic.gold_mart_congestion_window;
DROP VIEW IF EXISTS smart_traffic.gold_mart_priority_window_ranking;
DROP VIEW IF EXISTS smart_traffic.gold_mart_signal_operation_window;

DROP TABLE IF EXISTS smart_traffic.gold_processing_ledger;
DROP TABLE IF EXISTS smart_traffic.gold_fact_kpi_result;
DROP TABLE IF EXISTS smart_traffic.gold_fact_signal_operation_window;
DROP TABLE IF EXISTS smart_traffic.gold_fact_traffic_comparison;
DROP TABLE IF EXISTS smart_traffic.gold_fact_intersection_window;
DROP TABLE IF EXISTS smart_traffic.gold_fact_traffic_window;
DROP TABLE IF EXISTS smart_traffic.gold_dim_metric_definition;
DROP TABLE IF EXISTS smart_traffic.gold_dim_window;
DROP TABLE IF EXISTS smart_traffic.gold_dim_approach;
DROP TABLE IF EXISTS smart_traffic.gold_dim_intersection;
DROP TABLE IF EXISTS smart_traffic.gold_dim_scenario;
DROP TABLE IF EXISTS smart_traffic.gold_dim_run;

CREATE TABLE IF NOT EXISTS smart_traffic.gold_dim_run (
    simulation_run_id String,
    scenario_id LowCardinality(String),
    seed Nullable(String),
    producer_id String,
    started_at DateTime64(3, 'UTC'),
    ended_at Nullable(DateTime64(3, 'UTC')),
    run_status LowCardinality(String),
    contract_version LowCardinality(String),
    node_count Nullable(UInt32),
    source_bronze_run_id String,
    source_hash FixedString(64),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (simulation_run_id, definition_major, definition_minor);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_dim_scenario (
    scenario_id String,
    description String,
    source_hash FixedString(64),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (scenario_id, definition_major, definition_minor);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_dim_intersection (
    intersection_id String,
    intersection_name String,
    latitude Float64,
    longitude Float64,
    network_zone LowCardinality(String),
    connected_intersections Array(String),
    source_hash FixedString(64),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (intersection_id, definition_major, definition_minor);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_dim_approach (
    intersection_id String,
    direction LowCardinality(String),
    source_direction String,
    direction_mapping_version LowCardinality(String),
    source_hash FixedString(64),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (intersection_id, direction, definition_major, definition_minor);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_dim_window (
    window_id FixedString(64),
    window_size_sec UInt16,
    window_start_sim_sec Float64,
    window_end_sim_sec Float64,
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (window_size_sec, window_start_sim_sec, window_end_sim_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_dim_metric_definition (
    metric_code LowCardinality(String),
    metric_version LowCardinality(String),
    metric_name String,
    description String,
    grain String,
    formula_identifier LowCardinality(String),
    unit_code LowCardinality(String),
    approval_status LowCardinality(String),
    formula_json String,
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_code, definition_major, definition_minor);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_fact_traffic_window (
    simulation_run_id String,
    scenario_id LowCardinality(String),
    intersection_id String,
    direction LowCardinality(String),
    source_direction String,
    direction_mapping_version LowCardinality(String),
    window_id FixedString(64),
    window_size_sec UInt16,
    window_start_sim_sec Float64,
    window_end_sim_sec Float64,
    avg_vehicle_count Float64,
    max_vehicle_count UInt32,
    latest_vehicle_count UInt32,
    avg_pcu_equivalent Float64,
    max_pcu_equivalent Float32,
    latest_pcu_equivalent Float32,
    avg_speed_kmh Float64,
    min_speed_kmh Float32,
    max_speed_kmh Float32,
    latest_speed_kmh Float32,
    avg_queue_length_m Float64,
    max_queue_length_m Float32,
    latest_queue_length_m Float32,
    avg_waiting_vehicle_count Float64,
    max_waiting_vehicle_count UInt32,
    avg_occupancy_pct Float64,
    max_occupancy_pct Float32,
    avg_arrival_rate_pcu_per_sec Float64,
    max_arrival_rate_pcu_per_sec Float32,
    spillback_observation_count UInt64,
    spillback_ratio_pct Float64,
    latest_traffic_status LowCardinality(String),
    namespace String,
    source_set_hash FixedString(64),
    source_row_count UInt64,
    source_valid_row_count UInt64,
    source_min_simulation_time Float64,
    source_max_simulation_time Float64,
    source_min_offset Nullable(Int64),
    source_max_offset Nullable(Int64),
    source_tables Array(String),
    quality_status LowCardinality(String),
    quality_flags String,
    analytical_freshness_status LowCardinality(String),
    source_latest_simulation_time Float64,
    source_latest_processed_at DateTime64(3, 'UTC'),
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    revision_seq UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (namespace, simulation_run_id, scenario_id, intersection_id, direction, window_size_sec, window_start_sim_sec, definition_major, definition_minor, revision_seq);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_fact_intersection_window (
    simulation_run_id String,
    scenario_id LowCardinality(String),
    intersection_id String,
    window_id FixedString(64),
    window_size_sec UInt16,
    window_start_sim_sec Float64,
    window_end_sim_sec Float64,
    avg_total_vehicle_count Float64,
    max_total_vehicle_count Nullable(UInt32),
    latest_total_vehicle_count Nullable(UInt32),
    latest_overall_traffic_status LowCardinality(String),
    latest_derived_traffic_state LowCardinality(String),
    latest_phase LowCardinality(String),
    incident_observation_count UInt64,
    incident_occurrence UInt8,
    spillback_observation_count UInt64,
    spillback_occurrence UInt8,
    box_blocked_observation_count UInt64,
    box_blocked_occurrence UInt8,
    namespace String,
    source_set_hash FixedString(64),
    source_row_count UInt64,
    source_valid_row_count UInt64,
    source_min_simulation_time Float64,
    source_max_simulation_time Float64,
    source_min_offset Nullable(Int64),
    source_max_offset Nullable(Int64),
    source_tables Array(String),
    quality_status LowCardinality(String),
    quality_flags String,
    analytical_freshness_status LowCardinality(String),
    source_latest_simulation_time Float64,
    source_latest_processed_at DateTime64(3, 'UTC'),
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    revision_seq UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (namespace, simulation_run_id, scenario_id, intersection_id, window_size_sec, window_start_sim_sec, definition_major, definition_minor, revision_seq);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_fact_traffic_comparison (
    simulation_run_id String,
    scenario_id LowCardinality(String),
    intersection_id String,
    direction LowCardinality(String),
    source_direction String,
    direction_mapping_version LowCardinality(String),
    metric_code LowCardinality(String),
    current_window_id FixedString(64),
    current_window_size_sec UInt16,
    current_window_start_sim_sec Float64,
    current_window_end_sim_sec Float64,
    previous_window_id FixedString(64),
    previous_window_start_sim_sec Float64,
    previous_window_end_sim_sec Float64,
    current_value Nullable(Float64),
    previous_value Nullable(Float64),
    absolute_change Nullable(Float64),
    percent_change Nullable(Float64),
    change_direction LowCardinality(String),
    comparison_status LowCardinality(String),
    namespace String,
    source_set_hash FixedString(64),
    source_row_count UInt64,
    source_valid_row_count UInt64,
    source_min_simulation_time Float64,
    source_max_simulation_time Float64,
    source_min_offset Nullable(Int64),
    source_max_offset Nullable(Int64),
    source_tables Array(String),
    quality_status LowCardinality(String),
    quality_flags String,
    analytical_freshness_status LowCardinality(String),
    source_latest_simulation_time Float64,
    source_latest_processed_at DateTime64(3, 'UTC'),
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    revision_seq UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (namespace, simulation_run_id, scenario_id, intersection_id, direction, metric_code, current_window_size_sec, current_window_start_sim_sec, definition_major, definition_minor, revision_seq);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_fact_signal_operation_window (
    simulation_run_id String,
    scenario_id LowCardinality(String),
    intersection_id String,
    direction LowCardinality(String),
    source_direction String,
    direction_mapping_version LowCardinality(String),
    window_id FixedString(64),
    window_size_sec UInt16,
    window_start_sim_sec Float64,
    window_end_sim_sec Float64,
    observation_count UInt64,
    green_observation_count UInt64,
    red_observation_count UInt64,
    yellow_observation_count UInt64,
    other_status_count UInt64,
    green_share_pct Nullable(Float64),
    red_share_pct Nullable(Float64),
    yellow_share_pct Nullable(Float64),
    dominant_signal_status LowCardinality(String),
    dominant_phase LowCardinality(String),
    avg_configured_green_duration_sec Nullable(Float64),
    avg_configured_red_duration_sec Nullable(Float64),
    avg_configured_yellow_duration_sec Nullable(Float64),
    latest_timing_mode LowCardinality(String),
    ctx_avg_queue_length_m Nullable(Float64),
    ctx_max_queue_length_m Nullable(Float64),
    namespace String,
    source_set_hash FixedString(64),
    source_row_count UInt64,
    source_valid_row_count UInt64,
    source_min_simulation_time Float64,
    source_max_simulation_time Float64,
    source_min_offset Nullable(Int64),
    source_max_offset Nullable(Int64),
    source_tables Array(String),
    quality_status LowCardinality(String),
    quality_flags String,
    analytical_freshness_status LowCardinality(String),
    source_latest_simulation_time Float64,
    source_latest_processed_at DateTime64(3, 'UTC'),
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    revision_seq UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (namespace, simulation_run_id, scenario_id, intersection_id, direction, window_size_sec, window_start_sim_sec, definition_major, definition_minor, revision_seq);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_fact_kpi_result (
    simulation_run_id String,
    scenario_id LowCardinality(String),
    intersection_id String,
    direction LowCardinality(String),
    source_direction String,
    direction_mapping_version LowCardinality(String),
    window_id FixedString(64),
    window_size_sec UInt16,
    window_start_sim_sec Float64,
    window_end_sim_sec Float64,
    metric_code LowCardinality(String),
    metric_version LowCardinality(String),
    numeric_value Nullable(Float64),
    unit_code LowCardinality(String),
    status LowCardinality(String),
    explanation_json String,
    namespace String,
    source_set_hash FixedString(64),
    source_row_count UInt64,
    source_valid_row_count UInt64,
    source_min_simulation_time Float64,
    source_max_simulation_time Float64,
    source_min_offset Nullable(Int64),
    source_max_offset Nullable(Int64),
    source_tables Array(String),
    quality_status LowCardinality(String),
    quality_flags String,
    analytical_freshness_status LowCardinality(String),
    source_latest_simulation_time Float64,
    source_latest_processed_at DateTime64(3, 'UTC'),
    computed_at DateTime64(3, 'UTC'),
    gold_schema_version LowCardinality(String),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    revision_seq UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (namespace, simulation_run_id, scenario_id, metric_code, intersection_id, direction, window_size_sec, window_start_sim_sec, definition_major, definition_minor, revision_seq);

CREATE TABLE IF NOT EXISTS smart_traffic.gold_processing_ledger (
    namespace String,
    source_set_hash FixedString(64),
    definition_version LowCardinality(String),
    definition_major UInt16,
    definition_minor UInt16,
    revision_seq UInt64,
    disposition LowCardinality(String),
    computed_at DateTime64(3, 'UTC'),
    error_message String,
    gold_schema_version LowCardinality(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (namespace, source_set_hash, definition_major, definition_minor, revision_seq);

-- Current-row order (locked): definition_major DESC, definition_minor DESC,
-- revision_seq DESC, computed_at DESC, source_set_hash DESC.
-- Consumers must not deduplicate; each view emits <= 1 row per business identity.

CREATE VIEW IF NOT EXISTS smart_traffic.gold_mart_intersection_window_summary AS
SELECT * EXCEPT (_gold_rn)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY namespace, simulation_run_id, scenario_id, intersection_id, window_id
            ORDER BY definition_major DESC, definition_minor DESC, revision_seq DESC, computed_at DESC, source_set_hash DESC
        ) AS _gold_rn
    FROM smart_traffic.gold_fact_intersection_window
)
WHERE _gold_rn = 1;

CREATE VIEW IF NOT EXISTS smart_traffic.gold_mart_direction_window_summary AS
SELECT * EXCEPT (_gold_rn)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY namespace, simulation_run_id, scenario_id, intersection_id, direction, window_id
            ORDER BY definition_major DESC, definition_minor DESC, revision_seq DESC, computed_at DESC, source_set_hash DESC
        ) AS _gold_rn
    FROM smart_traffic.gold_fact_traffic_window
)
WHERE _gold_rn = 1;

CREATE VIEW IF NOT EXISTS smart_traffic.gold_mart_congestion_window AS
SELECT * EXCEPT (_gold_rn)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY namespace, simulation_run_id, scenario_id, intersection_id, window_id, metric_code
            ORDER BY definition_major DESC, definition_minor DESC, revision_seq DESC, computed_at DESC, source_set_hash DESC
        ) AS _gold_rn
    FROM smart_traffic.gold_fact_kpi_result
    WHERE metric_code = 'CONGESTION_SCORE_WINDOW'
)
WHERE _gold_rn = 1;

CREATE VIEW IF NOT EXISTS smart_traffic.gold_mart_priority_window_ranking AS
SELECT * EXCEPT (_gold_rn)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY namespace, simulation_run_id, scenario_id, intersection_id, window_id, metric_code
            ORDER BY definition_major DESC, definition_minor DESC, revision_seq DESC, computed_at DESC, source_set_hash DESC
        ) AS _gold_rn
    FROM smart_traffic.gold_fact_kpi_result
    WHERE metric_code IN ('INTERSECTION_PRIORITY_WINDOW', 'PRIORITY_RANK')
)
WHERE _gold_rn = 1;

CREATE VIEW IF NOT EXISTS smart_traffic.gold_mart_signal_operation_window AS
SELECT * EXCEPT (_gold_rn)
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY namespace, simulation_run_id, scenario_id, intersection_id, direction, window_id
            ORDER BY definition_major DESC, definition_minor DESC, revision_seq DESC, computed_at DESC, source_set_hash DESC
        ) AS _gold_rn
    FROM smart_traffic.gold_fact_signal_operation_window
)
WHERE _gold_rn = 1;

-- STRUCTURAL SERVING VIEW — POPULATED AFTER GOLD 2
-- Business identity: (namespace, simulation_run_id, scenario_id, window_id, metric_definition_set_version)
-- Current-row order locked identically; no fake network aggregation in Gold 1.
CREATE VIEW IF NOT EXISTS smart_traffic.gold_mart_network_window_overview AS
SELECT
    namespace,
    simulation_run_id,
    scenario_id,
    window_id,
    window_size_sec,
    window_start_sim_sec,
    window_end_sim_sec,
    avg_total_vehicle_count,
    latest_overall_traffic_status,
    quality_status,
    analytical_freshness_status,
    computed_at,
    definition_version,
    definition_major,
    definition_minor,
    revision_seq,
    source_set_hash,
    gold_schema_version
FROM smart_traffic.gold_fact_intersection_window
WHERE 0;

INSERT INTO smart_traffic.gold_dim_metric_definition
(metric_code, metric_version, metric_name, description, grain, formula_identifier, unit_code,
 approval_status, formula_json, definition_version, definition_major, definition_minor,
 computed_at, gold_schema_version)
VALUES
('CONGESTION_SCORE_WINDOW', 'v1.0', 'Congestion Score Window', 'Closed analytical window congestion score.', 'namespace,run,scenario,intersection,window', 'bd1_congestion_window_v1', 'SCORE_0_100', 'APPROVED', '{"weights":{"queue":0.35,"speed":0.30,"occupancy":0.20,"spillback":0.15}}', 'v1.0', 1, 0, now64(3, 'UTC'), 'k10-gold-m1-v1'),
('SIGNAL_OPERATION_SUMMARY_WINDOW', 'v1.0', 'Signal Operation Summary Window', 'Signal observation distribution - not a performance score.', 'namespace,run,scenario,intersection,direction,window', 'bd2_signal_operation_window_v1', 'COMPOSITE_SUMMARY', 'APPROVED', '{}', 'v1.0', 1, 0, now64(3, 'UTC'), 'k10-gold-m1-v1'),
('INTERSECTION_PRIORITY_WINDOW', 'v1.0', 'Intersection Priority Window', 'Closed analytical window priority score.', 'namespace,run,scenario,intersection,window', 'bd3_priority_window_v1', 'SCORE_0_100', 'APPROVED', '{"weights":{"congestion":0.45,"queue_level":0.20,"queue_growth":0.15,"spillback":0.10,"incident":0.10}}', 'v1.0', 1, 0, now64(3, 'UTC'), 'k10-gold-m1-v1'),
('PRIORITY_RANK', 'v1.0', 'Priority Rank', 'Ordinal rank for the priority window.', 'namespace,run,scenario,window,intersection', 'bd3_priority_rank_v1', 'ORDINAL', 'APPROVED', '{}', 'v1.0', 1, 0, now64(3, 'UTC'), 'k10-gold-m1-v1');
