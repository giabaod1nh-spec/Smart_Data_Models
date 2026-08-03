-- Migration version: k9-silver-v1
-- Normative source: docs/siliver/SILVER_1_CONTRACT_SCHEMA_PLAN.md §20–§21
-- Plan 1 overrides: Camera fact table; no scenario_type/weather columns.

CREATE DATABASE IF NOT EXISTS smart_traffic;

-- ── Dimensions ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS smart_traffic.silver_dim_run (
    simulation_run_id   String,
    scenario_id         LowCardinality(String),
    seed                Nullable(String),
    producer_id         String,
    started_at          DateTime64(3, 'UTC'),
    ended_at            Nullable(DateTime64(3, 'UTC')),
    run_status          LowCardinality(String) DEFAULT 'RUNNING',
    contract_version    LowCardinality(String),
    node_count          Nullable(UInt32),
    source_bronze_run_id String,
    created_at          DateTime64(3, 'UTC'),
    updated_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (simulation_run_id);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_dim_intersection (
    intersection_id     String,
    intersection_name   String,
    latitude            Float64,
    longitude           Float64,
    network_zone        LowCardinality(String) DEFAULT '',
    connected_intersections Array(String),
    source_hash         FixedString(64),
    source_bronze_event_id String,
    created_at          DateTime64(3, 'UTC'),
    updated_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (intersection_id);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_dim_approach (
    intersection_id     String,
    direction           LowCardinality(String),
    source_bronze_event_id String,
    created_at          DateTime64(3, 'UTC'),
    updated_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (intersection_id, direction);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_dim_scenario (
    scenario_id         String,
    description         String DEFAULT '',
    created_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (scenario_id);

-- ── Facts ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_traffic_observation (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    direction               LowCardinality(String),
    source_entity_id        String,

    vehicle_count           UInt32,
    pcu_equivalent          Float32,
    average_speed_kmh       Float32,
    queue_length_m          Float32,
    waiting_vehicle_count   UInt32 DEFAULT 0,
    occupancy_pct           Float32,
    arrival_rate_pcu_per_sec Float32 DEFAULT 0.0,
    traffic_status          LowCardinality(String) DEFAULT 'UNKNOWN',
    spillback_risk          UInt8 DEFAULT 0,
    dominant_waiting_reason LowCardinality(String) DEFAULT '',

    scenario_id             LowCardinality(String),

    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),

    quality_status          LowCardinality(String) DEFAULT 'VALID',
    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (simulation_run_id, intersection_id, direction, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_signal_state (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    direction               LowCardinality(String),
    source_entity_id        String,

    signal_status           LowCardinality(String),
    current_phase           LowCardinality(String),
    green_duration_sec      Nullable(Float32),
    red_duration_sec        Nullable(Float32),
    yellow_duration_sec     Nullable(Float32),
    timing_mode             LowCardinality(String) DEFAULT 'FIXED_TIME',

    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),

    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (simulation_run_id, intersection_id, direction, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_intersection_state (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    source_entity_id        String,

    overall_traffic_status  LowCardinality(String),
    derived_traffic_state   LowCardinality(String) DEFAULT 'STABLE',
    current_phase           LowCardinality(String),
    has_active_incident     UInt8 DEFAULT 0,
    has_spillback           UInt8 DEFAULT 0,
    is_box_blocked          UInt8 DEFAULT 0,
    total_vehicle_count     Nullable(UInt32),

    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),

    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (simulation_run_id, intersection_id, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_camera_observation (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    source_entity_id        String,

    vehicle_count           Nullable(UInt32),
    average_speed_kmh       Nullable(Float32),
    occupancy_pct           Nullable(Float32),
    traffic_status          LowCardinality(String) DEFAULT 'UNKNOWN',
    incident_detected       UInt8 DEFAULT 0,
    confidence              Float32 DEFAULT 1.0,
    recommended_signal_action LowCardinality(String) DEFAULT 'KEEP',
    incident_type           LowCardinality(String) DEFAULT 'NONE',
    incident_severity       LowCardinality(String) DEFAULT 'NONE',

    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),

    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (simulation_run_id, intersection_id, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_run_event (
    simulation_run_id       String,
    event_name              LowCardinality(String),
    event_simulation_time   Float64 DEFAULT 0,
    scenario_id             LowCardinality(String),
    producer_id             String,

    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),

    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (simulation_run_id, event_simulation_time);

-- ── Quarantine & Ledger ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS smart_traffic.silver_quarantine (
    silver_quarantine_id    String,
    source_bronze_event_id  String,
    raw_ingestion_id        String,
    simulation_run_id       Nullable(String),
    entity_id               Nullable(String),
    entity_type             Nullable(String),
    failure_stage           LowCardinality(String),
    error_code              String,
    error_message           String DEFAULT '',
    retryable               UInt8 DEFAULT 0,
    payload_hash            String DEFAULT '',
    source_payload          String CODEC(ZSTD(3)),
    created_at              DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (failure_stage, error_code, created_at);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_processing_ledger (
    checkpoint_namespace    LowCardinality(String),
    source_bronze_event_id  String,
    raw_ingestion_id        String,
    payload_hash            String,
    disposition             LowCardinality(String),
    target_table            String,
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (checkpoint_namespace, source_bronze_event_id);

-- ── Replay Tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_traffic_observation_replay (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    direction               LowCardinality(String),
    source_entity_id        String,
    vehicle_count           UInt32,
    pcu_equivalent          Float32,
    average_speed_kmh       Float32,
    queue_length_m          Float32,
    waiting_vehicle_count   UInt32 DEFAULT 0,
    occupancy_pct           Float32,
    arrival_rate_pcu_per_sec Float32 DEFAULT 0.0,
    traffic_status          LowCardinality(String) DEFAULT 'UNKNOWN',
    spillback_risk          UInt8 DEFAULT 0,
    dominant_waiting_reason LowCardinality(String) DEFAULT '',
    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),
    quality_status          LowCardinality(String) DEFAULT 'VALID',
    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1',
    replay_run_id           String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, simulation_run_id, intersection_id, direction, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_signal_state_replay (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    direction               LowCardinality(String),
    source_entity_id        String,
    signal_status           LowCardinality(String),
    current_phase           LowCardinality(String),
    green_duration_sec      Nullable(Float32),
    red_duration_sec        Nullable(Float32),
    yellow_duration_sec     Nullable(Float32),
    timing_mode             LowCardinality(String) DEFAULT 'FIXED_TIME',
    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),
    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1',
    replay_run_id           String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, simulation_run_id, intersection_id, direction, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_intersection_state_replay (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    source_entity_id        String,
    overall_traffic_status  LowCardinality(String),
    derived_traffic_state   LowCardinality(String) DEFAULT 'STABLE',
    current_phase           LowCardinality(String),
    has_active_incident     UInt8 DEFAULT 0,
    has_spillback           UInt8 DEFAULT 0,
    is_box_blocked          UInt8 DEFAULT 0,
    total_vehicle_count     Nullable(UInt32),
    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),
    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1',
    replay_run_id           String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, simulation_run_id, intersection_id, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_camera_observation_replay (
    simulation_run_id       String,
    cycle_sequence          UInt64,
    simulation_time_sec     Float64,
    intersection_id         String,
    source_entity_id        String,
    vehicle_count           Nullable(UInt32),
    average_speed_kmh       Nullable(Float32),
    occupancy_pct           Nullable(Float32),
    traffic_status          LowCardinality(String) DEFAULT 'UNKNOWN',
    incident_detected       UInt8 DEFAULT 0,
    confidence              Float32 DEFAULT 1.0,
    recommended_signal_action LowCardinality(String) DEFAULT 'KEEP',
    incident_type           LowCardinality(String) DEFAULT 'NONE',
    incident_severity       LowCardinality(String) DEFAULT 'NONE',
    scenario_id             LowCardinality(String),
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),
    quality_flags           String DEFAULT '',
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1',
    replay_run_id           String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, simulation_run_id, intersection_id, source_entity_id, simulation_time_sec);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_fact_run_event_replay (
    simulation_run_id       String,
    event_name              LowCardinality(String),
    event_simulation_time   Float64 DEFAULT 0,
    scenario_id             LowCardinality(String),
    producer_id             String,
    source_bronze_event_id  FixedString(64),
    source_raw_ingestion_id FixedString(64),
    source_topic            String,
    source_partition        Int32,
    source_offset           Int64,
    source_payload_hash     FixedString(64),
    processed_at            DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1',
    replay_run_id           String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, simulation_run_id, event_simulation_time);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_quarantine_replay (
    silver_quarantine_id    String,
    source_bronze_event_id  String,
    raw_ingestion_id        String,
    simulation_run_id       Nullable(String),
    entity_id               Nullable(String),
    entity_type             Nullable(String),
    failure_stage           LowCardinality(String),
    error_code              String,
    error_message           String DEFAULT '',
    retryable               UInt8 DEFAULT 0,
    payload_hash            String DEFAULT '',
    source_payload          String CODEC(ZSTD(3)),
    created_at              DateTime64(3, 'UTC'),
    migration_version       LowCardinality(String) DEFAULT 'k9-silver-v1',
    replay_run_id           String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id, failure_stage, error_code, created_at);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_dim_run_replay (
    simulation_run_id       String,
    scenario_id             LowCardinality(String),
    seed                    Nullable(String),
    producer_id             String,
    started_at              DateTime64(3, 'UTC'),
    ended_at                Nullable(DateTime64(3, 'UTC')),
    run_status              LowCardinality(String) DEFAULT 'RUNNING',
    contract_version        LowCardinality(String),
    node_count              Nullable(UInt32),
    source_bronze_run_id    String,
    created_at              DateTime64(3, 'UTC'),
    updated_at              DateTime64(3, 'UTC'),
    replay_run_id           String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (replay_run_id, simulation_run_id);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_dim_intersection_replay (
    intersection_id         String,
    intersection_name       String,
    latitude                Float64,
    longitude               Float64,
    network_zone            LowCardinality(String) DEFAULT '',
    connected_intersections Array(String),
    source_hash             FixedString(64),
    source_bronze_event_id  String,
    created_at              DateTime64(3, 'UTC'),
    updated_at              DateTime64(3, 'UTC'),
    replay_run_id           String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (replay_run_id, intersection_id);
