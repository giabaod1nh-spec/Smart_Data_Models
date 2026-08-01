-- K-7 Bronze v2. Migration version: k7-bronze-v2-v1
-- Engine: MergeTree — physical duplicates allowed; logical dedupe via raw_ingestion_id queries.

CREATE DATABASE IF NOT EXISTS smart_traffic;

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_entity_events
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    broker_timestamp DateTime64(3, 'UTC'),
    raw_consumed_at DateTime64(3, 'UTC'),
    event_id FixedString(64),
    event_type LowCardinality(String) DEFAULT 'TrafficEntityObserved',
    contract_version LowCardinality(String),
    event_version LowCardinality(String),
    source LowCardinality(String),
    producer_id Nullable(String),
    producer_session_id Nullable(String),
    simulation_run_id String,
    simulation_time Float64,
    scenario_id Nullable(String),
    node_id Nullable(String),
    cycle_sequence Int64,
    entity_sequence Int32,
    cycle_entity_count Int32,
    node_entity_count Nullable(Int32),
    captured_at DateTime64(3, 'UTC'),
    entity_id String,
    entity_type LowCardinality(String),
    entity_payload_hash FixedString(64),
    entity_payload_json String CODEC(ZSTD(3)),
    upstream_duplicate_event_id UInt8 DEFAULT 0,
    event_payload_json String CODEC(ZSTD(3)),
    bronze_canonical_hash FixedString(64),
    bronze_ingestion_id FixedString(64),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    source_contract_version LowCardinality(String),
    processed_at DateTime64(3, 'UTC'),
    validation_status LowCardinality(String) DEFAULT 'STORED',
    migration_version LowCardinality(String) DEFAULT 'k7-bronze-v2-v1'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_run_events
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    broker_timestamp DateTime64(3, 'UTC'),
    raw_consumed_at DateTime64(3, 'UTC'),
    event_type LowCardinality(String) DEFAULT 'TrafficSimulationRunStarted',
    contract_version LowCardinality(String),
    event_version LowCardinality(String),
    source LowCardinality(String),
    producer_id String,
    producer_session_id String,
    simulation_run_id String,
    started_at DateTime64(3, 'UTC'),
    scenario_id Nullable(String),
    event_payload_json String CODEC(ZSTD(3)),
    bronze_canonical_hash FixedString(64),
    bronze_ingestion_id FixedString(64),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    source_contract_version LowCardinality(String),
    processed_at DateTime64(3, 'UTC'),
    validation_status LowCardinality(String) DEFAULT 'STORED',
    migration_version LowCardinality(String) DEFAULT 'k7-bronze-v2-v1'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_quarantine
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    broker_timestamp DateTime64(3, 'UTC'),
    raw_consumed_at DateTime64(3, 'UTC'),
    event_id Nullable(String),
    event_type Nullable(String),
    simulation_run_id Nullable(String),
    failure_stage LowCardinality(String),
    error_code String,
    error_detail String,
    retryable UInt8 DEFAULT 0,
    payload_encoding LowCardinality(String),
    payload_reference String CODEC(ZSTD(3)),
    payload_bytes_hash FixedString(64),
    bronze_canonical_hash FixedString(64),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    quarantined_at DateTime64(3, 'UTC'),
    migration_version LowCardinality(String) DEFAULT 'k7-bronze-v2-v1'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_entity_events_replay
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    broker_timestamp DateTime64(3, 'UTC'),
    raw_consumed_at DateTime64(3, 'UTC'),
    event_id FixedString(64),
    event_type LowCardinality(String) DEFAULT 'TrafficEntityObserved',
    contract_version LowCardinality(String),
    event_version LowCardinality(String),
    source LowCardinality(String),
    producer_id Nullable(String),
    producer_session_id Nullable(String),
    simulation_run_id String,
    simulation_time Float64,
    scenario_id Nullable(String),
    node_id Nullable(String),
    cycle_sequence Int64,
    entity_sequence Int32,
    cycle_entity_count Int32,
    node_entity_count Nullable(Int32),
    captured_at DateTime64(3, 'UTC'),
    entity_id String,
    entity_type LowCardinality(String),
    entity_payload_hash FixedString(64),
    entity_payload_json String CODEC(ZSTD(3)),
    upstream_duplicate_event_id UInt8 DEFAULT 0,
    event_payload_json String CODEC(ZSTD(3)),
    bronze_canonical_hash FixedString(64),
    bronze_ingestion_id FixedString(64),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    source_contract_version LowCardinality(String),
    processed_at DateTime64(3, 'UTC'),
    validation_status LowCardinality(String) DEFAULT 'STORED',
    migration_version LowCardinality(String) DEFAULT 'k7-bronze-v2-v1',
    replay_run_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_run_events_replay
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    broker_timestamp DateTime64(3, 'UTC'),
    raw_consumed_at DateTime64(3, 'UTC'),
    event_type LowCardinality(String) DEFAULT 'TrafficSimulationRunStarted',
    contract_version LowCardinality(String),
    event_version LowCardinality(String),
    source LowCardinality(String),
    producer_id String,
    producer_session_id String,
    simulation_run_id String,
    started_at DateTime64(3, 'UTC'),
    scenario_id Nullable(String),
    event_payload_json String CODEC(ZSTD(3)),
    bronze_canonical_hash FixedString(64),
    bronze_ingestion_id FixedString(64),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    source_contract_version LowCardinality(String),
    processed_at DateTime64(3, 'UTC'),
    validation_status LowCardinality(String) DEFAULT 'STORED',
    migration_version LowCardinality(String) DEFAULT 'k7-bronze-v2-v1',
    replay_run_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (replay_run_id, topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_quarantine_replay
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    broker_timestamp DateTime64(3, 'UTC'),
    raw_consumed_at DateTime64(3, 'UTC'),
    event_id Nullable(String),
    event_type Nullable(String),
    simulation_run_id Nullable(String),
    failure_stage LowCardinality(String),
    error_code String,
    error_detail String,
    retryable UInt8 DEFAULT 0,
    payload_encoding LowCardinality(String),
    payload_reference String CODEC(ZSTD(3)),
    payload_bytes_hash FixedString(64),
    bronze_canonical_hash FixedString(64),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    quarantined_at DateTime64(3, 'UTC'),
    migration_version LowCardinality(String) DEFAULT 'k7-bronze-v2-v1',
    replay_run_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (replay_run_id, topic, partition, offset);
