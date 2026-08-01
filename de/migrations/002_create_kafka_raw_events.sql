-- K-4 Historical Raw (Kafka lineage). Migration version: k4-kafka-raw-v1
-- Engine: MergeTree — physical duplicates allowed under at-least-once.

CREATE DATABASE IF NOT EXISTS smart_traffic;

CREATE TABLE IF NOT EXISTS smart_traffic.kafka_raw_events
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    kafka_key Nullable(String),
    kafka_headers_json String DEFAULT '{}',
    broker_timestamp DateTime64(3, 'UTC'),
    broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable',
    consumed_at DateTime64(3, 'UTC'),
    captured_at Nullable(DateTime64(3, 'UTC')),
    event_id Nullable(String),
    event_type Nullable(String),
    event_version Nullable(String),
    contract_version Nullable(String),
    source Nullable(String),
    producer_id Nullable(String),
    producer_session_id Nullable(String),
    simulation_run_id Nullable(String),
    scenario_id Nullable(String),
    simulation_time Nullable(Float64),
    node_id Nullable(String),
    cycle_sequence Nullable(Int64),
    entity_sequence Nullable(Int32),
    cycle_entity_count Nullable(Int32),
    node_entity_count Nullable(Int32),
    entity_id Nullable(String),
    entity_type Nullable(String),
    payload_encoding LowCardinality(String),
    payload_stored String,
    payload_size_bytes UInt32,
    payload_bytes_hash FixedString(64),
    canonical_payload_hash Nullable(FixedString(64)),
    migration_version LowCardinality(String) DEFAULT 'k4-kafka-raw-v1'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.kafka_quarantine_events
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    kafka_key Nullable(String),
    kafka_headers_json String DEFAULT '{}',
    broker_timestamp DateTime64(3, 'UTC'),
    broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable',
    consumed_at DateTime64(3, 'UTC'),
    failed_at DateTime64(3, 'UTC'),
    error_code String,
    error_detail String,
    failure_stage LowCardinality(String),
    validator_version String DEFAULT '',
    schema_version_attempted String DEFAULT '',
    event_id Nullable(String),
    event_type Nullable(String),
    payload_encoding LowCardinality(String),
    payload_stored String,
    payload_size_bytes UInt32,
    payload_bytes_hash FixedString(64),
    canonical_payload_hash Nullable(FixedString(64)),
    migration_version LowCardinality(String) DEFAULT 'k4-kafka-raw-v1'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.kafka_raw_events_replay
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    kafka_key Nullable(String),
    kafka_headers_json String DEFAULT '{}',
    broker_timestamp DateTime64(3, 'UTC'),
    broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable',
    consumed_at DateTime64(3, 'UTC'),
    captured_at Nullable(DateTime64(3, 'UTC')),
    event_id Nullable(String),
    event_type Nullable(String),
    event_version Nullable(String),
    contract_version Nullable(String),
    source Nullable(String),
    producer_id Nullable(String),
    producer_session_id Nullable(String),
    simulation_run_id Nullable(String),
    scenario_id Nullable(String),
    simulation_time Nullable(Float64),
    node_id Nullable(String),
    cycle_sequence Nullable(Int64),
    entity_sequence Nullable(Int32),
    cycle_entity_count Nullable(Int32),
    node_entity_count Nullable(Int32),
    entity_id Nullable(String),
    entity_type Nullable(String),
    payload_encoding LowCardinality(String),
    payload_stored String,
    payload_size_bytes UInt32,
    payload_bytes_hash FixedString(64),
    canonical_payload_hash Nullable(FixedString(64)),
    migration_version LowCardinality(String) DEFAULT 'k4-kafka-raw-v1',
    replay_run_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (replay_run_id, topic, partition, offset);

CREATE TABLE IF NOT EXISTS smart_traffic.kafka_quarantine_events_replay
(
    topic String,
    partition Int32,
    offset Int64,
    raw_ingestion_id FixedString(64),
    kafka_key Nullable(String),
    kafka_headers_json String DEFAULT '{}',
    broker_timestamp DateTime64(3, 'UTC'),
    broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable',
    consumed_at DateTime64(3, 'UTC'),
    failed_at DateTime64(3, 'UTC'),
    error_code String,
    error_detail String,
    failure_stage LowCardinality(String),
    validator_version String DEFAULT '',
    schema_version_attempted String DEFAULT '',
    event_id Nullable(String),
    event_type Nullable(String),
    payload_encoding LowCardinality(String),
    payload_stored String,
    payload_size_bytes UInt32,
    payload_bytes_hash FixedString(64),
    canonical_payload_hash Nullable(FixedString(64)),
    migration_version LowCardinality(String) DEFAULT 'k4-kafka-raw-v1',
    replay_run_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (replay_run_id, topic, partition, offset);
