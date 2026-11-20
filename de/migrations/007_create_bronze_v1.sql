CREATE DATABASE IF NOT EXISTS smart_traffic;

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_observations_v1
(
    namespace LowCardinality(String),
    event_id String,
    semantic_revision UInt64,
    event_type LowCardinality(String),
    schema_version UInt16,
    simulation_run_id String,
    intersection_id String,
    scenario_id String,
    cycle_sequence Int64,
    simulation_step Int64,
    simulation_time_seconds Float64,
    captured_at DateTime64(6, 'UTC'),
    topology_digest String,
    payload_hash FixedString(64),
    immutable_envelope_hash FixedString(64),
    source_byte_hash FixedString(64),
    observation_payload_json String CODEC(ZSTD(3)),
    observation_envelope_json String CODEC(ZSTD(3)),
    bronze_canonical_hash FixedString(64),
    primary_raw_ingestion_id FixedString(64),
    primary_topic String,
    primary_partition Int32,
    primary_offset Int64,
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    normalization_version LowCardinality(String),
    bronze_schema_version LowCardinality(String),
    migration_version LowCardinality(String),
    processed_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (namespace, event_id, semantic_revision, primary_topic, primary_partition, primary_offset);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_delivery_dispositions_v1
(
    namespace LowCardinality(String),
    raw_ingestion_id FixedString(64),
    topic String,
    topic_identity String,
    partition Int32,
    offset Int64,
    physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    event_id Nullable(String),
    immutable_envelope_hash Nullable(String),
    payload_hash Nullable(String),
    disposition LowCardinality(String),
    semantic_revision Nullable(UInt64),
    observation_canonical_hash Nullable(String),
    processing_intent_id FixedString(64),
    error_code Nullable(String),
    error_detail Nullable(String),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    processed_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (namespace, topic, partition, offset, raw_ingestion_id);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_conflict_evidence_v1
(
    namespace LowCardinality(String),
    conflict_id FixedString(64),
    event_id String,
    simulation_run_id String,
    intersection_id String,
    cycle_sequence Int64,
    simulation_time_seconds Float64,
    previous_immutable_envelope_hash FixedString(64),
    conflicting_immutable_envelope_hash FixedString(64),
    previous_payload_hash FixedString(64),
    conflicting_payload_hash FixedString(64),
    previous_raw_ingestion_id FixedString(64),
    conflicting_raw_ingestion_id FixedString(64),
    semantic_revision UInt64,
    change_sequence UInt64,
    created_at DateTime64(6, 'UTC'),
    processor_version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (namespace, event_id, semantic_revision, conflict_id);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_quarantine_v1
(
    namespace LowCardinality(String),
    raw_ingestion_id FixedString(64),
    topic String,
    partition Int32,
    offset Int64,
    event_id Nullable(String),
    failure_stage LowCardinality(String),
    error_code String,
    error_detail String,
    source_byte_hash FixedString(64),
    payload_reference String CODEC(ZSTD(3)),
    processor_name LowCardinality(String),
    processor_version LowCardinality(String),
    quarantined_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (namespace, topic, partition, offset, raw_ingestion_id);

CREATE TABLE IF NOT EXISTS smart_traffic.bronze_invalidations_v1
(
    namespace LowCardinality(String),
    invalidation_id FixedString(64),
    event_id String,
    simulation_run_id String,
    intersection_id String,
    cycle_sequence Int64,
    simulation_time_seconds Float64,
    previous_semantic_state LowCardinality(String),
    new_semantic_state LowCardinality(String),
    previous_immutable_envelope_hash FixedString(64),
    conflicting_immutable_envelope_hash FixedString(64),
    original_raw_ingestion_id FixedString(64),
    conflicting_raw_ingestion_id FixedString(64),
    semantic_revision UInt64,
    created_at DateTime64(6, 'UTC'),
    invalidation_sequence UInt64,
    processor_name LowCardinality(String),
    processor_version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (namespace, invalidation_sequence, event_id, semantic_revision, invalidation_id);
