CREATE DATABASE IF NOT EXISTS smart_traffic;

CREATE TABLE IF NOT EXISTS smart_traffic.silver_traffic_observation_fact_v1
(
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    event_id FixedString(64),
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    intersection_id String,
    scenario_id String,
    captured_at DateTime64(3,'UTC'),
    bronze_semantic_revision UInt64,
    bronze_canonical_hash FixedString(64),
    source_payload_hash FixedString(64),
    source_raw_ingestion_id String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    source_physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    created_at DateTime64(6,'UTC'),
    silver_schema_version LowCardinality(String),
    source_direction String,
    vehicle_count Nullable(UInt32),
    pcu_equivalent Nullable(Float64),
    left_count Nullable(UInt32),
    straight_count Nullable(UInt32),
    right_count Nullable(UInt32),
    average_speed_kmh Nullable(Float64),
    waiting_vehicle_count Nullable(UInt32),
    queue_length_m Nullable(Float64),
    occupancy_pct Nullable(Float64),
    metric_availability_json String,
    direction_payload_json String,
    quality_status LowCardinality(String),
    quality_flags_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (simulation_run_id,topology_digest,event_id,intersection_id,source_direction,bronze_semantic_revision);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_traffic_observation_fact_replay_v1
(
    replay_run_id String,
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    event_id FixedString(64),
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    intersection_id String,
    scenario_id String,
    captured_at DateTime64(3,'UTC'),
    bronze_semantic_revision UInt64,
    bronze_canonical_hash FixedString(64),
    source_payload_hash FixedString(64),
    source_raw_ingestion_id String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    source_physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    created_at DateTime64(6,'UTC'),
    silver_schema_version LowCardinality(String),
    source_direction String,
    vehicle_count Nullable(UInt32),
    pcu_equivalent Nullable(Float64),
    left_count Nullable(UInt32),
    straight_count Nullable(UInt32),
    right_count Nullable(UInt32),
    average_speed_kmh Nullable(Float64),
    waiting_vehicle_count Nullable(UInt32),
    queue_length_m Nullable(Float64),
    occupancy_pct Nullable(Float64),
    metric_availability_json String,
    direction_payload_json String,
    quality_status LowCardinality(String),
    quality_flags_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id,simulation_run_id,topology_digest,event_id,intersection_id,source_direction,bronze_semantic_revision);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_signal_state_fact_v1
(
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    event_id FixedString(64),
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    intersection_id String,
    scenario_id String,
    captured_at DateTime64(3,'UTC'),
    bronze_semantic_revision UInt64,
    bronze_canonical_hash FixedString(64),
    source_payload_hash FixedString(64),
    source_raw_ingestion_id String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    source_physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    created_at DateTime64(6,'UTC'),
    silver_schema_version LowCardinality(String),
    source_direction String,
    signal_color LowCardinality(String),
    phase String,
    next_phase Nullable(String),
    phase_remaining_seconds Nullable(Float64),
    phase_duration_seconds Nullable(Float64),
    green_duration_seconds Nullable(Float64),
    yellow_duration_seconds Nullable(Float64),
    red_duration_seconds Nullable(Float64),
    timing_mode Nullable(String),
    signal_payload_json String,
    quality_status LowCardinality(String),
    quality_flags_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (simulation_run_id,topology_digest,event_id,intersection_id,source_direction,bronze_semantic_revision);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_signal_state_fact_replay_v1
(
    replay_run_id String,
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    event_id FixedString(64),
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    intersection_id String,
    scenario_id String,
    captured_at DateTime64(3,'UTC'),
    bronze_semantic_revision UInt64,
    bronze_canonical_hash FixedString(64),
    source_payload_hash FixedString(64),
    source_raw_ingestion_id String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    source_physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    created_at DateTime64(6,'UTC'),
    silver_schema_version LowCardinality(String),
    source_direction String,
    signal_color LowCardinality(String),
    phase String,
    next_phase Nullable(String),
    phase_remaining_seconds Nullable(Float64),
    phase_duration_seconds Nullable(Float64),
    green_duration_seconds Nullable(Float64),
    yellow_duration_seconds Nullable(Float64),
    red_duration_seconds Nullable(Float64),
    timing_mode Nullable(String),
    signal_payload_json String,
    quality_status LowCardinality(String),
    quality_flags_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id,simulation_run_id,topology_digest,event_id,intersection_id,source_direction,bronze_semantic_revision);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_intersection_state_fact_v1
(
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    event_id FixedString(64),
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    intersection_id String,
    scenario_id String,
    captured_at DateTime64(3,'UTC'),
    bronze_semantic_revision UInt64,
    bronze_canonical_hash FixedString(64),
    source_payload_hash FixedString(64),
    source_raw_ingestion_id String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    source_physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    created_at DateTime64(6,'UTC'),
    silver_schema_version LowCardinality(String),
    phase String,
    control_mode String,
    incidents_json String,
    derived_traffic_state Nullable(String),
    operational_state_json Nullable(String),
    spillback_detected Nullable(UInt8),
    intersection_box_blocked Nullable(UInt8),
    downstream_edge_full Nullable(UInt8),
    spillback_pressure Nullable(Float64),
    intersection_payload_json String,
    total_vehicle_count Nullable(UInt64),
    total_vehicle_count_quality LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (simulation_run_id,topology_digest,event_id,intersection_id,bronze_semantic_revision);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_intersection_state_fact_replay_v1
(
    replay_run_id String,
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    event_id FixedString(64),
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    intersection_id String,
    scenario_id String,
    captured_at DateTime64(3,'UTC'),
    bronze_semantic_revision UInt64,
    bronze_canonical_hash FixedString(64),
    source_payload_hash FixedString(64),
    source_raw_ingestion_id String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    source_physical_record_hash FixedString(64),
    source_byte_hash FixedString(64),
    created_at DateTime64(6,'UTC'),
    silver_schema_version LowCardinality(String),
    phase String,
    control_mode String,
    incidents_json String,
    derived_traffic_state Nullable(String),
    operational_state_json Nullable(String),
    spillback_detected Nullable(UInt8),
    intersection_box_blocked Nullable(UInt8),
    downstream_edge_full Nullable(UInt8),
    spillback_pressure Nullable(Float64),
    intersection_payload_json String,
    total_vehicle_count Nullable(UInt64),
    total_vehicle_count_quality LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id,simulation_run_id,topology_digest,event_id,intersection_id,bronze_semantic_revision);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_run_dimension_v1
(
    simulation_run_id String,
    run_generation UInt64,
    topology_digest String,
    manifest_identity FixedString(64),
    participant_ids_json String,
    manifest_metadata_json String,
    manifest_path String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (simulation_run_id,run_generation);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_run_dimension_replay_v1
(
    replay_run_id String,
    simulation_run_id String,
    run_generation UInt64,
    topology_digest String,
    manifest_identity FixedString(64),
    participant_ids_json String,
    manifest_metadata_json String,
    manifest_path String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (replay_run_id,simulation_run_id,run_generation);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_topology_dimension_v1
(
    topology_digest String,
    digest_algorithm_version String,
    catalog_hash FixedString(64),
    intersection_ids_json String,
    approaches_by_intersection_json String,
    catalog_path String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (topology_digest);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_topology_dimension_replay_v1
(
    replay_run_id String,
    topology_digest String,
    digest_algorithm_version String,
    catalog_hash FixedString(64),
    intersection_ids_json String,
    approaches_by_intersection_json String,
    catalog_path String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (replay_run_id,topology_digest);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_run_topology_binding_v1
(
    simulation_run_id String,
    run_generation UInt64,
    topology_digest String,
    manifest_identity FixedString(64),
    catalog_hash FixedString(64),
    participant_ids_json String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (simulation_run_id,run_generation,topology_digest);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_run_topology_binding_replay_v1
(
    replay_run_id String,
    simulation_run_id String,
    run_generation UInt64,
    topology_digest String,
    manifest_identity FixedString(64),
    catalog_hash FixedString(64),
    participant_ids_json String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (replay_run_id,simulation_run_id,run_generation,topology_digest);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_run_lifecycle_v1
(
    simulation_run_id String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    run_status LowCardinality(Nullable(String)),
    registry_state LowCardinality(String),
    registry_updated_at Nullable(DateTime64(6,'UTC')),
    manifest_run_status Nullable(String),
    failed_cycle_count Nullable(UInt64),
    manifest_file_hash Nullable(FixedString(64)),
    manifest_mirror_state LowCardinality(String),
    lifecycle_freshness LowCardinality(String),
    failure_evidence_json Nullable(String),
    observed_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (simulation_run_id,run_generation,observed_at);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_run_lifecycle_replay_v1
(
    replay_run_id String,
    simulation_run_id String,
    run_generation UInt64,
    manifest_identity FixedString(64),
    run_status LowCardinality(Nullable(String)),
    registry_state LowCardinality(String),
    registry_updated_at Nullable(DateTime64(6,'UTC')),
    manifest_run_status Nullable(String),
    failed_cycle_count Nullable(UInt64),
    manifest_file_hash Nullable(FixedString(64)),
    manifest_mirror_state LowCardinality(String),
    lifecycle_freshness LowCardinality(String),
    failure_evidence_json Nullable(String),
    observed_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
ORDER BY (replay_run_id,simulation_run_id,run_generation,observed_at);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_event_validity_v1
(
    namespace LowCardinality(String),
    event_id FixedString(64),
    simulation_run_id String,
    topology_digest String,
    semantic_revision UInt64,
    validity_state LowCardinality(String),
    bronze_change_sequence UInt64,
    bronze_invalidation_sequence Nullable(UInt64),
    invalidation_id Nullable(FixedString(64)),
    reason LowCardinality(String),
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (namespace,event_id,semantic_revision,created_at);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_event_validity_replay_v1
(
    replay_run_id String,
    namespace LowCardinality(String),
    event_id FixedString(64),
    simulation_run_id String,
    topology_digest String,
    semantic_revision UInt64,
    validity_state LowCardinality(String),
    bronze_change_sequence UInt64,
    bronze_invalidation_sequence Nullable(UInt64),
    invalidation_id Nullable(FixedString(64)),
    reason LowCardinality(String),
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id,namespace,event_id,semantic_revision,created_at);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_invalidation_v1
(
    namespace LowCardinality(String),
    invalidation_id FixedString(64),
    invalidation_sequence UInt64,
    event_id FixedString(64),
    semantic_revision UInt64,
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    intersection_id String,
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    previous_immutable_envelope_hash FixedString(64),
    conflicting_immutable_envelope_hash FixedString(64),
    original_raw_ingestion_id FixedString(64),
    conflicting_raw_ingestion_id FixedString(64),
    lineage_json String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (namespace,invalidation_sequence,event_id,invalidation_id);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_invalidation_replay_v1
(
    replay_run_id String,
    namespace LowCardinality(String),
    invalidation_id FixedString(64),
    invalidation_sequence UInt64,
    event_id FixedString(64),
    semantic_revision UInt64,
    simulation_run_id String,
    topology_digest String,
    run_generation UInt64,
    intersection_id String,
    cycle_sequence UInt64,
    simulation_time_seconds Float64,
    previous_immutable_envelope_hash FixedString(64),
    conflicting_immutable_envelope_hash FixedString(64),
    original_raw_ingestion_id FixedString(64),
    conflicting_raw_ingestion_id FixedString(64),
    lineage_json String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id,namespace,invalidation_sequence,event_id,invalidation_id);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_quarantine_v1
(
    namespace LowCardinality(String),
    quarantine_id FixedString(64),
    event_id Nullable(FixedString(64)),
    simulation_run_id Nullable(String),
    topology_digest Nullable(String),
    source_change_sequence Nullable(UInt64),
    failure_stage LowCardinality(String),
    error_code LowCardinality(String),
    error_detail String,
    source_payload_json String,
    source_raw_ingestion_ids_json String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (namespace,failure_stage,error_code,created_at,quarantine_id);

CREATE TABLE IF NOT EXISTS smart_traffic.silver_quarantine_replay_v1
(
    replay_run_id String,
    namespace LowCardinality(String),
    quarantine_id FixedString(64),
    event_id Nullable(FixedString(64)),
    simulation_run_id Nullable(String),
    topology_digest Nullable(String),
    source_change_sequence Nullable(UInt64),
    failure_stage LowCardinality(String),
    error_code LowCardinality(String),
    error_detail String,
    source_payload_json String,
    source_raw_ingestion_ids_json String,
    created_at DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (replay_run_id,namespace,failure_stage,error_code,created_at,quarantine_id);
