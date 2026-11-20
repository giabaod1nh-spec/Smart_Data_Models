CREATE DATABASE IF NOT EXISTS smart_traffic;
CREATE TABLE IF NOT EXISTS smart_traffic.v1_raw_events
(
topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64),
topic_identity String, physical_record_hash FixedString(64), destination LowCardinality(String),
payload_bytes_hash FixedString(64), value_is_null UInt8, key_is_null UInt8,
value_bytes_base64 String, key_bytes_base64 String, headers_json String,
broker_timestamp_ms Nullable(Int64), broker_timestamp_type Int8,
consumed_at DateTime64(6,'UTC'), event_id Nullable(String), payload_hash Nullable(String),
simulation_run_id Nullable(String), intersection_id Nullable(String),
immutable_envelope_hash Nullable(String), envelope_json Nullable(String),
payload_text Nullable(String), validator_version String, migration_version String,
failure_stage String DEFAULT '', error_code String DEFAULT '', error_detail String DEFAULT ''
) ENGINE=MergeTree PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic,partition,offset,raw_ingestion_id);
CREATE TABLE IF NOT EXISTS smart_traffic.v1_quarantine_events
(
topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64),
topic_identity String, physical_record_hash FixedString(64), destination LowCardinality(String),
payload_bytes_hash FixedString(64), value_is_null UInt8, key_is_null UInt8,
value_bytes_base64 String, key_bytes_base64 String, headers_json String,
broker_timestamp_ms Nullable(Int64), broker_timestamp_type Int8,
consumed_at DateTime64(6,'UTC'), event_id Nullable(String), payload_hash Nullable(String),
simulation_run_id Nullable(String), intersection_id Nullable(String),
immutable_envelope_hash Nullable(String), envelope_json Nullable(String),
payload_text Nullable(String), validator_version String, migration_version String,
failure_stage String DEFAULT '', error_code String DEFAULT '', error_detail String DEFAULT ''
) ENGINE=MergeTree PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic,partition,offset,raw_ingestion_id);
CREATE TABLE IF NOT EXISTS smart_traffic.v1_raw_events_replay
(
replay_run_id String,
topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64),
topic_identity String, physical_record_hash FixedString(64), destination LowCardinality(String),
payload_bytes_hash FixedString(64), value_is_null UInt8, key_is_null UInt8,
value_bytes_base64 String, key_bytes_base64 String, headers_json String,
broker_timestamp_ms Nullable(Int64), broker_timestamp_type Int8,
consumed_at DateTime64(6,'UTC'), event_id Nullable(String), payload_hash Nullable(String),
simulation_run_id Nullable(String), intersection_id Nullable(String),
immutable_envelope_hash Nullable(String), envelope_json Nullable(String),
payload_text Nullable(String), validator_version String, migration_version String,
failure_stage String DEFAULT '', error_code String DEFAULT '', error_detail String DEFAULT ''
) ENGINE=MergeTree PARTITION BY toYYYYMM(consumed_at)
ORDER BY (replay_run_id,topic,partition,offset,raw_ingestion_id);
CREATE TABLE IF NOT EXISTS smart_traffic.v1_quarantine_events_replay
(
replay_run_id String,
topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64),
topic_identity String, physical_record_hash FixedString(64), destination LowCardinality(String),
payload_bytes_hash FixedString(64), value_is_null UInt8, key_is_null UInt8,
value_bytes_base64 String, key_bytes_base64 String, headers_json String,
broker_timestamp_ms Nullable(Int64), broker_timestamp_type Int8,
consumed_at DateTime64(6,'UTC'), event_id Nullable(String), payload_hash Nullable(String),
simulation_run_id Nullable(String), intersection_id Nullable(String),
immutable_envelope_hash Nullable(String), envelope_json Nullable(String),
payload_text Nullable(String), validator_version String, migration_version String,
failure_stage String DEFAULT '', error_code String DEFAULT '', error_detail String DEFAULT ''
) ENGINE=MergeTree PARTITION BY toYYYYMM(consumed_at)
ORDER BY (replay_run_id,topic,partition,offset,raw_ingestion_id);

