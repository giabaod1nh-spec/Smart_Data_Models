# Phase 1 Implementation Plan — V1 Raw Ingestion

Status: planning-only. No application source was changed during this audit.

Scope: Phase 1 of docs/DEv1/Master_plan_DE_V1_.md. Establish a restart-safe, observable V1 Kafka-to-ClickHouse Raw boundary and the handover contract to Phase 2 Bronze.

Authoritative audit baseline: origin/final at 803c6e421ec8cd00f14ea394fd338fef785b9157. The checkout inspected is one commit behind at e4c2a2545920768085acdde8e87cb8667a8857e7.

## 1. Executive Summary and Phase Objective

Current DE Raw consumes the legacy V2 topic traffic.entity-events.v2 with group de-kafka-raw-v2, validates V2 schemas, and writes kafka_raw_events or kafka_quarantine_events. Upstream now emits complete IntersectionObservation V1 events to traffic.observations.v1 from a durable outbox.

Phase 1 adds an isolated V1 Raw runtime:

~~~text
SUMO/TraCI -> canonical snapshot -> IntersectionObservation V1
-> V1 SQLite outbox -> traffic.observations.v1 -> V1 Raw consumer
-> V1 Raw or V1 Quarantine -> physical delivery ledger
-> contiguous safe Kafka offset commit -> Phase 2 Bronze
~~~

Raw preserves every physical delivery and original Kafka bytes. Bronze owns semantic observation deduplication and the later policy for conflicting semantic identity. Phase 1 does not implement Bronze, Silver, Gold, Spring, Dashboard, Orion, or Projector migration. Completion requires actual fresh SUMO-to-Kafka-to-ClickHouse evidence, restart/recovery evidence, and stored acceptance artifacts.

## 2. Repository Baseline and Readiness

| Item | Observed |
|---|---|
| Branch | final |
| Local HEAD | e4c2a2545920768085acdde8e87cb8667a8857e7 |
| origin/final | 803c6e421ec8cd00f14ea394fd338fef785b9157 |
| Relation | local is behind upstream by one commit |
| Working tree | no ordinary source changes in git status; docs/DEv1 is ignored and checked separately |
| Recommendation | create codex/phase-1-v1-raw from synchronized origin/final |

Before implementation, re-run git status, local HEAD and origin/final. Synchronize only after preserving user changes; do not reset, discard, pull, merge, or switch automatically. Record the final audited commit in implementation-status.md.

Readiness findings:

- The checkout lacks the audited upstream projector commit; synchronization is a prerequisite.
- The audited upstream baseline contains V1DurablePublisher, frozen-byte outbox persistence, V1 topic/key, run manifest, archived topology catalog and topology digest.
- DE V1 Raw is absent; current de/kafka_raw is V2-shaped.
- Gold/Dashboard decisions do not block Phase 1 because Raw only preserves V1 envelope, bytes, provenance and Kafka lineage.
- Exact treatment of duplicate eventId with conflicting immutable content is assigned to the Phase 2 planning gate. Phase 1 preserves both deliveries.

Verdict: READY AFTER EXPLICIT APPROVAL. Approval is required for baseline synchronization and the isolated V1 service/group/ledger/table design. The Phase 2 semantic-conflict decision is deferred and is not a Raw blocker.

## 3. Master Plan Alignment

Preserve the five phases, Raw/Bronze ownership, incremental runtime support, V2 assets and Phase 5 canonical cutover rule.

Confirmed V1 facts:

- topic traffic.observations.v1;
- key simulationRunId:intersectionId;
- event type IntersectionObservation;
- schema version 1;
- event identity SHA-256 of 1|simulationRunId|intersectionId|cycleSequence;
- payload hash SHA-256 of canonical JSON payload;
- one complete observation per intersection/cycle;
- Raw grain one physical delivery per topic, partition, offset;
- no lifecycle event is valid on the V1 topic;
- no Orion, Projector or active-run selector dependency.

The inherited _append_run_started_durable method is not on the active V1 call path. Evidence is Visualize/app/traci_runner.py V1 mode -> V1DurablePublisher -> freeze_cycle -> persist_frozen. Raw still quarantines lifecycle/V2/unknown messages if they appear. No Master Plan conflict was verified.

## 4. Detailed Source Audit

### 4.1 Contract and producer evidence

| Source | Finding | Required use |
|---|---|---|
| contracts/events/intersection-observation-v1.schema.json | closed Draft 2020-12 V1 envelope, timing, digest, hash, payload and nullable metrics | load shared schema; do not copy it into de |
| contracts/traffic_observation.py:66-184 | canonical serializer, event ID, shared validate_observation, finite-number, identity and payload-hash checks | call shared validator |
| contracts/observation/V1_CONTRACT.md and V1_METRIC_CONTRACT.md | absent/null/unavailable/not-applicable semantics and full snapshot meaning | preserve bytes; never add defaults |
| contracts/topology/v1_digest.py:41-104 | digest and optional catalog resolution | store run/intersection/digest; archive lookup is not wire validation |
| Visualize/app/traci_runner.py:301-304,532-536,721-731 | active V1 mode constructs publisher, freezes, persists | use this as active call path |
| Visualize/integration/kafka/v1_publisher.py:31-133 | V1 frozen bytes and outbox persistence | compare outbox, Kafka and Raw byte hashes |
| Visualize/integration/kafka/outbox_store.py:417-733 | V1 append, ACK/retry and orphan recovery | producer retries can cause multiple physical deliveries |
| Visualize/runtime/run_manifest.py and active_run_registry.py | manifests and active selection | Raw does not use active selection |

The shared validator is archive-independent. A missing archive must not prevent valid Raw ingestion. Optional read-only archive checking is evidence-only unless a later approved decision changes the boundary.

### 4.2 Existing Raw classification

| Component | Classification | Evidence/treatment |
|---|---|---|
| de/kafka_raw/main.py | REUSE WITH ADAPTATION | add V1 entry point; retain V2 |
| de/kafka_raw/config.py | REPLACE FOR V1; LEGACY FOR V2 | new DE_V1_RAW_ settings |
| de/kafka_raw/validator.py | REPLACE FOR V1; LEGACY FOR V2 | currently selects V2 schemas and fields |
| batch_buffer.py | KEEP after tests | version-neutral buffering/pause limits |
| consumer.py | REUSE WITH ADAPTATION | reuse assign/revoke/flush/commit; fix V1 null bytes and batch ledger |
| clickhouse_repository.py | REUSE WITH ADAPTATION | reuse connection/ping/migration helper; V1 tables |
| ledger_store.py | REPLACE FOR V1 | completion-only V2 ledger is not enough and must not be shared |
| offset_tracker.py | REUSE WITH ADAPTATION | contiguous rule retained and tested |
| health_api.py and metrics.py | REUSE WITH ADAPTATION | V1 readiness must require persistence health |
| replay.py and raw_gap_repair.py | REUSE WITH ADAPTATION | V1 group/tables/ledger and no live mutation |
| migrations/002 | LEGACY ONLY | add numbered V1 migration; never edit 002 |
| Dockerfile/requirements.txt | KEEP unless packaging tests fail | contracts and required libraries already copied/installed |
| Compose services under v2-legacy | LEGACY ONLY | disabled from V1 live processing |

Gaps to close: current consumer turns null values into empty bytes; MergeTree is not an immediate uniqueness constraint; reconciliation lacks explicit destination/hash fault records; completion lacks batch identity and attempt state; V2 columns cannot preserve nested V1 payload and availability semantics.

## 5. Target V1 Raw Architecture

### 5.1 Processing sequence

~~~text
Kafka poll
-> capture exact metadata, key, headers, timestamps and value bytes
-> raw_ingestion_id = sha256(topic|partition|offset); byte hash
-> ledger RECEIVED
-> UTF-8 decode -> JSON parse -> eventType/schemaVersion
-> shared validate_observation -> Kafka key check
-> RAW or QUARANTINE
-> buffer by partition/destination
-> synchronous ClickHouse insert
-> reconcile ambiguous insert by physical ID and byte hash
-> ledger terminal confirmation
-> contiguous offset calculation
-> synchronous Kafka commit of next offset
~~~

Physical identity is topic/partition/offset and raw_ingestion_id. Semantic identity is eventId. Contract integrity is payloadHash. Byte integrity is payload_bytes_hash. immutable_envelope_hash is a comparison hash of immutable envelope fields. Raw does not perform a cross-offset eventId lookup, maintain a semantic ledger, or classify semantic duplicates/conflicts.

Same eventId at different offsets: preserve both without classification. Same physical delivery repeated: one ledger delivery and no second logical delivery. Same eventId with changed immutable content at another offset: preserve both hashes and bytes; Bronze performs the cross-offset semantic comparison. Same physical identity with different bytes: retain first durable evidence, record FAILED_INTEGRITY, fail closed and do not advance the partition.

### 5.2 Validator contract

Create V1RawValidator.classify(record). Sequence:

1. Capture transport metadata, including null-vs-empty value.
2. Hash exact bytes and derive physical identity.
3. Decode UTF-8; non-UTF8 -> Quarantine DECODE/NON_UTF8.
4. Parse JSON; invalid JSON/non-object -> Quarantine JSON_PARSE.
5. Require eventType IntersectionObservation and schemaVersion 1; V2/lifecycle/unknown -> Quarantine.
6. Call contracts.traffic_observation.validate_observation; do not duplicate it.
7. Require UTF-8 Kafka key equal to simulationRunId:intersectionId; mismatch -> Quarantine KAFKA_KEY.
8. Store complete envelope, run/intersection/digest, exact bytes and integrity hashes.

Preserve measured zero, null plus UNAVAILABLE, null plus NOT_APPLICABLE and absent optional fields. Do not apply V2 zero defaults.

### 5.3 Topic reuse protection

Use persistent Kafka data, idempotent topic creation, deletion disabled in the acceptance profile, and evidence with topic metadata, partition count and earliest/latest offsets. Record topic UUID/identity when available. If a recreated topic or reused offset has a different stored byte hash, fail closed and require explicit operator reconciliation or a new topic/group.

## 6. Locked Technical Decisions

### 6.1 Strategy

Use a dedicated V1 implementation reusing only version-neutral infrastructure and preserving V2:

Create:
- de/kafka_raw/v1_config.py
- de/kafka_raw/v1_models.py
- de/kafka_raw/v1_validator.py
- de/kafka_raw/v1_clickhouse_repository.py
- de/kafka_raw/v1_ledger_store.py
- de/kafka_raw/v1_consumer.py
- de/kafka_raw/v1_replay.py
- de/kafka_raw/v1_main.py

Adapt batch_buffer, offset_tracker, metrics, health patterns, ClickHouse connection and migration helpers only after tests. V1 must not share the V2 group, ledger, table or checkpoint. This isolates state, protects V2 history and gives Bronze a stable interface without an unrelated rewrite.

### 6.2 Archive boundary

Raw needs only shared wire validation and stores simulationRunId, intersectionId, topologyDigest and complete bytes. It does not require active-run selection, Orion, Projector or manifest lookup. Optional DE_V1_RAW_ARCHIVE_ROOT is read-only and evidence-only; unavailable archive is recorded as not checked.

### 6.3 Configuration

~~~text
DE_V1_RAW_BOOTSTRAP_SERVERS=kafka:9092
DE_V1_RAW_TOPIC=traffic.observations.v1
DE_V1_RAW_GROUP_ID=de-kafka-raw-v1
DE_V1_RAW_CLIENT_ID=de-kafka-raw-v1-node1
DE_V1_RAW_CLICKHOUSE_HOST=clickhouse
DE_V1_RAW_CLICKHOUSE_PORT=8123
DE_V1_RAW_CLICKHOUSE_DATABASE=smart_traffic
DE_V1_RAW_RAW_TABLE=v1_raw_events
DE_V1_RAW_QUARANTINE_TABLE=v1_quarantine_events
DE_V1_RAW_RAW_REPLAY_TABLE=v1_raw_events_replay
DE_V1_RAW_QUARANTINE_REPLAY_TABLE=v1_quarantine_events_replay
DE_V1_RAW_LEDGER_PATH=/app/de/artifacts/kafka_raw_v1/ledger.sqlite3
DE_V1_RAW_HEALTH_PORT=8097
DE_V1_RAW_BATCH_SIZE=500
DE_V1_RAW_FLUSH_MS=500
DE_V1_RAW_MAX_BUFFERED_RECORDS=5000
DE_V1_RAW_MAX_BUFFERED_BYTES=33554432
DE_V1_RAW_MIGRATION_PATH=/app/de/migrations/006_create_kafka_raw_v1.sql
DE_V1_RAW_ARCHIVE_ROOT=
~~~

## 7. Data Contracts and ClickHouse Schema

### 7.1 Tables and DDL

Add migration de/migrations/006_create_kafka_raw_v1.sql with v1_raw_events, v1_quarantine_events, v1_raw_events_replay and v1_quarantine_events_replay. Live tables use MergeTree, partition by toYYYYMM(consumed_at), order by topic/partition/offset/raw_ingestion_id. Replay adds replay_run_id and orders by replay ID first. V2 tables remain untouched.

Executable live Raw DDL specification:

~~~sql
CREATE DATABASE IF NOT EXISTS smart_traffic;
CREATE TABLE IF NOT EXISTS smart_traffic.v1_raw_events
(
 topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64),
 topic_identity Nullable(String), kafka_key_is_null UInt8 DEFAULT 0,
 kafka_key_bytes_base64 String, kafka_key_text Nullable(String),
 kafka_header_names Array(Nullable(String)), kafka_header_value_is_null Array(UInt8),
 kafka_header_values_base64 Array(String), kafka_headers_json String DEFAULT '{}',
 broker_timestamp Nullable(DateTime64(3,'UTC')), broker_timestamp_present UInt8 DEFAULT 0,
 broker_timestamp_type LowCardinality(String),
 consumed_at DateTime64(3,'UTC'),
 event_id FixedString(64), event_type LowCardinality(String), schema_version UInt16,
 simulation_run_id String, intersection_id String, scenario_id String,
 cycle_sequence Int64, simulation_step Int64, simulation_time_seconds Float64,
 captured_at DateTime64(3,'UTC'), topology_digest FixedString(72),
 payload_hash FixedString(64), immutable_envelope_hash FixedString(64),
 payload_encoding LowCardinality(String), value_is_null UInt8 DEFAULT 0,
 value_bytes_base64 String, payload_text String, payload_size_bytes UInt32,
 payload_bytes_hash FixedString(64), canonical_payload_hash FixedString(64),
 validator_version String, migration_version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic, partition, offset, raw_ingestion_id);
~~~

Quarantine has the same physical/key/time/byte fields, nullable attempted envelope fields, and failed_at, failure_stage, error_code, error_detail, validator_version and schema_version_attempted. Its authoritative byte representation is value_bytes_base64; value_is_null distinguishes Kafka null from an empty byte string, and payload_text is populated only after successful UTF-8 decoding. Replay tables have the corresponding columns plus replay_run_id String; use explicit column lists or verify CREATE AS followed by ADD COLUMN. DESCRIBE TABLE is mandatory. All four tables must be written explicitly in migration 006; the implementing agent must not invent missing columns.

### 7.2a Complete four-table DDL requirements

The migration must contain four explicit CREATE TABLE statements. The following is the complete column contract; replay tables repeat the live columns and add replay_run_id String NOT NULL. Quarantine envelope fields are Nullable where a record can fail before JSON parsing. The implementation must use these types, not approximate types:

~~~sql
-- v1_raw_events and v1_raw_events_replay use this complete column list.
topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64),
topic_identity Nullable(String),
kafka_key_is_null UInt8 DEFAULT 0, kafka_key_bytes_base64 String,
kafka_key_text Nullable(String),
kafka_header_names Array(Nullable(String)),
kafka_header_value_is_null Array(UInt8),
kafka_header_values_base64 Array(String),
kafka_headers_json String DEFAULT '{}',
broker_timestamp Nullable(DateTime64(3,'UTC')),
broker_timestamp_present UInt8 DEFAULT 0,
broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable',
consumed_at DateTime64(3,'UTC'),
captured_at Nullable(DateTime64(3,'UTC')),
 event_id FixedString(64), event_type LowCardinality(String), schema_version UInt16,
 simulation_run_id String, intersection_id String, scenario_id String,
 cycle_sequence Int64, simulation_step Int64, simulation_time_seconds Float64,
 topology_digest FixedString(72), payload_hash FixedString(64), immutable_envelope_hash FixedString(64),
 payload_encoding LowCardinality(String), value_is_null UInt8 DEFAULT 0,
 value_bytes_base64 String, payload_text Nullable(String), payload_size_bytes UInt32,
payload_bytes_hash FixedString(64), canonical_payload_hash Nullable(FixedString(64)),
validator_version String, migration_version LowCardinality(String)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic, partition, offset, raw_ingestion_id);

-- v1_quarantine_events and v1_quarantine_events_replay use the same physical
-- columns, plus these disposition columns:
failed_at DateTime64(3,'UTC'), failure_stage LowCardinality(String),
error_code String, error_detail String, schema_version_attempted Nullable(UInt16)
ENGINE = MergeTree
PARTITION BY toYYYYMM(consumed_at)
ORDER BY (topic, partition, offset, raw_ingestion_id);
~~~

The migration must materialize the comments above as four executable statements, not as fragments. Raw and Quarantine are separate destinations, so a row is not both. Replay tables add replay_run_id to the column list and sort by (replay_run_id, topic, partition, offset, raw_ingestion_id); live queries must never read them.

The implementing agent must use the following complete executable form (the replay statements intentionally repeat the live columns so replay isolation is visible in the schema):

~~~sql
CREATE TABLE IF NOT EXISTS smart_traffic.v1_raw_events
(
 topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64), topic_identity Nullable(String),
 kafka_key_is_null UInt8 DEFAULT 0, kafka_key_bytes_base64 String, kafka_key_text Nullable(String),
 kafka_header_names Array(Nullable(String)), kafka_header_value_is_null Array(UInt8), kafka_header_values_base64 Array(String), kafka_headers_json String DEFAULT '{}',
 broker_timestamp Nullable(DateTime64(3,'UTC')), broker_timestamp_present UInt8 DEFAULT 0, broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable', consumed_at DateTime64(3,'UTC'), captured_at Nullable(DateTime64(3,'UTC')),
 event_id FixedString(64), event_type LowCardinality(String), schema_version UInt16, simulation_run_id String, intersection_id String, scenario_id String, cycle_sequence Int64, simulation_step Int64, simulation_time_seconds Float64, topology_digest FixedString(72), payload_hash FixedString(64), immutable_envelope_hash FixedString(64),
 payload_encoding LowCardinality(String), value_is_null UInt8 DEFAULT 0, value_bytes_base64 String, payload_text String, payload_size_bytes UInt32, payload_bytes_hash FixedString(64), canonical_payload_hash FixedString(64), validator_version String, migration_version LowCardinality(String)
) ENGINE = MergeTree PARTITION BY toYYYYMM(consumed_at) ORDER BY (topic, partition, offset, raw_ingestion_id);

CREATE TABLE IF NOT EXISTS smart_traffic.v1_quarantine_events
(
 topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64), topic_identity Nullable(String),
 kafka_key_is_null UInt8 DEFAULT 0, kafka_key_bytes_base64 String, kafka_key_text Nullable(String),
 kafka_header_names Array(Nullable(String)), kafka_header_value_is_null Array(UInt8), kafka_header_values_base64 Array(String), kafka_headers_json String DEFAULT '{}',
 broker_timestamp Nullable(DateTime64(3,'UTC')), broker_timestamp_present UInt8 DEFAULT 0, broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable', consumed_at DateTime64(3,'UTC'), failed_at DateTime64(3,'UTC'),
 event_id Nullable(FixedString(64)), event_type Nullable(LowCardinality(String)), schema_version_attempted Nullable(UInt16), simulation_run_id Nullable(String), intersection_id Nullable(String), scenario_id Nullable(String), cycle_sequence Nullable(Int64), simulation_step Nullable(Int64), simulation_time_seconds Nullable(Float64), captured_at Nullable(DateTime64(3,'UTC')), topology_digest Nullable(FixedString(72)), payload_hash Nullable(FixedString(64)), immutable_envelope_hash Nullable(FixedString(64)),
 failure_stage LowCardinality(String), error_code String, error_detail String, validator_version String, payload_encoding LowCardinality(String), value_is_null UInt8 DEFAULT 0, value_bytes_base64 String, payload_text Nullable(String), payload_size_bytes UInt32, payload_bytes_hash FixedString(64), canonical_payload_hash Nullable(FixedString(64)), migration_version LowCardinality(String)
) ENGINE = MergeTree PARTITION BY toYYYYMM(consumed_at) ORDER BY (topic, partition, offset, raw_ingestion_id);

CREATE TABLE IF NOT EXISTS smart_traffic.v1_raw_events_replay
(
 replay_run_id String, topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64), topic_identity Nullable(String), kafka_key_is_null UInt8 DEFAULT 0, kafka_key_bytes_base64 String, kafka_key_text Nullable(String), kafka_header_names Array(Nullable(String)), kafka_header_value_is_null Array(UInt8), kafka_header_values_base64 Array(String), kafka_headers_json String DEFAULT '{}', broker_timestamp Nullable(DateTime64(3,'UTC')), broker_timestamp_present UInt8 DEFAULT 0, broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable', consumed_at DateTime64(3,'UTC'), captured_at DateTime64(3,'UTC'), event_id FixedString(64), event_type LowCardinality(String), schema_version UInt16, simulation_run_id String, intersection_id String, scenario_id String, cycle_sequence Int64, simulation_step Int64, simulation_time_seconds Float64, topology_digest FixedString(72), payload_hash FixedString(64), immutable_envelope_hash FixedString(64), payload_encoding LowCardinality(String), value_is_null UInt8 DEFAULT 0, value_bytes_base64 String, payload_text String, payload_size_bytes UInt32, payload_bytes_hash FixedString(64), canonical_payload_hash FixedString(64), validator_version String, migration_version LowCardinality(String)
) ENGINE = MergeTree PARTITION BY toYYYYMM(consumed_at) ORDER BY (replay_run_id, topic, partition, offset, raw_ingestion_id);

CREATE TABLE IF NOT EXISTS smart_traffic.v1_quarantine_events_replay
(
 replay_run_id String, topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64), topic_identity Nullable(String), kafka_key_is_null UInt8 DEFAULT 0, kafka_key_bytes_base64 String, kafka_key_text Nullable(String), kafka_header_names Array(Nullable(String)), kafka_header_value_is_null Array(UInt8), kafka_header_values_base64 Array(String), kafka_headers_json String DEFAULT '{}', broker_timestamp Nullable(DateTime64(3,'UTC')), broker_timestamp_present UInt8 DEFAULT 0, broker_timestamp_type LowCardinality(String) DEFAULT 'NotAvailable', consumed_at DateTime64(3,'UTC'), failed_at DateTime64(3,'UTC'), event_id Nullable(FixedString(64)), event_type Nullable(LowCardinality(String)), schema_version_attempted Nullable(UInt16), simulation_run_id Nullable(String), intersection_id Nullable(String), scenario_id Nullable(String), cycle_sequence Nullable(Int64), simulation_step Nullable(Int64), simulation_time_seconds Nullable(Float64), captured_at Nullable(DateTime64(3,'UTC')), topology_digest Nullable(FixedString(72)), payload_hash Nullable(FixedString(64)), immutable_envelope_hash Nullable(FixedString(64)), failure_stage LowCardinality(String), error_code String, error_detail String, validator_version String, payload_encoding LowCardinality(String), value_is_null UInt8 DEFAULT 0, value_bytes_base64 String, payload_text Nullable(String), payload_size_bytes UInt32, payload_bytes_hash FixedString(64), canonical_payload_hash Nullable(FixedString(64)), migration_version LowCardinality(String)
) ENGINE = MergeTree PARTITION BY toYYYYMM(consumed_at) ORDER BY (replay_run_id, topic, partition, offset, raw_ingestion_id);
~~~

The migration test must execute the exact file on the repository's ClickHouse image and fail if SHOW CREATE TABLE does not contain replay_run_id before the table is used.

Representation rules are fixed:

- Kafka key null: kafka_key_is_null=1, kafka_key_bytes_base64='', kafka_key_text=NULL. Empty key: kafka_key_is_null=0, base64 of zero bytes, kafka_key_text=''.
- Kafka value null/tombstone: value_is_null=1, value_bytes_base64='', payload_size_bytes=0, payload_bytes_hash=SHA-256 of an empty byte string, payload_text=NULL, and Quarantine NULL_VALUE. Empty non-null value: value_is_null=0 with the same empty-byte hash and invalid-JSON Quarantine.
- Valid UTF-8: value_bytes_base64 is authoritative, payload_text is the decoded text, payload_encoding='utf8'. Invalid UTF-8: base64 remains authoritative, payload_text=NULL, payload_encoding='base64'. Malformed JSON still retains the exact base64 bytes.
- Headers: the ordered Kafka header list is authoritative in the three arrays. Duplicate names remain separate, order is retained, null header values use a 1 in kafka_header_value_is_null and an empty base64 element, and binary values are base64. kafka_headers_json is diagnostic only and must never be used for reconstruction.
- Missing broker timestamp: broker_timestamp=NULL, broker_timestamp_present=0, broker_timestamp_type='NotAvailable'; consumed_at remains the consumer timestamp. Do not substitute consumed_at into broker_timestamp.
- value_bytes_base64 is chosen as the exact binary representation because the current Python client/ClickHouse String binary round trip is not assumed. The implementation must add a binary round-trip integration test that decodes the stored base64 and compares bytes exactly; a separate probe may document String behavior but cannot replace this authoritative representation.

The immutable-envelope hash is defined as SHA-256 of contracts.traffic_observation.canonical_json applied to this object, encoded as UTF-8:

~~~text
{
  eventId, eventType, schemaVersion, simulationRunId, intersectionId,
  scenarioId, cycleSequence, simulationStep, simulationTimeSeconds,
  capturedAt, topologyDigest, payloadHash
}
~~~

Fields are taken from the frozen producer event, sorted by canonical_json, with no omitted nulls because these are required V1 envelope fields. This is compatible with frozen-envelope semantics because it hashes the immutable top-level envelope and the payloadHash, while the full frozen event remains recoverable from value_bytes_base64. The producer does not currently emit this hash; Raw derives it deterministically and stores the algorithm/version in validator_version.

### 7.2 Column contract

| Group | Type/source/purpose |
|---|---|
| Physical identity | topic String, partition Int32, offset Int64, raw_ingestion_id FixedString(64) |
| Key/headers | exact kafka_key_bytes String, decoded nullable key text, headers JSON |
| Time | broker timestamp/type, consumed_at, captured_at |
| Envelope | event ID/type/version, run/intersection/scenario, cycle/step/time, topology digest |
| Integrity | payloadHash, immutable_envelope_hash, payload_bytes_hash, canonical_payload_hash, validator_version |
| Payload | payload_encoding, value_bytes_base64 String, nullable payload_text, payload_size_bytes |
| Disposition | no semantic conflict flag; Quarantine stage/code/detail only |
| Source | Kafka metadata and shared V1 envelope; no analytical defaults |

ClickHouse String receives original bytes. Raw does not flatten directional metrics. Retain live Raw/Quarantine through Phase 5 and downstream reconciliation; any later TTL requires approval and evidence export.

### 7.4 Immutable-envelope and binary verification

The validator must expose build_immutable_envelope_hash(event) -> str. It selects exactly eventId, eventType, schemaVersion, simulationRunId, intersectionId, scenarioId, cycleSequence, simulationStep, simulationTimeSeconds, capturedAt, topologyDigest and payloadHash; it serializes that object with contracts.traffic_observation.canonical_json, UTF-8 encodes it, and hashes SHA-256. It does not hash parsed dictionary order or reserialized payload bytes. The full frozen event remains recoverable from value_bytes_base64. Add an integration test that writes arbitrary bytes containing zero bytes, high bytes and invalid UTF-8, reads the ClickHouse row, base64-decodes it, and asserts byte-for-byte equality plus equal payload_bytes_hash.

### 7.5 V1 Raw logical-read contract

The authoritative Bronze interface is a repository query, not an unfiltered MergeTree table scan:

~~~python
read_v1_raw_logical_deliveries(topic: str, partition: int, start_offset: int,
    end_offset: int | None = None) -> list[ResolvedRawDelivery]
~~~

The query unions live Raw and Quarantine rows, groups by topic/partition/offset/raw_ingestion_id, and returns one row per physical identity with disposition_count, raw_count, quarantine_count, distinct_payload_hashes, distinct_byte_hashes, min/max ingestion timestamps and a disposition_status. Identical retry rows with one destination and one byte hash collapse to one logical delivery. Different byte hashes, both destinations, or more than one disposition are INTEGRITY_CONFLICT and are not delivered as a valid Bronze input. A missing physical identity in the ledger is UNRESOLVED; Bronze must not infer success from a ClickHouse row alone.

The implementation must provide this SQL shape and parameterize values with the ClickHouse client:

~~~sql
SELECT topic, partition, offset, raw_ingestion_id,
       count() AS physical_rows,
       uniqExact(payload_bytes_hash) AS byte_hashes,
       uniqExact(destination) AS destinations,
       min(consumed_at) AS first_seen, max(consumed_at) AS last_seen
FROM
(
 SELECT topic, partition, offset, raw_ingestion_id, payload_bytes_hash,
        'RAW' AS destination, consumed_at
 FROM smart_traffic.v1_raw_events
 UNION ALL
 SELECT topic, partition, offset, raw_ingestion_id, payload_bytes_hash,
        'QUARANTINE' AS destination, consumed_at
 FROM smart_traffic.v1_quarantine_events
)
WHERE topic = {topic:String} AND partition = {partition:Int32}
  AND offset >= {start:Int64}
GROUP BY topic, partition, offset, raw_ingestion_id
ORDER BY offset, raw_ingestion_id;
~~~

The repository joins the result to the V1 ledger and exposes only CONFIRMED_RAW, CONFIRMED_QUARANTINE, or CONFLICT/UNRESOLVED. Bronze advances its checkpoint only over contiguous ledger-terminal rows; broker offset gaps are valid only when Kafka reports the partition's log start has advanced past the gap or a bounded retention audit proves the records were never available. Quarantine is progression evidence, not a semantic acceptance. This interface prevents Phase 2 from redesigning Raw to collapse MergeTree retry rows.

### 7.6 V1 physical ledger DDL and state machine

Create the isolated SQLite file at DE_V1_RAW_LEDGER_PATH. The V1 schema is executable and must be applied with PRAGMA journal_mode=WAL, synchronous=FULL, foreign_keys=ON and busy_timeout=5000:

~~~sql
CREATE TABLE IF NOT EXISTS deliveries (
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_value INTEGER NOT NULL,
 raw_ingestion_id TEXT NOT NULL, topic_identity TEXT,
 disposition TEXT NOT NULL CHECK(disposition IN ('RAW','QUARANTINE')),
 state TEXT NOT NULL, expected_payload_bytes_hash TEXT NOT NULL,
 observed_payload_bytes_hash TEXT, expected_value_is_null INTEGER NOT NULL DEFAULT 0,
 batch_id TEXT, attempt_count INTEGER NOT NULL DEFAULT 0,
 last_error TEXT, failure_class TEXT, received_at TEXT NOT NULL,
 attempted_at TEXT, persistence_confirmed_at TEXT, commit_eligible_at TEXT,
 kafka_committed_at TEXT, updated_at TEXT NOT NULL,
 PRIMARY KEY(topic, partition_id, offset_value),
 UNIQUE(raw_ingestion_id),
 CHECK((state <> 'PERSISTENCE_CONFIRMED') OR observed_payload_bytes_hash = expected_payload_bytes_hash)
);
CREATE INDEX IF NOT EXISTS ix_deliveries_state ON deliveries(topic, partition_id, state, offset_value);
CREATE INDEX IF NOT EXISTS ix_deliveries_batch ON deliveries(batch_id, state);
CREATE TABLE IF NOT EXISTS partition_progress (
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, topic_identity TEXT,
 broker_log_start_offset INTEGER, broker_log_end_offset INTEGER,
 last_durable_record_offset INTEGER, committed_next_offset INTEGER,
 assignment_generation INTEGER, owner_instance_id TEXT, state TEXT NOT NULL,
 last_reconciled_at TEXT, last_error TEXT, updated_at TEXT NOT NULL,
 PRIMARY KEY(topic, partition_id)
);
CREATE TABLE IF NOT EXISTS batches (
 batch_id TEXT PRIMARY KEY, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
 first_offset INTEGER NOT NULL, last_offset INTEGER NOT NULL,
 row_count INTEGER NOT NULL, state TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
 clickhouse_request_id TEXT, created_at TEXT NOT NULL, attempted_at TEXT,
 confirmed_at TEXT, last_error TEXT
);
CREATE TABLE IF NOT EXISTS integrity_faults (
 fault_id TEXT PRIMARY KEY, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
 offset_value INTEGER NOT NULL, raw_ingestion_id TEXT NOT NULL,
 expected_hash TEXT NOT NULL, observed_hash TEXT, raw_count INTEGER NOT NULL DEFAULT 0,
 quarantine_count INTEGER NOT NULL DEFAULT 0, detail TEXT NOT NULL,
 created_at TEXT NOT NULL, resolved_at TEXT
);
~~~

State meanings: RECEIVED means Kafka metadata and expected hash were durably recorded; PERSISTENCE_ATTEMPTED means a ClickHouse request was issued; PERSISTENCE_CONFIRMED means the expected destination contains the identity and matching hash; QUARANTINED is the terminal confirmed quarantine disposition; COMMIT_ELIGIBLE means the delivery is terminal and contiguous calculation may use it; COMMITTED means the broker acknowledged the next offset; FAILED_RETRYABLE is not terminal; FAILED_INTEGRITY is a fault requiring operator resolution. Terminal persistence is never written without destination confirmation or positive reconciliation.

Allowed transition matrix:

| Current | Trigger | Guard | Durable operation | Next | Commit eligibility | Restart |
|---|---|---|---|---|---|---|
| absent | record received | metadata captured | insert delivery RECEIVED in SQLite transaction | RECEIVED | no | retry from Kafka |
| RECEIVED | insert begins | ledger owned | update batch and attempt | PERSISTENCE_ATTEMPTED | no | reconcile expected identity |
| ATTEMPTED | insert/reconcile finds exact expected destination/hash | one destination, matching hash | update observed hash and confirmation timestamp | PERSISTENCE_CONFIRMED or QUARANTINED | after contiguous check | continue |
| ATTEMPTED | timeout/connection error | outcome unknown | increment attempt and error | FAILED_RETRYABLE | no | query both tables before retry |
| ATTEMPTED | different bytes/destination or both tables | integrity conflict | insert integrity_fault and commit transaction | FAILED_INTEGRITY | no | stop partition/service |
| confirmed | preceding offsets terminal | contiguous tracker permits | set commit_eligible_at | COMMIT_ELIGIBLE | yes | retry broker commit |
| COMMIT_ELIGIBLE | Kafka commit acknowledged | broker response success | set committed timestamp | COMMITTED | yes | startup compares broker committed offset |
| any retryable | retry budget/health permits | no integrity fault | new attempt/batch | ATTEMPTED | no | resume reconciliation |

SQLite transaction boundaries: delivery receipt plus batch membership is one SQLite transaction; each successful ClickHouse batch is followed by one SQLite transaction that updates every confirmed delivery and batch; commit eligibility is a separate transaction after contiguous calculation; Kafka commit is outside SQLite; COMMITTED is written only after broker acknowledgement. No claim of one atomic transaction across systems is allowed.

## 8. Ordered Implementation Tasks

### P1-01 Synchronize and freeze contract

Inspect shared schema, contract docs, topology digest, V1 publisher/outbox and tests; synchronize branch; record commit; confirm topic/key/hash/lifecycle. Tests: git status/rev-parse and contract tests. Completion: synchronized baseline and no second schema.

Exact source audit targets are contracts/traffic_observation.py validate_observation, observation_event_id and canonical_json; Visualize/integration/kafka/v1_publisher.py freeze_cycle and persist_frozen; Visualize/integration/kafka/outbox_store.py append_v1_cycle and recovery methods; and Visualize/app/traci_runner.py run. No source modification belongs in this task. Expected output is a committed baseline record and a written list of verified contract invariants.

### P1-02 V1 models and validation

Create v1_models.py, v1_validator.py and de/tests/kafka_raw/test_v1_validator.py. Implement V1 records, exact-byte classification, key/header/timestamp capture, immutable envelope hash and shared validator call. Cover all contract cases listed in Section 11. Completion: deterministic Raw/Quarantine and no V2 defaults.

Required interfaces: V1Record(topic, partition, offset, key_bytes, value_bytes, headers, broker_timestamp_ms, broker_timestamp_type); V1ClassifiedRecord(destination, row, failure_stage, error_code, error_detail); V1RawValidator.classify(record). Reuse contracts.traffic_observation.validate_observation and canonical_json directly. Add tests for ordered duplicate/null headers, null versus empty key/value, binary bytes and full immutable-envelope hash. No eventId lookup or semantic ledger is permitted in this task.

### P1-03 V1 storage, migration and ledger

Create migration 006, v1_clickhouse_repository.py, v1_ledger_store.py and migration/ledger tests. Use synchronous insert and positive physical-ID/byte-hash reconciliation. Ledger states: RECEIVED, PERSISTENCE_ATTEMPTED, PERSISTENCE_CONFIRMED, QUARANTINED, COMMIT_ELIGIBLE, COMMITTED, FAILED_RETRYABLE, FAILED_INTEGRITY. Completion: clean/repeated/V2-upgrade migration passes and no false durable state.

Required methods: V1ClickHouseRepository.insert_raw(rows, batch_id), insert_quarantine(rows, batch_id), find_physical_delivery(identity), and verify_tables(); V1LedgerStore.receive(delivery), begin_batch(batch), confirm_batch(rows), mark_retryable(delivery), record_integrity_fault(fault), contiguous_terminal_prefix(topic, partition, start), and record_broker_commit(topic, partition, next_offset). Exact SQLite DDL and transaction boundaries are in Section 7.6. Tests must inject insert success/timeout/partial result and verify the ledger states rather than only row counts.

### P1-04 V1 consumer and offset safety

Create v1_config.py, v1_consumer.py, v1_main.py and recovery tests. Capture then ledger RECEIVED; buffer by destination; insert/reconcile; batch-confirm ledger; mark contiguous offsets; synchronously commit next offset. Revoke flushes/reconciles before return. Completion: restart, non-contiguous, mixed, partial, rebalance and commit-failure tests.

Required functions: V1RawKafkaConsumer._handle_msg, _flush_partition, _reconcile_batch, _commit_partition, _on_assign, _on_revoke, _startup_reconcile and health; V1OffsetTracker.contiguous_terminal_prefix and next_commit_offset. Use enable.auto.commit=false and enable.auto.offset.store=false. Commit only the next offset after the terminal contiguous prefix; a per-partition commit error leaves COMMIT_ELIGIBLE state. Add the post-Kafka-commit/pre-COMMITTED crash test and the broker-committed-ahead startup test.

### P1-05 Replay, repair, health and metrics

Create v1_replay.py and v1_raw_gap_repair.py; adapt health/metrics. Replay group de-kafka-raw-v1-replay-id, tables and ledger under replay/id. Live group/tables/ledger/V2 never mutate. /ready requires migration, schema, assignment, ClickHouse, ledger and non-stale commits. Completion: outage/restart/replay isolation tests.

Required entry points are the three CLIs in Section 10. Replay calls the same V1 validator and repository with replay_run_id and a separate ledger; it does not call the live commit path. Gap repair first runs report-only, records beginning/end offsets and retention status, and only then permits isolated replay repair. Health must distinguish liveness, assigned/operational, ready, caught-up idle, degraded and faulted states.

### P1-06 Docker/Compose integration

Modify docker-compose.yml, docker/kafka-init.sh and migration runner only as required. Add de-kafka-raw-v1, V1 migration service/profile, persistent artifact mount, port 8097 and healthcheck. Depend only on Kafka, ClickHouse and V1 migration. Keep V2 services under v2-legacy. Completion: minimal runtime works without Bronze/Silver/Gold/Spring/Dashboard/Mongo/Orion/Projector.

Verify the actual de/Dockerfile build contains contracts/ and de/ and that the V1 migration path is mounted/copied. Verify kafka-init creates traffic.observations.v1 idempotently, and record the Kafka client capability probe for topic identity. The service must have restart policy, bounded memory-compatible batch settings, no V2 environment variables, and a healthcheck that calls /ready without making an idle caught-up consumer fail.

### P1-07 Evidence and handover

Create docs/DEv1/phase-1/acceptance.md, handover.md, implementation-status.md, decisions.md and blockers.md; evidence under de/artifacts/v1_raw/acceptance/<UTC-run-id>. Run a fresh SUMO V1 run and capture Outbox/Kafka/group/ledger/ClickHouse/health/failure/restart evidence. Completion: all gates pass and handover is complete.

Use the actual producer command from Visualize/app/traci_runner.py. In PowerShell, from the repository root: $runId = "phase1-raw-" + (Get-Date -Format yyyyMMddHHmmss); Set-Location Visualize; python -m app.traci_runner --no-gui --no-orion --no-api --v1-observations --v1-publish --nodes A,B,C,D,M --simulation-run-id $runId --max-sim-time 1200 --log-level INFO. The plan must record the exact run ID and actual command used; it must not claim that this or any runtime test was executed during planning.

## 9. Failure Handling and Recovery Matrix

| Failure | Durable state | Commit? | Required action/test |
|---|---|---|---|
| before insert | RECEIVED/ATTEMPTED | no | retry same record |
| insert rejected | retryable | no | backoff/pause |
| response lost | ambiguous | only after positive hash/destination reconciliation | reconcile; conflict faults |
| ledger update fails | ClickHouse row, unresolved ledger | no | restart reconciliation |
| Kafka commit fails | COMMIT_ELIGIBLE | no beyond committed boundary | replay idempotently |
| crash after persistence | ClickHouse + terminal ledger, Kafka uncommitted | after restart check | no loss |
| same physical delivery repeats | existing same hash | after contiguous check | no second ledger delivery |
| mixed/partial/non-contiguous | later rows held by tracker | only through earliest gap | retry missing rows |
| revoke/shutdown | flushed or unresolved | only confirmed contiguous | recovery test |
| Kafka/ClickHouse/ledger outage | degraded/faulted | no unsafe commit | readiness/retry test |
| topic recreation/offset reuse | integrity fault | no | fail closed and operator repair |

Ordering is capture -> persistence/reconciliation -> durable ledger -> contiguous offset -> Kafka commit of next offset. Invalid records advance only after durable Quarantine.

### 9.1 Persistence reconciliation algorithm

The repository interface is:

~~~python
insert_batch(batch_id: str, destination: Literal['RAW','QUARANTINE'], rows: Sequence[V1Row]) -> None
find_physical_delivery(topic: str, partition: int, offset: int, raw_ingestion_id: str) -> list[PhysicalEvidence]
reconcile_delivery(expected: DeliveryExpectation) -> ReconciliationResult
~~~

Algorithm: create one batch_id and mark every row PERSISTENCE_ATTEMPTED; issue one synchronous insert with a request ID; on success, query both destinations for every physical identity; on error or timeout, query both destinations before retrying. For each expected delivery, exactly one destination with one byte hash equal to expected gives CONFIRMED_RAW or QUARANTINED. Multiple identical rows in the same destination collapse to one logical delivery. Missing rows are unresolved and retried individually after the first ambiguous batch; the worker does not repeatedly reinsert already confirmed rows. A row in both destinations, a different hash, a different raw_ingestion_id, or a key/lineage mismatch creates integrity_fault, moves the delivery to FAILED_INTEGRITY, pauses the partition and prevents all later offset commits. A missing expected row remains FAILED_RETRYABLE and blocks its partition. Mixed batches are reconciled per delivery, so confirmed rows become terminal while missing rows are retried.

After reconciliation, the worker executes one SQLite transaction for the confirmed subset, recording observed hash, destination, confirmation time and state. It then recomputes the earliest unresolved offset and commits only the contiguous terminal prefix. No unresolved ClickHouse, ledger or integrity result can advance Kafka.

### 9.2 Kafka settings, startup and commit pseudocode

V1 consumer settings are explicit: enable.auto.commit=false, enable.auto.offset.store=false, auto.offset.reset=earliest, isolation.level=read_committed, group.id=de-kafka-raw-v1, partition.assignment.strategy=cooperative-sticky unless the installed client/source tests require range, session.timeout.ms=45000, heartbeat.interval.ms=15000, max.poll.interval.ms=300000, and max.poll.records=500. The worker uses on_assign and on_revoke callbacks and never calls store_offsets. Retry/backoff does not stop poll/heartbeat beyond max.poll.interval.ms; a full buffer pauses assigned partitions while the loop continues polling.

Startup reconciliation: call AdminClient/consumer committed for each assigned partition; query beginning_offsets/end_offsets; compare broker committed next offset with partition_progress and delivery ledger. If broker committed is ahead of local ledger, do not reset it or claim local persistence: mark the range BROKER_COMMITTED_UNRECONCILED, fetch retained records where possible, and require evidence/repair before Bronze handover. If broker committed is behind a contiguous ledger prefix, reprocess safely and commit only after hash reconciliation. If offsets are out of range, record log start/end, retention loss and a blocked gap; never invent missing rows.

Normal progression:

~~~text
for each partition:
  terminal = ledger rows in CONFIRMED_RAW, QUARANTINED, COMMITTED
  candidate = smallest offset at or after last broker-committed offset
  while candidate is terminal and no integrity fault:
      mark COMMIT_ELIGIBLE in SQLite
      candidate += 1
  commit next_offset = first unresolved offset
  on synchronous commit response:
      update partition_progress and delivery COMMITTED in SQLite
~~~

Partial per-partition commit responses update only acknowledged partitions; unacknowledged partitions remain COMMIT_ELIGIBLE and are retried. Revoke pauses, flushes and reconciles owned partitions before returning; lost ownership stops writes/commits for that partition. Graceful shutdown flushes within a bounded timeout, records unresolved rows, and does not mark them committed.

The crash case ClickHouse success -> ledger persistence confirmation -> Kafka commit success -> process crash before COMMITTED is recovered from the broker, not only SQLite: on restart, broker committed next offset is authoritative for the consumed boundary; rows below it are marked COMMITTED only after their ClickHouse physical hash/destination is revalidated. If retained Kafka no longer redelivers them, the evidence is still complete because the broker commit plus positive ClickHouse reconciliation proves the delivery; if reconciliation fails, mark BROKER_COMMITTED_UNRECONCILED and block readiness/handover.

### 9.3 Topic identity and offset reuse

At startup, use the installed confluent-kafka AdminClient/metadata API to test whether describe_topics exposes a stable topic_id for the target Kafka version. Persist the returned value in partition_progress.topic_identity and acceptance evidence. If this client version does not expose a topic UUID, persist a deployment topic fingerprint consisting of cluster_id when describe_cluster exposes it, topic name, partition count and first-seen partition log-start metadata; label it as a fingerprint, not a UUID. The implementation must not claim a broker UUID that the client cannot retrieve.

Compare the current identity/fingerprint with the ledger on startup. A changed identity, reduced partition count, or an offset below recorded log-start after topic recreation blocks the service and requires a new V1 ledger namespace or explicit operator migration. An offset outside retained broker ranges is recorded as RETENTION_GAP and is not silently reused. Operational prevention is persistent Kafka data, no topic deletion in the acceptance profile, and an idempotent topic-init script; tests verify both prevention configuration and detection fallback.

## 10. Migration, Runtime and Commands

Migration 006 is additive/idempotent. Add a --v1-raw runner profile selecting only 006; never edit 001-005. Clean install creates V1 objects; upgrade adds them beside V2; failed migration stops V1 only; rollback is safe-disable and never drops tables.

Compose must add de-kafka-raw-v1, port 8097, group de-kafka-raw-v1, persistent V1 ledger/artifact mount, optional read-only archive mount, Kafka/ClickHouse/V1 migration dependencies, and no analytics/Orion/Projector dependency. V2 stays under v2-legacy.

PowerShell from repository root:

~~~powershell
docker compose up -d kafka clickhouse kafka-init
docker compose run --rm de-migrate-v1
docker compose up -d de-kafka-raw-v1
Invoke-WebRequest http://localhost:8097/health | Select-Object -Expand Content
Invoke-WebRequest http://localhost:8097/ready | Select-Object -Expand Content
docker compose ps
docker compose logs --no-color de-kafka-raw-v1
docker compose restart de-kafka-raw-v1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic traffic.observations.v1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group de-kafka-raw-v1
~~~

Verify rendered Compose names before execution. ClickHouse evidence includes SHOW CREATE TABLE, DESCRIBE TABLE, counts by run/partition, uniqExact(raw_ingestion_id), hashes and Quarantine counts. Export the V1 SQLite ledger.

Configuration validation is part of startup. Types and guards: bootstrap/topic/group/client/host/table names/paths are non-empty strings; ports are integers 1-65535; batch size and buffer limits are positive; flush and retry intervals are non-negative; max.poll.interval.ms is greater than the worst-case bounded flush time; replay_id matches a safe path component and is mandatory in replay mode; live mode rejects replay table names and replay paths; V1 mode rejects the V2 topic/group/ledger; archive root is optional and read-only when mounted. Emit effective values in redacted health/config evidence.

Replay interfaces are fixed:

~~~text
python -m de.kafka_raw.v1_replay --topic traffic.observations.v1 --partition P --start-offset A --end-offset B --replay-run-id R --bootstrap-servers kafka:9092
python -m de.tools.v1_raw_gap_repair --topic traffic.observations.v1 --partition P --from-offset A --to-offset B --report-only
python -m de.tools.v1_raw_gap_repair --topic traffic.observations.v1 --partition P --from-offset A --to-offset B --repair --replay-run-id R
~~~

Replay uses group de-kafka-raw-v1-replay-R, tables v1_*_replay, ledger de/artifacts/kafka_raw_v1/replay/R/ledger.sqlite3, and no live commit or live ledger. Report-only never writes ClickHouse or SQLite. Repair writes replay tables only, records Kafka-retention gaps, and never changes live Raw, Quarantine, Bronze, V2 or consumer offsets. Use de/artifacts/v1_raw for acceptance evidence and de/artifacts/kafka_raw_v1 for live/replay state. Preserve ignored docs/DEv1 through repository documentation policy or a separately approved tracking decision; do not change Git settings here.

### 10.1 Health and readiness semantics

Health responses contain status, state, consumer_thread_alive, assigned_partitions, group_id, topic, broker connectivity, ClickHouse connectivity, ledger connectivity, migrations_ok, schemas_ok, buffered_records/bytes, raw_persisted, quarantine_persisted, retry_count, reconciliation_count, last_successful_persistence_time, last_successful_commit_time, committed_next_offsets, durable_contiguous_offsets, consumer_lag, unresolved_count, integrity_fault_count, topic_identity and failure_reason. These are physical-ingestion snapshots, not semantic Bronze acceptance.

GET /health is liveness: HTTP 200 while the process and HTTP server are alive, even when dependencies are down. GET /ready is HTTP 200 only when the worker is alive, at least one assignment exists, migrations and schemas are loaded, Kafka/ClickHouse/ledger are available, no unresolved integrity fault exists, and persistence is operational. It is HTTP 503 for no assignment, dependency outage, unresolved reconciliation, broker identity change or faulted state. A healthy idle consumer with no new messages remains ready; an old last-commit timestamp alone never makes it unready. Lag is reported and may make /cutover-ready false, but does not make an assigned idle consumer unready. /metrics returns snapshots; /cutover-ready is 503 until all assigned partitions have no unresolved rows and lag is within the configured bound.

Health tests cover idle ready, no assignment, ClickHouse down, ledger down, unresolved reconciliation, lagging processing, permanent integrity fault and recovery.

## 11. Test Matrix

| ID | Type/scenario | Expected/PASS |
|---|---|---|
| C-01 | valid real V1 and fixture | Raw; shared validator and exact hash pass |
| C-02 | wrong type/version, V2 entity, lifecycle | Quarantine with reason; no Raw |
| C-03 | missing field, bad event ID/hash/key | Quarantine; original bytes |
| C-04 | malformed UTF8/JSON, null/tombstone | Quarantine; null and empty distinct |
| C-05 | zero, unavailable, not-applicable, absent optional | Raw; unchanged semantics/no defaults |
| I-01 | same event ID at two offsets | two physical deliveries |
| I-02 | same offset reprocessed | one ledger delivery/same hash |
| I-03 | same event ID changed immutable envelope | both preserved; Raw performs no cross-offset classification |
| I-04 | same physical ID changed bytes | fault/no commit |
| I-05 | multiple runs/intersections/out-of-order | all physical rows retained |
| P-01 | Raw, Quarantine, mixed/partial batch | ClickHouse and ledger agree |
| P-02 | outage, response lost, ledger/commit failure, crash | retry/reconcile; no unsafe commit |
| P-03 | non-contiguous, rebalance, graceful shutdown | gap blocks; recovery safe |
| R-01 | isolated replay | live state unchanged |
| R-02 | gap report/repair | report read-only; repair isolated |
| M-01 | clean migration | exact V1 schema |
| M-02 | V2 upgrade/repeated migration | V2 unchanged; idempotent |
| O-01 | fresh SUMO run | fresh run reaches Kafka and Raw |
| O-02 | Outbox/Kafka/Raw comparison | bytes agree; physical/semantic counts explained |
| O-03 | restart before commit | no loss/unsafe duplicate |
| O-04 | readiness/outage | ready is 503 when persistence unavailable |

Malformed/conflict injections use an isolated topic/group or disposable environment. Never delete Kafka data or ClickHouse volumes to simplify tests.

Correctness-critical test detail (every row records durable state, expected next offset, evidence and PASS):

| ID | Setup/input/injection | Expected durable state and Kafka offset | PASS condition |
|---|---|---|---|
| B-01 | publish value containing 00, FF and invalid UTF-8 | Quarantine or Raw as contract dictates; base64 decodes exactly; offset advances only after destination confirmation | decoded bytes and SHA-256 equal producer bytes |
| B-02 | send null key, empty key, duplicate ordered headers and null header value | correct null flags, arrays preserve order/duplicates, expected key failure quarantined | ClickHouse arrays and evidence match Kafka metadata |
| B-03 | ClickHouse insert succeeds then repository throws timeout | ledger remains ATTEMPTED until both tables are queried; offset does not advance during uncertainty | retry finds exact one destination/hash and then advances safely |
| B-04 | insert only first half of batch | confirmed rows terminal, missing rows retryable, offset stops before earliest missing row | no commit skips missing delivery |
| B-05 | inject same physical identity into Raw and Quarantine | integrity_fault and FAILED_INTEGRITY; partition paused | no offset beyond fault and /ready is 503 |
| B-06 | insert success, ledger SQLite write failure | ClickHouse row exists, ledger unresolved, no commit | restart reconciles and never falsely reports prior persistence |
| B-07 | broker commit returns error or partial per-partition success | acknowledged partitions update progress; others remain COMMIT_ELIGIBLE | no lost partition boundary |
| B-08 | broker commit succeeds, process crashes before COMMITTED | broker committed next offset is compared at startup; rows below it revalidated and then marked COMMITTED | no redelivery is required for proof and no gap is silently accepted |
| B-09 | revoke/lost ownership during in-flight batch | callback stops writes/commit for lost partition; owned rows flush/reconcile | no commit by an owner after revoke |
| B-10 | valid broker log gap caused by retention | RETENTION_GAP recorded with log start/end; no invented Raw row | readiness/handover remains blocked until explicit audit |
| B-11 | topic fingerprint/UUID changes | identity fault and new namespace required | old ledger identity is never reused |
| B-12 | idle assigned consumer with no new messages | ready HTTP 200, lag 0 or reported, old commit timestamp tolerated | idle does not become unready |
| B-13 | duplicate physical ClickHouse rows with identical hashes | logical-read interface returns one delivery and physical_rows > 1 | Bronze sees one resolved delivery plus duplicate evidence |

These cases must be implemented as named tests in de/tests/kafka_raw/test_v1_validator.py, test_v1_ledger.py, test_v1_consumer_recovery.py, test_v1_logical_read.py, test_v1_topic_identity.py and de/tests/tools/test_v1_raw_gap_repair.py, with Docker integration tests under de/tests/integration/test_v1_raw_runtime.py. The tests must assert expected Kafka next-offset values, not only database contents.

## 12. Runtime Acceptance Procedure

1. Record HEAD, Compose config, image versions and redacted configuration.
2. Start only Kafka, Kafka-init and ClickHouse; run V1 migration; verify V1 and unchanged V2 tables.
3. Start V1 Raw; capture health/ready/metrics, assignment and initial offsets.
4. Start a fresh SUMO V1 run; record new run ID, manifest, digest and participants; do not use an old record as proof.
5. Inspect V1 Outbox rows: IDs, frozen bytes/hash, topic, key, queue/ACK state.
6. Inspect actual Kafka records, partitions, offsets, keys and hashes; inspect group offsets.
7. Query ClickHouse by fresh run and compare Outbox/Kafka/Raw bytes. Report physical deliveries, distinct semantic IDs, duplicates, Quarantine and physical Raw rows separately.
8. For N complete cycles and M participating intersections, verify N*M distinct semantic observations against actual captured cycles/participants; do not hard-code five.
9. Inspect ledger and prove no committed offset is ahead of an unresolved earlier delivery.
10. Restart around a commit opportunity; recheck counts, hashes, ledger, group offsets and health.
11. Inject ClickHouse outage; verify ready false, offsets stop, retry/reconciliation evidence and lossless recovery.
12. Run replay/gap repair; prove live tables, ledger, offsets and V2 assets unchanged.
13. Store commit, configuration, migration, Compose, logs, health, manifest, Outbox, Kafka/group, ledger, ClickHouse, failure and restart evidence under de/artifacts/v1_raw/acceptance/<UTC-run-id>.

PowerShell failure-injection commands for the implemented local runtime are:

~~~powershell
docker compose pause clickhouse
Invoke-WebRequest http://localhost:8097/ready -UseBasicParsing
docker compose unpause clickhouse
docker compose restart de-kafka-raw-v1
docker compose stop de-kafka-raw-v1
docker compose start de-kafka-raw-v1
~~~

The evidence must show that the pause produces HTTP 503 or a degraded response, no unsafe group-offset advance, retry/reconciliation counters, and successful recovery. For ledger failure, run the consumer with a temporary read-only or unavailable V1 ledger path in an isolated Compose override; never alter the live ledger. For commit failure, use the consumer test double that returns a per-partition commit error; do not delete topics or volumes. Shutdown evidence must show buffered rows either confirmed before exit or present as unresolved ledger rows with no false commit.

These are expected results, not observed results. Do not fabricate PASS.

## 13. Definition of Done

Every criterion needs a command/test, evidence, expected result and PASS/FAIL decision. Missing mandatory runtime evidence is FAIL/BLOCKED.

1. Fresh V1 records reach the correct topic.
2. V1 Raw uses the isolated V1 group.
3. Shared V1 validation is correct.
4. Valid events are durable.
5. Invalid events are durable Quarantine.
6. Original bytes and lineage are preserved.
7. Zero/unavailable/null semantics are unchanged.
8. Physical delivery identity is unique in the ledger.
9. Separate deliveries of one semantic observation remain for Bronze.
10. Offsets never advance before persistence.
11. Restart/recovery loses no logical delivery.
12. Ambiguous persistence reconciles safely.
13. Replay is isolated.
14. Health/readiness reflects actual state.
15. Clean and V2-upgrade migrations succeed.
16. V2 assets remain preserved/readable.
17. Raw runs without Projector or Orion.
18. Real SUMO-to-ClickHouse acceptance passes.
19. Phase 2 handover is documented.

## 14. Phase 2 Handover Contract

Bronze reads the v1 logical-read interface in Section 7.5 over smart_traffic.v1_raw_events and smart_traffic.v1_quarantine_events as append-only physical evidence by topic, partition, offset and raw_ingestion_id. value_bytes_base64 is authoritative and is decoded before contract validation. Required fields include eventId, payloadHash, payload_bytes_hash, immutable_envelope_hash, run/intersection/cycle, topology digest, Kafka lineage, timestamps, validator version and migration version. No semantic_conflict_candidate field is produced by Raw; Bronze compares event IDs and hashes across the resolved physical deliveries.

Bronze discovers deliveries by partition/offset progression and checks Quarantine/gaps before declaring absence. Terminal Raw means physical persistence, not semantic acceptance. Bronze owns semantic deduplication, conflict policy, run/catalog/participant validation and the semantic ledger. Silver and Gold consume Bronze disposition and do not invent another duplicate policy.

Replay uses replay tables, replay_run_id and replay ledger paths only. Raw promises durable physical evidence and safe offsets, not cross-system exactly-once.

## 15. Implementation Readiness Verdict

READY AFTER EXPLICIT APPROVAL.

Prerequisites:
- synchronize to approved origin/final without discarding user work;
- approve dedicated V1 service/group/ledger/table isolation;
- approve the Phase 2 planning-gate policy for conflicting immutable content under an already-consumed eventId; this does not block Phase 1 preservation.

Deferred later phases: Bronze conflict ledger; Silver facts/topology/configuration provenance; Gold metric-definition/API/Dashboard compatibility; final canonical deployment/cutover and long-run multi-window acceptance.

Recommended next action: synchronize/create the implementation branch, re-run the audit, approve isolation, then implement P1-01 through P1-07 in order. Do not start application coding in this planning session.

Final self-review: the plan identifies files, preserves V2, uses the shared validator, separates physical and semantic identity, specifies schema/commands, defines recovery, keeps Raw independent from Projector/Orion, and provides objective tests and acceptance gates. The only unresolved semantic decision is explicitly assigned to Phase 2.

