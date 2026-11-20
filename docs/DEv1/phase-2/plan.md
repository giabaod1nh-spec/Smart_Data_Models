# Phase 2 Implementation Plan — V1 Bronze Materialization

**Plan status:** READY AFTER EXPLICIT APPROVAL

**Planning boundary:** This document is an implementation plan only. It does not implement Bronze, Silver, Gold, Spring, Dashboard, or any application source change.

**Audited repository:** `https://github.com/giabaod1nh-spec/Smart_Data_Models`

**Audited branch and commit:** `codex/phase-1-v1-raw` at `757060f8175fdb22d9ee27e7f8d175c82c0e2c45` (`feat(de): complete phase 1 V1 raw ingestion pipeline`).

**Phase 1 parent:** `803c6e421ec8cd00f14ea394fd338fef785b9157`.

**Recommended implementation branch:** `codex/phase-2-v1-bronze`, created from the audited commit only after this plan is approved. Do not switch, reset, merge, pull, or discard the current working tree automatically.

## 1. Executive Summary and Phase Objective

Phase 1 is complete and provides an immutable, byte-preserving Raw boundary for `traffic.observations.v1`. The current Bronze code is the older V2 entity/run pipeline. It reads V2 Raw tables directly, validates `TrafficEntityObserved` and `TrafficSimulationRunStarted`, uses integer-contiguous offsets, treats `raw_ingestion_id` existence as persistence proof, and records an upstream duplicate flag without owning semantic uniqueness.

Phase 2 converts that existing Bronze layer in place into the canonical V1 materialization stage:

```text
Raw V1 logical_read([start,end))
  -> consistent safe-boundary source batch
  -> strict byte/hash/contract verification
  -> one authoritative eventId state machine
  -> one valid logical observation or durable disposition
  -> content-aware ClickHouse reconciliation
  -> CAS checkpoint
  -> Silver handover change stream and conflict invalidation work
```

The target grain is one accepted logical observation per valid V1 `eventId`. Every physical Raw delivery remains represented by a Bronze delivery disposition and lineage row. An identical semantic duplicate never creates a second valid observation. A changed immutable envelope or payload transitions the identity monotonically to `CONFLICTED`; the identity is immediately excluded from the canonical valid-observation interface and a durable invalidation work item is created. Later identical retries do not restore validity.

Phase 2 includes Raw logical-read integration, strict V1 observation handling, semantic identity, duplicate/conflict state, delivery lineage, invalidation source, Bronze ClickHouse storage, checkpoint/recovery, replay, migration, runtime integration, observability, and actual Raw-to-Bronze acceptance. It excludes Raw redesign, Silver facts/dimensions, Gold, Spring, Dashboard, Projector redesign, and destructive V2 cleanup.

## 2. Repository Baseline and Phase 1 Readiness

### 2.1 Verified baseline

The audit found:

| Item | Audited result |
|---|---|
| Branch | `codex/phase-1-v1-raw` |
| HEAD | `757060f8175fdb22d9ee27e7f8d175c82c0e2c45` |
| Working tree | Clean at audit time; preserve any new user changes before implementation |
| Remote | `origin` points to the GitHub repository named above |
| Phase 1 status | `PHASE 1 COMPLETE — READY FOR PHASE 2 PLANNING` |
| Raw topic | `traffic.observations.v1` |
| Raw group | `de-kafka-raw-v1` |
| Raw ledger | `de/artifacts/kafka_raw_v1/ledger.sqlite3` |
| Raw migration | `de/migrations/006_create_kafka_raw_v1.sql` |
| Raw health | port `8097` in the main profile; isolated acceptance uses `18097` |
| Certified historical evidence | `phase1-isolated-long01`, 1200 simulation seconds, 6000 valid observations and 12 quarantined probe deliveries, if artifacts still exist |

The exact historical acceptance evidence is documented, not automatically reproduced by this planning audit. The implementation agent must verify whether the Raw database directory, WAL/SHM files, matching ledger snapshot, and acceptance artifacts are still available. If not, Phase 2 must create a new uniquely identified run; it must not reconstruct a source ledger from ClickHouse rows.

### 2.2 Required baseline checks before implementation

Run and record, without changing source:

```powershell
git status --short --branch
git rev-parse HEAD
git show --stat --oneline 757060f8175fdb22d9ee27e7f8d175c82c0e2c45
git diff 757060f8175fdb22d9ee27e7f8d175c82c0e2c45 -- de/kafka_raw de/migrations/006_create_kafka_raw_v1.sql
python -m pytest contracts/tests de/tests/kafka_raw de/tests/integration/test_v1_final_handover.py -q
```

Before starting Bronze, capture Raw schema, topic identity, ledger identity, per-partition safe boundaries, Raw counts, and a read-only SQLite backup made with SQLite backup APIs while the Raw directory remains available. The backup is evidence only; it is not a substitute for directory-level WAL access during live processing.

### 2.3 Confirmed blockers and approval items

No Phase 1 blocker was found. Phase 2 requires explicit approval of the following planning-gate choices before coding:

1. The proposed SQLite semantic authority and ClickHouse analytical mirror are accepted as the single-authority design.
2. The downstream invalidation contract is accepted: Bronze emits durable invalidation work; Silver later applies tombstone-or-rebuild semantics; Gold suppresses or recomputes affected materialization under its Phase 4 revision policy.
3. A narrowly scoped Raw logical-read correctness amendment is permitted if the implementation test confirms that the current status comparison can accept same stored hash with different persisted content. This is a Raw interface correction, not a Raw redesign, and must carry Phase 1 regression tests.
4. Exact Phase 2 table names below are accepted, or changed before implementation without changing the invariants.

Detailed Gold, Dashboard, and Silver physical design remains deferred to their own phase gates. It must not block Bronze's ability to ingest Raw observations.

## 3. Master Plan Alignment and Source Migration Strategy

The Master Plan remains authoritative. This plan applies its Phase 2 scope, incremental runtime verification rule, five-phase numbering, V2 preservation, and Bronze ownership of semantic identity.

The only localized Master Plan amendment made in this planning session is the Phase 2 **Approved source migration strategy** paragraph: compatible Bronze code is reused and refactored in place; obsolete V2 behavior is replaced; new files require responsibility-based justification; V2 source/history is preserved through Git/separate storage. No existing section was deleted, shortened, or reorganized.

The implementation must not create parallel copies named `v1_main.py`, `v1_config.py`, `v1_models.py`, `v1_processor.py`, `v1_transformer.py`, `v1_validator.py`, `v1_checkpoint_store.py`, `v1_clickhouse_repository.py`, or `v1_replay.py` under `de/bronze/`. The existing Phase 1 `de/kafka_raw/v1_*` modules remain unchanged unless the narrowly scoped Raw interface correction is approved.

Migration classes are:

| Existing area | Phase 2 treatment |
|---|---|
| Existing Bronze processor/config/repository/checkpoint/health/replay entry points | REFACTOR IN PLACE |
| V2 entity/run validation and transformation branches | REPLACE OBSOLETE LOGIC |
| Canonical hash helpers that only serve V2 fields | REFACTOR IN PLACE or retire after acceptance |
| Instance lock and metrics patterns | KEEP AS-IS where semantics remain valid; extend for V1 |
| New semantic identity/invalidation state responsibility | ADD JUSTIFIED FUNCTIONALITY; may be owned by the extended checkpoint store, avoiding a second authority |
| Historical V2 SQL and data | KEEP AS-IS; no destructive migration |

No V2 profile will be described as executable after its entry point is migrated unless the actual entry point remains runnable and is tested. Historical V2 source is recoverable from Git at the audited parent/earlier commits; V2 tables and artifacts remain separate.

## 4. Actual Phase 1 Raw Interface Audit

### 4.1 Actual tables and transport contract

Migration `006_create_kafka_raw_v1.sql` creates:

- `smart_traffic.v1_raw_events`
- `smart_traffic.v1_quarantine_events`
- `smart_traffic.v1_raw_events_replay`
- `smart_traffic.v1_quarantine_events_replay`

All are `MergeTree`, partitioned by `toYYYYMM(consumed_at)`. Live tables sort by `(topic, partition, offset, raw_ingestion_id)`; replay tables prepend `replay_run_id` to the sort key. Hashes `raw_ingestion_id`, `physical_record_hash`, and `payload_bytes_hash` are `FixedString(64)`. The actual DDL is authoritative; do not copy an earlier proposed Raw schema.

`V1Record.raw_ingestion_id` is SHA-256 of `topic|partition|offset`. `physical_record_hash` is SHA-256 of canonical JSON containing topic, partition, offset, topic identity, key/value null flags and Base64 values, ordered headers, and broker timestamp/type; it excludes `consumed_at`. `payload_bytes_hash` is SHA-256 of the original value bytes (empty bytes for null value, while the null flag remains separate). `value_bytes_base64`, `key_bytes_base64`, and `headers_json` preserve the original transport representation. `payload_text` is only the strict UTF-8 text representation, never a source for reconstructing bytes.

For valid observations, `envelope_json` contains the twelve fields in `v1_validator.ENVELOPE_FIELDS`: `eventId`, `eventType`, `schemaVersion`, `simulationRunId`, `intersectionId`, `scenarioId`, `cycleSequence`, `simulationStep`, `simulationTimeSeconds`, `capturedAt`, `topologyDigest`, and `payloadHash`. `immutable_envelope_hash` is SHA-256 of shared canonical JSON over those fields. Raw validation uses strict UTF-8, strict JSON, `IntersectionObservation`, schema version 1, the shared `validate_observation`, and the Kafka key from `observation_key`.

### 4.2 Physical ledger and statuses

`V1LedgerStore` owns the physical Kafka delivery ledger in `de/artifacts/kafka_raw_v1/ledger.sqlite3` with WAL, `synchronous=FULL`, and `busy_timeout=5000`:

- `deliveries` primary key `(topic, partition_id, offset_value)` and unique `raw_ingestion_id`;
- `partition_progress` with topic identity, broker committed-next and Raw safe-next offsets;
- `batches` for receipt/attempt state;
- `integrity_faults` for fail-closed faults.

The ledger states `RECEIVED`, `PERSISTENCE_ATTEMPTED`, `PERSISTENCE_CONFIRMED`, `COMMIT_ELIGIBLE`, `COMMITTED`, `FAILED_RETRYABLE`, and `FAILED_INTEGRITY`. A terminal confirmation requires `confirmed_hash = physical_record_hash`. `V1ClickHouseRepository.logical_read(ledger, topic, partition, start_offset, end_offset)` reads the ledger and Raw tables, collapses identical physical retries, and returns `identity`, `status`, `safe_next_offset`, `physical_rows`, and a record body only for confirmed results. Its interval is `[start_offset,end_offset)`.

Logical statuses are `CONFIRMED_RAW`, `CONFIRMED_QUARANTINE`, `UNRESOLVED`, and `CONFLICT`. An unresolved ledger delivery or physical evidence conflict is not a valid body. Bronze must not classify the event or advance past it until the source status is safely resolved.

### 4.3 Safe-boundary audit and required reader correction

The current `logical_read` returns each selected ledger delivery and repeats the current partition `safe_next_offset`; it does not itself filter every row to that boundary. It also determines a successful pair using `(physical_record_hash, destination)`, while the evidence query returns full rows. Therefore the Bronze reader must:

1. Open a read-only `V1LedgerReader` connection over the Raw SQLite directory, with `query_only=ON`.
2. Begin a consistent SQLite read transaction or use a coordinated backup/snapshot for the entire batch.
3. Capture `safe_next_offset` and topic identity before selecting the range.
4. Select only `[checkpoint_next, min(requested_end, safe_next_offset))`.
5. Require the returned rows to be ordered by delivered offset and re-check the same safe boundary before checkpointing.
6. Reject `UNRESOLVED` and `CONFLICT`; do not treat an absent row as a valid Kafka gap.
7. Require content-aware physical evidence verification, not only stored hash equality, before accepting a Raw body.

If implementation tests reproduce a same-hash/different-content acceptance path, add a narrowly scoped Raw method such as `logical_read_consistent(...)` or a private evidence comparator that compares all immutable stored transport fields and destination. Preserve all existing `logical_read` callers and add Phase 1 regression coverage for same-hash content mismatch, safe-boundary exclusion, and concurrent Raw writes. Do not add eventId comparison to Raw.

### 4.4 Raw result handling contract

| Raw result | Bronze action | Checkpoint effect |
|---|---|---|
| `CONFIRMED_RAW` | Decode Base64, verify source byte hash and transport fields, then validate V1 | May complete after Bronze outputs are durable |
| `CONFIRMED_QUARANTINE` | Record `RAW_QUARANTINE_SKIPPED` delivery disposition with lineage | May complete; it is a durable terminal source disposition |
| `UNRESOLVED` | Retry/reconcile; no semantic decision | Blocks that partition beyond the delivery |
| `CONFLICT` | Raise source integrity fault and persist fault locally | Blocks that partition until operator resolution |
| No selected delivery inside a valid safe boundary | Treat as an allowed Kafka offset gap only after Raw ledger/progress proves no missing delivered history | May advance using delivered-order cursor, never by assuming numeric contiguity |
| Safe boundary absent/changed/topic identity changed | Fail closed | No advancement |

Raw retention loss, missing committed history, invalid ledger identity, and physical evidence mismatch are faults, not valid gaps. Live and replay Raw tables/ledgers are explicit source modes and must never be mixed.

### 4.5 Bronze integrity boundary

`CONFIRMED_QUARANTINE` is the only Raw-level invalid input that Bronze may terminally skip: its original Raw lineage is recorded as `RAW_QUARANTINE_SKIPPED`, the disposition/intent mirrors are reconciled, and then the source cursor may advance. A confirmed Raw row with invalid Base64, a changed byte hash, a changed physical transport hash, unexpected null flags, or inconsistent immutable envelope is `FAILED_INTEGRITY`, not `BRONZE_QUARANTINED`; the partition remains fail-closed. A Raw-valid row rejected by the shared V1 validator is also a source/validator compatibility fault until source contract version, validator version, bytes and hashes are independently verified. Phase 2 introduces no additional business rejection rule by default. Only a separately approved Bronze-only rule may produce `BRONZE_QUARANTINED`, and its rule/version must be persisted in the intent and quarantine row.

## 5. Existing Bronze Source Audit

| Existing file | Existing functions/classes | Current responsibility | V2 assumption/gap | Required V1 behavior | Classification and strategy | Test impact |
|---|---|---|---|---|---|---|
| `de/bronze/config.py` | `BronzeSettings`, `get_settings` | `K7_` settings for V2 topic, partitions, V2 schemas, 003 migration | V2 defaults and shared checkpoint path | V1 topic, read-only Raw path, V1 tables, separate state, source mode, readiness settings | REFACTOR IN PLACE; preserve field compatibility only where harmless | Config allowlist, namespace-guard, compose tests |
| `de/bronze/models.py` | `RawRow`, `ResolvedRecord`, `PendingLedgerEntry`, `TransformResult` | V2 rows and ENTITY/RUN result types | Wrong grain and incomplete lineage | V1 observation, delivery disposition, conflict/invalidation result types | REFACTOR IN PLACE; replace obsolete result types | Model round-trip and null/zero tests |
| `de/bronze/payload_codec.py` | `decode_payload` | Decodes stored V2 text | Uses unsafe replacement/does not verify original bytes | Strict Base64, byte hash, UTF-8, JSON and canonical payload checks | REPLACE OBSOLETE LOGIC in place | Non-UTF8, malformed JSON, byte mismatch |
| `de/bronze/validator.py` | `BronzeValidator`, `ValidationOutcome` | V2 entity/run JSON schema validation | ENTITY/RUN and contract 2.0.0 | Reuse shared V1 validator/contract; no synthetic lifecycle | REFACTOR IN PLACE; remove V2 branches | Shared-contract, lifecycle quarantine |
| `de/bronze/transformer.py` | `BronzeTransformer` | V2 entity/run/quarantine rows | Flattens V2 semantics and duplicate flag | Materialize complete V1 payload plus typed metadata; preserve availability | REPLACE OBSOLETE LOGIC in place | Nested payload and hash parity |
| `de/bronze/canonical_hash.py` | entity/run/quarantine hashes | V2 canonical IDs/hashes | Hash does not represent V1 envelope/content | Add explicit Bronze canonical hash distinct from source immutable hash | REFACTOR IN PLACE | Hash vectors and changed metadata |
| `de/bronze/lineage_resolver.py` | `resolve`, `resolve_batch`, gap timers | Reads V2 Raw rows and waits on integer offsets | Direct table reads; contiguous assumptions | Adapter over Raw logical-read safe bounds and delivered-order gaps | REFACTOR IN PLACE | Sparse offsets, unresolved/conflict, safe boundary |
| `de/bronze/offset_tracker.py` | completion/prefix methods | Numeric contiguous prefix | Valid Kafka gaps stall | Delivered-record cursor plus explicit safe-boundary cursor | REPLACE OBSOLETE LOGIC in place | Gap and checkpoint property tests |
| `de/bronze/checkpoint_store.py` | SQLite schema and `commit_batch`, `advance`, `mark_complete` | V2 checkpoint/ledger | No semantic authority, no CAS, update can regress | Own V1 checkpoint, physical disposition, semantic state, invalidation work with CAS | REFACTOR IN PLACE; one authority | Crash/CAS/concurrency matrix |
| `de/bronze/clickhouse_repository.py` | Raw reads, V2 inserts, ID lookup/reconcile | V2 table access and existing-ID-only proof | Wrong tables and insufficient reconciliation | V1 repository methods for content/destination/state/lineage/invalidation | REFACTOR IN PLACE; no parallel full repository | Lost response, mismatched content, schema verification |
| `de/bronze/processor.py` | `BronzeProcessor`, loop, `_process_batch` | V2 poll/transform/write/checkpoint | Reads max offset, ENTITY/RUN, numeric prefix, checkpoint before semantic authority | Safe Raw batch, state machine, durable outputs, CAS checkpoint | REFACTOR IN PLACE | End-to-end restart/failure tests |
| `de/bronze/replay.py` | backfill/parity/rebuild CLI | V2 manifest/replay tables and live-ish processor | V2 source and possible live mutation | Explicit Raw live/replay source mode and Bronze replay namespace | REFACTOR IN PLACE | Replay isolation/parity/context tests |
| `de/bronze/health_api.py` | health/ready/metrics routes | V2 processor health | Does not expose source-boundary/conflict backlog | V1 dependency, cursor, invalidation backlog, faulted/readiness state | REFACTOR IN PLACE | Idle, outage, stale and conflict readiness |
| `de/bronze/metrics.py` | counters/snapshot | V2 batch metrics | Missing dispositions/backlog | Add accepted/duplicate/conflict/quarantine/invalidation/CAS metrics | REFACTOR IN PLACE | Snapshot schema and runtime evidence |
| `de/bronze/instance_lock.py` | OS/SQLite ownership lock | Single V2 owner | Must bind V1 checkpoint and group/topic namespace | KEEP AS-IS, extend namespace identity check | Lock contention and stale-owner tests |
| `de/bronze/main.py` | service startup/health thread | V2 entry point | Legacy V2 entry point is not V1-safe | Migrate entry point to V1 only after accepted refactor; retain historical Git reference | REFACTOR IN PLACE; no claim of old V2 executability | Compose and startup verification |
| `de/migrations/003_create_bronze_v2.sql` | V2 schema | Historical V2 tables | Must not change | Keep immutable | KEEP AS-IS | Upgrade inventory/sentinel |

The current Silver code is inspected only to define the handover. No Silver source is changed in Phase 2. Current Gold/Spring functions are inspected only for later continuity; no Gold/Spring implementation is introduced.

## 6. Target Bronze V1 Architecture

### 6.1 Runtime flow

```text
V1LedgerReader + V1ClickHouseRepository.logical_read_consistent
  -> RawSourceReader (partition discovery, snapshot, safe range)
  -> strict payload codec + shared traffic_observation validator
  -> semantic identity authority in V1 checkpoint SQLite
  -> ClickHouse observation / delivery / conflict / quarantine rows
  -> content-aware reconciliation
  -> durable invalidation work for late conflicts
  -> CAS physical checkpoint
  -> change cursor for future Silver
```

Bronze reads Raw ledger SQLite read-only and never calls Raw `transition`, `receive_batch`, or schema initialization. Bronze does not consume Kafka directly. Only Raw owns Kafka commits and physical delivery resolution.

### 6.2 Authority rules

SQLite is authoritative for semantic identity state, physical delivery disposition, checkpoint, and invalidation work because it supports atomic state transitions and CAS. ClickHouse is the durable analytical mirror/output and audit surface. A mirror mismatch is an integrity fault; a MergeTree sort key or eventual merge is never treated as uniqueness.

The authoritative valid-observation reader joins the latest SQLite semantic state to content-addressed Bronze observation rows, or queries a repository method that applies the equivalent state rule. It returns only state `ACCEPTED` and the selected accepted revision. A `CONFLICTED` identity is excluded immediately even if an older ClickHouse observation row remains physically present.

### 6.3 Source reader pseudocode

```text
open Raw V1LedgerReader over the persistent ledger directory, query_only=ON
verify Raw topic, topic_identity, ledger schema and source mode
discover partitions from Raw partition_progress plus confirmed Raw evidence
for each partition:
    cp_next = checkpoint.next_source_offset(partition)
    begin one read transaction/snapshot on the Raw ledger
    safe = Raw progress.safe_next_offset
    end = min(cp_next + batch_size_source, requested_end, safe)
    if end <= cp_next:
        record EMPTY_SAFE_RANGE and do not advance
    rows = logical_read_consistent([cp_next,end))
    verify every returned delivery is within [cp_next,end)
    verify Raw topic identity and safe boundary did not regress
    reject UNRESOLVED/CONFLICT or missing ledger history
    yield rows in delivered offset order
```

A valid numeric Kafka gap is represented by the absence of a delivered ledger row within a verified safe boundary; it is not filled with synthetic observations and does not make Bronze wait forever. If the source cannot prove that the gap is valid, processing stops.

## 7. Locked Technical Decisions

| Decision | Locked design | Evidence/rationale |
|---|---|---|
| Source | `V1ClickHouseRepository.logical_read` through a read-only adapter; no Kafka duplicate consumer | Phase 1 handover and Raw ledger ownership |
| Safe boundary | Capture Raw `safe_next_offset` for every batch and process only below it | Current logical_read does not automatically filter rows |
| Semantic key | `eventId` validated by shared V1 contract | Master Plan Decision 2 |
| Duplicate | Same eventId, same immutable envelope and verified payload -> `SEMANTIC_DUPLICATE`; retain delivery lineage, no second observation | Required invariant |
| Conflict | Any changed payload hash or immutable envelope field -> monotonic `CONFLICTED`; no valid output | Master Plan semantic-conflict invariant |
| Conflict after acceptance | Durable state transition plus invalidation work in same SQLite transaction; valid reader suppresses immediately | Prevents stale analytical validity |
| Ledger authority | One V1 Bronze SQLite authority; ClickHouse mirrors are reconciled | Avoids two semantic stores |
| Source byte handling | Base64 decode, hash compare, strict UTF-8/JSON; never serialize parsed JSON to recreate bytes | Raw handover |
| Observation content | Store complete canonical V1 payload JSON and typed envelope metadata; no V2 NGSI-LD recreation | Phase 2 boundary |
| Checkpoint | Exclusive `next_source_offset` plus delivered-row dispositions; CAS monotonic advancement | Valid gaps and restart safety |
| Replay | Explicit `live` or isolated `raw_replay`; a separate SQLite database per replay run and shared ClickHouse V1 tables guarded by mandatory `namespace='replay:<run_id>'` | Phase 1 replay isolation; avoids SQLite live-state contamination without duplicating CH schema |
| Downstream | Bronze emits invalidation source; Silver later applies tombstone/rebuild; Gold later suppresses/recomputes affected windows | Later phase ownership |
| Physical content mismatch | Fail closed and preserve integrity fault; do not reinterpret as semantic conflict | Raw physical vs semantic distinction |
| Semantic history | Append-only `bronze_v1_semantic_changes` is the durable downstream cursor; `bronze_v1_semantic_identity` is current state only | Current-state table cannot reconstruct prior ACCEPTED changes |
| Persistence state | `processing_status` is separate from semantic `disposition`; only `COMPLETED` intents are checkpoint-eligible | A semantic decision does not prove ClickHouse persistence |

## 8. Bronze Data Contract and Storage Design

### 8.1 Logical output contract

`bronze_observations_v1` has one logical accepted row per `(namespace,event_id,semantic_revision)`; the valid reader exposes exactly one current accepted row per event ID in the live namespace. The row preserves all V1 top-level fields, complete nested payload, source immutable envelope hash, Bronze canonical content hash, source byte hash, Raw transport identity, and processor versions.

Normalization may materialize typed columns and canonical JSON, but must not change the canonical observation payload. Measured zero, null, `UNAVAILABLE`, `NOT_APPLICABLE`, absent optional metrics, direction keys, scenario changes, out-of-order cycles, and historical runs remain exactly distinguishable. No timestamp is invented and no unavailable value becomes zero.

`immutable_envelope_hash` is the source hash over the twelve Raw envelope fields. `bronze_canonical_hash` is SHA-256 of a documented canonical JSON object containing the validated full V1 observation plus the selected normalization/schema version and is not substituted for the source hash. If typed metadata changes without payload change, the Bronze hash/version changes; the source immutable hash does not.

### 8.2 Proposed migration 007 DDL

Migration `de/migrations/007_create_bronze_v1.sql` is additive and executable. It must not edit migrations 001–006. The following is the target DDL; implementation must verify exact ClickHouse types with the installed client and `DESCRIBE`:

```sql
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

```

Replay uses the same five V1 tables with a mandatory `namespace = 'replay:<run_id>'` value rather than redundant copies. This is deliberate: namespace is already part of every logical identity and primary sorting contract, and one set of schemas prevents live/replay drift. The repository must reject a replay write with `namespace='live'`, and all replay queries must require the exact namespace. If deployment isolation later requires physically separate replay tables, that is an explicit approval item and the implementation must provide full column-for-column DDL plus a separate namespace guard; it must not use an unreviewed `CREATE TABLE ... AS` shorthand.

### 8.3 Views and logical readers

Do not use `ReplacingMergeTree` for correctness. Implement repository methods backed by SQLite authority and explicit ClickHouse joins/reconciliation:

```python
read_valid_observations(
    namespace: str,
    event_ids: list[str] | None = None,
    simulation_run_id: str | None = None,
    after_key: tuple[str, int] | None = None,
    limit: int = 500,
) -> BronzeObservationPage

read_semantic_changes(
    namespace: str,
    after: tuple[int, str] | None,
    limit: int,
    high_water: int | None = None,
) -> SemanticChangePage  # rows ordered (change_sequence, event_id)

read_pending_invalidations(
    namespace: str,
    after: tuple[int, str] | None,
    limit: int,
    high_water: int | None = None,
) -> InvalidationPage  # rows ordered (invalidation_sequence, invalidation_id)

read_current_semantic_state(namespace: str, event_id: str) -> SemanticIdentityState
read_delivery_lineage(namespace: str, event_id: str) -> list[DeliveryLineage]
```

`SemanticChangePage` and `InvalidationPage` include `rows`, `next_cursor`, `high_water`, and `has_more`. A reader captures `high_water` in the same SQLite read transaction before selecting `sequence > after AND sequence <= high_water`; it never reads beyond that page boundary. The cursor is exclusive, stable across equal timestamps, and advances only to the last returned `(sequence,id)`. A restart repeats the last unacknowledged page safely; deterministic IDs make repeats idempotent. The journal is append-only and retained for at least the maximum supported Silver replay/backfill horizon plus the documented operational recovery period; pruning requires a consumer-watermark check and is not part of initial Phase 2.

The valid reader must exclude `CONFLICTED`, `QUARANTINED`, `PENDING`, and missing-mirror states. Each bounded page opens a fresh SQLite read transaction, selects current ACCEPTED identities ordered by `(event_id, semantic_revision)` after the exclusive keyset cursor, and holds that snapshot while ClickHouse observation rows are fetched and fully checked against the frozen canonical hash/content. Identical physical retries collapse; conflicting rows and missing mirrors raise an integrity fault. The page is a current-state snapshot, not historical as-of time travel; concurrent conflicts after a page returns are discovered from the durable change stream. An unavailable SQLite authority never falls back to ClickHouse-only validity. Replay readers require an exact `replay:<run_id>` namespace and cannot query live rows.

## 9. Semantic Identity and Conflict State Machine

### 9.1 SQLite schema

Extend `de/bronze/checkpoint_store.py` in place with a V1 schema version and namespace-aware tables. Live processing uses `de/artifacts/bronze_v1/checkpoint.sqlite3`; every replay run uses a separate database at `de/artifacts/bronze_v1/replay/<run_id>/checkpoint.sqlite3`. Every primary key includes namespace even in a replay database:

```sql
CREATE TABLE IF NOT EXISTS bronze_v1_checkpoints (
  namespace TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
  next_source_offset INTEGER NOT NULL, source_start_offset INTEGER NOT NULL,
  raw_topic_identity TEXT NOT NULL, source_fingerprint TEXT NOT NULL,
  raw_safe_next_offset INTEGER NOT NULL,
  generation INTEGER NOT NULL DEFAULT 0, processor_version TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(namespace, topic, partition_id),
  CHECK(next_source_offset >= source_start_offset),
  CHECK(raw_safe_next_offset >= next_source_offset)
);
CREATE TABLE IF NOT EXISTS bronze_v1_delivery_ledger (
  namespace TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
  offset_value INTEGER NOT NULL, raw_ingestion_id TEXT NOT NULL,
  disposition TEXT NOT NULL, event_id TEXT, source_byte_hash TEXT,
  physical_record_hash TEXT, semantic_revision INTEGER, bronze_hash TEXT,
  processing_intent_id TEXT NOT NULL, processing_status TEXT NOT NULL,
  error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY(namespace, topic, partition_id, offset_value),
  UNIQUE(namespace, raw_ingestion_id),
  CHECK(processing_status IN ('PREPARED','PENDING_PERSISTENCE','PERSISTENCE_CONFIRMED','COMPLETED','FAILED_RETRYABLE','FAILED_INTEGRITY'))
);
CREATE TABLE IF NOT EXISTS bronze_v1_processing_intents (
  namespace TEXT NOT NULL, processing_intent_id TEXT NOT NULL,
  topic TEXT NOT NULL, topic_identity TEXT NOT NULL, partition_id INTEGER NOT NULL,
  offset_value INTEGER NOT NULL, raw_ingestion_id TEXT NOT NULL,
  event_id TEXT, semantic_disposition TEXT NOT NULL,
  semantic_revision INTEGER, processing_status TEXT NOT NULL,
  expected_observation_ref TEXT, expected_destination TEXT NOT NULL,
  expected_output_json TEXT NOT NULL, expected_observation_hash TEXT,
  expected_conflict_id TEXT, expected_invalidation_id TEXT,
  expected_lineage_hash TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT, prepared_at TEXT NOT NULL, completed_at TEXT,
  processor_name TEXT NOT NULL, processor_version TEXT NOT NULL,
  bronze_schema_version TEXT NOT NULL,
  PRIMARY KEY(namespace, processing_intent_id),
  UNIQUE(namespace, raw_ingestion_id),
  CHECK(processing_status IN ('PREPARED','PENDING_PERSISTENCE','PERSISTENCE_CONFIRMED','COMPLETED','FAILED_RETRYABLE','FAILED_INTEGRITY')),
  CHECK(semantic_disposition IN ('ACCEPTED_NEW','SEMANTIC_DUPLICATE','SEMANTIC_CONFLICT','ALREADY_CONFLICTED','RAW_QUARANTINE_SKIPPED','BRONZE_QUARANTINED'))
);
CREATE TABLE IF NOT EXISTS bronze_v1_semantic_identity (
  namespace TEXT NOT NULL, event_id TEXT NOT NULL,
  state TEXT NOT NULL, semantic_revision INTEGER NOT NULL,
  immutable_envelope_hash TEXT NOT NULL, payload_hash TEXT NOT NULL,
  observation_bronze_hash TEXT, primary_raw_ingestion_id TEXT,
  simulation_run_id TEXT NOT NULL, intersection_id TEXT NOT NULL,
  cycle_sequence INTEGER NOT NULL, simulation_time_seconds REAL NOT NULL,
  change_sequence INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY(namespace, event_id), UNIQUE(namespace, change_sequence),
  CHECK(state IN ('PENDING','ACCEPTED','CONFLICTED','QUARANTINED'))
);
CREATE TABLE IF NOT EXISTS bronze_v1_semantic_changes (
  namespace TEXT NOT NULL, change_sequence INTEGER NOT NULL,
  event_id TEXT NOT NULL, semantic_revision INTEGER NOT NULL,
  change_type TEXT NOT NULL, previous_state TEXT, new_state TEXT NOT NULL,
  observation_content_ref TEXT, observation_bronze_hash TEXT,
  invalidation_id TEXT, processing_intent_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(namespace, change_sequence),
  UNIQUE(namespace, event_id, semantic_revision),
  CHECK(change_type IN ('ACCEPTED','CONFLICTED','QUARANTINED'))
);
CREATE INDEX IF NOT EXISTS bronze_v1_changes_cursor
  ON bronze_v1_semantic_changes(namespace, change_sequence, event_id);
CREATE TABLE IF NOT EXISTS bronze_v1_conflicts (
  namespace TEXT NOT NULL, conflict_id TEXT NOT NULL, event_id TEXT NOT NULL,
  previous_revision INTEGER NOT NULL, new_revision INTEGER NOT NULL,
  previous_immutable_envelope_hash TEXT NOT NULL,
  conflicting_immutable_envelope_hash TEXT NOT NULL,
  previous_payload_hash TEXT NOT NULL, conflicting_payload_hash TEXT NOT NULL,
  previous_raw_ingestion_id TEXT NOT NULL, conflicting_raw_ingestion_id TEXT NOT NULL,
  created_at TEXT NOT NULL, UNIQUE(namespace, conflict_id),
  PRIMARY KEY(namespace, conflict_id)
);
CREATE TABLE IF NOT EXISTS bronze_v1_invalidations (
  namespace TEXT NOT NULL, invalidation_id TEXT NOT NULL, event_id TEXT NOT NULL,
  simulation_run_id TEXT NOT NULL, intersection_id TEXT NOT NULL,
  cycle_sequence INTEGER NOT NULL, simulation_time_seconds REAL NOT NULL,
  previous_state TEXT NOT NULL, new_state TEXT NOT NULL,
  previous_immutable_envelope_hash TEXT NOT NULL,
  conflicting_immutable_envelope_hash TEXT NOT NULL,
  original_raw_ingestion_id TEXT NOT NULL, conflicting_raw_ingestion_id TEXT NOT NULL,
  semantic_revision INTEGER NOT NULL, invalidation_sequence INTEGER NOT NULL,
  reconciliation_status TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY(namespace, invalidation_id),
  UNIQUE(namespace, event_id, semantic_revision),
  UNIQUE(namespace, invalidation_sequence),
  CHECK(reconciliation_status IN ('PENDING','MIRRORED','RECONCILE_FAILED'))
);
CREATE TABLE IF NOT EXISTS bronze_v1_integrity_faults (
  namespace TEXT NOT NULL, fault_id TEXT NOT NULL, topic TEXT, partition_id INTEGER,
  offset_value INTEGER, fault_type TEXT NOT NULL, detail TEXT NOT NULL,
  resolved_at TEXT, created_at TEXT NOT NULL, PRIMARY KEY(namespace, fault_id)
);
CREATE INDEX IF NOT EXISTS bronze_v1_delivery_event ON bronze_v1_delivery_ledger(namespace,event_id);
CREATE INDEX IF NOT EXISTS bronze_v1_invalidations_status ON bronze_v1_invalidations(namespace,reconciliation_status,created_at);
CREATE INDEX IF NOT EXISTS bronze_v1_changes_event ON bronze_v1_semantic_changes(namespace,event_id,semantic_revision);
CREATE INDEX IF NOT EXISTS bronze_v1_intents_status ON bronze_v1_processing_intents(namespace,processing_status,prepared_at);
```

`change_sequence` is allocated transactionally per namespace by a dedicated allocator row, not by `MAX(change_sequence)+1`:

```sql
CREATE TABLE IF NOT EXISTS bronze_v1_sequence_allocators (
  namespace TEXT PRIMARY KEY,
  next_change_sequence INTEGER NOT NULL CHECK(next_change_sequence >= 1),
  next_invalidation_sequence INTEGER NOT NULL CHECK(next_invalidation_sequence >= 1)
);
```

Within the same `BEGIN IMMEDIATE` transaction as the semantic transition, update the allocator with `next_change_sequence = next_change_sequence + 1` and capture the old value using a SQLite `UPDATE ... RETURNING` statement, then insert the immutable journal row. Allocate `invalidation_sequence` from the same row and insert the invalidation row in that transaction. A failed insert rolls back both the allocator and semantic state. `conflict_id = sha256(namespace|event_id|previous_revision|conflicting_immutable_envelope_hash|conflicting_raw_ingestion_id)`. `invalidation_id` uses the same conflict identity plus the invalidation contract version. `processing_intent_id = sha256(namespace|topic|partition|offset|raw_ingestion_id|semantic_disposition|semantic_revision_or_empty|processor_version)`. `expected_lineage_hash` is the canonical hash of the complete expected delivery-lineage object. `expected_output_json` is frozen canonical JSON for every required output obligation, so recovery does not depend on changed configuration or a re-run parser. Duplicate/already-conflicted intents contain disposition/lineage obligations and explicitly contain no accepted-observation obligation. IDs are deterministic and retries are `INSERT ... ON CONFLICT DO NOTHING`.

The five ClickHouse tables are immutable mirrors/output. The observation row is written before publication and has no `change_sequence`; the published sequence is read from SQLite. Delivery rows carry a frozen disposition and intent ID, not mutable processing status. Invalidation audit rows carry immutable identity and sequence, while mirror progress lives in SQLite. Verify the exact schema with `DESCRIBE`.

### 9.2 State transitions

| Input | Transition | Required outputs |
|---|---|---|
| New valid eventId | absent -> `PENDING` -> `ACCEPTED` | observation, primary lineage, delivery `ACCEPTED`, change sequence |
| Same eventId + identical envelope and payload | `ACCEPTED` remains `ACCEPTED` | delivery `SEMANTIC_DUPLICATE`, lineage only |
| Same eventId + changed payload or envelope | `ACCEPTED` -> `CONFLICTED` after any earlier PENDING intent has completed | conflict evidence, delivery `SEMANTIC_CONFLICT`, invalidation work, valid-reader suppression |
| Delivery for already `CONFLICTED` identity | remains `CONFLICTED` | `ALREADY_CONFLICTED`, lineage/evidence if new |
| Same eventId conflict repeats | remains `CONFLICTED` | idempotent conflict/invalidation record; no new uncontrolled work |
| Physically intact body rejected by an explicitly approved Bronze-only rule | no semantic reservation | `BRONZE_QUARANTINED`, quarantine row, checkpoint may advance only after complete persistence |
| Raw-confirmed body fails byte/transport/shared-contract verification | no semantic reservation | `FAILED_INTEGRITY`, persisted fault, partition blocked; never ordinary quarantine/advance |
| Raw quarantine | no semantic processing | `RAW_QUARANTINE_SKIPPED`, checkpoint may advance |
| Raw unresolved/physical conflict | no state transition | local fault and checkpoint stop |

Conflict detection compares `immutable_envelope_hash`, `payload_hash`, and the canonical envelope/payload content retrieved from the actual Raw row. A stored hash is an index/integrity assertion, not a substitute for content reconciliation when evidence is available. An identical retry after conflict is never an acceptance candidate.

### 9.3 Processing status and persistence-intent transitions

Semantic disposition and persistence status are separate columns and separate state machines. The canonical processing statuses are:

| Processing status | Meaning | Allowed next states | Checkpoint eligible |
|---|---|---|---|
| `PREPARED` | SQLite intent exists with frozen decision and expected outputs | `PENDING_PERSISTENCE`, `FAILED_INTEGRITY` | No |
| `PENDING_PERSISTENCE` | ClickHouse obligations are not all confirmed | `PERSISTENCE_CONFIRMED`, `FAILED_RETRYABLE`, `FAILED_INTEGRITY` | No |
| `PERSISTENCE_CONFIRMED` | Every expected ClickHouse output has been positively reconciled | `COMPLETED`, `FAILED_INTEGRITY` | No, until completed |
| `COMPLETED` | Semantic state, disposition, lineage, mirrors and intent are durable | terminal | Yes |
| `FAILED_RETRYABLE` | Temporary dependency failure; frozen intent remains authoritative | `PENDING_PERSISTENCE`, `FAILED_INTEGRITY` | No |
| `FAILED_INTEGRITY` | Content, destination, identity, source or state mismatch | terminal until operator resolution | No |

The semantic dispositions are `ACCEPTED_NEW`, `SEMANTIC_DUPLICATE`, `SEMANTIC_CONFLICT`, `ALREADY_CONFLICTED`, `RAW_QUARANTINE_SKIPPED`, and `BRONZE_QUARANTINED`. A disposition never implies `COMPLETED`. Every intent stores the exact expected output JSON/reference, destination, hashes, conflict/invalidation IDs, processor/schema versions, and attempt metadata. Recovery reuses that frozen intent; it does not reclassify it from the current state.

### 9.4 Atomic semantic/invalidation transaction

For a late conflict, one SQLite `BEGIN IMMEDIATE` transaction must insert/update the frozen processing intent, delivery disposition intent, conflict evidence, transition identity to `CONFLICTED`, append the immutable `bronze_v1_semantic_changes` row, allocate and insert the deterministic invalidation work item, and commit. The change row records `previous_state='ACCEPTED'`, `new_state='CONFLICTED'`, `semantic_revision`, `observation_content_ref`, `invalidation_id`, and `processing_intent_id`. If any insert fails, roll back all state and allocator updates; do not advance the physical checkpoint.

### 9.5 Deterministic protocols by semantic case

**New valid observation:** verify Raw bytes and V1 contract; begin SQLite transaction; reserve identity as `PENDING`; create `ACCEPTED_NEW` intent with frozen expected observation/lineage outputs; commit preparation; write/reconcile ClickHouse observation and lineage; begin SQLite finalization transaction; verify identity is still `PENDING` and no conflict exists; set identity `ACCEPTED`, append one `ACCEPTED` change, finalize delivery and intent `COMPLETED`; then CAS the source checkpoint. A PENDING identity is never returned by the valid reader.

**Identical semantic duplicate:** verify Raw and compare the incoming immutable envelope and payload against the current accepted identity; create a frozen duplicate intent and delivery lineage; reconcile only the required disposition/lineage output; do not create an observation or ACCEPTED change; finalize the delivery and intent `COMPLETED`; then include it in the checkpoint prefix.

**Conflict after earlier acceptance:** verify the incoming delivery is wire-valid; begin one SQLite transaction; create the frozen conflict intent; persist conflict evidence; transition identity to `CONFLICTED`; allocate/append one `CONFLICTED` journal entry; create deterministic invalidation; commit; immediately suppress the identity in the canonical reader; reconcile ClickHouse conflict, lineage and invalidation mirrors; finalize the intent only after all mandatory outputs match; then CAS the checkpoint. A ClickHouse outage cannot leave the identity ACCEPTED.

**Conflict involving PENDING:** the supported single-owner processor serializes operations per event ID. Recover or finish the earlier frozen intent before classifying another delivery for that ID, including deliveries in one batch. Consequently a second semantic decision cannot observe PENDING in the supported runtime; it waits or fails closed if the earlier intent cannot be recovered. Test same-batch and restart serialization. An earlier observation may be ACCEPTED only after its own outputs reconcile; the later conflict then follows the ACCEPTED-to-CONFLICTED protocol. No unsupported PENDING-to-CONFLICTED supersession is performed.

**Already CONFLICTED:** preserve each new physical delivery and create `ALREADY_CONFLICTED` with a completed lineage/intent after required mirrors reconcile. Identical or different-content retries do not restore ACCEPTED, do not increment semantic revision or change sequence, and do not create another invalidation. New conflict evidence may be stored only as a distinct deterministic delivery evidence row, not as a semantic transition.

If ClickHouse persistence succeeds but SQLite finalization fails, the output remains hidden or suppressed by the SQLite authority until startup recovery completes. If an expected output has unexpected content, mark the intent `FAILED_INTEGRITY`, persist the fault, and block the affected partition; never mark it complete because an ID exists.

## 10. Downstream Invalidation Contract

### 10.1 Chosen Phase 2 contract

Bronze is the source of truth for semantic invalidation. It emits a durable work item for every accepted identity that becomes conflicted. The canonical valid Bronze reader suppresses the identity immediately. Phase 2 does not mutate Silver or Gold.

The locked target downstream policy is event-level revision/tombstone in Silver, followed by deterministic affected-window recomputation in Gold. This is the recommended option because the existing Silver readers already preserve source lineage and can suppress/revise a fact by event identity, while Gold already has revision/window mechanisms that can recompute affected windows without inventing a second semantic identity. A full fact rebuild remains an implementation detail of Silver when a fact has no safe tombstone representation, but it must produce the same revision-aware invalidation result.

The target policy is:

1. Silver consumes the append-only semantic change journal for accepted observations and invalidations through deterministic cursors.
2. Silver identifies every fact by the Bronze `event_id` lineage column and writes an event-level tombstone/revision for the conflicted event. If a current fact cannot be safely tombstoned, it deterministically rebuilds all facts derived from that event and records the same invalidation lineage.
3. Gold consumes Silver invalidation/rebuild signals, identifies every affected 60-second/300-second materialization window from Silver lineage, and recomputes the window under the approved Phase 4 late/revision policy. A window with unresolved reconciliation is not finalized as valid.
4. Serving views suppress affected outputs while reconciliation is pending; no stale-but-valid exception is permitted. Once recomputation is committed, the new materialization revision replaces the invalidated one.

The target policy is locked here; the physical Silver choice between a tombstone write and a deterministic fact rebuild is a Phase 3 implementation detail within that policy. The Bronze obligation is not deferred: the durable invalidation source, event identity, previous/conflicting hashes, lineage, revision, and status are implemented and contract-tested here.

### 10.2 Future consumer interface

```python
read_valid_observations(namespace, event_ids=None, simulation_run_id=None,
                        after_key=None, limit=500) -> BronzeObservationPage
read_semantic_changes(namespace, after=(sequence, event_id) | None,
                      limit=500, high_water=None) -> SemanticChangePage
read_current_semantic_state(namespace, event_id) -> SemanticIdentityState
read_invalidations(namespace, after=(sequence, invalidation_id) | None,
                   limit=500, high_water=None) -> InvalidationPage
read_delivery_lineage(namespace, event_id) -> list[DeliveryLineage]
```

Consumers use `(change_sequence,event_id)` and `(invalidation_sequence,invalidation_id)` cursors, not only `processed_at`. Equal timestamps and later state changes are observable. Bronze owns immutable invalidation creation and its own mirror reconciliation status. Silver will own a durable consumption cursor and fact-reconciliation state in its separate Phase 3 store; Gold will own its later progress. Bronze exposes no downstream write or `ack_invalidation` API. A test reader may simulate Silver-owned cursor persistence, but Phase 2 does not claim Silver/Gold end-to-end completion.

### 10.3 Required contract test

The Phase 2 implementation must insert an accepted observation, consume it through a test downstream reader, inject a valid conflicting delivery, and demonstrate: identity `CONFLICTED`; valid Bronze query returns zero; the journal contains the earlier ACCEPTED row and later CONFLICTED row; one deterministic invalidation is discoverable; a later identical retry leaves the state conflicted; live Raw evidence remains unchanged. The evidence must explicitly say this is a Bronze invalidation contract test, not Silver/Gold implementation.

## 11. Raw Source Reader and Checkpoint Algorithm

### 11.1 Partition discovery and initialization

Discover partitions from the Raw ledger's `partition_progress` and confirmed physical evidence for the configured topic identity. Do not rely only on a static `0,1,2` list. Empty partitions are initialized with a source start and safe boundary when Raw can prove the partition exists; no checkpoint is advanced solely because a ClickHouse query returned no rows. The current Phase 1 ledger binds each progress row to `topic_identity`, but does not expose a separate immutable ledger-generation identifier. Bronze records `(resolved ledger path, topic, topic_identity, schema version, namespace)` as its deployment binding. This path fingerprint cannot detect a replacement database at the same path. The supported source invariant requires the persistent Raw ledger to remain complete and unpruned from its declared initial ingestion position through the captured `safe_next_offset`; history before that position cannot be certified without separate evidence. A changed topic identity, regressed safe boundary, missing expected history, or unexpected ClickHouse evidence fails closed.

On first start, obtain the minimum confirmed/source-safe boundary from Raw and initialize `next_source_offset` to that explicit value. On restart, require stored topic identity to match current Raw identity and require the checkpoint namespace, processor version, source mode, and schema version to match. Identity replacement, retention loss, or a missing committed history is fail-closed.

### 11.2 Batch and checkpoint algorithm

```text
for each partition:
  capture source binding = (ledger path fingerprint, topic, topic_identity, schema version)
  cp = SQLite checkpoint with next_source_offset and generation
  open one consistent Raw ledger snapshot
  safe_1 = Raw safe_next_offset from that snapshot
  requested_end = min(safe_1, cp.next_source_offset + batch_size)
  read confirmed logical deliveries in [cp.next_source_offset, requested_end)
  prove source completeness for this bounded interval using the supported Raw ledger contract
  verify every returned status/evidence and capture safe_2/source binding again
  if binding changed or safe_2 < requested_end: fault; do not advance
  if an unresolved/physical-conflict/missing-history condition exists: fault; do not advance
  for each delivered row in order:
      if physical disposition already terminal: reuse original disposition
      else decode/verify/validate; classify semantic state transactionally
      persist CH expected rows and reconcile identity/destination/content
      finalize SQLite delivery/semantic/invalidation state
  define new_next = requested_end only when every delivered record in the range is COMPLETED
  and every absent numeric offset is proven a valid gap; otherwise stop before first incomplete
  CAS (namespace,topic,partition,source binding,generation,current next)
      -> (same binding,generation+1,new_next,new safe boundary)
```

The cursor is an exclusive `next_source_offset`, never a last-completed offset. For `checkpoint_next=100`, `requested_end=110`, and confirmed delivered offsets `100,101,104,105`, Bronze advances to `110` only if the Raw source contract proves that every absent offset in `[100,110)` is an unrecorded valid Kafka gap and no unresolved/missing history exists. It does not advance to `105` or `106` merely because `105` is the last returned row. A fully empty verified range may advance to `requested_end`; an empty query without completeness proof must not. An unresolved row or physical conflict blocks the partition, not the entire system; other partitions may progress. A terminal semantic conflict permits physical progression after evidence, state, invalidation, and mirror reconciliation are durable.

### 11.3 Checkpoint CAS

`commit_checkpoint(namespace, topic, partition, expected_topic_identity, expected_source_fingerprint, expected_generation, expected_next, new_next, captured_safe)` must update exactly one row with `WHERE namespace=? AND topic=? AND partition_id=? AND raw_topic_identity=? AND source_fingerprint=? AND generation=? AND next_source_offset=?`. It rejects regression, source identity/fingerprint changes, `new_next > captured_safe`, and any intent not `COMPLETED`. The transaction must verify `rowcount == 1`; zero rows is a CAS failure and must not be overwritten or retried with a new expected value silently. The instance lock remains required for the supported single-owner deployment; CAS is defense in depth and is tested independently. A path fingerprint is only a deployment binding, not proof against replacement with another database at the same path.

## 12. Persistence and Recovery Matrix

### 12.1 Processing pseudocode

```text
read a consistent bounded Raw range below safe_next_offset
startup/recovery first loads every non-COMPLETED processing_intent
for each physical delivery:
  if terminal intent exists: reuse its frozen semantic disposition and expected outputs
  else decode Base64 and verify source byte hash/transport evidence
  if CONFIRMED_QUARANTINE: prepare RAW_QUARANTINE_SKIPPED intent
  if physical/transport/shared-contract mismatch: prepare FAILED_INTEGRITY and stop partition
  if an explicitly approved Bronze-only rule rejects it: prepare BRONZE_QUARANTINED intent
  else load semantic identity and prepare one of ACCEPTED_NEW, SEMANTIC_DUPLICATE,
       SEMANTIC_CONFLICT or ALREADY_CONFLICTED
  commit SQLite preparation transaction with expected output JSON/references and PENDING status
  persist only the obligations named by that frozen intent
  positively reconcile destination, namespace, identity, content, lineage and hashes
  finalize the appropriate SQLite semantic/disposition/journal/invalidation records
  set processing status PERSISTENCE_CONFIRMED then COMPLETED in SQLite
CAS advance next_source_offset only when every delivery in the verified source prefix
  has processing_status=COMPLETED and no integrity fault exists
```

There is no cross-system transaction claim. For a new observation, the preparation transaction is committed before ClickHouse write; for a conflict, the semantic `CONFLICTED` transition, append-only change, and invalidation are committed together before mirror writes. A PENDING observation is hidden; a conflicted identity is suppressed immediately even if ClickHouse mirrors are pending. Deterministic processing intents prevent lost outputs or different decisions after a crash.

The single checkpoint-eligibility predicate is:

```text
eligible(delivery) iff
  semantic decision is durable
  AND physical delivery disposition is durable
  AND processing_intent.processing_status = COMPLETED
  AND every expected ClickHouse destination/content/lineage row reconciles
  AND required conflict evidence and invalidation are durable
  AND no FAILED_INTEGRITY fault applies
```

`SEMANTIC_CONFLICT`, `ACCEPTED_NEW`, and other semantic dispositions are never substituted for `COMPLETED`. A terminal semantic decision with incomplete persistence remains behind the checkpoint and is recovered from its intent.

### 12.2 Failure matrix

| Failure point | Durable state | Visible logical state | Checkpoint | Recovery/test/PASS |
|---|---|---|---|---|
| Before semantic reservation | Raw only | none | unchanged | retry makes one reservation |
| Confirmed Raw quarantine | Raw quarantine evidence | none; delivery is not semantic input | advances only after `RAW_QUARANTINE_SKIPPED` intent is `COMPLETED` | lineage/disposition reconciliation |
| Raw-confirmed byte/transport mismatch | integrity fault + failed intent | none | unchanged; partition blocked | operator/source repair, never ordinary quarantine |
| Raw-valid/shared-contract disagreement | source/validator fault | none | unchanged | version/hash investigation, no auto-advance |
| After reservation before CH insert | `PENDING` | hidden | unchanged | startup reconciles/retries; no duplicate |
| CH insert rejected | SQLite PENDING or unchanged | hidden | unchanged | retry after dependency recovery |
| CH insert succeeds, response lost | CH row | hidden until semantic finalize | unchanged | content-aware reconcile then finalize |
| Partial batch persistence | subset CH rows | only finalized accepted rows | stop at first nonterminal | reconcile each expected identity |
| Observation succeeds, lineage fails | CH observation only | hidden/PENDING | unchanged | repair lineage or fail closed |
| Semantic finalized, CH mirror fails | SQLite ACCEPTED/CONFLICTED plus incomplete intent | accepted observation remains hidden until its required mirror is complete; CONFLICTED identity is suppressed immediately | unchanged | frozen intent reconciliation; readiness degraded; never make CH absence look valid |
| Conflict transition/invalidation fails | transaction rolls back | prior state only | unchanged | retry atomically; no lost invalidation |
| Invalidation succeeds, checkpoint fails | invalidation durable | identity remains conflicted | unchanged | retry CAS; idempotent |
| CH succeeds, SQLite fails | CH orphan possible | not valid until authority exists | unchanged | reconcile expected row; no CH-only validity |
| Terminal semantic decision, incomplete persistence intent | semantic decision/intention | PENDING hidden or CONFLICTED suppressed | unchanged | recover intent before checkpoint |
| CAS failure | prior checkpoint | prior terminal dispositions | unchanged | stale worker abort/reload |
| Duplicate after restart | terminal disposition | one observation | may advance | preserve original disposition |
| Conflict after restart | conflict/invalidation authority | excluded | may advance after durable evidence | never clear conflict |
| Raw unavailable | prior state | prior output | unchanged | retry, readiness not ready |
| Bronze CH unavailable | SQLite pending/authority | output may be hidden | unchanged | reconcile after recovery |
| Checkpoint store unavailable | no new durable state | prior output | unchanged | stop/fault; no advancement |
| Graceful shutdown | bounded pending state | only finalized | last CAS | restart reconciliation |
| Raw progresses concurrently | snapshot safe boundary | bounded batch only | not beyond captured safe | next loop handles new rows |
| stale/second worker | lock/CAS fault | unchanged | unchanged | reject startup; test PASS |

### 12.3 Startup reconciliation

```text
acquire V1 instance lock
open checkpoint SQLite FULL/WAL and verify schema
for every PENDING delivery/semantic identity:
  load its immutable processing_intent, not current reclassification logic
  inspect every expected CH row by destination, namespace, identity and canonical content
  if all expected outputs match: mark PERSISTENCE_CONFIRMED then finalize the frozen decision
  if outputs are missing: retry only missing obligations
  if any output has unexpected content/destination/lineage: mark FAILED_INTEGRITY and block
for every CONFLICTED identity:
  verify conflict evidence, CONFLICTED journal row and invalidation exist in SQLite
  verify CH conflict/lineage/invalidation mirrors; repair only missing obligations idempotently
  never transition back to ACCEPTED
for every checkpoint behind a COMPLETED intent:
  verify all earlier intents are COMPLETED and source boundary is unchanged
  retry the exact CAS; do not jump based on ClickHouse maximum offset
verify checkpoint <= Raw safe boundary and topic identity
verify no terminal delivery lacks required disposition/lineage
only then expose READY
```

## 13. Detailed Implementation Tasks

### P2-01 — Baseline, Raw reader and source-boundary adapter

Refactor `de/bronze/lineage_resolver.py`, `models.py`, and `clickhouse_repository.py` to consume the actual V1 Raw interface through `V1LedgerReader`; add a small adapter only if needed to preserve the existing repository seam. Implement partition discovery, consistent bounded reads, safe boundary capture, source-mode checks, content-aware Raw evidence comparison, and explicit result-status handling.

Tests: Raw status mapping, safe-boundary exclusion, valid sparse offsets, unresolved/physical conflict blocking, retention/missing-history fault, read-only SQLite/WAL access, live/replay separation. Exit: no direct Bronze writes to Raw ledger and no direct Kafka consumer.

### P2-02 — V1 model, codec, validator and transformer

Refactor `payload_codec.py`, `validator.py`, `transformer.py`, `canonical_hash.py`, and `models.py` in place. Reuse `contracts.traffic_observation`; decode strict Base64/UTF-8/JSON; verify source bytes and immutable envelope; preserve full payload and availability semantics. Remove ENTITY/RUN synthetic branches and the upstream duplicate flag.

Tests: valid payload, malformed bytes/JSON, wrong event type/version/key, payload/envelope hash mismatch, null/zero/unavailable/absent fields, out-of-order cycle and historical run. Exit: valid V1 maps to a complete observation row without V2 compatibility JSON.

### P2-03 — Semantic identity and invalidation authority

Extend `checkpoint_store.py` in place with V1 SQLite DDL, transaction methods, current semantic state, the append-only change journal, deterministic sequence allocators, processing intents, state transitions, deterministic conflict/invalidation IDs, revisions, terminal disposition idempotency, and source-bound CAS. Add no second semantic authority. A separate semantic-ledger module is not justified; replay uses a separate SQLite database file only for state isolation, with the same store implementation.

Tests: within-batch/cross-batch/cross-partition/restart/replay duplicate and conflict, late conflict after acceptance, conflict after fake downstream consumption, identical retry after conflict, atomic rollback, deterministic IDs, CAS race. Exit: monotonic `CONFLICTED`, no lost invalidation.

### P2-04 — ClickHouse migration and repository

Add `de/migrations/007_create_bronze_v1.sql` with the explicit V1 objects. Refactor `de/bronze/clickhouse_repository.py` to insert/reconcile immutable observations, dispositions, conflict evidence, quarantine, and invalidations by destination/namespace/content. Published change sequence and mirror progress remain SQLite owned. Keep 003 unchanged and V2 tables readable.

Tests: clean/upgrade/repeated migration, DESCRIBE schema/type checks, partial insert, lost response, mismatched existing row, replay isolation, V2 sentinel preservation. Exit: MergeTree rows are treated as physical evidence, not uniqueness.

### P2-05 — Processor, checkpoint, health and runtime wiring

Refactor `processor.py`, `offset_tracker.py`, `config.py`, `main.py`, `health_api.py`, `metrics.py`, and `instance_lock.py`. Implement the P2 algorithm, separate semantic/persistence statuses, startup intent recovery, source-fingerprint-bound CAS checkpoint, bounded recovery, V1 settings, read-only Raw mount, dependency readiness, invalidation backlog metrics, health/ready behavior, and additive Docker Compose service. The service must run against persisted Raw before Phase 5.

Tests: runtime startup, idle readiness, source/CH/checkpoint outage, second owner, safe gaps, pending recovery, restart and throughput/backlog metrics. Exit: real Raw-to-Bronze processing and no Phase 5-only integration.

### P2-06 — Replay/backfill and parity

Refactor `de/bronze/replay.py` to support explicit live Raw bounded replay and isolated Raw replay source modes. Use a separate `de/artifacts/bronze_v1/replay/<run_id>/checkpoint.sqlite3` plus `namespace='replay:<run_id>'` for all shared ClickHouse tables. Add resume, report, parity and safe shutdown commands. Never mutate live Kafka, Raw, Bronze, V2, or live invalidation state.

Tests: accepted/duplicate/conflict parity, insufficient context detection, live-state invariance, Raw/V2 invariance, interrupted resume. Exit: parity claims are limited to the selected source range and semantic context.

### P2-07 — Evidence, handover and acceptance

Add/update `docs/DEv1/phase-2/acceptance.md`, `handover.md`, implementation status, decisions, blockers, and evidence under `de/artifacts/bronze_v1/acceptance/`. Run the full matrix and real runtime stages. Document exact commit, schemas, configurations, commands, counts, current semantic state, append-only journal, processing intents, conflict/invalidation state, checkpoints, logs, resource/backlog observations, limitations, and PASS/FAIL/BLOCKED gates.

## 14. Replay, Backfill and Parity

### 14.1 Supported modes

`live-raw-range`: read persisted live Raw logical deliveries over `[start,end)` using the live Raw ledger read-only. Bronze writes only `namespace='replay:<run_id>'` rows to the shared V1 ClickHouse tables and a separate SQLite database at `de/artifacts/bronze_v1/replay/<run_id>/checkpoint.sqlite3`.

`raw-replay-range`: read Phase 1 replay tables and the matching isolated replay ledger for an explicit `replay_run_id`. It is never mixed with live Raw in one run. Its Bronze SQLite database and ClickHouse namespace are distinct from every live and other replay run.

The replay manifest records source mode, table names, ledger path, topic identity, partition bounds, start offset, exclusive end, captured safe boundary, semantic context range, and processor/schema versions. A partial range that lacks earlier identity context is reported as `CONTEXT_INSUFFICIENT`; it cannot claim full live parity.

### 14.2 Commands

The implementation must expose commands with these semantics (exact module options may follow the existing CLI convention):

```powershell
python -m de.bronze.replay --mode live-raw-range --manifest .\de\artifacts\bronze_v1\replay\RUN\manifest.json --run-id RUN --resume
python -m de.bronze.replay --mode raw-replay-range --raw-replay-run RAWRUN --manifest .\de\artifacts\bronze_v1\replay\RUN\manifest.json --run-id RUN --resume
python -m de.bronze.replay --report --run-id RUN
python -m de.bronze.replay --parity --run-id RUN --manifest .\de\artifacts\bronze_v1\replay\RUN\manifest.json
python -m de.bronze.replay --safe-shutdown --run-id RUN
```

The replay command must print and persist the resolved source mode, Raw ledger path, Bronze SQLite path, ClickHouse namespace, topic identity, bounds, safe boundary and high-water values before processing. A replay process must refuse a live checkpoint path, `namespace='live'`, a live invalidation namespace, or a source mode/table mismatch.

Parity compares logical accepted event IDs, semantic state/dispositions, canonical observation content, conflict set, delivery lineage, and physical retry counts using documented exclusions for processing timestamps and runtime audit IDs. It does not require live mutation or identical ingestion timestamps.

## 15. Configuration, Migration and Runtime Deployment

### 15.1 V1 settings

Refactor the existing `BronzeSettings` structure and use a dedicated explicit prefix (for example `DE_BRONZE_`) to prevent accidental V2 binding. Required variables:

| Variable | Type/default rule |
|---|---|
| `DE_BRONZE_RAW_TOPIC` | string, must equal configured V1 topic |
| `DE_BRONZE_RAW_LEDGER_PATH` | path to Raw ledger directory/file; required and read-only |
| `DE_BRONZE_RAW_SOURCE_MODE` | `live` or `replay`; no implicit mixing |
| `DE_BRONZE_RAW_REPLAY_RUN_ID` | required for replay mode |
| `DE_BRONZE_PARTITIONS` | optional discovery allowlist; empty means discover |
| `DE_BRONZE_CLICKHOUSE_HOST/PORT/DATABASE/USER/PASSWORD` | connection settings |
| `DE_BRONZE_OBSERVATION_TABLE`, `...DISPOSITION_TABLE`, `...INVALIDATION_TABLE` | validated V1 allowlist; immutable audit rows only |
| `DE_BRONZE_CHECKPOINT_PATH` | live default `de/artifacts/bronze_v1/checkpoint.sqlite3`; replay is forcibly resolved to `de/artifacts/bronze_v1/replay/<run_id>/checkpoint.sqlite3` |
| `DE_BRONZE_NAMESPACE` | `live` or `replay:<id>` |
| `DE_BRONZE_PROCESSOR_VERSION`, `DE_BRONZE_SCHEMA_VERSION`, `DE_BRONZE_MIGRATION_VERSION` | required nonempty provenance |
| `DE_BRONZE_BATCH_SIZE`, `POLL_INTERVAL_SEC`, `RETRY_BACKOFF_SEC`, `QUERY_TIMEOUT_SEC` | positive bounded values |
| `DE_BRONZE_SOURCE_READ_TIMEOUT_SEC`, `HEALTH_PORT` | positive values |
| `DE_BRONZE_SINGLE_INSTANCE` | must remain true for certified local deployment |

Reject V2 topic/table/checkpoint combinations at startup. Compute and persist the supported Raw source fingerprint from the resolved ledger directory/path and required WAL/SHM presence; compare it on every startup and checkpoint CAS. Raw SQLite is opened with `mode=ro`, `query_only=ON`; Bronze receives the persistent directory including WAL/SHM, not only a copied active `.sqlite3` file. If Docker read-only directory mounting prevents safe WAL reads on Windows, use a tested consistent SQLite backup handoff or a host-controlled read-only directory access mechanism; never copy an active main file unsafely.

### 15.2 Migration runner and Compose

Add migration 007 to the existing migration runner as an explicit `--v1-bronze` selection. Keep 001–006 immutable. Clean install runs 001–007 in order. Upgrade runs 007 over populated V2 tables and Phase 1 Raw data. Repeated execution is idempotent. Failed migration leaves earlier schemas intact, reports failure, and blocks Bronze readiness.

Add a `v1-bronze` Compose service with a health port distinct from Raw, dependency on Kafka only if needed for no reason is forbidden (Bronze does not consume Kafka), dependency on ClickHouse migration and Raw persistent directory availability, V1 environment, V1 checkpoint path, and no Orion/Projector/Silver/Gold dependency. The runtime verification stack must include Kafka, ClickHouse, Raw V1, Bronze V1, migration/init, and a real SUMO producer only as needed for fresh streaming acceptance. Do not stop unrelated user workloads.

## 16. Comprehensive Test Matrix

### 16.1 Health, readiness and observability semantics

Health is a truthful runtime contract, not a semantic-validity shortcut. `/health` is HTTP 200 while the process is alive and reports `state`, `source_identity`, `source_safe_next_offset`, `checkpoint_next_source_offset`, `offset_distance`, `pending_delivery_count`, `pending_intent_count`, `pending_clickhouse_reconciliation`, `pending_invalidations`, `integrity_fault_count`, `last_successful_batch_at`, and `processor_version`. `/ready` is HTTP 200 only when the V1 migration/schema, SQLite authority, read-only Raw source, ClickHouse dependency, instance ownership, source binding, and processor loop are healthy. It is HTTP 503 for source/ClickHouse/checkpoint unavailability, unresolved physical faults, stale ownership, source identity change, or an integrity-faulted partition.

Pending future Silver/Gold acknowledgement does not block Bronze ingestion or readiness because those consumers are outside Phase 2. Pending Bronze invalidation persistence, ClickHouse mirror reconciliation, or a source integrity fault does block the affected partition and is visible in readiness/metrics. `offset_distance = safe_next_offset - checkpoint_next_source_offset` is reported separately from `pending_delivery_count`; valid numeric gaps may produce distance without pending records. Metrics must include accepted-new, semantic-duplicate, semantic-conflict, already-conflicted, Raw-quarantine-skipped, Bronze-quarantined, prepared/pending/completed/failed intents, reconciliation retries, recovery attempts, CAS failures, integrity faults, batch latency, throughput, and catch-up time.

Example degraded response:

```json
{
  "ready": false,
  "state": "DEGRADED",
  "source_identity": "traffic.observations.v1/<uuid>",
  "checkpoint_next_source_offset": {"0": 120},
  "source_safe_next_offset": {"0": 125},
  "offset_distance": {"0": 5},
  "pending_delivery_count": {"0": 1},
  "pending_intent_count": 1,
  "pending_invalidations": 0,
  "integrity_fault_count": 0,
  "reason": "CLICKHOUSE_UNAVAILABLE"
}
```

An integrity fault response is HTTP 503 and names the partition/fault ID without exposing credentials. A healthy idle worker remains ready with zero pending records even when its last successful batch is older than a generic wall-clock threshold; readiness uses dependency and state truth, not false lag.

Mandatory unit/source tests:

- strict bytes/Base64/UTF-8/JSON and shared V1 contract;
- envelope and Bronze hash vectors;
- zero/null/availability/absent/nested/direction semantics;
- Raw status mapping, safe bounds, read-only ledger, valid gaps and missing-history distinction;
- semantic state transitions, duplicate/conflict monotonicity, late invalidation and deterministic IDs;
- SQLite WAL/FULL durability, CAS monotonicity, stale-worker rejection;
- repository destination/content/namespace reconciliation;
- V2 source/schema preservation.

Mandatory failure tests:

- crash before/after reservation;
- CH rejection, lost response, partial batch and orphan mirror;
- lineage/semantic/invalidation/checkpoint failure;
- restart at every pending state;
- Raw/CH/SQLite outages and recovery;
- graceful shutdown with pending work;
- concurrent Raw progression;
- second worker/lock/CAS conflict;
- conflict after accepted/future-reader consumption and identical retry.

Mandatory replay tests:

- bounded live Raw replay, Raw replay mode, resume, duplicate/conflict replay, parity, insufficient context, live Raw/Bronze/V2 invariance.

Mandatory migration/runtime tests:

- clean migration, upgrade over V2 and populated Raw, repeated migration, schema verification, real Raw-to-Bronze historical processing, fresh SUMO-to-Raw-to-Bronze, restart, dependency outage/recovery, idle readiness, Projector/Orion independence, sustained backlog recovery.

Each failure mode must have a separate test result and evidence file. Optional performance investigations must not be combined with mandatory correctness PASS.

The following individually verifiable tests are mandatory. The implementing agent may split them into more files, but must preserve the IDs and evidence mapping:

| Test ID | Test file | Setup/input | Fault point | Expected state/outputs | Checkpoint | Verification command / PASS |
|---|---|---|---|---|---|---|
| SRC-01 | `de/tests/bronze/test_v1_source_reader.py` | confirmed Raw valid row | none | `CONFIRMED_RAW`, byte/hash verified | advances after `COMPLETED` | `pytest ... -k SRC-01`; exact row and hash match |
| SRC-02 | same | confirmed Raw quarantine | none | `RAW_QUARANTINE_SKIPPED`, lineage | advances after disposition mirror | disposition durable, no semantic row |
| SRC-03 | same | `UNRESOLVED` row | read | no semantic state | unchanged | partition blocked and 503/degraded |
| SRC-04 | same | Raw `CONFLICT` evidence | read | `FAILED_INTEGRITY` | unchanged | fault persisted, no quarantine |
| SRC-05 | same | leading/internal/trailing valid gaps | read | all returned deliveries complete | advances to exclusive requested end | no wait on numeric gap |
| SRC-06 | same | empty range with proven completeness | read | no rows | advances to requested end | safe empty-range proof |
| SRC-07 | same | missing history/retention | read | source fault | unchanged | never treated as gap |
| SRC-08 | same | changed topic/ledger fingerprint | startup | source fault | unchanged | startup refuses processing |
| SRC-09 | same | same stored physical hash, changed content | reconcile | integrity fault | unchanged | full-row comparison catches mismatch |
| SRC-10 | same | Raw ledger adapter | attempted write | read-only failure | unchanged | `V1LedgerReader` rejects mutation |
| SEM-01 | `de/tests/bronze/test_v1_semantic_state.py` | new valid event | none | `ACCEPTED_NEW`, revision 1, one journal row | eligible after complete | one valid observation |
| SEM-02 | same | duplicate same envelope/payload | second delivery | `SEMANTIC_DUPLICATE` | both deliveries complete | no second observation/change |
| SEM-03 | same | duplicate across partition/restart | restart | same identity | both complete | one accepted row and two lineage rows |
| SEM-04 | same | changed payload hash | second delivery | `CONFLICTED`, one conflict journal/invalidation | complete after mirrors | valid reader returns zero |
| SEM-05 | same | same payload hash, changed envelope | second delivery | `CONFLICTED` | complete after mirrors | metadata conflict detected |
| SEM-06 | same | conflicting delivery arrives while earlier intent is PENDING | same batch/restart | earlier frozen intent recovered first, then conflict after ACCEPTED | no premature advance | no competing unresolved reservation |
| SEM-07 | same | retry after conflict | later delivery | `ALREADY_CONFLICTED` | complete | no revision/invalidation storm |
| SEM-08 | `de/tests/bronze/test_v1_change_journal.py` | equal timestamps/multiple events | pagination | append-only sequence 1,2,3 | n/a | cursor returns each exactly once |
| SEM-09 | same | restart during journal append | crash | prior committed rows retained | unchanged until recovery | no overwritten ACCEPTED history |
| INT-01 | `de/tests/bronze/test_v1_processing_intents.py` | new intent before CH | crash | `PREPARED/PENDING_PERSISTENCE` | unchanged | recovery uses frozen expected output |
| INT-02 | same | lost CH response | response loss | `PERSISTENCE_CONFIRMED` after reconcile | then complete | missing-only retry |
| INT-03 | same | partial batch | after first insert | incomplete intent | stops at first incomplete | no blind whole-batch completion |
| INT-04 | same | conflict SQLite commit then CH outage | dependency | `CONFLICTED`, invalidation `PENDING` | unchanged | valid output suppressed immediately |
| INT-05 | same | stale worker CAS | concurrent writer | prior checkpoint | unchanged | exactly zero successful stale update |
| INT-06 | same | terminal semantic state, incomplete mirror | restart | state unchanged, intent pending | unchanged | reconciliation before advancement |
| INT-07 | `de/tests/bronze/test_v1_integrity.py` | invalid Base64/byte/transport hash | Raw-confirmed row | `FAILED_INTEGRITY` | unchanged | partition fail-closed |
| INT-08 | same | Raw-valid/shared-contract disagreement | validator drift | source/contract fault | unchanged | not ordinary quarantine |
| API-01 | `de/tests/bronze/test_v1_readers.py` | PENDING and CONFLICTED rows | read | neither visible | n/a | canonical reader is SQLite-authoritative |
| API-02 | same | accepted then conflict journal | cursor | both historical changes | n/a | current valid read returns zero |
| API-03 | same | page boundary/equal timestamps | pagination | stable exclusive cursor/high-water | n/a | restart/repeat is idempotent |
| RPL-01 | `de/tests/bronze/test_v1_replay.py` | separate replay run | replay | replay rows only | live unchanged | distinct SQLite path/CH namespace |
| RPL-02 | same | insufficient semantic context | partial range | `CONTEXT_INSUFFICIENT` | live unchanged | no false parity claim |
| RPL-03 | same | replay conflict/duplicate | replay | parity under context | live unchanged | no live state/offset mutation |
| RT-01 | `de/tests/integration/test_v1_bronze_runtime.py` | clean/upgrade/repeated migration | migration | exact 007 schema | n/a | real ClickHouse DESCRIBE passes |
| RT-02 | same | historical Raw dataset | runtime | accepted/disposition counts derived from source | all safe | real Raw-to-Bronze reconciliation |
| RT-03 | same | fresh SUMO flow | runtime | new observations and catch-up | all safe | actual Outbox→Kafka→Raw→Bronze |
| RT-04 | same | CH outage/restart | dependency | pending intents recover | no unsafe advance | persisted evidence proves recovery |
| RT-05 | `de/tests/integration/test_v1_bronze_runtime.py` | V2 sentinel plus Phase 1 Raw | migration/runtime | unchanged V2/Raw inventory | n/a | preservation report passes |
| RT-06 | same | idle, outage, recovery | health probe | truthful READY/DEGRADED/503 | n/a | endpoint/metrics evidence passes |

Example execution form after implementation: `python -m pytest de/tests/bronze de/tests/integration/test_v1_bronze_runtime.py -q --junitxml=de/artifacts/bronze_v1/acceptance/test-results.xml`. Each ID must be independently selectable with `-k` and have a corresponding evidence record.

## 17. Runtime Acceptance Procedure

### Stage A — Phase 1 protection

Record audited commit, Raw health, topic identity, Raw schemas, ledger path/source fingerprint, a consistent read-only SQLite backup hash, per-partition safe boundaries, counts and service inventory. Do not require the active ledger file hash to remain unchanged while the independent Raw consumer is legitimately ingesting. Run Phase 1 regression tests. Verify Bronze has read-only Raw access and cannot call mutating ledger methods.

### Stage B — Historical Raw-to-Bronze

Use `phase1-isolated-long01` only if the actual Raw directory and matching ledger remain available; otherwise create a unique run. Derive counts from Raw, not from the plan: Raw physical rows, logical valid deliveries, distinct event IDs, Raw quarantine, Bronze accepted, semantic duplicates, semantic conflicts, Bronze quarantine, dispositions and final checkpoints. The documented clean expectation is 6000 accepted observations only if the verified source is unchanged and contains no added duplicates/conflicts.

### Stage C — Fresh streaming

Run a fresh unique SUMO `simulationRunId` through Outbox → Kafka → Raw → Bronze. Verify bytes, hashes, safe boundaries, valid output, quarantine and catch-up. Historical evidence cannot substitute for this stage.

### Stage D — Duplicate/conflict

In an isolated namespace inject: original valid event; identical retry at another offset; same eventId with changed valid payloadHash; same payloadHash with changed immutable envelope metadata; identical retry after conflict. Verify Raw physical preservation, one valid observation before conflict, zero valid observation after conflict, durable conflict/invalidation, monotonic state and complete lineage. Do not inject into the user's historical live dataset.

### Stage E — Recovery

Inject real ClickHouse outage/recovery, restart during processing, checkpoint failure, semantic conflict interruption, invalidation retry and graceful shutdown. Prove no silent loss, no logical duplicate, no conflicted reappearance, no lost invalidation, and no unsafe checkpoint.

### Stage F — Replay/parity

Run a bounded isolated replay, compare accepted set/state/content/conflict/lineage under sufficient context, and capture before/after live Bronze checkpoint, semantic-state, observation-validity, invalidation, V2 state and Kafka-offset evidence. For Raw, use a frozen bounded snapshot or a quiescent verification interval; do not require active Raw files to remain byte-identical during authorized ingestion.

### Stage G — Sustained workload

Run a representative workload or sufficiently large certified persisted dataset. Measure Raw delivery rate, Bronze rate, backlog, checkpoint progression, batch latency, duplicate/conflict counts, retry count, resource use and catch-up time. Phase 1's ~15.57 deliveries/second is an observed workload measurement, not a capacity target.

### Stage H — Independence/preservation

Run without Orion, Projector, Silver, Gold, Spring or Dashboard. Verify V2 tables/history and Phase 1 Raw assets unchanged and no unrelated workload was stopped.

### 17.1 Executable commands and evidence contract

The implementation must record the exact resolved values and outputs of commands equivalent to:

```powershell
git status --short --branch
git rev-parse HEAD
docker compose --profile v1-raw up -d --build de-kafka-raw-v1
docker compose --profile v1-bronze up -d --build de-migrate-v1-bronze de-bronze-v1
Invoke-RestMethod http://localhost:8097/ready
Invoke-RestMethod http://localhost:8098/ready
Invoke-RestMethod http://localhost:8098/metrics
docker compose ps
docker compose logs --tail 200 de-bronze-v1
python -m pytest contracts/tests de/tests/kafka_raw de/tests/bronze -q --junitxml=de/artifacts/bronze_v1/acceptance/unit.xml
python -m pytest de/tests/integration/test_v1_bronze_runtime.py -q --junitxml=de/artifacts/bronze_v1/acceptance/runtime.xml
```

Before/after SQLite inspection must use a consistent backup or read transaction, for example:

```powershell
python -m de.tools.bronze_v1_snapshot --namespace live --output de/artifacts/bronze_v1/acceptance/live-snapshot.sqlite
python -m de.tools.bronze_v1_report --namespace live --checkpoint de/artifacts/bronze_v1/checkpoint.sqlite3
```

Required ClickHouse evidence queries include:

```sql
SELECT count(), uniqExact(event_id) FROM smart_traffic.bronze_observations_v1 WHERE namespace='live';
SELECT disposition, count() FROM smart_traffic.bronze_delivery_dispositions_v1 WHERE namespace='live' GROUP BY disposition;
SELECT count() FROM smart_traffic.bronze_invalidations_v1 WHERE namespace='live';
```

The semantic change journal is authoritative SQLite, so its evidence query is:

```sql
SELECT change_sequence, event_id, semantic_revision, change_type, previous_state, new_state,
       observation_content_ref, invalidation_id
FROM bronze_v1_semantic_changes
WHERE namespace='live' ORDER BY change_sequence;
```

Do not issue a query for a non-existent ClickHouse semantic-journal table. If a later implementation adds a ClickHouse journal mirror, it must be additive and documented as a mirror, never a second authority.

Each Stage A–H evidence directory contains `commands.txt`, `config-redacted.json`, `health.json`, `metrics.json`, `schemas.json`, `sqlite-snapshot.json`, `clickhouse-queries.json`, logs, test XML and a PASS/FAIL/BLOCKED report. Service restoration is recorded after every outage test; no destructive cleanup is automated.

## 18. Definition of Done

Each gate requires implementation result, exact command/test, stored evidence, objective PASS, and blocking condition:

1. Baseline is the verified completed Phase 1 commit.
2. Raw modules/certified behavior remain preserved.
3. Bronze uses reuse-first in-place migration.
4. Actual Raw logical-read interface is consumed.
5. Raw ledger is strictly read-only.
6. Safe boundaries are enforced.
7. Valid offset gaps do not stall processing.
8. Unresolved/physical faults block unsafe advancement.
9. Original bytes/hashes are verified.
10. Shared V1 validation is reused.
11. One unique eventId yields one logical observation.
12. Semantic duplicates yield no second observation.
13. All delivery lineage/dispositions persist.
14. Changed immutable content yields conflict.
15. Conflicted identity is absent from valid output.
16. Conflict is monotonic across retries/restarts/replay.
17. Invalidation is durable/discoverable.
18. Null/zero/availability/absent semantics are preserved.
19. Historical/out-of-order observations remain processable.
20. Reconciliation checks destination, namespace, identity, content, state and lineage.
21. Checkpoints are monotonic/CAS/restart-safe.
22. Crash recovery loses no logical output/conflict/invalidation.
23. Clean/upgrade/repeated migrations pass.
24. V2 and Raw assets are preserved.
25. Replay is isolated.
26. Health/readiness are truthful.
27. Historical Raw-to-Bronze runtime passes.
28. Fresh SUMO-to-Raw-to-Bronze runtime passes.
29. Sustained backlog recovery is evidenced.
30. Orion/Projector/later DE layers are not required.
31. Silver handover contract is complete.

Any missing real-runtime evidence, unresolved integrity fault, failed conflict invalidation, unsafe checkpoint, schema mismatch, or V2/Raw mutation is BLOCKED and cannot be marked complete.

The 31 gates map to independently verifiable evidence as follows:

| Gate | Test/evidence IDs | Required durable state/output | Blocking condition |
|---:|---|---|---|
| 1 | SRC-08, RT-01 | audited Phase 1 commit and source binding | wrong baseline |
| 2 | Phase 1 regression, RT-05 | unchanged Raw modules and evidence | Raw regression |
| 3 | source audit, P2 task diff | only in-place Bronze refactor/new justified responsibility | parallel tree |
| 4 | SRC-01..04 | actual V1 logical-read results | Kafka duplicate consumer |
| 5 | SRC-10 | `V1LedgerReader` query-only/read-only | Raw mutation |
| 6 | SRC-05, SRC-06 | bounded safe source range | unsafe boundary |
| 7 | SRC-05 | leading/internal/trailing gap advances correctly | numeric stall |
| 8 | SRC-03, SRC-04, INT-07 | fault and unchanged cursor | silent advance |
| 9 | SRC-01, INT-07 | original bytes and hashes | reconstructed bytes |
| 10 | SEM-01, INT-08 | shared contract result | duplicated V2 validator |
| 11 | SEM-01 | one accepted observation | zero/multiple logical rows |
| 12 | SEM-02, SEM-03 | duplicate lineage, no extra observation/change | second observation |
| 13 | SRC-02, SEM-02, RT-02 | every delivery disposition/lineage | missing physical evidence |
| 14 | SEM-04, SEM-05 | conflict evidence and state | changed content accepted |
| 15 | API-01, API-02 | current valid reader returns zero | conflicted row visible |
| 16 | SEM-07, SEM-09 | monotonic state/revision | conflict cleared/revision storm |
| 17 | INT-04, RPL-03 | journal and invalidation durable | lost invalidation |
| 18 | codec contract tests | exact null/zero/availability semantics | unavailable becomes zero |
| 19 | historical/out-of-order fixtures | accepted distinct event IDs | freshness filtering |
| 20 | INT-02, SRC-09 | destination/content/lineage reconciliation | ID-only proof |
| 21 | INT-05, SRC-05 | CAS checkpoint and generation | regression/stale overwrite |
| 22 | INT-01..06 | restart recovery from intents | lost output/conflict |
| 23 | RT-01 | clean/upgrade/repeated 007 | migration failure |
| 24 | RT-05 | V2/Raw inventories and sentinels | destructive mutation |
| 25 | RPL-01..03 | separate replay SQLite/namespace | live mutation |
| 26 | RT-06 | truthful health/readiness/metrics | false ready |
| 27 | RT-02 | verified historical Raw-to-Bronze counts | missing real evidence |
| 28 | RT-03 | fresh SUMO flow | historical-only proof |
| 29 | RT-04 and Stage G report | bounded backlog convergence | indefinite growth |
| 30 | Stage H | independent of later services | Orion/Silver dependency |
| 31 | API-02/03 and handover artifact | stable change/invalidation/read interfaces | redesign required by Silver |

The command for the complete mandatory set is `python -m pytest de/tests/bronze de/tests/integration/test_v1_bronze_runtime.py -q --junitxml=de/artifacts/bronze_v1/acceptance/all.xml`; runtime gates additionally require the real-service commands in Section 17.1. IDs such as RT-05/RT-06 are named acceptance cases that must be added to the implementation test files if not already present; they are not claimed as current test results.

## 19. Phase 3 Silver Handover Contract

Bronze hands over:

- canonical valid observation reader and table name;
- eventId logical grain and one-row guarantee;
- full nested V1 payload JSON;
- source immutable envelope hash and Bronze canonical hash;
- semantic state/revision and change cursor;
- physical lineage/disposition reader;
- conflict/quarantine semantics;
- invalidation reader and future Silver-owned consumption cursor contract;
- run/intersection/scenario/cycle/time/topology provenance;
- replay namespace and source context;
- processor/schema/migration versions;
- checkpoint/safe-boundary visibility and recovery rules.

Silver discovers accepted observations by a deterministic `(change_sequence,event_id)` cursor, not maximum `processed_at`. It discovers invalidations with a separate monotonic invalidation cursor. It can process historical runs, out-of-order cycles and later conflict transitions. Bronze does not resolve the archived topology catalog into facts; it preserves `simulationRunId`, `intersectionId`, `topologyDigest`, cycle/time, scenario and all nested direction fields. Manifest/catalog participant and topology interpretation remain owned by the approved Silver boundary unless a later contract explicitly assigns a specific validation to Bronze.

The concrete result structures are:

```python
class BronzeObservation:
    namespace: str
    event_id: str
    semantic_revision: int
    change_sequence: int  # supplied from authoritative SQLite at read time
    state: str  # ACCEPTED only from the valid reader
    event_type: str
    schema_version: int
    simulation_run_id: str
    intersection_id: str
    scenario_id: str | None
    cycle_sequence: int
    simulation_step: int
    simulation_time_seconds: float
    captured_at: datetime
    topology_digest: str
    payload_hash: str
    immutable_envelope_hash: str
    bronze_canonical_hash: str
    observation_payload_json: str
    source_lineage: list[DeliveryLineage]
    processor_version: str
    normalization_version: str

class SemanticChangePage:
    rows: list[SemanticChange]
    next_cursor: tuple[int, str] | None
    high_water: int
    has_more: bool

class InvalidationPage:
    rows: list[InvalidationWork]
    next_cursor: tuple[int, str] | None
    high_water: int
    has_more: bool
```

`read_valid_observations` is a bounded current-state keyset page and never returns PENDING/CONFLICTED rows. `read_semantic_changes` is an append-only historical read and may return an earlier ACCEPTED row followed by a later CONFLICTED row for the same event ID. `read_invalidations` returns `invalidation_sequence`, deterministic ID, event ID, previous/conflicting hashes, semantic revision, source lineage and Bronze mirror status. Silver's future consumption cursor belongs to Silver state. Each page uses a fresh SQLite snapshot; cross-page current-state pagination is not an atomic historical view. A temporary authority/database error returns an explicit error and no cursor advancement.

Silver owns fact/dimension materialization and topology-aware transformations. Bronze owns semantic deduplication and canonical disposition. Phase 2 creates no Silver fact tables.

## 20. Implementation Readiness Verdict

### 20.1 Final document consistency review

The amended contract is internally aligned as follows: `bronze_v1_semantic_identity` stores only current state; `bronze_v1_semantic_changes` is append-only history; `bronze_v1_processing_intents` freezes decisions and expected outputs; semantic disposition is distinct from processing status; conflict transition, change row and invalidation are one SQLite transaction; ClickHouse mirrors are content-reconciled and cannot restore validity; `next_source_offset` is exclusive and CAS-bound to source identity/fingerprint; valid gaps require completeness proof; missing history and physical mismatch fail closed; the canonical reader uses SQLite current state and excludes PENDING/CONFLICTED; replay uses a separate SQLite database and shared ClickHouse tables with mandatory namespace; and Silver/Gold are consumers only, not Phase 2 implementations.

The implementation review must use these checks before coding approval:

1. Confirm the `change_sequence`/`invalidation_sequence` allocator is transactional and no `MAX()+1` exists.
2. Confirm every intent can reconstruct its exact output without reclassification.
3. Confirm a previously accepted observation is suppressed immediately after the committed CONFLICTED transaction, even when ClickHouse is unavailable.
4. Confirm a terminal semantic disposition with incomplete intent remains non-eligible.
5. Confirm journal history is not overwritten by current-state updates.
6. Confirm all reader cursors have explicit sequence fields, high-water boundaries and exclusive restart semantics.
7. Confirm all required ClickHouse DDL fields and SQLite intent/journal fields are present.
8. Confirm Raw-confirmed quarantine, integrity fault, shared-contract disagreement and semantic conflict take different paths.
9. Confirm Phase 1 Raw source completeness limitations are documented rather than invented away.
10. Confirm every replay command resolves a separate SQLite path and non-live ClickHouse namespace.

No application source or Phase 1 document was changed by this amendment.

**READY AFTER EXPLICIT APPROVAL**

Confirmed blockers: none in the audited Phase 1 baseline. The repository is ready for a Phase 2 implementation branch after approval, but implementation must not begin until the following exact planning-gate decisions are accepted:

1. **Raw interface correction:** approve the narrowly scoped `logical_read_consistent`/full-evidence comparator only if tests reproduce the current same-hash/different-content weakness; otherwise preserve the current interface and implement the source adapter around it.
2. **Replay ClickHouse isolation:** approve shared V1 tables with mandatory `namespace='replay:<run_id>'` guards (recommended) rather than redundant replay schemas; replay SQLite is always a separate database per run.
3. **Downstream target policy:** approve Silver event-level revision/tombstone (with deterministic rebuild fallback) and Gold affected-window recomputation, with serving suppression while pending. Phase 3/4 still own physical implementation details.

Routine implementation details—method naming, test-file placement, health port assignment, and evidence filenames—are not approval blockers when they preserve the locked contracts above.

Deferred to later phases: Silver physical fact schema/topology catalog resolution/fact cardinality; Gold late-data close/revision policy; Gold definition-version compatibility; Spring nullable DTO; Dashboard and final canonical cutover. The cross-layer reconciliation target is no longer deferred; only its later physical implementation is.

Required next action: approve this plan and the Section 2.3 decisions, then create `codex/phase-2-v1-bronze` from `757060f8175fdb22d9ee27e7f8d175c82c0e2c45`. The implementation agent must re-run the baseline checks, preserve the Phase 1 handover, implement only P2-01 through P2-07, and stop before Silver.
