# Phase 3 implementation plan — Silver V1

Status: implementation-ready proposal for review; Phase 3 implementation is not authorized by this planning task. Scope follows `docs/DEv1/Master_plan_DE_V1_.md` §335 onward and the audited local source baseline in `audit.md`.

## Goal and non-goals

Materialize complete canonical `IntersectionObservation` V1 events from Bronze into topology-aware, nullable Silver facts with durable provenance, conflict invalidation, restartable persistence, bounded replay and an executable runtime acceptance. Preserve V2 SQL/data and current local changes.

Out of scope: implement Gold formulas/windows, Orion/NGSI, Projector, Spring/Dashboard, Phase 5 deployment, destructive conversion/deletion, Bronze semantic classification, Phase 2 certification, and any changes to the Master Plan. Silver never infers/repairs Bronze disposition.

## Target architecture and contracts

```text
Bronze SQLite semantic authority (:ro) ── changes/high-water/current/invalidation/lineage
              │                                  │
              │ canonical point lookup           ├─ Silver SQLite: own cursor, frozen intents,
              ▼                                  │  event validity and replay state (:rw)
     Bronze ClickHouse observations (:read/reconcile) ──► V1 Silver processor
                                                         │ facts/dimensions/tombstone mirrors
                                                         ▼
                                              shared ClickHouse (migration 008)
                                                         │
                                   Gold-facing canonical Silver reader + validity overlay
```

Bronze SQLite is read-only and remains semantic authority. Silver uses its own SQLite WAL/FULL database, locks and checkpoints. SQLite source changes are authoritative; Bronze CH is a validated envelope/content mirror, not the incremental cursor. The exact additive Bronze interface needed is `bootstrap(namespace, simulation_run_id?, limit, high_water?)` returning a frozen accepted identity set plus change high-water from one read transaction and `read_observation(namespace,event_id,revision)` returning verified complete envelope, Bronze canonical hash, semantic revision, accepted current state and delivery lineage. It must open mode=ro/query_only, reject non-ACCEPTED/stale revision, and require fully reconciled CH obligations. Owner is Bronze; tests are listed in audit/blockers.

Live bootstrap captures one source high-water and durable Silver bootstrap obligation. It processes accepted state at that boundary; then it consumes journal sequences strictly above it. Normal operation captures both journal high-water values, reads bounded pages through each fixed boundary and advances each cursor only after frozen output and validity obligations reconcile. An event lookup always checks current Bronze disposition so an old ACCEPTED journal entry cannot re-accept a now-CONFLICTED event. Concurrent later changes remain above the frozen high-water. If authority is unavailable, no cursor advances. If CH mirror is incomplete, the event stays unavailable to canonical readers and retries.

Bronze duplicate changes are idempotent: event identity is `(simulationRunId,topologyDigest,eventId)`, with `eventId` as Bronze's globally semantic key. A current conflict invalidates all output rows for that event ID across directions. Do not materialize rejected or conflicted observations as valid facts. Preserve per-delivery lineage from `read_delivery_lineage`; identify all source deliveries in local obligations, while one logical fact set is created per event.

## Proposed additive V1 schema

Migration `de/migrations/008_create_silver_v1.sql` is next after existing 007. `IF NOT EXISTS`; no ALTER/DROP of 004 tables; include main and replay targets; migration runner must create every required table before worker starts. All ClickHouse objects use explicit column lists (no implicit defaults for domain values). Facts are append-only MergeTree and include `created_at` as processing metadata, but logical ordering is by event coordinates. Monthly partition by `toYYYYMM(created_at)` is operational only; no source-event semantics depend on processing month. ORDER BY starts with run/digest/event for fact locality. Bronze eventId is unique semantic identifier; CH may physically duplicate rows, so canonical reads reconcile and dedupe by identity plus full content.

Common provenance columns on each fact: `simulation_run_id String`, `topology_digest String`, `run_generation UInt64`, `event_id FixedString(64)`, `cycle_sequence UInt64`, `simulation_time_seconds Float64`, `intersection_id String`, `scenario_id String`, `captured_at DateTime64(3,'UTC')`, `bronze_semantic_revision UInt64`, `bronze_canonical_hash FixedString(64)`, `source_payload_hash FixedString(64)`, `source_topic String`, `source_partition Int32`, `source_offset Int64`, `source_raw_ingestion_ids_json String`, `source_lineage_json String`, `created_at DateTime64(6,'UTC')`, `silver_schema_version LowCardinality(String)`. Lineage JSON is the canonical bounded serialization of all completed Bronze delivery dispositions for the accepted event; the raw ID list is explicit so no arbitrary duplicate delivery is selected.

### `silver_traffic_observation_fact_v1`

**Grain/business identity:** one row per `(simulation_run_id, topology_digest, event_id, intersection_id, source_direction)`, exactly for directions declared for that event's archived topology. Columns: common provenance; `source_direction String`; required directional metrics with source type preserved: `vehicle_count Nullable(UInt32)`, `pcu_equivalent Nullable(Float64)`, `left_count Nullable(UInt32)`, `straight_count Nullable(UInt32)`, `right_count Nullable(UInt32)`, `average_speed_kmh Nullable(Float64)`, `waiting_vehicle_count Nullable(UInt32)`, `queue_length_m Nullable(Float64)`, `occupancy_pct Nullable(Float64)`; `metric_availability_json String` (per-metric explicit status; measured numeric values have no unavailable status); `direction_payload_json String` (canonical complete supported direction object, retaining optional fields without inventing columns); `quality_status LowCardinality(String)` with `VALID` or `VALID_WITH_UNAVAILABLE`; `quality_flags_json String` (stable array of codes). Invalid topology never produces a fact row; it is a permanent provenance/quarantine state. Preserve optional `queue_length_vehicles`, queue-by-movement, density, estimated density, arrival rate, waiting reason counts, dominant reason, theoretical speed, moto share, vehicle-class composition and storage/spillback fields in the raw direction payload; preserve absence versus value and availability separately. Nullability follows schema; required-null status is explicit.

Engine: `MergeTree`; partition `toYYYYMM(created_at)`; `ORDER BY (simulation_run_id,topology_digest,event_id,intersection_id,source_direction,bronze_semantic_revision)`. No DEFAULT zero. Duplicate reconciliation is full row/content hash, not `ReplacingMergeTree` eventual merge.

### `silver_signal_state_fact_v1`

Same direction-level grain and common provenance. Additional columns: `source_direction String`, `signal_color LowCardinality(String)` (event's color map entry), `phase String`, nullable `next_phase Nullable(String)`, `phase_remaining_seconds Nullable(Float64)`, `phase_duration_seconds Nullable(Float64)`, `green_duration_seconds Nullable(Float64)`, `yellow_duration_seconds Nullable(Float64)`, `red_duration_seconds Nullable(Float64)`, `timing_mode Nullable(String)`, `signal_payload_json String` for exact supported timing/color context, `quality_status LowCardinality(String)`, `quality_flags_json String`. `colors` and `phase` are guaranteed by the metric contract; optional timing fields remain NULL when absent. Engine/partition as above; ORDER BY same direction grain.

### `silver_intersection_state_fact_v1`

**Grain/business identity:** `(simulation_run_id,topology_digest,event_id,intersection_id)`. Common provenance; `phase String`; `control_mode String` (guaranteed state); `incidents_json String` (empty array means none); `derived_traffic_state Nullable(String)`; `operational_state_json String` (only when present); `spillback_detected Nullable(UInt8)`; `intersection_box_blocked Nullable(UInt8)`; `downstream_edge_full Nullable(UInt8)`; `spillback_pressure Nullable(Float64)`; `intersection_payload_json String` (canonical payload state without duplicating directional rows); `quality_status LowCardinality(String)`; `quality_flags_json String`. No `total_vehicle_count` is proposed because it is not a field in the audited V1 contract. Optional top-level fields absent from payload stay absent in JSON and NULL in extracted nullable columns. Do not derive a partial total. Engine MergeTree, monthly created_at partition, ORDER BY `(simulation_run_id,topology_digest,event_id,intersection_id,bronze_semantic_revision)`.

### Dimensions and obligations

`silver_run_dimension_v1` complete columns: `simulation_run_id String`, `run_generation UInt64`, `topology_digest String`, `run_status LowCardinality(String)`, `participant_ids_json String`, `manifest_hash FixedString(64)`, `manifest_metadata_json String` (canonical supported run manifest fields, preserving optional seed/profile/configuration/threshold/network metadata without inventing a normalized value), `manifest_path String`, `created_at DateTime64(6,'UTC')`; immutable content reconciliation. Engine MergeTree, ORDER BY `(simulation_run_id,run_generation)`.

`silver_topology_dimension_v1` complete columns: `topology_digest String`, `digest_algorithm_version String`, `catalog_hash FixedString(64)` (hash of exact catalog bytes for provenance, distinct from semantic digest), `intersection_ids_json String`, `approaches_by_intersection_json String`, `catalog_path String`, `created_at DateTime64(6,'UTC')`; engine MergeTree, ORDER BY `topology_digest`.

`silver_run_topology_binding_v1` complete columns: `simulation_run_id String`, `run_generation UInt64`, `topology_digest String`, `manifest_hash FixedString(64)`, `catalog_hash FixedString(64)`, `participant_ids_json String`, `created_at DateTime64(6,'UTC')`; engine MergeTree, ORDER BY `(simulation_run_id,run_generation,topology_digest)`. This captures the Master Plan run/topology binding. The three dimension tables are immutable facts of the archive and avoid unnecessary intersection, approach, scenario or camera dimensions.

`silver_event_validity_v1` complete columns: `namespace LowCardinality(String)`, `event_id FixedString(64)`, `simulation_run_id String`, `topology_digest String`, `semantic_revision UInt64`, `validity_state LowCardinality(String)`, `bronze_change_sequence UInt64`, `bronze_invalidation_sequence Nullable(UInt64)`, `invalidation_id Nullable(FixedString(64))`, `reason LowCardinality(String)`, `created_at DateTime64(6,'UTC')`; engine MergeTree, partition by created_at month, ORDER BY `(namespace,event_id,semantic_revision,created_at)`. `silver_invalidation_v1` complete columns: `namespace LowCardinality(String)`, `invalidation_id FixedString(64)`, `invalidation_sequence UInt64`, `event_id FixedString(64)`, `semantic_revision UInt64`, `simulation_run_id String`, `topology_digest String`, `run_generation UInt64`, `intersection_id String`, `cycle_sequence UInt64`, `simulation_time_seconds Float64`, `previous_immutable_envelope_hash FixedString(64)`, `conflicting_immutable_envelope_hash FixedString(64)`, `original_raw_ingestion_id FixedString(64)`, `conflicting_raw_ingestion_id FixedString(64)`, `lineage_json String`, `created_at DateTime64(6,'UTC')`; engine MergeTree, partition by created_at month, ORDER BY `(namespace,invalidation_sequence,event_id,invalidation_id)`. Silver SQLite validity is the immediate Gold-facing suppression authority; CH copies are publication/reconciliation mirrors. Gold-facing canonical reader must fail closed on missing/unknown state and expose only currently valid events. Do not expose raw V1 fact tables as a supported Gold source.

The frozen processing obligation ledger, event validity current state, cursors and invalidation outbox are Silver SQLite-owned tables (not additional ClickHouse obligation tables); its exact schema belongs in P3-05's SQLite migration and must include namespace, source sequence/type, event ID, expected content JSON/hash, status, attempts, errors and timestamps with unique `(namespace,source_kind,source_sequence)`. `silver_quarantine_v1` ClickHouse mirror columns: `namespace LowCardinality(String)`, `quarantine_id FixedString(64)`, nullable `event_id FixedString(64)`, `simulation_run_id Nullable(String)`, `topology_digest Nullable(String)`, `source_change_sequence Nullable(UInt64)`, `failure_stage LowCardinality(String)`, `error_code LowCardinality(String)`, `error_detail String`, `source_payload_json String`, `source_raw_ingestion_ids_json String`, `created_at DateTime64(6,'UTC')`; engine MergeTree, partition by created_at month, ORDER BY `(namespace,failure_stage,error_code,created_at,quarantine_id)`. It is only for permanent malformed/inconsistent archive or impossible source context after shared V1 validation; transient archive/source/CH failure is retry, not quarantine. Original event JSON and provenance are retained under bounded storage rules. Bronze Raw quarantine is a completed skip disposition from Bronze, not a new Silver semantic quarantine.

Replay mirrors: each fact/dimension/publication table has an isolated replay table, with `replay_run_id String` first in ORDER BY and included in identity. Silver SQLite replay state lives under separate `de/artifacts/silver_v1/replay/<id>/`. Replay cannot write main tables; do not implement omission of topology/run dimensions in replay.

## Archive resolver

Configuration `SILVER_V1_ARCHIVE_ROOT` points to producer artifacts root; archive path is `<root>/runs/<simulationRunId>/run_manifest.json` and `network_topology_catalog.json`, read-only bind mounted in container at a fixed documented path. Verify event run/digest syntax, manifest `simulationRunId`, positive/integer generation, manifest topologyDigest, exact declared participant list and READY/RUNNING status; verify catalog's V1 digest using `contracts.topology.v1_digest.topology_digest`; resolve node via `node_to_tls` and approaches via `approach_edges`; require event intersection among participants and exact direction key equality. Preserve catalog/manifest exact hashes and file paths in dimensions/binding. No active topology fallback. Cache immutable verified contexts by run/generation/digest plus file identity/mtime/size and content hashes. Re-read fingerprints before use; changed content under same identity is permanent integrity failure, not cache replacement. `FileNotFoundError`, `TimeoutError`, temporary IO/permission interruption may retry with no cursor advancement; malformed JSON, digest/manifest disagreement and missing participants/approaches are permanent integrity failures. Replay uses same resolver and historical archive.

## Availability and quality semantics

| Source state | Silver representation | Coverage consequence for Phase 4 |
|---|---|---|
| Numeric including zero, with no availability entry | Numeric value preserved; status `MEASURED`/no unavailable entry | Counts as valid measured value |
| `null` + `UNAVAILABLE` | NULL plus exact availability code | Expected but not measured |
| `null` + `NOT_APPLICABLE` | NULL plus exact code | Excluded from applicable denominator, still explicitly classified |
| Optional field absent | NULL typed projection and absent key in canonical payload JSON; not a zero/default | No measurement claimed; Gold can distinguish absent from unavailable via JSON/presence metadata |
| Expected direction absent | Do not fabricate a direction fact; mark event/topology quality invalid and quarantine/block before any valid facts | Invalid topology, never complete denominator |
| Unexpected direction | Reject event facts as analytically valid; durable provenance error | Integrity failure; no widened topology |
| Partial intersection total | NULL and `PARTIAL_TOTAL` quality code when supported total cannot be verified; don't sum unknown/partial values into a complete total | Gold sees incomplete total |
| Optional top-level state absent | NULL plus absent key in payload JSON | Not zero/false/prior state |
| Scenario changes | Preserve each event's `scenarioId` on facts; run demand profile remains separate manifest metadata | Segment in Gold by event scenario as defined later |
| Direction metric availability differs | Independent nullable value/status per metric, never one aggregate direction flag | Per-metric coverage possible |

`quality_status`: `VALID` means topology/context verified and all present values satisfy shared contract; `VALID_WITH_UNAVAILABLE` means event is structurally/topologically valid but at least one required metric is explicitly unavailable or not applicable; `INVALID_TOPOLOGY` never enters canonical valid facts and is only a durable failure state. `quality_flags_json` stable codes include `METRIC_UNAVAILABLE:<name>`, `METRIC_NOT_APPLICABLE:<name>`, `OPTIONAL_ABSENT:<name>` only where useful, `PARTIAL_TOTAL`, and archive error codes on quarantine. Don't call a valid event invalid just because a metric is explicitly unavailable.

## Invalidation, persistence and checkpoints

SQLite schema (owned by Silver, additive) contains source binding and change/invalidation cursors with generation CAS; frozen event/output obligations and attempt/fault state; bootstrap manifest/high-water; event validity state; local invalidation outbox; replay manifests. `BEGIN IMMEDIATE` protects state transition/obligation publication. A frozen obligation contains exact target, complete row content, expected content hash and source sequence. Ordering: (1) capture bounded authority record/high-water; (2) verify current Bronze state and archive; (3) persist frozen Silver intent/validity preparation; (4) write dimensions then facts/tombstones to CH; (5) content-reconcile every expected row; (6) finalize local output state and checkpoint CAS. On invalidation, atomically set local event validity `INVALIDATED` and freeze outbox before any canonical read; CH mirror/tombstone retries independently. Do not checkpoint invalidation sequence until frozen local state and outbox exist; retry publication by exact content. If a crash occurs after local suppression but before CH write, Gold canonical reader still excludes event and readiness reports backlog. If CH accepted a write but response was lost, reconciliation decides. If state authority unavailable, fail closed.

Cursor means “all Bronze change or invalidation journal entries through this exclusive sequence have durable Silver state and frozen/reconciled output obligations”; it is not a Kafka offset or processed wall-clock watermark. Compare-and-swap includes namespace, source authority path/binding, expected sequence, next sequence and generation. For two journals use separate cursors. In-flight work is recovered before new entries. Batch boundary is source journal high-water and maximum item count; never advance over a failed or unresolved record.

Replay manifest freezes replay ID, source namespace, run ID, source bootstrap high-water, change and invalidation high-waters, archive generation/digest, and schema/processor version. Bronze does not provide arbitrary historical as-of current state. Therefore historical replay is explicitly a current-state snapshot at captured high-water unless Bronze adds snapshot/export API; it cannot claim original as-of state when a later conflict already changed current identity. Keep replay state and CH target isolated. A replay conflict only invalidates replay namespace.

## Runtime integration

Add `de-migrate-v1-silver` one-shot using explicit `--v1-silver`; it depends on shared ClickHouse health and completes migration 008. Add `de-silver-v1` under Compose profile `v1-silver`, command `python -m de.silver.main`, only after migration success. Reuse shared ClickHouse. Mount Bronze artifact directory `/app/de/artifacts/bronze_v1:ro` (preserve WAL/SHM companion files), archive root read-only, Silver V1 state directory read-write; do not mount a copied SQLite main DB alone. Single worker/OS lock, distinct V1 checkpoint path. Health endpoint can remain 8095 only if historical profile cannot collide; otherwise choose documented V1-specific host/container port.

Readiness requires lock held, worker alive, CH schema present, Bronze SQLite readable/query_only, archive resolver initialized, cursors source-bound, no integrity fault, non-stale snapshot. Pending CH retries/invalidation backlog can produce degraded/not-ready according to defined thresholds; invalidated facts are suppressed locally even while mirror pending. Configure bounded batches (e.g. max 100 changes, archive cache and CH queries), conservative CPU/memory limits appropriate to Windows Docker Desktop; avoid new Kafka or database services. Runtime acceptance starts actual SUMO → Outbox → Kafka → Raw → Bronze → Silver on shared stack and proves source/output lineage, not just synthetic fixtures.

Metrics: change/invalidation high-waters and Silver cursors; change lag; pending frozen intents; retries by class; invalidation backlog and oldest age; quarantine/permanent provenance errors; archive available/retry/failure count; CH reconciliation missing/mismatch counts; events/direction facts per second; batch count, duration and p50/p95 latency; checkpoint CAS conflicts; validity counts; source/CH/SQLite readiness. Never include credentials or full payload in health.

## Work packages and handover checkpoints

### P3-01 — Bronze-to-Silver input adapter

**Objective:** Consume semantic changes/invalidation pages safely from authoritative Bronze SQLite and retrieve each accepted complete event with verified CH mirror and lineage.

**Preconditions:** Read-only Bronze SQLite V1 contract verified; atomic bootstrap/point lookup API implemented or explicitly supplied. Current `read_page`/`current` is insufficient alone for bootstrap.

**Reuse / files:** `de/bronze/checkpoint_store.py`, `de/bronze/clickhouse_repository.py`, `de/bronze/lineage_resolver.py`, Phase 2 handover; refactor `de/silver/readers.py`, `input_models.py`, `config.py`; add no second Silver tree.

**Interfaces:** `BronzeSemanticSource.open_ro()`, `bootstrap(namespace,run_id?,limit,high_water?)`, `read_changes(after,limit,high_water)`, `read_invalidations(...)`, `current(event_id)`, `read_observation(event_id,revision)`, `read_lineage(event_id,after,limit)`, `close()`. Return typed record with envelope, revisions/hashes, high-water and delivery lineage; explicit `AUTHORITY_UNAVAILABLE`, `MIRROR_INCOMPLETE`, `CONFLICTED`, `CURSOR_GAP` outcomes.

**Schema/steps:** Add Silver source binding/cursor records; open URI mode=ro and `query_only`; validate Bronze schema/version; capture high-water; test snapshot bootstrap; validate current state; fetch exact revision and reconcile; convert to typed source record; freeze source cutoff for batch. Never read/write Raw ledger or Bronze SQLite writes.

**Dependencies:** none for unit scaffolding; verified Bronze contract before integration.

**Unit/integration/fault tests:** exclusive pagination/high-water; restart resume; concurrent change during page; accepted then conflict before read; missing/corrupt CH mirror; missing lineage; source DB unavailable/permissions; read-only write rejection; journal gap/cursor binding mismatch; source snapshot bootstrap concurrency.

**Runtime/evidence:** read-only adapter report and XML/JUnit; source schema fingerprint; high-water/cursor snapshot; authority outage and mirror-unavailable results in `de/artifacts/silver_v1/acceptance/p3-01/`.

**PASS/DoD:** no changes omitted through frozen high-water; no conflict accepted; no mirror assumed present; raw lineage ties to all dispositions; Bronze remains byte-for-byte/read-only; cursor advances only on completed downstream obligations.

### P3-02 — Run archive and topology resolver

**Objective:** Provide immutable verified run context and exact approach sets by simulation run.

**Preconditions:** producer manifest/catalog formats and V1 digest contract fixed.

**Reuse/files:** `Visualize/runtime/run_manifest.py`, `contracts/topology/v1_digest.py`, topology fixtures, `de/silver/config.py`; add `de/silver/archive_resolver.py`, models/context and fixtures only if needed.

**Interfaces:** `ArchiveResolver.resolve(simulation_run_id,expected_generation?,expected_digest) -> VerifiedRunContext`; context has manifest/catalog hashes, participants, approaches map, generation, digest, metadata/provenance path. Typed retryable-unavailable and permanent-integrity errors.

**Steps/schema:** configure root; read only run package; strict JSON/required keys; verify identity/generation/status/digest/participant and topology relationships; derive participant/approach sets; immutable cache and fingerprint invalidation; provide context to pure builders.

**Dependencies:** P3-01 event identity; independent unit tests may precede it.

**Tests:** valid fixture; 3/4-way intersection and subset; absent run, transient permission/read timeout; malformed manifest/catalog; wrong run/generation/digest; missing participant/node/TLS/approach; extra/missing event directions; same-identity archive rewrite invalidates cache; no active topology fallback.

**Runtime/evidence:** mounted fixture and real generated run archive resolution record, file hashes and exact participant/approach inventory under `p3-02/`.

**PASS/DoD:** every context carries verified manifest/catalog identity; transient failure blocks checkpoint; integrity mismatch cannot yield valid facts; historical path used for replay.

### P3-03 — V1 migration and fact builders

**Objective:** Create three V1 grains with exact event/topology identity and source lineage.

**Preconditions:** approved D3-04/06; migration chain confirmed through 007.

**Reuse/files:** existing pure builder/test patterns; refactor `models.py`, `fact_builders.py`, `engine.py`; add `de/migrations/008_create_silver_v1.sql`; update `de/scripts/migrate_clickhouse.py` selector.

**Interfaces:** `build_event_facts(event,context,lineage) -> 2*direction_count + one intersection row`; strict identity fields and complete content hashes. Builders must not classify Bronze dispositions.

**Schema/steps:** implement additive tables from this plan; migration selector only applies required 006/007/008 prerequisites; schema verifier enumerates all required live and replay objects; map each contract field without defaulting.

**Dependencies:** P3-02 context; P3-04 availability rule contract coordinated before field mapping.

**Tests:** exact schema types/nullability/order keys; event identity/hash parity; row grains; source event lineage; all supported field projection; duplicate content determinism; migration clean/repeat/upgrade/sentinel; rollback not destructive.

**Runtime/evidence:** SHOW CREATE inventory and before/after V2 sentinel report; deterministic builder oracle under `p3-03/`.

**PASS/DoD:** no V2 table altered; additive migration repeats; all values map from event/catalog; row count equals topology-driven cardinality and identities match independent oracle.

### P3-04 — Dimensions, availability and quality

**Objective:** Persist run/topology/binding once per verified identity and preserve metric value/presence/status exactly.

**Preconditions:** metric contract and P3-02 context.

**Reuse/files:** refactor `dimension_builders.py`, `dimension_state.py`, `normalizers.py`, `validators.py`; add availability helpers/models.

**Steps:** populate immutable run dimension from manifest, topology dimension from catalog and binding; build separate explicit availability per metric; preserve optional payload key presence; calculate no Gold aggregates; quarantine topology discrepancy before valid persistence.

**Tests:** numeric zero, positive, required null/status, both statuses, absent optionals, unexpected availability for numeric, partial totals, missing/unexpected direction, scenario transition, different run generations/digests; JSON payload versus typed columns round trip.

**Runtime/evidence:** source-to-Silver field-by-field oracle and availability inventory in `p3-04/`.

**PASS/DoD:** no missing/null metric becomes zero; no unsupported field invented; every metric's applicable/available state can be distinguished by Gold; invalid topology never appears valid.

### P3-05 — Invalidation and durable persistence

**Objective:** Ensure conflicted events disappear from canonical Gold-facing Silver reads immediately after detection and all CH mirrors are retryable.

**Preconditions:** Bronze invalidation/current API; approved D3-06 canonical-reader boundary.

**Reuse/files:** refactor `batch_ledger.py`, `repositories.py`, `checkpoint_store.py`, `processor.py`; additive schema from 008.

**Steps:** SQLite transaction freezes intents/content hashes; event validity gate suppresses uncommitted/invalidated rows; persist dimensions/facts/tombstone; reconcile exact full content; atomically finalize event state/outbox; canonical reader consults validity and Bronze current; retry tombstone/lineage; no async delete dependency.

**Tests/fault injection:** ACCEPTED→CONFLICTED before and after materialization; CH unavailable before write; partial fact inserts; insert success/lost response; tampered frozen intent; invalidation mirror missing/content mismatch; crash after local suppression/before CH insert; crash after CH insert/before cursor; pending invalidation canonical-read suppression; repeat idempotence.

**Runtime/evidence:** fault matrix, before/after canonical query and tombstone mirror report under `p3-05/`.

**PASS/DoD:** no known-conflicted event visible via supported canonical reader; fact rows remain audit-only; outbox retry is idempotent; checkpoint never passes unrecorded invalidation.

### P3-06 — Checkpoint, recovery and replay

**Objective:** Resume from exact source journal sequences and replay with bounded, honest scope.

**Preconditions:** P3-01 and P3-05 state interfaces.

**Reuse/files:** refactor `checkpoint_store.py`, `batch_ledger.py`, `instance_lock.py`, `replay.py`, `processor.py`.

**Steps:** source binding and compare-and-swap cursors for changes and invalidations; frozen high-water; recover pending intents before reads; restart each failure boundary; manifest freezes run/archive/source cutoffs and namespace; separate SQLite/CH targets; replay current-state snapshot semantics explicitly, no as-of claim.

**Tests:** CAS race; restart after preparation/fact/dimension/tombstone/checkpoint; source outage/recovery; bounded replay, changed manifest refusal, repeat resume, conflict isolation and live invariance; archive mismatch; high-water concurrent append; unavailable current-state snapshot.

**Runtime/evidence:** restart/replay JUnit, cursor and state inventories before/after, frozen manifest hash under `p3-06/`.

**PASS/DoD:** cursors represent fully durable obligations, no skipped sequence; replay never alters live state; deterministic repeat; scope limitation appears in manifest/report.

### P3-07 — Compose, migrations, observability

**Objective:** Start one V1 Silver owner only after all tables exist, with source/archive mounts read-only and independent health.

**Preconditions:** migration 008 and config contract available.

**Reuse/files:** `docker-compose.yml`, `de/scripts/migrate_clickhouse.py`, `main.py`, `health_api.py`, `metrics.py`; tests `test_compose_render.py` and `test_container_runtime_contract.py` rewritten/extended.

**Steps:** add `--v1-silver`; one-shot `de-migrate-v1-silver`; `de-silver-v1` `v1-silver` profile; shared ClickHouse; mount exact Bronze and archive roots `:ro`, Silver state `:rw`; configure low-memory limits/bounded batch/cache; unique V1 lock/state; health `/health`,`/ready`; dependency and shutdown behavior.

**Tests:** Compose render; required mount modes; startup blocked on migration; missing authority/archive; double owner lock; stale health; CH outage and recovery; memory limit parse; secret redaction; schema absent fail startup.

**Runtime/evidence:** container IDs/config, SHOW CREATE, mount inspect, lock/readiness, bounded resource samples in `p3-07/`.

**PASS/DoD:** no second infra stack, no V2 services/data modified, one live owner, migration-before-worker, actual health reflects dependencies and invalidation lag.

### P3-08 — End-to-end acceptance and handover

**Objective:** Prove complete SUMO-to-Silver correctness and publish Phase 4 reader contract.

**Preconditions:** P3-01..07 pass; latest Phase 2 upstream contracts verified; actual runtime resources available.

**Files:** acceptance harness/tests under `de/tests/silver/` and `de/tests/integration/`, docs acceptance/handover; no implementation of Gold.

**Steps:** clean and V2-upgrade migration; run deterministic source fixtures; run actual SUMO/Raw/Bronze/Silver on shared Compose; reconcile every event/fact/dimension/lineage/availability and invalidation; exercise restart and replay; capture resource/backlog/latency; independently query canonical Gold-facing reader; reconcile all matrix rows.

**Runtime evidence:** `de/artifacts/silver_v1/acceptance/<run-id>/` with JUnit XML, source snapshot, manifest/catalog hashes, source-to-Silver oracle, migration/mount/health/resource reports, and replay/invalidation state. Never fabricate PASS from an unexecuted test.

**PASS/DoD:** all acceptance matrix required assertions pass; zero unexplained events/facts; exact topology-derived counts and identity/content oracles; zero unavailable-to-zero conversions; no known-conflicted row in canonical output; restart/replay live isolation; migration preservation; actual runtime ready/converged; handover includes precise reader and invalidation interfaces.

## Dependencies and execution order

```text
P3-02 ─┐
       ├─ P3-03 ─ P3-04 ─ P3-05 ─ P3-06 ─ P3-07 ─ P3-08
P3-01 ┘
```

P3-01 adapter unit work and P3-02 archive work can proceed in parallel by one implementation session in separate commits, but source-dependent P3-01 acceptance waits for Bronze interface. P3-04 availability modeling can be specified alongside P3-03; schema freeze before P3-05. P3-07 Compose can be prepared after migration interface. P3-08 depends on all runtime packages and current Phase 2 interface verification.

## Phase 4 handover contract

Gold consumes only Silver canonical reader methods: bounded valid observation/fact pages; run/topology/binding dimensions; per-metric nullable value/availability/presence; source lineage; source hashes/revisions; and validity/invalidation sequence/high-water. The reader overlays SQLite event validity and Bronze current state, fails closed when validity is unknown/stale, and never exposes raw append-only facts directly as current truth. Gold computes coverage and 60s/300s windows in Phase 4. It must suppress all facts for conflicted event IDs, carry tombstone/invalidation references into window recomputation/revision, segment by run/digest and preserve scenario changes. Handover lists archive retention, SQLite mount, read high-water protocol, replay cutoff limits and the approved Phase 4 reconciliation policy.

