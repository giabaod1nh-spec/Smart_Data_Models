# Master Implementation Plan — Data Engineering V1

**Plan status:** READY as the authoritative planning baseline; implementation remains gated by the decisions and blockers listed in Section 10.

**Audit basis:** `origin/final` at `803c6e421ec8cd00f14ea394fd338fef785b9157`.

**Local workspace note:** the checked-out working tree remains at `e4c2a2545920768085acdde8e87cb8667a8857e7`. The later `origin/final` commit was inspected with Git object access; no application source was modified by this review.

## 1. CURRENT SOURCE STATE

### Repository and upstream V1

The latest fetched `final` branch contains:

```text
e4c2a25  feat(pipeline): replace V2 publishing with V1 durable Kafka pipeline
803c6e4  feat(pipeline): refactor projector from V2 consume to V1 consume
```

Implemented upstream V1 components include:

- `contracts/events/intersection-observation-v1.schema.json`
- `contracts/traffic_observation.py`
- Canonical snapshot and immutable V1 observation construction
- Event identity: `sha256("1|simulationRunId|intersectionId|cycleSequence")`
- Payload hash: SHA-256 of canonical JSON payload
- Topic: `traffic.observations.v1`
- Kafka key: `simulationRunId:intersectionId`
- Durable SQLite Outbox with cycle-atomic append
- Kafka `acks=all`, idempotence, retries, and bounded in-flight delivery
- Run manifest, active-run registry, archived topology catalog, and V1 topology digest
- Exact participant-set validation before outbox persistence
- Frozen-byte retry and outbox recovery tests

The following DE capabilities are still missing:

- V1 Raw consumer and ClickHouse storage
- V1 Bronze observation materialization
- V1 Silver direct observation mapper and topology dimensions
- V1 Gold readers and nullable/late-data semantics
- V1 DE Compose chain and migrations
- Real V1 DE-to-Dashboard acceptance evidence

### V1 Projector: implemented, independent from DE

The latest source adds `Visualize/integration/projector/v1_runtime.py`, `v1_store.py`, `v1_projection.py`, `v1_orion.py`, and `Visualize/tools/v1_projector_consumer.py`, with Phase 4 evidence and tests.

Compose now runs `orion-projector-v1` on `traffic.observations.v1`, with its own group/database and read-only run-archive mount. It resolves the selected active run, reconciles Orion entities, validates topology, deduplicates event IDs, and applies V1 observations.

This is upstream evidence only. DE must not use Orion state, Projector ledgers, or Projector active-run filtering for historical analytics.

### Lifecycle-event inconsistency review

#### Finding: NOT REACHABLE IN ACTIVE V1 RUNTIME

The earlier claim that the active V1 publisher publishes `TrafficSimulationRunStarted` into the observation topic is not confirmed by the actual call path.

Evidence:

- `Visualize/app/traci_runner.py:301-304` enables V1 mode.
- `Visualize/app/traci_runner.py:532-536` constructs `V1DurablePublisher`.
- `Visualize/app/traci_runner.py:721-731` calls `freeze_cycle(...)` then `persist_frozen(...)`.
- `Visualize/integration/kafka/v1_publisher.py:45-46` overrides `append_observation_cycle()` and raises, requiring the freeze/persist path.
- Current V1 tests assert frozen rows use `traffic.observations.v1` and contain no `TrafficSimulationRunStarted` payload.
- The inherited `_append_run_started_durable()` remains reachable from legacy/base publishing methods, but not from the active V1 path.

This is not a confirmed active contract violation and is not a DE coordination blocker. It remains a latent maintenance hazard: future V1 code must not call the base observation-cycle method, and Raw must quarantine lifecycle events if they ever appear on the observation topic.

V1 has no lifecycle events for DE. Run status is obtained from the run manifest and archive package, not Kafka.

### Current DE and runtime state

Current DE is still V2:

- Raw validates `TrafficEntityObserved` and `TrafficSimulationRunStarted`.
- Default topic/group are `traffic.entity-events.v2` / `de-kafka-raw-v2`.
- Bronze uses V2 entity/run tables.
- Silver expects NGSI-LD `VehicleSensor`, `TrafficLight`, `Intersection`, `Camera`, and `RunStarted` payloads.
- Silver has V2 defaults such as `waiting_vehicle_count DEFAULT 0` and `arrival_rate_pcu_per_sec DEFAULT 0.0`.
- Gold has a mature business contract, but V2-shaped Silver inputs and four-direction assumptions remain.
- Gold locks `allowed_lateness_sec=0`, `watermark_delay_sec=0`, and `MAX_REVISION_SEQ=1`; compatibility with V1 delayed delivery is unproven.

Compose initializes the V1 topic and now has a live V1 Projector, but DE Raw/Bronze/Silver/Gold services remain under `v2-legacy`. `de-migrate` still applies the V2-oriented `002`–`005` chain. No V1 DE consumer or DE run-archive mount exists.

V1 DE can begin independently because the upstream event supplies run, intersection, scenario, cycle, time, digest, and complete state, while the run manifest/catalog supplies participants and topology. DE must not depend on Projector, Orion, or the active-run selector for historical processing.

## 2. CURRENT DE PIPELINE

```text
Kafka: traffic.entity-events.v2
              │
              ▼
V2 Kafka Raw consumer
              │
              ▼
ClickHouse kafka_raw_events
              │
              ▼
V2 Bronze entity/run tables
              │
              ▼
Silver NGSI-LD unwrap and entity routing
              │
              ▼
Gold Runtime: 60s / 300s
              │
              ▼
ClickHouse Gold marts/views
              │
              ▼
Spring Analytics API
              │
              ▼
Dashboard
```

The current path is V2 at Raw classification, Raw columns, Bronze grain, entity hashing, Silver NGSI-LD parsing, entity routes, Camera/RunStarted inputs, defaults, and Gold readers. The separate latest Projector path is:

```text
Kafka: traffic.observations.v1 → V1 Projector → Orion / Realtime context
```

It is not a DE source.

## 3. TARGET V1 DE ARCHITECTURE

```text
SUMO / TraCI
     │
     ▼
Canonical Snapshot → IntersectionObservation V1 → Durable SQLite Outbox
     │
     ▼
Kafka: traffic.observations.v1
     │
     ▼
V1 Raw: all physical deliveries, validation, quarantine, physical ledger
     │
     ▼
V1 Bronze: one logical observation per eventId, semantic identity ledger
     │                         │
     │                         ├── Run manifest
     │                         └── Archived topology catalog
     ▼
V1 Silver: direct facts, topology directions, run/scenario/topology dimensions,
           nullable values, availability, quality and coverage
     │
     ▼
Gold Runtime: V1 readers, topology-specific population, 60s/300s windows,
              approved late-data revisions
     │
     ▼
ClickHouse Gold marts/views → Spring Analytics API → Dashboard
```

Projector/Orion is outside this chain. The Dashboard may use Orion for realtime discovery/context, but that is separate from DE ingestion and historical analytics.

## 4. GAP ANALYSIS

| Area | Current source evidence | Required V1 state | Gap | Action |
|---|---|---|---|---|
| Raw topic | `de/kafka_raw/config.py` defaults to V2 | Consume `traffic.observations.v1` | V2-only config/validator | Add V1 Raw runtime |
| Raw storage | V2-shaped `kafka_raw_events` | Preserve complete V1 envelope/bytes | Missing digest/payload fields | Additive V1 Raw tables |
| Physical delivery | Offset ledger exists | Preserve every delivery and commit safety | Needs V1 group/path | Reuse mechanism with V1 namespace |
| Semantic identity | Not authoritative | One event identity model | No V1 identity materialization | One Bronze semantic ledger |
| Bronze grain | Entity/run events | One logical intersection observation | Wrong grain | Add V1 Bronze tables |
| Duplicates | Physical offset handling | Same event ID counted once | Semantic dedupe absent | Bronze reconciliation |
| Conflicts | No V1 policy | Same event ID + changed envelope is conflict | Missing disposition | Conflict quarantine/block |
| Ordering | Offset-oriented | Cycle/time-aware | Late older cycles risk loss | Retain/reconcile by cycle/time |
| Provenance | V2 metadata | Manifest/catalog verified by digest | No DE resolver | Add run-package resolver |
| Silver | NGSI-LD unwrap | Direct V1 mapping | No V1 mapper | Add V1 facts/dimensions |
| Nulls | Defaults can create zero | Value plus availability | Non-nullable V2 fields | V1 nullable model |
| Directions | Four-direction/default risk | Catalog-derived approaches | Hard-coded cardinality | Use archived catalog |
| Camera | Separate V2 stream | No V1 Camera entity | May remain incorrectly required | Remove/define derived evidence |
| Gold cadence | Compose says 10s | Measured producer cadence | Value is not universal | Measure/configure per deployment |
| Gold lateness | Zero lateness, one revision | Delayed/repeated delivery | Current policy unproven | Review runtime contract |
| Gold null aggregation | Non-nullable inputs; absent totals use `0.0` | Per-metric coverage/nullable output | Correctness gap | Phase 4 contract change |
| Gold storage | Existing tables/views | V1 live + V2 historical isolation | No explicit discriminator | V1 marker/filter or compatibility views |
| Migrations | `002`–`005` V2-oriented | Clean + additive upgrade | No V1 chain | Add restart-safe V1 migrations |
| Compose | DE is V2 legacy | Canonical V1 DE chain | No V1 services/mounts | Add services and health dependencies |
| Spring | Fixed Gold objects, namespace `live` | Preserve API/nullability | Intersection total is primitive `double` | Evidence-based DTO/OpenAPI change |
| Dashboard | Analytics plus Orion/realtime bootstrap | Historical API independent; realtime boundary explicit | No E2E proof | Phase 5 UI/API gates |

## 5. DESIGN DECISIONS THAT MUST BE LOCKED BEFORE CODING

### Decision 1: Lifecycle events

Finding is **NOT REACHABLE IN ACTIVE V1 RUNTIME**. Active code uses `freeze_cycle()`/`persist_frozen()`; the V1 override rejects `append_observation_cycle()`. Keep the observation topic observation-only and quarantine lifecycle events if encountered. No current upstream coordination blocker exists, but this invariant must be tested in future changes.

### Decision 2: One cross-layer identity model

| Concept | Definition | Owner |
|---|---|---|
| Physical delivery identity | `(topic, partition, offset)` | Raw row and physical ledger |
| Semantic observation identity | `eventId` = schema/run/intersection/cycle identity | One Bronze semantic identity ledger |
| Payload integrity | `payloadHash` over canonical payload | Contract validator + Bronze conflict check |
| Immutable envelope integrity | Frozen canonical envelope including identity, time, scenario, digest, and payload hash | Raw bytes + Bronze identity record |

Raw stores every physical delivery and commits offsets only after durable persistence. It does not decide analytical uniqueness. Bronze owns the only semantic ledger. Same event ID plus identical immutable envelope is a valid duplicate and yields one logical observation. Same event ID with any changed payload hash or immutable field is a conflict and yields no analytical fact. Different cycle event IDs are retained even when out of order. Replay applies the same rules in a separate namespace and never edits live disposition.

If a conflict is discovered after the original event has already been accepted or consumed downstream, the disposition must be monotonic and cross-layer consistent: Raw retains both physical deliveries and records the conflict reference; Bronze transitions the semantic identity to `CONFLICTED` and emits a durable reconciliation/invalidation work item; Silver prevents the conflicted event from remaining analytically valid and records invalidation or rebuild evidence; Gold does not silently continue serving the affected materialization, but marks the affected window conflicted or recomputes it under the approved revision policy. A later identical retry cannot clear a conflict. The exact physical mechanism for Silver tombstones versus deterministic window rebuild, and the serving-view behavior while reconciliation is pending, is a mandatory Phase 2 planning-gate decision; the invariant that no layer serves a known-conflicted observation is not optional.

### Decision 3: Canonical V1 storage/isolation

- **Raw:** additive `*_v1` tables for observations, quarantine, and replay mirrors; preserve full envelope, bytes/JSON, payload hash, digest, Kafka lineage, status, and conflict reference. V2 Raw is untouched.
- **Bronze:** additive V1 observation, semantic identity, conflict, quarantine, and replay tables. Logical key is `eventId`; physical lineage remains one-to-many.
- **Silver:** additive V1 fact/dimension tables, not in-place mutation of V2 entity tables. Include event ID, digest, run provenance, nullable values, availability, quality, and lineage.
- **Gold:** preserve formulas and serving meanings; add an explicit V1/V2 materialization discriminator or equivalent V1 compatibility views. V1 live output must be selected explicitly; V2 history must not enter V1 windows.
- **Quarantine/replay:** additive V1 tables and `replay:<id>` namespaces; never write live rows during replay.
- **Checkpoints/ledgers:** separate V1 artifact paths such as `de/artifacts/v1/{raw,bronze,silver,gold}`; never reuse V2 checkpoint/lock/ledger files.
- **Kafka groups:** new V1 groups such as `de-v1-raw`; never reuse `de-kafka-raw-v2`.
- **Clean install:** V1 migrations create all V1 objects without needing V2 history.
- **Existing install:** V1 migrations are additive and restart-safe; V2 tables/topics/groups/artifacts remain intact; no destructive conversion in initial cutover.

### Decision 4: Run/topology provenance

- Event envelope is authoritative for run, intersection, scenario, cycle, simulation time, and topology digest.
- The run's `run_manifest.json` is authoritative for participants, generation, status, configuration/profile/threshold metadata.
- The run's archived `network_topology_catalog.json`, verified by digest, is authoritative for expected approaches and topology interpretation.
- Cadence is measured from event simulation-time deltas and checked against producer/run configuration; 10 seconds is not universal.

Archive temporarily unavailable means retry without checkpoint advancement. Missing/malformed archive means provenance quarantine/block; never use current topology. Digest mismatch, missing required direction, and unexpected direction prevent Silver/Gold output. Older runs use explicit replay/run scope and their own archives, never the active-run selector. Different run/digest versions never share coverage denominators or dimensions.

### Decision 5: Availability/aggregation

- Measured `0` is zero and contributes to coverage.
- `null + UNAVAILABLE` remains null and contributes no numeric value.
- `null + NOT_APPLICABLE` remains null and is excluded from that metric's denominator with an explicit flag.
- Absent optional fields are not silently defaulted to zero.
- Missing directions create missing coverage, not zero rows.
- Intersection totals are numeric only when required source values are sufficiently complete; partial sums are not complete totals.
- Gold coverage is per metric, with valid/expected counts, ratio, status, and flags.
- API nullable values remain null; status/quality communicates unavailability.

Confirmed migration concerns are V2 Silver defaults for waiting/arrival metrics and current Gold intersection aggregation returning `0.0` when no totals exist. V1 must not reuse them without an availability-preserving adapter.

### Decision 5a: Silver V1 output contract and cardinality

The logical V1 Silver grain is fixed as follows:

| Output | Logical grain | Required identity/provenance |
|---|---|---|
| Traffic Observation Fact | One row per `(simulationRunId, topologyDigest, eventId, intersectionId, sourceDirection)` for each direction declared by that event's archived topology | Event ID, cycle, simulation time, payload hash, run generation, source lineage, value and availability for every metric |
| Signal State Fact | One row per `(simulationRunId, topologyDigest, eventId, intersectionId, sourceDirection)` carrying that direction's color plus shared phase/timing state | Same event/run/topology lineage; nullable timing fields remain nullable |
| Intersection State Fact | One row per `(simulationRunId, topologyDigest, eventId, intersectionId)` | Overall state, phase, incidents, spillback/box flags, total-value availability, source lineage |
| Run Dimension | One immutable row per `(simulationRunId, runGeneration)` | Manifest identity, scenario/configuration/threshold provenance, participant set, topology digest, status |
| Topology Dimension / Run Binding | One catalog identity per `topologyDigest`, plus a run binding for `(simulationRunId, topologyDigest)` | Archived catalog hash, expected participants, approach set, topology version, and provenance location |

For the current archived five-intersection fixture, the illustrative cardinality for one complete cycle is five Intersection State Facts and seventeen direction-level Traffic Observation Facts plus seventeen direction-level Signal State Facts. The fixture has approach counts `3, 3, 4, 3, 4` for nodes `A, B, C, D, M`, respectively, and therefore seventeen declared approaches. These are verification examples from the current archive, not implementation constants; subset runs and future catalogs must derive their cardinality from the run archive.

Threshold and configuration provenance is mandatory for historical interpretation. The run dimension/binding must preserve the manifest's profile ID/version, configuration hash, threshold profile, threshold parameters such as `K_JAM`, PCU parameters, network/catalog hashes, and any scenario-specific configuration used to derive traffic state. A current deployment configuration must never be substituted when replaying an older run.

### Decision 6: Camera and RunStarted

V1 has neither a Camera entity nor Kafka lifecycle events. Camera is not required for V1 watermark/source streams unless a separately specified V1 source exists. Camera output is omitted or explicitly derived under a documented/tested contract; static Camera entities are not fabricated. Run dimensions come from the manifest/catalog, not invented RunStarted events.

### Decision 7: Gold late-data policy

Gold formulas and definition version remain locked unless an oracle proves a formula defect. However, `ALLOWED_LATENESS_SEC=0`, `WATERMARK_DELAY_SEC=0`, `MAX_REVISION_SEQ=1`, and V1's possible older/repeated late deliveries prove that the current delivery policy is not yet certified for V1. Phase 4 must approve either a measured close delay or repeated deterministic revisions until a documented stability/termination condition. This changes finalization/revision mechanics, not business formulas.

### Decision 8: Spring/Dashboard boundary

Spring reads Gold objects in namespace `live`, filters by run/scenario/window/definition, and maps quality/freshness. Direction DTO numeric fields are nullable, but `IntersectionWindowDto.avgTotalVehicleCount` is primitive `double` and its mapper is non-null. If V1 totals can be unavailable, Phase 4 must make this field/OpenAPI nullable or prove a non-null contract; null must not become zero.

Dashboard run/scenario context may come from Realtime/Orion or manual filters. Orion is a realtime UI dependency, not DE input. Phase 5 must test Analytics with Projector/Orion stopped, full UI with them active, and clear degraded behavior when realtime discovery is unavailable.

## 6. MASTER IMPLEMENTATION PLAN

| Phase | Goal | Main scope | Input | Output | Exit criteria |
|---|---|---|---|---|---|
| 1 | V1 Raw ingestion | Contract gate, validator, additive Raw schema, physical ledger, consumer, quarantine, health | Kafka V1 | Durable V1 Raw/quarantine | Real V1 bytes persist and offsets are safe |
| 2 | V1 Bronze materialization | Semantic ledger, observation grain, conflicts, lineage, checkpoint/replay | V1 Raw | One logical Bronze observation/event ID | Duplicate/conflict/restart/replay proven |
| 3 | V1 Silver analytics preparation | Direct mapping, archive resolution, topology, availability, quality | V1 Bronze + archives | V1 Silver facts/dimensions | Full/subset/topology/null cases correct |
| 4 | Gold and serving compatibility | V1 readers, coverage, nullable aggregation, late revisions, API | V1 Silver | V1 Gold marts/views/API | 60s/300s outputs correct/queryable |
| 5 | Runtime cutover and E2E acceptance | Compose, migrations, clean/upgrade, real SUMO, observability, V2 retirement | Complete V1 chain | Canonical V1 DE runtime | Objective gates pass through Dashboard |

### Incremental runtime verification rule

Each phase must introduce and verify the schema migrations, configuration, service/runtime support, health/readiness behavior, artifact/checkpoint paths, and executable verification needed for that phase's capability. Runtime integration is incremental:

- Phase 1 must run the V1 Raw consumer against Kafka and ClickHouse.
- Phase 2 must run Bronze against persisted V1 Raw data, including restart/replay behavior.
- Phase 3 must run Silver against V1 Bronze and mounted run archives.
- Phase 4 must run Gold against V1 Silver and exercise serving/API compatibility.
- Phase 5 owns the final canonical Compose deployment, upgrade/cutover, and end-to-end promotion; it is not the first runtime integration of all prior phases.

No phase may defer its executable migration, configuration, health, or failure-path verification to Phase 5.

### Phase 1 — V1 Raw ingestion

**Goal:** Persist real V1 deliveries without losing bytes, provenance, validation failures, or offset safety.

**Scope:** V1 topic/group; envelope/hash validation; complete payload storage; topology digest; quarantine; physical ledger; batching; durable-before-commit; health/readiness; V1 replay input.

**Source areas:** `de/kafka_raw/`, `de/migrations/`, V1 contracts, `docker-compose.yml`, Raw tests, `de/Dockerfile`.

**Data/schema:** Add V1 Raw, quarantine, and replay mirror tables. Physical ledger is `(topic, partition, offset)`, not semantic identity.

**Runtime support:** Add the V1 migration, topic/group/checkpoint configuration, service wiring, health/readiness checks, and an executable Kafka/ClickHouse verification command in this phase.

**Reuse:** Existing consumer, buffer, pause/resume, commit safety, health API, and ClickHouse repository patterns.

**Verification:** Real Kafka V1; malformed/quarantine; zero/unavailable cases; duplicate physical offsets; crash after persistence before commit; restart; lag/commit health.

**Main Risks:** V1 records may be misclassified because of the legacy validator; offsets may be committed before ClickHouse durability; clean-install and upgrade migrations may diverge; or the V1 consumer may run without the required archive/configuration paths. Prevent these through contract fixtures, failure injection, migration runs against both empty and V2-populated databases, readiness checks, and an executable Kafka/ClickHouse smoke run in Phase 1.

**Exit:** Valid deliveries are durable/recoverable, invalid deliveries quarantined, offsets never advance before persistence, and physical ledger survives restart.

**Dependency:** Upstream V1 contract invariant verified.

### Phase 2 — V1 Bronze materialization

**Goal:** Produce exactly one logical Bronze observation per valid semantic V1 event ID while preserving all Raw delivery evidence.

**Scope:** V1 model; one semantic ledger; immutable envelope comparison; duplicates; conflicts; late/out-of-order cycles; lineage; checkpoint CAS; replay; partial-persistence recovery.

**Source areas:** `de/bronze/`, V1 migrations, Bronze checkpoint/ledger/replay modules, tests, Raw gap tooling.

**Data/schema:** `bronze_observations_v1`, semantic identity/conflict records, quarantine, replay output.

**Runtime support:** Add Bronze migration/configuration, V1 checkpoint and lock paths, service dependency/readiness wiring, and an executable Raw-to-Bronze restart/replay verification in this phase.

**Reuse:** Bronze checkpoints, locks, repository reconciliation, gap detection, replay namespace.

**Verification:** Same ID at multiple offsets; changed payload; out-of-order cycles; restart between insert/checkpoint; replay isolation; multiple runs/digests.

**Main Risks:** A conflict may be discovered only after an observation has already been accepted by downstream processing; a restart may apply different conflict dispositions; or replay may create a second semantic decision. Prevent these through a durable monotonic identity state, downstream invalidation/reconciliation evidence, idempotent conflict transitions, and executable restart/replay tests before Phase 2 exit.

**Exit:** Valid duplicates never duplicate facts; conflicts never produce facts; dispositions survive restart/replay; older cycles remain available.

**Dependency:** Phase 1.

### Phase 3 — V1 Silver analytics preparation

**Goal:** Map complete V1 observations directly into topology-aware facts and dimensions without recreating NGSI-LD.

**Scope:** Direct mapping; direction/signal/intersection facts; incidents; run/scenario/topology dimensions; archive resolver; exact approach set; nullable availability; per-metric quality; Camera policy; replay/checkpoint.

**Source areas:** `de/silver/`, V1 migrations, `contracts/topology/v1_digest.py`, run/archive readers, Silver tests, quality/lineage modules.

**Data/schema:** Additive V1 Silver facts/dimensions containing event ID, payload hash, digest, run generation, lineage, nullable metrics, availability, coverage, and quality flags.

**Runtime support:** Add Silver migrations, archive mount/configuration, V1 source allowlists, checkpoint/ledger paths, health/readiness checks, and an executable Bronze-to-Silver verification in this phase.

**Reuse:** Silver repository/checkpoint/lock, dimension hashing, lineage, failure injection where semantics remain valid.

**Verification:** Five-intersection; subset; fewer-than-four directions; digest mismatch; missing/unexpected direction; zero/unavailable; absent optional; partial total; scenario split; historical replay.

**Main Risks:** Silver may silently recreate V2 defaults, use the current topology instead of the historical archive, emit the wrong fact cardinality, or accept a partial intersection total as complete. Prevent these with archive-unavailable retry tests, digest/direction mismatch quarantine tests, cardinality oracles, nullable-schema assertions, and a runtime Silver smoke against a mounted real run package.

**Exit:** Silver matches payload/catalog, never converts unavailable to zero, never marks partial totals complete, and traces every row to one Bronze event.

**Dependency:** Phase 2.

### Phase 4 — Gold and serving compatibility

**Goal:** Produce correct V1 Gold analytics while preserving locked formulas and the Spring API wherever valid.

**Scope:** V1 readers; topology expected population; cadence; 60s/300s boundaries; missing observations; per-metric coverage; nullable aggregation; watermark; late/repeated late; revisions; run termination/final windows; Camera source removal; V1/V2 isolation; evidence-based Spring SQL/DTO/OpenAPI changes.

**Functional continuity:** Migration acceptance must preserve all currently in-scope Analytics functions backed by the existing Gold artifacts and Spring repositories: intersection window summaries, direction window summaries, traffic comparisons/trends, congestion windows, priority scores/rankings, and signal-operation windows. The network-overview endpoint remains an explicitly disabled/`WHERE 0` scaffold in the current source and must not be presented as newly functional unless separately approved. Each preserved function requires a V1 result comparison against an independent oracle, including empty/partial/nullable cases and quality/freshness fields.

**Source areas:** `de/gold/`, `de/gold_runtime/`, Gold migrations/tests, Spring Analytics repositories/DTOs/mappers/OpenAPI, Dashboard integration tests.

**Data/schema:** Preserve business identities/formulas; add V1/V2 discriminator or compatibility views; add nullable fields only when source evidence requires.

**Runtime support:** Add Gold migrations/configuration, V1 Silver source allowlists, cadence/late-data settings, revision/checkpoint paths, serving probes, and executable Gold/API verification in this phase.

**Required review:** Current zero-lateness/one-revision settings require explicit V1 approval. Formulas remain locked; close/revision mechanics may change.

**Definition-version compatibility:** If V1 changes metric semantics, eligible samples, coverage interpretation, null handling, or aggregation behavior, the existing Gold definition version must not be retained merely to preserve a label. The phase must either prove semantic equivalence and keep the version, or increment the appropriate definition major/minor version, update Gold metric definitions and Spring selection/configuration, and verify that V1 and V2 materializations are not mixed under one definition identity.

**Reuse:** Gold calculators, window IDs, lineage, quality, serving views, revision ledger, Spring allowlists, nullable DTOs.

**Verification:** Actual cadence; topology denominators; scenario changes; `[start,end)` windows; watermark; missing/late/repeated late; final run; isolation; API nullable values.

**Main Risks:** Gold may close a window before delayed V1 observations arrive, calculate coverage from the wrong topology, mix V1 and V2 materializations, or preserve definition `v1.0` after changing eligible samples or metric semantics. Prevent these with independent Gold oracles, late/revision failure injection, run/digest isolation queries, definition-version compatibility tests, and an executable Gold/API runtime smoke before Phase 4 exit.

**Exit:** Independent Gold oracle matches runtime for both windows, approved late policy is followed, and Spring returns compatible results without Projector data.

**Dependency:** Phase 3.

### Phase 5 — Runtime cutover and end-to-end acceptance

**Goal:** Make V1 DE canonical and prove producer-to-Dashboard operation in clean and upgraded environments.

**Scope:** V1 Compose; archive mounts; migration startup; health/readiness; V1 groups/artifacts; clean install; additive upgrade; observability; throughput/lag/backlog evidence; V2 retirement; rollback/replay; Dashboard.

**Final runtime acceptance:** Execute a real SUMO run long enough to produce multiple completed 300-second analytical windows (at least two completed 300-second windows must be evidenced; the simulation horizon, producer cadence, and run-termination policy must be recorded). The same run must verify 60-second and 300-second outputs, independent Gold calculation correctness, worker health, processing throughput, consumer lag, backlog stability, API results, and real Dashboard integration. Record the complete runtime configuration, commit, run manifest, topology digest, service logs, health snapshots, metrics, ClickHouse verification queries, API responses, and Dashboard evidence.

**Source areas:** Compose, `de/Dockerfile`, migration runner, deployment files, E2E scripts, integration/Spring/frontend tests.

**Data/schema:** Additive V1 migrations and Gold compatibility. No initial deletion of V2 data/assets.

**Runtime support:** Assemble the canonical Compose dependency graph, final archive/artifact mounts, upgrade path, observability, and cutover controls; this phase promotes the already verified phase runtimes rather than introducing them for the first time.

**Reuse:** Compose health patterns, migration runner, health APIs, Gold acceptance tools, V2 archival tools.

**Verification:** Empty ClickHouse; V2 upgrade; real SUMO/V1 flow; Raw/Bronze/Silver/Gold reconciliation; restart; duplicates/conflicts; late data; Projector/Orion stopped; full UI; V2 live disabled.

**Main Risks:** Cutover may start the wrong consumer group, omit run-archive mounts, leave V2 live processors active, or pass fixture-based checks while the real Dashboard path is incomplete. Prevent these with clean and upgrade deployments, service inventory checks, real SUMO evidence, health/lag/backlog snapshots, Projector-stopped acceptance, and browser/API evidence.

**Exit:** All Section 8 gates pass with stored evidence under the actual local producer rate.

**Dependency:** Phases 1–4.

## 7. PHASE DEPENDENCY GRAPH

```text
Latest-source review and decisions
              │
              ▼
Phase 1 — V1 Raw
              │
              ▼
Phase 2 — V1 Bronze
              │
              ▼
Phase 3 — V1 Silver
              │
              ▼
Phase 4 — Gold/API compatibility
              │
              ▼
Phase 5 — Runtime cutover
              │
              ▼
End-to-End acceptance and handover
```

Only non-runtime scaffolding may proceed in parallel after Phase 1. A shared-contract change requires a Master Plan revision and approval.

## 8. END-TO-END ACCEPTANCE CRITERIA

```text
SUMO → V1 Outbox → Kafka → Raw → Bronze → Silver → Gold
     → ClickHouse marts/views → Spring Analytics API → Dashboard
```

Each gate requires evidence; compilation/unit tests alone cannot complete a phase.

| Gate | What is tested | Required evidence | Objective PASS condition |
|---|---|---|---|
| A Clean deployment | Empty ClickHouse/V1 artifacts | Compose, migration, health, schema logs | All V1 services ready; migrations restart-safe |
| B V2 upgrade | V1 migration over V2 history | Before/after inventory and V2 smoke | V2 assets readable; no destructive conversion |
| C Real producer | SUMO → Outbox → Kafka | Run package, Outbox rows, Kafka records | Bytes/hashes match end-to-end |
| D Raw reconciliation | Kafka, Raw, quarantine, offsets | Reconciliation report | Every tested offset durable/quarantined; no unsafe commit |
| E Bronze identity | Duplicate/conflicting IDs/restarts | Identity ledger and conflicts | Valid duplicate = one fact; conflict = zero facts, stable state |
| F Silver cardinality | Five-intersection/subset | Per-run/cycle/direction queries | Counts match archive; no fabricated directions |
| G Metric correctness | Zero/null/availability/partial totals | Source-to-Silver report | Zero stays zero; unavailable stays null/status; partial not complete |
| H Gold oracle | Independent 60s/300s calculation | Oracle/runtime comparison | Values, coverage, quality, boundaries match |
| I Late delivery | Delayed/repeated late and restart | Late/revision ledger and lineage | Approved policy; no stale overwrite/unbounded work |
| J Isolation | Runs/digests/scenarios/replay | Namespace/dimension queries | No cross-run/digest joins; replay cannot alter live |
| K API | Spring SQL/DTO/null/quality/freshness | API tests and live responses | Contract remains valid; null is not zero |
| L Dashboard | Projector active/stopped | Browser/E2E, screenshots, API traces | Historical analytics works with explicit filters; degradation is clear |
| M Projector independence | DE with Orion/Projector stopped | DE health, Gold rows, API | DE continues independently |
| N V2 retirement | V2 live services disabled | Compose inventory and V2 query | No V2 live processing; history remains |
| O Throughput/backlog | Actual producer rate, lag, latency | Metrics over documented interval | Backlog stable/non-increasing; no invented target |
| P Long-run continuity | Real SUMO run with multiple completed 300s windows | Runtime configuration, run package, worker health, lag/backlog metrics, Gold oracle, API responses, Dashboard evidence | At least two completed 300s windows and corresponding 60s windows are correct, workers remain healthy, lag/backlog are stable under the measured producer rate, and API/Dashboard results are usable |

Evidence records commit, configuration, run IDs, digests, commands, queries, timestamps, and PASS/FAIL.

## 9. LEGACY V2 RETIREMENT STRATEGY

After Phase 5 acceptance, V1 DE becomes the default analytics path. V2 DE services and groups are disabled from live processing but remain in an explicit `v2-legacy` rollback/replay profile during the retention period. `orion-projector-v1` remains independently deployable.

Preserve V2 topic evidence, Raw/Bronze/Silver/Gold tables, groups, replay/audit tooling, migrations `001`–`005`, artifacts, and reports. Delete V2 topic data, groups, tables, code/profile, or artifacts only after a separately approved retention/archive and destructive-change review. Initial V1 migration is additive.

## 10. RECOMMENDED FIRST PHASE TO IMPLEMENT

### Recommended phase

Phase 1 — V1 Raw ingestion.

### Why it is first

Raw establishes complete Kafka delivery history, immutable bytes, validation, quarantine, physical offset safety, and evidence required by every downstream phase.

### Source to re-read before the phase-specific plan

- Latest `origin/final` source and relevant commits
- V1 schema, `contracts/traffic_observation.py`, Kafka topic/delivery contracts
- `Visualize/app/traci_runner.py`
- V1 publisher, durable publisher, Outbox store, and worker
- Run manifest, active registry, V1 topology digest
- V1 Projector runtime only for archive/error semantics
- `docker-compose.yml`
- `de/kafka_raw/*`
- `de/migrations/002_create_kafka_raw_events.sql`
- Current V1 architecture tests

### Implementation boundary

```text
traffic.observations.v1
→ V1 envelope validation
→ V1 Raw or quarantine
→ durable ClickHouse persistence
→ physical delivery ledger
→ safe Kafka offset commit
```

Do not implement Bronze, Silver, Gold, Spring, Dashboard, Projector, or V2 deletion in Phase 1.

### Decision timing and phase gates

The following decisions must be approved before Phase 1 because they affect Raw correctness, persistence, or safe cutover of the first consumer:

- V1 topic is observation-only and lifecycle events are quarantined.
- Physical delivery identity versus Bronze semantic identity ownership.
- V1 Raw table/quarantine/replay naming, migration boundary, consumer-group identity, checkpoint path, and clean/upgrade safety.
- Required run-manifest/catalog access behavior for Raw validation, including retry versus permanent provenance failure.

The following decisions must not block Phase 1 and are resolved at the relevant later planning gate:

- Phase 2: exact downstream conflict invalidation/tombstone versus deterministic rebuild mechanism and pending-conflict serving behavior.
- Phase 3: final Silver physical table names, fact-column mapping, topology/run dimension implementation, and Camera derivation policy.
- Phase 4: Gold V1/V2 discriminator or compatibility-view design, per-metric nullable aggregation, late-data close/revision policy, definition-version changes, and nullable Spring API fields.
- Phase 5: canonical Compose cutover ordering, long-run acceptance interval/observability thresholds, and Dashboard realtime degradation evidence.

Detailed Gold, Spring, Dashboard, and throughput decisions may be refined later without changing the Phase 1 Raw boundary, provided they do not contradict this Master Plan.

### Mandatory execution and handover governance

Every future Agent phase must:

1. Re-read this Master Plan.
2. Inspect the latest working source and commits.
3. Verify prior phase acceptance evidence.
4. Produce a detailed plan for only the current phase.
5. Wait for plan approval.
6. Implement only approved scope.
7. Run verification and record evidence.
8. Record files, migrations, commands, tests, rollback, and actual deviations.
9. Update implementation status.
10. Produce a handover contract for the next phase.

Persistent documentation under `docs/DEv1/` should contain:

```text
Master_plan_DE_V1_.md       authoritative roadmap
implementation-status.md    phase status/evidence index
decisions.md                approved decisions/contract changes
blockers.md                 confirmed blockers/owners
phase-1/plan.md             approved detailed plan
phase-1/acceptance.md       phase evidence
phase-1/handover.md         next-phase contract
phase-2/...                 same pattern later
```

A later Agent must not silently diverge. New source evidence requires an explicit Master Plan revision, impacted-phase list, approval, and commit reference.

### Summary of changes from the previous Master Plan

- Updated audit basis from `e4c2a25` to fetched `origin/final` `803c6e4`.
- Added the implemented V1 Projector and clarified DE independence.
- Reclassified lifecycle publishing as `NOT REACHABLE IN ACTIVE V1 RUNTIME` with call-path/test evidence.
- Separated physical delivery identity, semantic event identity, payload integrity, and immutable envelope integrity.
- Assigned one semantic ledger to Bronze and retained Raw's ledger for physical delivery safety.
- Locked additive V1 storage and V2 preservation for clean-install and upgrade cases.
- Defined archive failure, digest mismatch, direction mismatch, replay, and topology-version behavior.
- Added zero/null/availability/partial-total rules across all layers.
- Identified Gold nullable-total and zero-lateness/one-revision limitations as required Phase 4 contract work.
- Verified Spring and Dashboard boundaries, including Orion's realtime-only role and the nullable intersection-total issue.
- Added objective acceptance gates and persistent cross-phase governance.

### Remaining confirmed blockers

1. V1 DE ingestion and downstream V1 DE stages are not implemented.
2. V1 storage migrations and canonical V1 DE Compose services do not exist.
3. Gold zero-lateness/one-revision policy is not proven compatible with V1 late delivery.
4. Existing Silver/Gold defaults and non-nullable fields can violate V1 unavailable semantics.
5. Spring's intersection average-total DTO currently uses primitive `double`; nullable API handling needs a Phase 4 decision.
6. Dashboard realtime discovery remains Orion/Projector-dependent even if historical Analytics is DE-independent.
7. Checked-out local HEAD is behind `origin/final`; implementation sessions must update or explicitly use the reviewed commit.

### Decisions still requiring approval

- Exact V1 table names and migration numbering.
- Gold V1/V2 discriminator versus compatibility-view implementation.
- V1 late-data close/revision policy.
- Camera behavior: omitted/evidence-only versus separately specified derivation.
- Nullable API contract for intersection totals and other V1-unavailable values.
- Throughput/backlog acceptance interval and threshold, measured from the real local producer rather than invented.

### Master Plan readiness verdict

**READY** as the authoritative planning baseline. It is ready for a future Phase 1-specific planning session, but implementation must wait until the approval decisions above are recorded and the latest source is re-read.

### Recommended next action

Approve the outstanding decisions, update the working branch to the reviewed `origin/final` baseline, and produce—not implement—the detailed Phase 1 V1 Raw plan.

Do not start Phase 1 implementation in this session.
