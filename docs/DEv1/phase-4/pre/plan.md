# Phase 4A-Pre implementation plan

**Owner approval:** Option A, the Section 10 contract, and P4Pre-01–08 implementation are authorized at the approved planning baseline `a2f3ac6b98c7ca325213b617e603825481202023`. Stage A uses the Silver public API and a test consumer. Gold production integration and isolated `gold_v1` state are Core work.


**Status:** approved implementation plan. Use [implementation-status.md](implementation-status.md) for the package and evidence gates.
**Contract:** Option A and the initial request and server bounds are approved in [decisions.md](decisions.md).
**Evidence baseline:** Phase 3 commit `443f40e3ddde0f9cf657785b3fbf1851281897ea`.

**Final revision:** [contract-design.md](contract-design.md) defines the exact public API and verified-prefix algorithm. [capacity-and-backpressure.md](capacity-and-backpressure.md) records the end-to-end source audit, `NOT MEASURED` result, and separately gated remediation. Acceptance is split into Pre Stage A and Phase 4A-Core Stage B; neither implements application code in this plan.

## Guardrails

- Silver owns canonical meaning and only Silver's public API serves Gold.
- Change and invalidation sequences remain independent and contiguous.
- Every successful change batch has one justified disposition for every sequence.
- No cross-database transaction, arbitrary as-of snapshot, or current-row historical proof is claimed.
- Memory, entry count, encoded bytes, and individual bundle size are bounded.
- Work proceeds in order; later packages depend on earlier contract gates.
- Phase 3 semantics and existing Gold-disabled/unpublished boundary remain intact.

## Dependency order and packages

### P4Pre-01 — Contract freeze

- **Inspect/change:** `docs/DEv1/phase-3/decisions.md`, `handover.md`; `docs/DEv1/phase-4/decisions.md`, `plan.md`, `blockers.md`; the four Phase 4A-Pre design documents. No runtime files.
- **New files:** none beyond the planning documents already requested.
- **Dependencies:** owner review of Option A, dispositions, no-as-of semantics, binding, highwaters, caps, and invalidation ordering.
- **Failure modes:** unresolved meaning for superseded accepted rows; ambiguity in run inventory, event-free runs, journal retention, or receipt serialization.
- **Tests/evidence:** decision checklist signed; examples for normal acceptance, conflict, append race, binding replacement, and gap are unambiguous and internally consistent.
- **Acceptance:** versioned contract fields, typed errors, transition table, receipt digest serialization, maximum single-event behavior, and owner-set workload/caps documented.
- **Phase 3 compatibility:** records no behavior change; all current authority and no-prune assumptions retained.

### P4Pre-02 — Public DTO/API

- **Inspect/change:** `de/silver/canonical_reader.py`, `de/silver/__init__.py` (if public exports exist), `de/bronze/checkpoint_store.py` reader interface, `de/silver/source_binding.py`, and Silver/Gold docs. Add the API only in Silver's public module.
- **New files:** only if the DTO/API can be cleanly isolated (for example `de/silver/incremental_contract.py`); avoid a second canonicalization implementation.
- **Dependencies:** P4Pre-01.
- **Failure modes:** Gold depends on private `_boundary`, opaque tokens, internal SQL, or incompatible DTO details; API leaks Bronze implementation as public authority.
- **Tests/evidence:** serialization round trips, strict version parsing, DTO immutability, public import test, compatibility tests for existing `CanonicalReadQuery`/page behavior.
- **Acceptance:** stable versioned request/result types; typed errors; bounded request fields validated; no change to current canonical read semantics.
- **Phase 3 compatibility:** additive API only; existing reader users continue to work.

### P4Pre-03 — Sequence and disposition verification

- **Inspect/change:** `de/silver/processor.py`, `de/silver/checkpoint_store.py`, `de/silver/source_binding.py`, `de/bronze/checkpoint_store.py`, `de/bronze/clickhouse_repository.py`, migrations 007/008, and focused Bronze/Silver tests. Reuse existing persisted source journal and frozen obligations; do not add a competing validity authority.
- **New files:** only a focused verifier module if separation is necessary and it consumes established repository interfaces.
- **Dependencies:** P4Pre-02 DTO definitions.
- **Failure modes:** equating current `silver_v1_event_validity=VALID` to all prior accepted sequences; missing/corrupt journal, current identity mismatch, invalid ACCEPTED→CONFLICTED linkage, fabricated skip disposition, archive corruption.
- **Tests/evidence:** accepted then conflicted; accepted still current; conflict provenance; corrupted/missing bundle evidence; missing Silver obligation; impossible transition; independent oracle compares raw fixture journal, persisted obligations, CH facts and emitted disposition.
- **Acceptance:** exactly one disposition per contiguous sequence; conflict history never leaks a superseded bundle; every unverifiable sequence fails closed without receipt.
- **Phase 3 compatibility:** preserve worker's `SUPERSEDED_BY_CONFLICT` handling and fault-on-integrity-error behavior; no journal/schema rewrite.

### P4Pre-04 — Frozen boundary and receipt

- **Inspect/change:** `de/silver/canonical_reader.py`, `de/silver/source_binding.py`, Bronze sequence reader, and receipt/version documentation. Use allocator highwaters rather than the reader's present `max(sequence)` as the boundary authority.
- **New files:** none unless receipt serialization is isolated from canonicalization as in P4Pre-02.
- **Dependencies:** P4Pre-03.
- **Failure modes:** included source identity/binding changes during the page; receipt issued for incomplete pages; accidental reliance on live H staying globally unchanged. Unrelated appends above captured H are expected and deferred, not stale-batch failures.
- **Tests/evidence:** frozen H with append above H; append/conflict touching included identity; source replacement; exact interval and digest validation; receipt absent on stale/error paths.
- **Acceptance:** entries exactly cover `(after, through]`; appends above H are deferred; stale relevant changes produce no receipt; receipt fields and digest reproduce deterministically.
- **Phase 3 compatibility:** source checks remain stricter or equal; no existing Bronze atomicity claim is broadened.

### P4Pre-05 — Multi-run discovery, bootstrap and recovery

- **Inspect/change:** `de/silver/archive_resolver.py`, `de/silver/source_binding.py`, `de/silver/canonical_reader.py`, Bronze journal/allocator/index query paths, and Phase 3 run identity/archive tests. Keep the namespace-global change and invalidation journals as the only sequence authorities.
- **New files:** none unless a Silver-only run discovery iterator is needed. A run registry is not added without owner approval and evidence that journal discovery cannot meet the required scope.
- **Dependencies:** P4Pre-04 and the frozen `SourceDiscovery`/binding DTO from P4Pre-02.
- **Failure modes:** per-run filtering skips global sequences; initializing at latest high-water silently loses history; mixing run generations or topology digests; late event assigned to wrong run; event-free run implied discoverable; missing retained journal history; ordinary restart rescans all history; source replacement silently resets progress.
- **Tests/evidence:** multiple retained runs with different topology digests/generations; interleaved and late events; run reuse/new generation; archived identity verification; concurrent run creation during paging; cold replay from zero; partial-ack restart; source replacement; missing earliest/next sequence; explicit empty-run limitation; retention/backup assumptions.
- **Acceptance:** every in-scope global sequence is accounted for through bounded pages; no latest-only cursor initialization; run identity is immutable and checked against archive/manifest evidence; unchanged-binding restart resumes without a full-history scan; event-free-run limitation and journal retention are explicit.
- **Phase 3 compatibility:** retains sequence-zero bootstrap, immutable archive identity checks, existing run/generation semantics, and the no-prune/no-rewind assumption.

#### Required bootstrap and run-discovery procedure

1. **Create a binding.** Call `discover_canonical_source` with the namespace and no expected binding ID only for a new consumer binding. Silver verifies the configured Bronze source identity, schema/reader versions, allocator values, and the retained change and invalidation journals from sequence 1 through their captured highwaters using bounded streaming pages. A source binding ID includes namespace, canonical source path/file identity, schema fingerprint, and Bronze version; highwaters are not part of that ID. Discovery reports `bootstrap_required=true` until continuity/linkage checks succeed. A failed or incomplete check yields no usable bootstrap result.
2. **Initialize cursors at zero.** Only after successful cold verification may the consumer durably create its binding record and initialize its exclusive change and invalidation cursors to `(0, 0)`. Never initialize either cursor to the latest high-water. This means full-namespace coverage starts at sequence one; any explicitly scoped business processing must still durably account for every out-of-scope sequence as not processed and cannot call itself full-namespace completion.
3. **Pin immutable run identity.** For every first-seen journaled event, verify its run ID, generation, topology digest, and immutable manifest/archive identity through Silver's existing resolver and obligations. Persist these identity fields with the consumer's per-sequence durable intent. A later event for an existing run must match that run generation's immutable identity; legitimate run-ID reuse is a new generation only when the upstream journal/archive authority says so. Never use run ID as a filter for advancing the namespace-global sequence cursor.
4. **Catch up historically in bounded pages.** Request pages from the saved exclusive cursor, validate receipt/version/binding, and durably record sequence dispositions/work intents before advancing the consumer cursor. Interleaved historical runs are naturally covered by sequence order. A late observation for a previously discovered run is processed at its later global sequence and checked against the already pinned run identity; it is not inserted into an earlier sequence position.
5. **Discover concurrent runs.** A run that starts while a consumer is processing earlier runs becomes discoverable when its first journaled observation or transition is appended. It appears in a later page after the global cursor reaches that sequence. Appends above a page's frozen high-water are deferred to a later request; they cannot change the current receipt. There is no separate run-list snapshot or promise that all currently active runs are known.
6. **Resume after partial processing.** Persist receipt/disposition/work intent and cursor advancement atomically in the consumer's local durable transaction. If the transaction did not commit, restart from its prior cursor and replay the same source interval; if it committed, restart from the stored returned-through sequence. Replays under a newer boundary may legitimately change effective disposition for an unacknowledged accepted event, but immutable transition identity must match. A committed sequence with a different immutable transition hash is a source integrity fault.
7. **Handle source replacement explicitly.** If source binding ID, namespace, schema/version, file identity, or monotonic sequence history changes, stop without resetting cursors. Owner authorizes a new binding generation and a governed rebootstrap/reconciliation path. Do not merge old and new source histories or call replacement a normal restart. Historical reconciliation/backfill is a separate isolated operation.
8. **Retain journal evidence.** The implementation depends on Bronze change/invalidation journals and required Silver obligations remaining available from sequence 1 for cold bootstrap, and on no prune/rewind for ordinary resume. Missing history is `RETENTION_GAP` and blocks progress. Document backup/restore and retention monitoring; any future pruning/checkpoint compaction protocol is out of scope and needs its own design and owner approval.

Normal unchanged-binding restart uses `verify_bronze_source(full_history=False)` plus cursor range checks, source identity checks, and exact next-page continuity; it does not prove arbitrary integrity of already-consumed rows against later manual edits. That residual reliance on immutable retained history/no rewind is explicit. Current Silver startup calls full-history verification by default, so implementation must distinguish cold binding/rebootstrap from ordinary resume without weakening cold verification.

### P4Pre-06 — Bounded consumption

- **Inspect/change:** canonical reader paging and bundle assembly, CH reconciliation access paths, DTO byte serialization, Silver tests/bench harness. Remove complete-scan accumulation from incremental path only; do not alter the legacy scan API without separate compatibility review.
- **New files:** a focused streaming/batch test harness only if existing tests cannot express memory/load evidence.
- **Dependencies:** P4Pre-05.
- **Failure modes:** unbounded list/staging, one record exceeding cap, hidden N+1 evidence queries, encoded-size estimate differs from wire bytes, no backpressure.
- **Tests/evidence:** page entry/byte caps, maximum bundle, oversized bundle fault without cursor movement, peak RSS across growing history, query-count and serialization-size evidence.
- **Acceptance:** measured memory is bounded by configured in-flight batch/staging caps plus fixed overhead; no history-sized accumulation; overflow fails closed.
- **Phase 3 compatibility:** existing complete-scan method and token behavior remain compatible; incremental path is additive.

### P4Pre-07 — Invalidation and recovery contract

- **Inspect/change:** Silver existing `read_invalidation_page`, checkpoint store, source binding, Bronze invalidation provenance and mirror logic; Gold proposed SQLite runtime state design/docs only. The new Silver API is additive; preserve the legacy reader behavior. No Gold runtime code belongs to Pre.
- **New files:** none required for Silver unless a typed invalidation receipt needs a dedicated DTO module.
- **Dependencies:** P4Pre-04, P4Pre-05, P4Pre-06.
- **Failure modes:** treating invalidation as change acknowledgment; lost invalidation cursor; conflict race between data staging and local dirty intent; stale binding silently resets cursor; incomplete batch acknowledged.
- **Tests/evidence:** Stage A test-only harness covers independent cursors, invalidation before/after change receipt, fixed Pi ordering, crash before/after its simulated durable acknowledgment, and replay. Actual Gold SQLite rollback, suppression-before-work and dirty-CAS races are Stage B in Phase 4A-Core.
- **Acceptance:** Pre proves Silver's additive feed and receipt behavior with the harness; it hands Gold the rule that each local cursor commits atomically with its complete obligations, suppression survives independently of work rows, and source change never silently resets progress. Actual Gold enforcement is not claimed until Stage B.
- **Phase 3 compatibility:** preserves Silver invalidation-before-change processing, independent feed sequence and current local validity semantics.

### P4Pre-08 — Integration and sustained-load acceptance

- **Inspect/change:** public Silver contract, Gold adapter/checkpoint integration from Phase 4A, all focused Bronze/Silver/Gold tests, Compose only for approved integration environment. This plan does not authorize those runtime changes now.
- **New files:** reproducible workload/measurement scripts and result artifact only if no current harness exists; keep them separate from runtime code.
- **Dependencies:** P4Pre-01 through P4Pre-07; owner-defined representative workload, retention assumptions, resource caps and throughput/backlog target.
- **Failure modes:** steady-state backlog growth, batch cap starvation, memory drift, incorrect dispositions under conflict churn, divergent replay, Phase 3 regression.
- **Tests/evidence:** acceptance matrix in `acceptance.md`; sustained run records entries/sec, bytes/sec, RSS, CPU, I/O, DB query counts/latencies, and backlog slope with workload definition and environment.
- **Acceptance:** exact oracle match and phase3 regressions pass; measured resource caps are respected; backlog meets owner-defined target. No target is fabricated by this plan.
- **Phase 3 compatibility:** existing Phase 3 tests are mandatory regression gates; no Bronze/Silver semantic relaxation to improve throughput.

## Code areas likely in implementation scope

| Area | Likely disposition |
|---|---|
| `de/silver/canonical_reader.py` | Add a bounded incremental read surface while retaining legacy scan/page API. |
| `de/silver/source_binding.py` | Expose/reuse verified stable binding and allocator highwaters for the public receipt. |
| `de/silver/checkpoint_store.py` | Read per-sequence obligations/cursors; schema change only if proof identifies an actual missing durable fact. |
| `de/silver/processor.py` | Reuse existing transition processing; fix only verified gaps in obligations/sequence verification. |
| `de/bronze/checkpoint_store.py` | Add a narrow allocator-bound read method only if current reader API cannot provide the needed consistency; do not expose it to Gold. |
| `de/bronze/clickhouse_repository.py` | Reuse current exact evidence reconciliation; no authority change. |
| Migrations | None expected. Any proposed schema change requires a demonstrated missing evidence fact, migration and rollback plan, and separate approval. |

## Phase 4A dependency and exit

Phase 4A baseline adapter, scheduler mechanics, and generic state design can proceed independently where current plan allows. Production source progress, source cursor advancement, replay from source, and activation remain gated on P4Pre-08 and the existing P4-D10/P4-D30/P4-D31 owner decisions. The work exits only when the public contract is implemented, acceptance evidence is recorded, and owners accept resource budgets and residual eventually-consistent invalidation window.

## Final implementation-readiness gate

The ordered packages above are the implementation handoff and each lists existing files, dependencies, failure modes, tests/evidence, acceptance and Phase 3 compatibility. Their common independent oracle is the Phase 3 Bronze journal + Silver obligations + ClickHouse/archive evidence; the Stage A harness is not production Gold. Owner-frozen page byte/single-entry/time caps remain explicit inputs and are not to be guessed by the coding agent. The initial `max_entries<=100` is a configurable conservative API ceiling, not a throughput target.

| Package | Handoff state | Exact gate |
|---|---|---|
| P4Pre-01 Contract freeze | **APPROVED** | Contract v1 and initial resource limits approved by the owner. |
| P4Pre-02 Public DTO/API | **APPROVED FOR EXECUTION** | Add the three methods and versioned DTOs while retaining Phase 3 reader behavior. |
| P4Pre-03 Sequence/disposition verification | **APPROVED FOR EXECUTION** | Compare per-sequence evidence against the independent fixture oracle. |
| P4Pre-04 Frozen batches/receipts | **APPROVED FOR EXECUTION** | Validate captured bounds, serialized bytes, receipt vectors and relevant final identities. |
| P4Pre-05 Multi-run/bootstrap/recovery | **APPROVED FOR EXECUTION** | Verify sequence-zero bootstrap, run identity, resume and explicit source replacement handling. |
| P4Pre-06 Bounded consumption | **APPROVED FOR EXECUTION** | Verify page caps, query costs and bounded memory across retained-history growth. |
| P4Pre-07 Invalidation/recovery contract | **APPROVED FOR EXECUTION** | Verify additive invalidation paging and durable recovery using the Stage A consumer. |
| P4Pre-08 Stage A correctness/feed capacity | **APPROVED FOR EXECUTION** | Stage A includes the approved five-intersection profile, 10 minutes sustained, two minutes burst, five minutes recovery, and 512 MiB test-consumer RSS budget. Whole-path capacity remains `NOT MEASURED`. |

The package gates are implementation and evidence gates. Production Gold source progress requires Core Stage B.

## End-to-end remediation boundary

Pipeline scheduling, batch-size, query, worker-count, or SUMO-pacing changes are separate gated work after measurement; do not rewrite earlier accepted phases under the Pre contract package. Silver's 10-entry/2-second setting is a source-confirmed limit with roughly five journal entries/second idealized ceiling before processing cost, not measured production throughput. Current whole-path capacity is `NOT MEASURED`; see [capacity-and-backpressure.md](capacity-and-backpressure.md).
