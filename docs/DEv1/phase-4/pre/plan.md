# Phase 4A-Pre implementation plan

**Status:** planning only; no implementation is included in this plan artifact.  
**Prerequisite:** owner approval of the proposed decisions in `decisions.md` and API/receipt freeze.  
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
- **Failure modes:** producer append changes the boundary mid-page; receipt issued for incomplete pages; source binding or relevant event changes; accidental reliance on H staying globally unchanged.
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

- **Inspect/change:** Silver `read_invalidation_page`, checkpoint store, source binding, Bronze invalidation provenance and mirror logic; Gold proposed SQLite runtime state design/docs. Runtime Gold files are in scope only after separate Phase 4A package approval.
- **New files:** none required for Silver unless a typed invalidation receipt needs a dedicated DTO module.
- **Dependencies:** P4Pre-04, P4Pre-05, P4Pre-06.
- **Failure modes:** treating invalidation as change acknowledgment; lost invalidation cursor; conflict race between data staging and local dirty intent; stale binding silently resets cursor; incomplete batch acknowledged.
- **Tests/evidence:** independent cursors, invalidation before/after change receipt, races in both orderings, Gold local transaction rollback, crash before/after durable ack, same receipt replay.
- **Acceptance:** Gold changes each local cursor only with its complete durable obligations; invalidation is idempotent and eventual; a source change never silently resets progress.
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

## Implementation-wide decisions and gates

The normative API and race behavior are in `contract-design.md`; this section records package gates and cross-phase boundaries. It supersedes any implication that full live Bronze catch-up is required before Gold can read an already applied Silver prefix.

### P4Pre-01 — exact decisions to freeze

Freeze `Pc=min(Hc,Sc)`, `Pi=min(Hi,Si)`; per-entry current-identity/invalidation safety truncation; `PREFIX_BLOCKED`; empty response semantics; immutable transition/effective disposition split; exact API and DTO schemas; deterministic canonical JSON/digest; typed errors/retryability; source binding and bootstrap/resume; entry/byte/single-event/deadline maxima; Stage A versus Stage B; capacity worksheet and verdict taxonomy. Owner provides workload and numeric resource targets before capacity can pass.

### P4Pre-02 — public DTO/API

Add only the additive public Silver surface. Export fixed request/response/error types. Reject invalid sequence types (including bool), negative cursors, unknown versions, over-server-limit caps and namespace/binding mismatches. `EMPTY` means `through=after`, zero entries, no receipt and no progress. `PREFIX_BLOCKED` is retryable and never represented as successful empty coverage. `has_more` means source sequences remain within captured `min(Hc,Sc)`; the next sequence may still be blocked by conflict safety.

### P4Pre-03 — transition and effective-state verification

Keep transition identity immutable: source binding, change sequence, event ID, semantic revision, change type, previous/new state, transition evidence hash and linked invalidation ID. Keep effective disposition, current identity revision/state, superseding sequence, invalidation reference, canonical bundle/hash and boundary identity separate. A saved receipt is deterministic; a newer boundary may supersede accepted state without immutable journal corruption. Hash/sequence/link disagreement is permanent integrity failure.

### P4Pre-04 — verified-prefix receipt

Select `Pc/Pi` from captured Hc/Hi/Sc/Si; verify each sequence on demand in order; truncate before the first transition not safely applied; record the actual returned through sequence and safe invalidation watermark. Enforce the byte cap while encoding. A single oversized item yields actionable `RECORD_TOO_LARGE`; no silent skip or unchanged-limit retry loop. Unrelated appends beyond capture are deferred. Included identity conflict before final check makes the page stale; post-receipt conflict is eventual invalidation convergence and no serving output.

### P4Pre-06 — bounded consumption and source efficiency

Use source pagination and only bounded in-flight serialization/evidence. Caps include entries, actual serialized bytes with receipt overhead, maximum single bundle and processing deadline. A timeout or oversized item cannot produce a receipt past the affected sequence. Measure query amplification by operation/table before batch-query redesign. Safe page-level sharing is limited to static binding, captured allocator/cursor boundary and archive identity proven identical within a batch. Keep per-event current identity, local validity, obligation hashes, physical fact keys/hashes and final relevant-identity checks. No broad cache, duplicate persistent payload store or unproven full-history stage is authorized here.

### P4Pre-07 — invalidation and test-only consumer

Stage A harness stores binding, receipt bytes, dispositions and exclusive cursor in a minimal test-only durable store. It injects pre/post-commit crashes and verifies idempotent replay. It is not Gold runtime and has no production work/window semantics. It consumes invalidations through each change receipt's fixed Pi before acknowledging the change receipt. Gold SQLite work intents, dirty-generation CAS, recompute and replay remain Stage B.

### P4Pre-08 — independently staged gates

**Stage A correctness:** real public API/authority fixtures and independent journal oracle; actual ClickHouse reconciliation where needed; test-only acknowledgment harness. **Stage A capacity:** test-only consumer and real API under sustained/burst/outage workloads; Silver feed rate, source verification/query amplification, RSS, backlog slope, bootstrap/resume cost. No Gold runtime claim. **Phase 3 regression:** existing Bronze/Silver obligations, reader, archives, nullable semantics and invalidation recovery. **Stage B Phase 4A-Core:** actual Gold cursors/work intents/windows, dirty fencing, recovery/replay and production integration after implementation. Core work must not be a prerequisite for Pre acceptance.

### End-to-end remediation boundary

Pipeline scheduling, batch-size, query, worker-count, or SUMO-pacing changes are separate gated work after measurement; do not rewrite earlier accepted phases under the Pre contract package. Silver's 10-entry/2-second setting is a source-confirmed limit with roughly five journal entries/second idealized ceiling before processing cost, not measured production throughput. Current whole-path capacity is `NOT MEASURED`; see [capacity-and-backpressure.md](capacity-and-backpressure.md).
