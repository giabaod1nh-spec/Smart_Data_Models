# Phase 4A implementation plan

Planning only. No code has been implemented. Each package is an approval-gated unit; exact class/function names are verified in `audit.md`.

**Phase 4A-Pre dependency:** the proposed Silver canonical incremental contract and its packages/gates are documented in [pre/contract-design.md](pre/contract-design.md), [pre/plan.md](pre/plan.md), and [pre/acceptance.md](pre/acceptance.md). Production incremental progress remains gated on owner approval and successful pre-phase acceptance.

**Capacity note:** [pre/capacity-and-backpressure.md](pre/capacity-and-backpressure.md) records the source-grounded SUMO→Gold audit. Whole-pipeline capacity is `NOT MEASURED`; polling intervals and fixture acceptance do not establish sustained rate.

**Acceptance dependency:** Pre Stage A proves the Silver contract with real source authorities and a test-only durable cursor harness. Gold SQLite/window/invalidation integration is Stage B under Phase 4A-Core after Pre; Gold runtime implementation is not a Pre acceptance prerequisite.

## Target architecture

```text
Bronze V1 SQLite authority (read-only journals / current event authority)
             │ consumed by Silver only
             ▼
Silver V1 canonical contract
  complete bundles + source binding + applied/change/invalidation barriers
  invalidation feed (global sequence; local validity is authoritative)
             │ read-only `CanonicalReadSession` / opaque pagination
             ▼
Gold V1 in-place adapter → bounded scan staging → source-bound work coordinator
                                      │
                         Gold-owned SQLite state / CAS / lease
                         source cursor · invalidation cursor · work units
                         windows · dirty obligations · replay manifest
                                      │
                         foundation status/metrics only (Phase 4A)
                                      ╳ no business mart publication
                                      ▼
            Phase 4B approved analytics → later API/Dashboard
```

Gold obtains Silver context only through the canonical Python interface. It uses SELECT-only ClickHouse credentials and Silver read-only SQLite paths supplied by Silver settings/session; never opens Bronze as an alternate event reader. The SQLite runtime store owns Gold checkpoints, independent invalidation cursor/binding, work/window state, retry/fault disposition and replay. Avoid storing full Silver payload copies; keep event key, source hashes/revision/provenance needed for idempotency and impact mapping. A scan stage is temporary and non-authoritative until a public scan receipt proves complete scope; page `complete=True` is not that receipt. No source cursor advances from a partial or unreceipted stage.

## Contracts and state model

Full processing identity:

```text
(source_namespace,
 simulation_run_id,
 run_generation,
 topology_digest,
 manifest_identity,
 source_schema_version,
 processor_version)
```

Generic work-window identity is exactly:

```text
(source_namespace, simulation_run_id, run_generation, topology_digest,
 manifest_identity, window_spec_version, window_duration_sec,
 start_simulation_time, end_simulation_time)
```

`scenario_id` is deliberately excluded from window identity and preserved on each observation/input-set hash. One window may contain multiple scenarios; 4A neither splits nor relabels it. Invalidation maps by event run identity/time to every containing work unit, independent of scenario. Event membership uses canonical event cycle/simulation-time in `ObservationBundle`, never record arrival time. Event ID alone cannot merge run generations or topology generations.

Recommended state (all additive to the existing Gold SQLite store; exact schema is an approved implementation decision):

| State | Key / invariants |
|---|---|
| Source binding | Namespace; source file identity/path/schema fingerprint/version and immutable expected binding. Changed authority blocks processing and requires explicit new binding/recovery. |
| Change progress | Exclusive global source sequence and binding. Proposed boundary is verified applied prefix `min(Bronze Hc, Silver Sc)`, further truncated before any transition whose effective validity/invalidation evidence is unsafe. Advance only after every disposition/work intent through the actual returned through-sequence is durable. Current reader lacks this feed (P4-D10). |
| Invalidation progress | Exclusive global invalidation sequence + exact saved source binding; atomically acknowledge page with durable event suppression and affected dirty-work records. Pending mirror is visible, not a reason to ignore the event. |
| Event/work identity | Full run identity + event ID/revision/content hash; repeated same-content observation is idempotent. Conflicting identity is integrity fault, not last-write-wins. |
| Window dimensions | Candidate/discovery; source boundary status; window completeness (`UNKNOWN` until an expected-source contract exists); publication finality (pending/disabled in 4A); dirty generation; processing disposition. Keep them as independent columns/dimensions, not one enum. `FINALIZED` is not reachable in 4A. |
| Work obligation | Deterministic identity over full window identity, input-set hash and processor/schema version; pending/running/retry/fault/complete with CAS and durable error disposition. |
| Replay | Frozen manifest/hash, source boundary, code/schema versions, isolated namespace, SQLite path and lock; never shares live cursor/lease/output target. |

Within one Gold SQLite transaction, Gold can atomically persist a receipt reference, staged-work intent, dirty generation and local cursor CAS. Silver validates complete pages at its own checked boundary. There is no shared transaction across Bronze, Silver, Gold SQLite and ClickHouse; source may advance after Silver's last check and before Gold commits local metadata. The public receipt identifies the exact checked boundary; it does not lock the source. A later durable invalidation marks all impacted work dirty, and dirty-generation CAS prevents an older work attempt from overwriting a newer state. This is eventual convergence, not distributed snapshot isolation. 4A metadata is not safe to serve as analytics. Keep coordination and work receipts in SQLite and publish no CH marts.

## Reader adapter and scan integrity

1. Construct one `CanonicalReadSession` from Gold settings; verify RO SQLite and SELECT-only CH credentials at deployment. Ensure context-managed close on normal shutdown, exceptions and retry.
2. Call `read_valid_page(query, after_key, limit, token)` only with token/continuation returned by the same session/query. Treat token as opaque; do not decode, synthesize, persist across changed binding, or accept external tokens. Restart at first page after `SNAPSHOT_STALE`.
3. **Baseline:** use `read_complete_scan(query,page_size)` only for explicitly bounded fixture/smoke scopes. It accumulates all bundles in RAM and returns no public receipt. Do not use it for unbounded production scans/checkpoint advancement.
4. **Production candidate:** Phase 4A-Pre defines additive `read_canonical_changes_page` and independent invalidation page with typed `CanonicalBatchReceipt`. Each page records captured Hc/Hi/Sc/Si and returns exact sequences through its verified applied prefix; a complete full-history scan receipt is not required for incremental polling. Gold may stage at most bounded pages/work; staging remains invisible until local Gold transaction/CAS.
5. Do not require Hc==Sc or Hi==Si. Use `Pc=min(Hc,Sc)` and `Pi=min(Hi,Si)` plus per-event current identity/invalidation guard. Persist receipt and dispositions with work intent; advance only the actual returned sequence in one Gold SQLite transaction after coverage/binding validation. Gold consumes invalidations through the receipt's fixed safe Pi before change acknowledgment. A final check cannot prevent later mutation; no cross-database snapshot is claimed.
6. Map each Silver error exactly by the authoritative `pre/contract-design.md` error table; do not add aliases. Retry only its retryable codes (`PREFIX_BLOCKED`, `SOURCE_LAG`, `INVALIDATION_LAG`, `AUTHORITY_UNAVAILABLE`, `CLICKHOUSE_UNAVAILABLE`, `BOUNDARY_STALE`, `DEADLINE_EXCEEDED`) with bounded backoff and no cursor advance without receipt. Stop on binding/integrity/history/transition/version faults, including `SOURCE_BINDING_CHANGED`, `SOURCE_REWOUND`, `RETENTION_GAP`, `SILVER_CURSOR_REGRESSION`, `SOURCE_INTEGRITY_ERROR`, `CANONICAL_EVIDENCE_MISSING`, `INVALID_TRANSITION`, `INVALIDATION_LINK_MISMATCH`, `UNSUPPORTED_CONTRACT_VERSION`, and `CURSOR_AHEAD`; correct `INVALID_REQUEST` at the caller. `RECORD_TOO_LARGE` and `ENTRY_TIMEOUT` remain blocked until an approved cap change or dependency repair; never retry them indefinitely unchanged. `CANONICAL_CONTENT_CONFLICT` is a Gold-local committed-idempotency integrity fault, not an alias for a Silver API code. Retry ceilings are observable, not infinite loops.
7. Adapter must not use ClickHouse insert timestamps, row counts, raw fact SQL, page completeness as whole-scan proof, or current topology fallback.

Incremental limitation and recommendation are in `audit.md`/P4-D10 and Phase 4A-Pre decision documents. Option A is a Silver-owned bounded sequence API with canonical bundles/dispositions, verified applied prefix, source binding and per-page receipt; its independent invalidation prefix is coordinated but not conflated. Option B is complete canonical rescan plus Gold dedupe, correct only for fully inventoried scopes and approved measured resource budget. Event-key continuation alone is unsafe. P4-D30 is revised to per-page `CanonicalBatchReceipt`; Gold must not call private `_boundary()`, decode legacy tokens, query Bronze journals, or infer progress from ClickHouse.

The options have distinct operational guarantees. Option A must discover more than one retained/active run, bind each item to generation/digest/manifest, discover late events regardless of event key, and make its sequence continuation recoverable or force a full restart at the frozen boundary. Option B needs a trustworthy run inventory and repeats every selected run/time scope; without public run enumeration, it can only claim completeness for explicitly configured run IDs. Both options keep the invalidation cursor independent, replay from a frozen receipt/boundary, reject stale scans without committing, and require actual multi-run, late-event, conflict, restart and invalidation-race tests. Option A costs API implementation and change-proportional reads; Option B costs O(N) reads and stage I/O each pass. Full comparison is recorded in `decisions.md` P4-D10.

## Bounded-memory and temporary staging strategy

Compare these approaches before selecting production implementation:

| Approach | Complexity and fit | Recovery/churn/commit rule |
|---|---|---|
| Enforced run/time bounded RAM scan | O(page-size) per page but O(scope-size) total RAM with `read_complete_scan()`. Feasible for controlled fixtures only when bundle count/serialized bytes/time are capped and measured. Current API can run it but provides no public boundary receipt. | On stale/error discard tuple and restart. No durable source checkpoint without a receipt. |
| Page-by-page temporary Gold SQLite staging | O(page-size) RAM, O(N) scan time, O(N) SQLite disk and extra writes. Good production candidate only with an API exposing page iteration and final typed receipt; current `read_complete_scan()` cannot feed pages incrementally to a sink. | Key staging by scan ID and page ordinal; receipt only after final boundary check; on churn/error mark ABANDONED, never read as accepted work, then cleanup on restart/TTL under quota. In one Gold transaction bind receipt, create work intent, and CAS progress. Crash before commit resumes/restarts from receipt contract; no partial acknowledgement. |
| Silver streaming/incremental source API | O(page-size) RAM and bounded downstream staging; incremental scan can avoid O(history) per poll. Requires Silver API/change and run discovery design. | Silver owns boundary checks and complete-bundle validity; page retries are bounded; stale stream aborts scan ID; Gold commits only after public end receipt. |

Configure `max_scan_bundles`, `max_scan_bytes`, `max_scan_duration_sec`, `max_stage_bytes` and `max_concurrent_scans` (values approved from measurements, not guessed). Metrics: scanned bundles/bytes, page bytes, max RSS, stage bytes/rows, elapsed/read/retry time, stale restarts, abandoned stages and cleanup backlog. Reject over-budget scans with explicit nonretryable policy/config error; do not silently truncate. Acceptance I05 measures peak and restart cleanup on the constrained target environment.

## Simulation windows, eligibility and finality

Use half-open `[start,end)` membership. Existing `window_start_for()` aligns to epoch zero; evaluate/reuse it for generic candidate scheduling, with explicit precision and run-boundary handling before persisting production identities. Candidate generation, event membership, work intent, dirty/reopen state and restart safety are foundation mechanics; they do not need publication finality approval. Preserve cycle sequence separately from simulation time: out-of-order cycles remain distinguishable; order data by simulation time plus stable event identity, not Kafka/ClickHouse arrival. Do not hardcode a cadence, population, or expected observations. Archived topology supplies valid populations, but window completeness needs an expected-observation contract; otherwise it remains `UNKNOWN`. Scenario changes inside a window preserve all event-level labels and do not split the work window.

Model three gates separately:

1. **Silver/source completeness:** current source binding; exact contiguous page through the verified applied prefix `min(Bronze Hc, Silver Sc)`; separate safe invalidation watermark `min(Hi,Si)`; per-event validity and receipt checks pass. Silver need not catch up to live source highs; no unsafe transition may be included.
2. **Window completeness:** every required canonical observation for the declared window/coverage contract is accounted for. Until an expectation/cadence/coverage contract is approved, report `UNKNOWN`, not complete.
3. **Publication finality:** approved lateness/revision and lifecycle policy permits a consumer-visible result. Current V2 zero-lateness/zero-delay/revision-1 constants are not approved defaults. Mechanical scheduler and dirty/reopen/recompute state are implementable in 4A. Publication finality stays disabled; do not set `FINALIZED` or block candidate scheduling while P4-D13 is open.

Support configurable durations, including the requested 60/300-second capability, without treating them as approved dashboard outputs. Candidate identity/membership tests proceed with a documented alignment candidate. A COMPLETE run does not cure missing observations; INCOMPLETE/STOPPED runs remain available with lifecycle metadata but have no approved publication rule. Scheduler can mark candidates/source status while finality remains pending/disabled.

## Invalidation, impact and recovery

Consume the independent feed in sequence order. In one Gold SQLite transaction, verify binding/expected cursor, create suppression for event identity, mark all matching work windows dirty, enqueue deterministic recompute obligations, increment dirty generation and CAS feed cursor. If work is in flight, completion CAS must match its captured dirty generation; otherwise it records stale/dirty and cannot overwrite the newer work state. Repeated invalidation is idempotent. If an invalidation occurs after a scan's checked boundary but before Gold commits staged work, the local transaction may still commit metadata for the earlier receipt. This is allowed only as non-serving foundation state: the durable feed later dirties it; a known invalidation blocks completion/finalization. Crash recovery replays unacknowledged sequence and converges. No cross-database atomicity is claimed. Expose pending Silver CH mirror and Gold invalidation lag separately.

Race timeline (expected convergence, not a shared snapshot):

```text
Gold validates Silver page/receipt at boundary B
        → Bronze commits semantic conflict at B+1
        → Silver records durable invalidation sequence I
        → Gold may commit earlier receipt/work metadata in its own SQLite
        → Gold consumes I and atomically suppresses event + dirties work + advances feed cursor
        → dirty-generation CAS rejects any older in-flight completion
        → restart replays unacknowledged I and converges
```

If Gold consumes the invalidation before its earlier work-intent transaction, that transaction must recheck the local dirty generation and either record a dirty intent or retry. If the invalidation is not yet visible at the last source check, metadata can exist temporarily for boundary B; it is never published in Phase 4A. Silver's feed retention and Gold's committed cursor provide eventual repair. No final recheck can prevent a conflict committed immediately afterward.

Restart scan order: validate runtime schema/lease → verify saved source binding → recover/abandon temporary scan stages → replay feed from last committed exclusive sequence → dirty-fence incomplete work → obtain/verify new canonical scan receipt → resume only work matching receipt and dirty generation. On outage, retain last confirmed cursors and bounded retry. Binding/archive immutable identity change stops/faults; archive unavailability is distinct from registry currentness and source completeness.

## Replay and backfill

Replay is current-effective-state replay, not historical as-of. Manifest freezes full source binding, both Bronze high-waters, both Silver applied cursors, run/generation/digest/manifest identity, query/window scope, source/reader/schema versions, Gold processor and work identity version. Under lock, preflight exact current boundary and archive identity before materialization, during bounded work and before completion. If semantic source advances, durably abort that replay ID; create a new manifest/ID for the new boundary. A transient authority outage is retryable, not proof of mismatch. Replay owns separate SQLite DB, lock, namespace and any output target; tests snapshot live state before/after.

## Work packages and handovers

Each package must pass its gate before dependent implementation. Proposed file paths are existing files unless explicitly marked **new**; confirm references in `audit.md`.

### P4A-01 — Audit and contract freeze

- **Goal/files:** freeze this audit and D3-10; inventory `de/gold*/`, migration selector, Compose, `de/tests/gold/`, `de/tests/integration/`, Silver public reader and producer contracts.
- **Interface/state:** signed-off module disposition, source contract/error map and decisions P4-D10..18. No schema change.
- **Steps:** verify line-level call graph; identify all V2 readers/writers/service dependencies; identify any persistence/migration code before changes.
- **Failure/tests:** static import/query/write inventory; test that planned source has no V1 raw-fact read and one active Gold service.
- **Depends/blockers:** none; retain incremental source API gap as explicit decision.
- **Acceptance/handover:** owner approves architecture, in-place cutover and unresolved-decision list; pass audit contract to P4A-02.
- **Non-goals:** no implementation or runtime migration.

### P4A-02A — Canonical adapter baseline

- **Goal/files:** replace `de/gold_runtime/silver_readers.py`; adapt `de/gold_runtime/repositories.py` and `de/gold_runtime/context_builder.py`; reuse `de/silver/canonical_reader.py` public API; add Gold adapter/error-mapping module only if it owns genuinely distinct translation.
- **Interface/state:** complete bundle model retaining event ID, cycle/time, run ID/generation, digest/manifest, scenario, directions, values/nulls, availability, quality, Bronze revision/hash and lineage. Reader sessions close deterministically. No source cursor committed here.
- **Steps:** source-bound read-only session; opaque pagination; bounded fixture/run-time scans; transient/permanent error map; topology-driven bundle transform; preserve nullable values and per-event scenario. Since `read_complete_scan()` accumulates all pages in RAM and returns no receipt, enforce count/byte/time ceilings and do not claim production incremental behavior or commit a durable source cursor from it.
- **Failure/tests:** invalid/stale token, page-size boundary, changed filters/binding/high-water, incomplete or conflicting bundle, lag, transient authority loss, source churn and connection close; fake and actual Silver integration. Select-only credentials must reject INSERT.
- **Depends/blockers:** P4A-01 only; no source API change needed for baseline fixtures/smokes. Production progress belongs to P4A-02B and needs P4-D10/P4-D30.
- **Acceptance/handover:** bounded fixture has only canonical bundles; partial in-memory results are discarded; nullable values intact; sessions close; explicitly report no public receipt/source high-water persisted. Deliver adapter to generic P4A-03 work.
- **Non-goals:** raw table reader, business KPI, direct Bronze event consumption.

### P4A-02B — Gold integration of approved Silver incremental contract

- **Goal/files:** consume the Phase 4A-Pre-approved Silver methods `discover_canonical_source`, `read_canonical_changes_page`, and `read_canonical_invalidations_page`. Silver API design/implementation is P4A-Pre, not this package. Gold-side implementation changes `de/gold_runtime/silver_readers.py`, `processor.py`, `checkpoint_store.py`, and `replay.py` only after Stage A acceptance.
- **Interface/state:** global bounded change dispositions through verified applied prefix; full canonical `ObservationBundle` only for `BUNDLE_VALID`; exact per-page `CanonicalBatchReceipt` binds namespace/source, requested/returned sequence interval, captured Bronze Hc/Hi, Silver Sc/Si, safe invalidation watermark, versions, count/bytes and digest. Invalidations have independent cursor and receipt.
- **Steps:** implement Gold adapter and durable local acknowledgment, invalidation-before-change-ack through receipt Pi, work intents and recovery. Option B rescan is not a production substitute without full run inventory and approved measured scope budget.
- **Failure/tests:** append between pages, conflict before/during/after scan, changed source binding, changed archive, duplicate page, retry/lost response, empty run, late event sorting before earlier event ID, source advancement after receipt, invalidation race and restart/replay.
- **Depends/blockers:** owner approval P4-D10/P4-D30 and Silver cross-phase change. This blocks only production incremental polling, source-cursor advancement and production activation; baseline adapter, state, scheduler and invalidation mechanics may proceed.
- **Acceptance/handover:** durable cursor means every disposition through actual returned `through_sequence` and downstream intent are atomically durable; every namespace sequence is accounted across runs, including late IDs; receipt exactly describes its page; boundary stale leaves cursor unchanged. Actual Gold/work/window integration is Stage B, not a Phase 4A-Pre gate.
- **Non-goals:** raw Silver query by Gold, direct Bronze journal access, arbitrary historical as-of.

### P4A-03 — Gold durable state and source progress

- **Goal/files:** refactor `de/gold_runtime/checkpoint_store.py`, `processing_ledger.py`, `cursor.py`, `instance_lock.py`; update settings in `config.py`; reuse SQLite CAS/lease mechanics. Repository config and Compose currently point at `de/artifacts/gold/checkpoint.sqlite3` and `de/artifacts/gold/instance.lock`; recommended V1 paths are `de/artifacts/gold_v1/checkpoint.sqlite3` and `de/artifacts/gold_v1/instance.lock`, subject to OPEN P4-D34 approval.
- **Interface/state:** additive schema for binding, generic work intents/receipts, invalidation cursor, window dimensions, dirty obligations, retries/faults and replay/version; no full fact duplication. Source change cursor storage may be added as nullable/uninitialized metadata, but it cannot advance until P4A-02B receipt exists.
- **Steps:** define exact keys/state transitions; preserve migration compatibility; CAS source binding/generation; atomic invalidation receipt + cursor; recovery scans for nonterminal work; never rewrite old state silently. Use physically separate V1 DB and lock; do not silently open or migrate V2 state. Preserve a single active runtime/service and V2 data.
- **Failure/tests:** simultaneous cursor CAS (one winner), wrong namespace/binding, journal gaps/lag, locked/full DB, abrupt death around intent/ack/CAS, repeated delivery, migration twice, recovery without cursor skip, clean/populated V2 isolation, V1 path resolution, startup failure and rollback path.
- **Depends/blockers:** P4A-01; P4-D11 schema and P4-D34 physical path/isolation approval. Generic CAS, lease, work and invalidation state can proceed before incremental API. Source progress integration depends on P4A-02B/P4-D30. No Gold CH schema unless documented need passes gate.
- **Acceptance/handover:** every acknowledged invalidation cursor has durable suppression/dirty intent; source cursor tests remain disabled/uninitialized without receipt; reopen recovers; migration additive/idempotent; V2 hashes remain unchanged; V1 DB/lock are distinct; exactly one active service. Hand to scheduler/invalidation packages.
- **Non-goals:** output fact ledger or KPI result tables.

### P4A-04 — Generic simulation-time window orchestration

- **Goal/files:** refactor `de/gold_runtime/window_scheduler.py`, `revisions.py`, `de/gold/windowing.py`, and scheduler portions of `context_builder.py`; remove implicit business completeness from `expected_rows_for` until contract approved.
- **Interface/state:** versioned half-open window work identity, separate source/window completeness/finality, lifecycle and dirty status.
- **Steps:** reuse/evaluate `window_start_for()` epoch-origin candidate; define float precision and run bounds; derive half-open membership from event simulation time; support arbitrary archived topology; keep scenario per event and out of window key; maintain source status, unknown completeness, finality-disabled, dirty generation as separate fields.
- **Failure/tests:** exact start/end, adjacent windows, float normalization, out-of-order cycle/time, scenario transitions inside 60/300-second candidate, non-four-way topology, missing cycles, restart, same run ID with different generation/digest, lifecycle states.
- **Depends/blockers:** P4A-01 and generic P4A-03 state; P4-D12 identity/alignment version decision before production IDs. P4-D13 does not block scheduler mechanics; it blocks only publication finality/revision selection.
- **Acceptance/handover:** deterministic IDs/membership across restart; no scenario is dropped or selected as the window label; unknown completeness stays unknown; no `FINALIZED` state or business write. Hand to invalidation integration.
- **Non-goals:** 60/300 chart outputs or coverage/KPI definitions.

### P4A-05 — Invalidation and dirty-window coordination

- **Goal/files:** canonical feed adapter over the new additive `read_canonical_invalidations_page`; preserve the legacy `read_invalidation_page()` behavior; persist in `checkpoint_store.py`; coordinate `processor.py`/`window_scheduler.py` dirty generations.
- **Interface/state:** sequence-ordered invalidation receipt, event suppression and deterministic affected-window work obligations; mirror-pending observable.
- **Steps:** verify saved binding; page the finite frozen invalidation prefix; atomically persist receipt, event-keyed suppression, impact, dirty-generation increment and feed cursor; before creating new work, consult durable suppression/dirty state; race-check dirty generation at work completion; enqueue idempotent recompute marker. If invalidation committed before later change work creation, suppress that event from the new work. If it arrives during execution, fence completion. If it arrives after work commit, create durable suppression/dirty work and converge. Feed processing may race with work creation; its durable transaction repairs prior metadata without cross-database atomicity.
- **Failure/tests:** crash before/after local transaction; feed repeats; binding mismatch; pending CH mirror; continuous unrelated invalidation append; requested ceiling above Silver applied prefix; invalidation between page scan and finalize; before/during/after-work transaction order; multiple invalidations; historical physical fact remains but never trusted as valid.
- **Depends/blockers:** P4A-03/04 and API contract; window lookback/impact needs event simulation time from feed. If invalidation coordinates cannot map to an indexed work identity, perform bounded indexed lookup, not CH raw validity scan.
- **Acceptance/handover:** no work completion when invalidation is known or dirty generation differs; cursor and suppression recover together; eventual dirty convergence after cross-database race; never claim distributed atomicity. Hand to replay.
- **Non-goals:** calculate replacement analytics while 4B is deferred.

#### Normative transaction-order matrix (Phase 4A-Core)

| Ordering | Required local durable state | Safety rule |
|---|---|---|
| Invalidation commits before change work creation | Event-keyed suppression + invalidation receipt/cursor + dirty/recompute obligation | Later work creation reads suppression before membership and cannot add the event as valid. |
| Invalidation commits during change work execution | Suppression + incremented dirty generation + invalidation receipt/cursor | Completion CAS uses the generation captured at work start and must fail if it is now stale. |
| Invalidation commits after change work commit | Suppression + affected-work dirty/recompute obligation + invalidation cursor | Preserve immutable prior transition/receipt; schedule correction, never silently rewrite acknowledged history. |
| Crash before invalidation acknowledgment transaction commits | No new cursor or partial suppression is durable | Restart replays the same invalidation interval. |
| Crash after invalidation acknowledgment transaction commits | Full suppression/dirty obligations and cursor are durable together | Restart continues at stored cursor; duplicate delivery is idempotent. |
| Replay under a newer source boundary | Immutable transition hash remains identical; effective disposition/correction may differ | Do not relabel committed immutable change; apply effective correction through invalidation/suppression and fenced recompute. |

There is no transaction spanning Silver, Bronze, Gold and ClickHouse. The table is a Core handover contract and is tested at Stage B; Pre Stage A's test consumer only validates source receipts and cursor replay.

### P4A-06 — Recovery, replay and backfill foundation

- **Goal/files:** refactor `de/gold_runtime/replay.py`, recovery parts of `processor.py`, `processing_ledger.py` and checkpoint state.
- **Interface/state:** canonical source-bound replay manifest, stable hash and isolated replay owner.
- **Steps:** freeze source and archive identity; preflight/resume boundary; recover work intents and invalidation feed; isolate DB/lock/namespace; refuse current-boundary mismatch.
- **Failure/tests:** all crash points; changed source/archive before/during/finalize; benign duplicate; transient unavailable authority; replay while live advances; repeated replay and replay ID; snapshot live state; no arbitrary as-of claim.
- **Depends/blockers:** isolated replay metadata/state and fake-source failure tests may start after P4A-03. Source-scoped production replay depends on P4A-02B receipt, P4-D15 and dirty-feed contract. No arbitrary as-of claim.
- **Acceptance/handover:** deterministic logical work state for unchanged boundary; changed boundary aborts and never mutates live state; handed replay evidence to runtime integration.
- **Non-goals:** historical backfill beyond available Silver current-effective-state contract.

### P4A-07 — In-place runtime, configuration and observability

- **Goal/files:** refactor existing `de/gold_runtime/main.py`, `processor.py`, `config.py`, `health_api.py`, `metrics.py`; Compose and selector only after architecture approval; preserve service/entrypoint identity and historical DB.
- **Interface/state:** one active V1 runtime; typed source configuration; foundation health separate from analytics readiness; status includes source binding, both lag dimensions, feed lag, stale lifecycle/archive, dirty queue, retry/fault, lease and scan/recovery latency.
- **Steps:** remove V2 startup/read/write wiring; enforce RO sources; select V1-only config and narrowly scoped migration; configure V1 checkpoint/lock at approved isolated `gold_v1` paths; preserve V2 directory; sequence Bronze/Silver health dependencies; use the existing service, not a sibling service; deploy under maintenance gate after shadow/acceptance.
- **Failure/tests:** old V2 profile cannot start as analytical runtime; one owner lock; missing schema/binding leaves not-ready; SQLite/CH/Silver outage degrades; no migration 005 changes; verify V2 sentinels before/after.
- **Depends/blockers:** local health/config/refactor work can proceed after adapter/state contracts. Production activation requires resolved P4-D10/D30 incremental/receipt or accepted rescan budget, P4-D31 memory/resource limits, P4-D16 cutover, P4-D17 thresholds and migrations decision. P4-D13 business finality is not required while publication remains disabled.
- **Acceptance/handover:** Compose shows exactly one Gold analytical service and selected migrations; V1 canonical source ready; historical V2 object/data hashes unchanged; no analytics-ready claim. Hand to acceptance.
- **Non-goals:** Spring, Dashboard, Gold marts.

### P4A-08 — Foundation acceptance and Phase 4B handover

- **Goal/files:** tests in existing `de/tests/gold/` and `de/tests/integration/`; docs handover; acceptance evidence separate from code aggregate counts.
- **Interface/state:** machine-readable assertion evidence for CORE, RESILIENCE and INTEGRATION; explicit output contract for Phase 4B consumers.
- **Steps:** run tests against fake reader and actual Phase 3 Silver; capture source/config/migration/runtime inventories, V2 sentinels, crash/replay state and readiness samples; review no mart writes.
- **Failure/tests:** execute each row of `acceptance.md`; preserve failures and environment limits; do not accept aggregate pass count as criterion closure.
- **Depends/blockers:** foundation suites and fake contract tests may be developed throughout packages. Final production acceptance requires selected P4A-02B/rescan path, receipt/scan resource acceptance, recovery convergence, cutover and readiness gates. P4B requirements do not block foundation-only acceptance.
- **Acceptance/handover:** all CORE/RESILIENCE/INTEGRATION assertions pass; open business requirements remain clearly deferred; owner reviews handover before 4B starts.
- **Non-goals:** API/dashboard acceptance or final analytics correctness.

## Runtime and data-preservation approach

Keep migration 005 and all historical Gold V2 data read-only during this effort. Migrate the existing process/service/profile in place after a release gate; do not start a parallel Gold V1 service. Use an additive, versioned SQLite state migration unless an approved shared CH state need proves otherwise. Before/after compare database/table inventory and deterministic row hashes on retained V2 sentinels. Do not use rollback as a reason to rewrite/delete V2 data; code rollback must preserve additive V1 state and leave old marts untouched. Runtime execution/deployment is a later authorized implementation action, not part of this planning task.

## Revised implementation gates and dependency order

Status labels classify scope/dependency, not authorization: **APPROVED FOUNDATION CAPABILITY** means the capability is within the proposed 4A foundation and may be implemented only after owner approval of this plan; it does not authorize implementation now. **TECHNICAL DECISION PENDING** means the interface/policy needs explicit resolution. **PRODUCTION ACTIVATION BLOCKED** means local development may proceed but live activation cannot. **BUSINESS ANALYTICS DEFERRED** remains Phase 4B.

| Capability | Status | May start when | Production gate |
|---|---|---|---|
| P4A-01 architecture/contract freeze | APPROVED FOUNDATION CAPABILITY | Now, after owner accepts D3-10 direction and open contract register. | Close audit and name owners for cross-phase requests. |
| P4A-02A adapter baseline | APPROVED FOUNDATION CAPABILITY | P4A-01; current public API. | Bounded fixture limits; no production cursor claim. |
| P4A-03 generic SQLite state/CAS/lease and invalidation state | APPROVED FOUNDATION CAPABILITY | P4A-01 and state schema review. | Source progress separately waits for P4A-02B receipt. |
| P4A-04 candidate scheduler, identity, membership and dirty mechanics | APPROVED FOUNDATION CAPABILITY | P4A-01/03; identity/alignment version recorded. | No finality/publication required; finality remains disabled. |
| P4A-05 durable invalidation consumption and dirty tracking | APPROVED FOUNDATION CAPABILITY | P4A-03; current public invalidation feed. | Feed binding/CAS/crash/race acceptance. |
| P4A-06 isolated replay metadata and failure model | APPROVED FOUNDATION CAPABILITY | P4A-03; fake source. | Production replay waits for receipt and frozen boundary. |
| P4A-02B incremental source cursor | TECHNICAL DECISION PENDING | Silver/Gold contract review. | API or measured rescan approved; receipt, late/multi-run proof and resource budget pass. |
| P4A-07 live runtime activation | PRODUCTION ACTIVATION BLOCKED | Refactor/config/health development may proceed after baseline contracts. | Public P4-D30 receipt plus selected P4-D10 Option A API or measured Option B scope; bounded-memory strategy; race convergence; cutover and readiness approvals. |
| P4A-08 foundation acceptance | APPROVED FOUNDATION CAPABILITY / staged | Build tests incrementally. | Full applicable CORE/RESILIENCE/INTEGRATION gates and evidence. |
| 4B KPI/marts/serving | BUSINESS ANALYTICS DEFERRED | Separate requirements approval. | No dependency for Phase 4A mechanics. |

Suggested critical path: P4A-01 → parallelize baseline adapter and generic state design → P4A-04 candidate scheduler + P4A-05 invalidation state → P4A-06 fake-source recovery → settle P4A-02B API/receipt and P4-D31 staging limits → integrate source progress/replay → P4A-07 production activation → P4A-08 final actual-runtime evidence. Candidate scheduling and invalidation mechanics are intentionally not gated on KPI, dashboard or finality approval.
