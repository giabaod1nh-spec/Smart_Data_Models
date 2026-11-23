# Phase 4A-Pre decisions — proposed

**Decision status:** proposed; owner approval required before implementation.  
**Evidence baseline:** Phase 3 commit `443f40e3ddde0f9cf657785b3fbf1851281897ea`.

**Final revision:** The verified-prefix, independent invalidation, exact API, and acceptance-stage decisions below supersede any earlier requirement to wait for Silver to reach the latest Bronze high-water. See [capacity-and-backpressure.md](capacity-and-backpressure.md) for source-grounded throughput and overload findings.

## Recommendation

Adopt **Option A: a Silver-owned, namespace-global, sequence-based canonical change feed with frozen bounded batches and explicit receipts**, plus an independent invalidation feed. Do not use event IDs, timestamps, row counts, or latest-state scans as source progress. Keep Silver and Bronze as the semantic authority. Gold consumes only the public Silver DTO/API and owns its durable acknowledgments.

This recommendation follows from the existing source model: Bronze already persists append-only change and independent invalidation sequences; Silver already applies them contiguously and persists per-sequence obligations. A sequence contract extends that authority to Gold. It can prove coverage by exact source sequence and read work proportional to change volume. A full-rescan API currently requires explicit run/digest/time scopes, orders by event ID, stages the complete scan in memory, and supplies no durable public boundary receipt.

## Option comparison

| Dimension | Option A: sequence feed + frozen batches | Option B: bounded canonical full-rescan + Gold dedupe |
|---|---|---|
| Correctness | Exact global sequence coverage, if Silver validates every transition and emits one disposition per sequence. Captured boundary and receipt support durable replay. | Can converge to current facts only if every relevant run/scope is known and rescanned. Does not itself prove historical change coverage or accepted-then-conflicted transitions. |
| Liveness | New work is proportional to sequence backlog. New sequences above the frozen highwater wait for the next batch. | Every pass repeats all selected scopes. Late events are found only when their scope is rescanned. Missing run inventory silently limits completeness. |
| Resource complexity | O(new changes + bounded verification) source work; O(batch cap) memory/staging. Indexed source queries and per-record evidence checks still need measurement. | O(retained canonical population) time and staging I/O per pass; Gold dedupe storage grows with retained identities. Memory can be bounded only if reader is streamed; current complete scan stages all bundles. |
| Crash recovery | Gold atomically records dispositions/work and advances its local cursor. Replay uses exclusive cursor and idempotency keys. Silver need not store a consumer-specific acknowledgment. | Restart repeats the scan and dedupe. Without a public receipt, Gold cannot distinguish a complete durable scan from a partial pass unless it adds its own staged scan protocol. |
| Source churn | Appends above frozen H are naturally deferred. Changes to included current identities make the batch stale; conflict races are repaired by independent invalidation. | Churn can make each scan stale or force restart. Event-key pagination is not a stable global boundary. |
| Contract evolution | Versioned DTO/receipt and explicit binding migration; old cursors fail closed on incompatible source. | Changes to canonical schema/query scopes require full re-baselining and can change dedupe semantics. Run inventory remains a separate gap. |
| Long-term scale | Scales with source change rate, subject to journal retention and per-entry evidence cost. | Cost scales with total retained population on each poll; becomes unattractive as history grows even at low change rates. |

Option B may be useful for fixture validation or a deliberately small, explicitly enumerated one-time baseline. It is not equivalent as the production incremental contract. In particular, it must not be described as complete unless run inventory and all required scope boundaries are available and its repeated scan/staging budget is measured and approved.

## Proposed decision register

| ID | Proposed decision | Owner approval / unresolved point |
|---|---|---|
| P4Pre-D01 | Option A is the production source strategy. | Silver and Gold owners approve API ownership and cross-phase implementation. |
| P4Pre-D02 | Feed is global per namespace and includes all source sequences; run filtering occurs downstream after coverage is established. | Confirm namespace partitioning and Gold's treatment of event-free runs. |
| P4Pre-D03 | One entry per change sequence, with only `BUNDLE_VALID`, `SUPERSEDED_BY_CONFLICT`, and `CONFLICTED` success dispositions. | Confirm whether a future durable Silver quarantine may be added under a new contract version. |
| P4Pre-D04 | Current event-validity row is not historical proof; transition journal plus per-sequence Silver obligations establish disposition. | Approve explicit non-as-of semantics for superseded accepted revisions. |
| P4Pre-D05 | Batch receipt carries frozen Bronze change/invalidation highwaters, Silver applied cursors, source binding and exact range/digest. | Freeze stable serialized receipt fields and digest algorithm in P4Pre-01. |
| P4Pre-D06 | Change and invalidation are separate streams and separate Gold cursors. Gold applies invalidations idempotently through the change receipt's finite safe applied invalidation watermark before acknowledging that change receipt. | Approve the `Pi=min(Hi,Si)` watermark; do not wait for live `Hi`. |
| P4Pre-D07 | Cold bootstrap starts at zero after full retained-history verification; no “start at latest” shortcut. | Confirm source retention/no-prune operational commitment and empty-run discovery requirement. |
| P4Pre-D08 | Per-batch entry and byte caps plus single-record cap; overflow faults without progress. | Owner sets numeric caps from workload evidence; no numbers are invented in this plan. |
| P4Pre-D09 | No cross-database atomicity or arbitrary historical snapshot claim. Conflicts after receipt are repaired through the independent feed and replay/current-state check; Phase 4A output stays unpublished. | Confirm downstream idempotency and unpublished-state boundary. |
| P4Pre-D10 | A change batch may cover only verified applied prefix `Pc=min(Hc,Sc)`, then truncates before the first transition whose effective disposition/invalidation proof is unsafe. | Approve `PREFIX_BLOCKED` and no-skip semantics; never require `Sc==Hc` under continuous arrival. |
| P4Pre-D11 | Independent invalidation prefix is `Pi=min(Hi,Si)`; change receipt carries frozen Pi. | Gold Core consumes invalidations through that fixed watermark before acknowledging the receipt. |
| P4Pre-D12 | Public API methods are exactly `discover_canonical_source`, `read_canonical_changes_page`, and `read_canonical_invalidations_page`, with DTOs in `contract-design.md`. | Freeze byte, single-entry, deadline caps and exact serialization in P4Pre-01. |
| P4Pre-D13 | Immutable transition identity is distinct from boundary-dependent effective disposition; same saved receipt is deterministic, later boundary may supersede accepted state. | Approve old receipt/version verification and conflict correction semantics. |
| P4Pre-D14 | Stage A Pre acceptance uses real Silver API, real authority fixtures, independent journal oracle and test-only durable consumer. Gold SQLite/window integration is Stage B in Phase 4A-Core. | Approve test-only harness scope; it cannot certify production Gold performance. |
| P4Pre-D15 | Cold bootstrap verifies full history once per new binding. Normal polling/restart verifies stable binding, cursor bounds and next-prefix continuity without rescanning full history. | Keep the no-prune/no-rewind assumption; source replacement requires explicit new generation. |
| P4Pre-D16 | Whole-pipeline capacity is `NOT MEASURED` until sustained/burst/outage benchmarks meet owner-selected workload and resource caps. | Owner completes capacity worksheet and acceptance objectives. |
| P4Pre-D17 | Initial overload policy is durable buffering plus visible degraded/fail-stop behavior; no silent drop or automatic SUMO cadence change. | SUMO pacing and worker concurrency require separate approval. |
| P4Pre-D18 | Raw/Bronze/Silver throughput remediation remains a separately gated workstream; Pre implements the Silver API contract only. | Avoid rewriting accepted Phase 1–3 runtime scope. |

## Consistency protocol decisions

### 1. Cold bootstrap

1. Gold calls Silver `discover(namespace, expected_binding=None)`.
2. For a new binding Silver verifies stable Bronze identity, allocator highwaters, contiguous change and invalidation history from sequence 1 and available Silver linkage/obligations using a streaming walk. It does not require applied cursors to equal live source highs.
3. Silver returns binding and bootstrap boundary only after verification; consumer cursors initialize at zero. Verification must not materialize history in memory.
4. Gold persists the binding and cursors `(0, 0)` in its own transaction, then begins bounded reads. A failed verification creates no initialized cursor.

### 2. Batch request and source boundary

1. Gold requests `(after_change, expected_binding, max_entries, max_bytes)`.
2. Silver validates binding, captures allocator highwaters `Hc, Hi` and Silver cursors `Sc, Si`, requires `0<=Sc<=Hc` and `0<=Si<=Hi`, and sets `Pc=min(Hc,Sc)`, `Pi=min(Hi,Si)`. It does not wait for full source catch-up.
3. Silver sets `through<=Pc`, visits exact sequence order, truncates before any entry whose effective validity/invalidation proof is not safely applied, and enforces entry/byte/time limits during serialization. No sequence may be skipped.
4. For each sequence, validate journal transition, linked current identity and/or later conflict, Silver obligation, archive identity, and canonical output evidence. Assign exactly one disposition.
5. Validate source binding and relevant identities again. Do not require allocator H to remain unchanged. If an included event advanced beyond the boundary and its disposition is no longer valid, return `STALE_BATCH`, no receipt. New unrelated append sequences above H remain for the next call.
6. Emit receipt only for an exact, nonempty, bounded and validated interval. It records Hc/Hi/Sc/Si and safe invalidation watermark Pi; it is not a cross-database snapshot. Empty no-op never advances a cursor; an unsafe first entry returns retryable `PREFIX_BLOCKED`, not empty success.

### 3. Per-sequence disposition

- Accepted `s`, no later conflict `<= Hc`: verify current identity still equals this accepted sequence/revision and all Silver/CH/archive obligations; return `BUNDLE_VALID` with complete bundle.
- Accepted `s`, later linked conflict `c <= Hc`: verify both transitions, invalidation link and Silver obligations; return `SUPERSEDED_BY_CONFLICT` for `s`, no bundle. Sequence `c` separately returns `CONFLICTED`.
- Conflict `c`: verify current/source transition, independent invalidation row and local invalidation obligation; return `CONFLICTED`, no bundle.
- Missing sequence, corrupt/absent evidence, impossible transition, missing obligation or unknown state: fail the batch, emit no receipt, do not advance Gold.

### 4. Gold durable acknowledgment

1. Gold verifies binding, interval, digest, supported versions, and exact coverage.
2. In one Gold-local transaction, persist receipt and every entry/disposition/content hash, create downstream work intents for valid bundles, mark superseded/conflicted identities suppressed or dirty, and compare-and-swap change cursor from `after` to `through`.
3. Commit is the acknowledgment. Repeated receipt/entries are idempotent. Cursor advancement without all dispositions/work intents is forbidden.
4. Silver receives no remote ack. A crash before commit leaves the old cursor and replay occurs; a crash after commit recovers the new cursor and durable work intent.

### 5. Invalidation coordination

Invalidations have their own cursor and receipt and are delivered only through `Pi=min(Hi,Si)`. Gold reads them independently and records conflict suppression/dirty obligations and its invalidation cursor in one local transaction. Before committing a change receipt, Gold must durably consume invalidations through that receipt's finite `safe_invalidation_through_sequence` (Pi), not live Hi. A later source/Silver append is outside that receipt and is handled on a later request. If an invalidation races with Gold work, its local transaction must compare dirty generation and retain a recompute/suppress intent; no final cross-database check eliminates a later conflict.

### 6. Restart, binding change, and concurrent producer

- **Restart before Gold commit:** cursor remains old; request again from old exclusive cursor. Silver may capture a newer H; exact sequence dedupe and disposition hash checks detect divergence.
- **Restart after Gold commit:** continue from durable `through`; work intent is recoverable independently.
- **Binding changed:** fail closed with `SOURCE_BINDING_CHANGED`. Do not reset cursor or accept a different path/file under the old namespace. Owner creates an explicit new bootstrap generation or namespace and reconciles downstream state.
- **Producer appends during read:** records above frozen H are deferred. The batch is valid if its own range and relevant identities remain valid. If a conflict is appended after H for an event in the range before Silver's final current-state check, stale the batch and retry. If committed after receipt, invalidation feed eventually repairs Gold; Phase 4A does not publish metadata.

## Required compatibility with existing decisions

This proposal extends Phase 3's Bronze semantic authority, append-only journal and separate invalidation stream, current-effective-state semantics, and no-prune/no-rewind operational assumption. It does not authorize a Gold direct Bronze reader, Silver raw-fact bypass, latest-state bootstrap, historical as-of claims, or weakening archive/quality/lineage reconciliation. Existing Phase 4A P4-D10/P4-D30 remain open until this contract is approved and implemented; see the Phase 4A cross-reference in `../decisions.md` and `../plan.md`.

### Acceptance staging and capacity result

**Stage A — Pre contract acceptance:** test the real Silver public API with real Bronze/Silver authority fixtures, independent raw-journal oracle, real ClickHouse reconciliation where required, and a minimal test-only durable consumer harness for receipt/cursor crash replay. No Gold application dependency or production Gold claim. **Stage B — Phase 4A-Core:** actual Gold SQLite cursors, window work intents, invalidation dirty-generation fencing, replay and recovery. Stage B occurs after Stage A and must not gate Phase 4A-Core implementation. Current whole-path capacity verdict is **NOT MEASURED**; see `capacity-and-backpressure.md`.
