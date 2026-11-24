# Phase 4A-Core decisions and approval register

This register describes the approved Core architecture and remaining gates. Approval authorizes the plan only; implementation requires a separate explicit owner decision after review. Phase 4A-Pre implementation and Stage A evidence are recorded in [`pre/implementation-status.md`](pre/implementation-status.md) and [`pre/acceptance-result.md`](pre/acceptance-result.md).

## Approved decisions

| ID | Status | Decision and evidence / implementation consequence |
|---|---|---|
| P4-D01 | APPROVED | Refactor `de/gold/` and `de/gold_runtime/` in place; one active production Gold runtime. No `gold_v1` parallel packages/service. Authority: Phase 3 D3-10. |
| P4-D02 | APPROVED | Gold consumes only the Silver public canonical feed and independent invalidation feed. Silver remains the validity authority; its internal physical-fact reconciliation is allowed. |
| P4-D03 | APPROVED | Preserve migration 005 and historical V2 state. No destructive reset, V2 state adoption, or business-mart writes during Core. |
| P4-D04 | APPROVED | Bind work to namespace, run ID, generation, topology digest, immutable manifest identity, source/schema versions and Gold processor version. Do not fall back to current topology. |
| P4-D05 | APPROVED | Preserve nullability, per-field availability and quality; never turn missing/unavailable values into zero. |
| P4-D06 | APPROVED | Realtime Orion-LD/projector/API remains operational only, not Gold's analytical source. |
| P4-D10 | APPROVED | Option A is the sole active production source strategy: Silver namespace-global change sequence, exact contiguous dispositions and frozen per-page receipts. Do not reopen Option B absent a demonstrated defect in the approved contract. Stage A passed; Gold Stage B is pending. |
| P4-D11 | APPROVED | Gold owns durable processing metadata in SQLite and one active owner. The change cursor, invalidation cursor, bindings, receipts, suppression, membership/work intent, dirty generations, replay metadata and migration state are Gold-owned. |
| P4-D12 | APPROVED | Pure simulation-time Tumbling Windows of 60s and 300s; deterministic half-open `[start,end)`, one membership per duration, scenario-neutral generic identity. Epoch-zero alignment is preferred; precision and run-boundary checks precede durable identity freeze. |
| P4-D13 | APPROVED — finality disabled | Window completeness remains `UNKNOWN` until Phase 4B defines an expected-observation contract. `publication_finality=DISABLED` throughout Core; `FINALIZED` is unreachable. Lateness, coverage and publication policy remain deferred. |
| P4-D14 | APPROVED | Durable event suppression, dirty generations, recompute obligations and completion CAS fencing. Do not mutate immutable acknowledged transition history or inherit the legacy one-revision cap. |
| P4-D15 | APPROVED | Current-effective-state replay only, with frozen source boundary and isolated namespace, SQLite file and lock. No arbitrary historical as-of reconstruction. |
| P4-D16 | APPROVED — deployment gate remains | Refactor/cut over the existing runtime in place with controlled rollback; preserve both stores and never run two active Gold owners. The concrete operational cutover/rollback runbook must pass deployment review. |
| P4-D17 | APPROVED | Foundation processing health is distinct from analytics/business readiness. Core has no business-result readiness claim. |
| P4-D18 | APPROVED | No new Gold ClickHouse migration in Core unless a concrete infrastructure requirement is documented and separately approved. Preserve migration 005 definitions and data. |
| P4-D30 | APPROVED | Use the implemented public per-page `CanonicalBatchReceipt`, RFC 8785 JCS projection and SHA-256 digest from the normative Pre contract. Stage A receipt correctness passed; Gold verification and durable acknowledgment remain Stage B. |
| P4-D31 | APPROVED | Production consumption is bounded incremental page processing. `read_complete_scan()` is fixture/compatibility/smoke only; no ordinary O(history) production rescan or mandatory whole-history staging. |
| P4-D32 | APPROVED | Generic work-window identity is scenario-neutral; scenario remains event provenance and input-set content. |
| P4-D33 | APPROVED | Guarantee Gold-local atomicity and eventual convergence through the durable feed. No distributed transaction or snapshot-isolation claim. |
| P4-D34 | APPROVED | Separate V1 state at `de/artifacts/gold_v1/checkpoint.sqlite3` and `de/artifacts/gold_v1/instance.lock`; preserve V2 under `de/artifacts/gold/`. Actual config, Compose and migration cutover are still unimplemented. |

## Remaining owner/deployment gates

| Item | Proposed resolution and alternatives | Evidence, effects and gate |
|---|---|---|
| Window precision and run bounds | Prefer epoch-zero aligned half-open intervals. Specify canonical time precision/normalization and whether windows may extend beyond a run's declared end; do not use run-relative alignment unless comparability requirements justify it. Alternative is reject events outside bounds or create clipped windows, each changing membership/key semantics. | `de/gold_runtime/window_scheduler.py:114-151` and `de/gold/windowing.py:27-40` are candidates, not a complete V1 identity contract. Resolve in P4A-04; blocks durable window-ID freeze, not feed/state implementation. |
| Event-free run discovery | The feed discovers journaled runs only. If empty runs must be represented, approve a separate registration authority and lifecycle/identity contract; otherwise state that empty runs are outside feed discovery. The alternative adds a second inventory source and reconciliation burden. | `SourceDiscovery.run_discovery_scope="journaled-runs-only"`. Does not block sequence coverage for journaled changes; affects empty-run visibility. |
| Capacity objectives/resource ceilings | Use A–H measurements on actual pipeline and owner hardware, then approve workload, resource ceilings, duration/headroom and catch-up objectives. Alternative is keep capacity uncertified and not activate production; do not adopt a 10-second latency target or Pre test-consumer rate as capacity. | Whole pipeline is `NOT MEASURED`. Blocks capacity certification/production activation, not Core implementation or correctness acceptance. P4A-08/I05/I07. |
| Cutover/rollback and readiness thresholds | Keep one service/owner and V2 data untouched; define maintenance/cutover, code rollback, health thresholds, restore procedure and disk headroom. Alternative is defer live activation after Core acceptance. | P4A-07/I04 activation gate; no destructive rollback or cursor reset. Requires deployment-owner approval, not implementation of local foundation. |

## Deferred Phase 4B decisions

P4-D20–P4-D29 and business decisions B4B-01–15 remain deferred: KPI formulas/weighting, analytical grains, coverage and null/missing semantics, scenario comparison, finality and business correction/revision selection, mart schemas, serving queries, Spring API and Dashboard interactions. The V2 formulas and migration 005 are inventory only. See [`deferred-phase-4b.md`](deferred-phase-4b.md).

## Compatibility invariants

- Verified prefixes are `Pc=min(Hc,Sc)` and `Pi=min(Hi,Si)`, subject to the normative per-page validation and effective-disposition checks in [`pre/contract-design.md`](pre/contract-design.md). Gold need not wait for a moving Bronze high-water.
- Gold verifies receipt version, namespace, binding, requested exclusive cursor, returned interval, contiguous entry coverage, entry count, exact JCS encoded byte length, digest/receipt ID, captured boundaries and safe invalidation watermark. It commits only the actual returned `through_sequence` after all local obligations are durable.
- It never derives progress from event IDs, ClickHouse timestamps/counts, private Silver methods, decoded legacy tokens, or direct Bronze-journal reads.
- A changed effective disposition under a later boundary is a valid correction path. Immutable `source_transition_hash` for an already committed sequence/binding cannot change; `bundle_hash`, effective disposition and old receipt are tracked distinctly.
- `has_more=false` is page-local to the captured eligible prefix only. It does not imply Silver/Bronze catch-up, SUMO catch-up, invalidation completion, window completeness or system health.
- Gold-local transactions atomically couple a feed cursor with its receipt and every required local suppression, membership, work and dirty obligation. Invalidations through the change receipt's fixed safe `Pi` are durably acknowledged before that change cursor. The invalidation cursor remains independent.
