# Phase 4A blockers and package gates

Status reflects source inspection at `038261bed5fc59d86baa9d8007becad7ce2ad6ea` (current planning-document review baseline). “Hard” is scoped to the dependent capability; it does not imply all of Phase 4A is blocked.

**Phase 4A-Pre proposal:** [contract design](pre/contract-design.md) and [acceptance matrix](pre/acceptance.md) define the recommended resolution for B4A-01/B4A-02. These blockers remain until the Silver/Gold owners approve the contract and implementation evidence passes.

**Capacity status:** the end-to-end audit in [pre/capacity-and-backpressure.md](pre/capacity-and-backpressure.md) found source-level batch/poll limits and durable-buffer behavior but no retained continuous-load result. Whole-pipeline capacity is `NOT MEASURED`; no service is certified sustainable from the Phase 3 fixture report.

## Hard technical blocker for a dependent capability

| ID | Finding | Blocked capability | Resolution / owner |
|---|---|---|---|
| B4A-01 | `CanonicalSilverReader` has bounded pages and complete scans but no public accepted-change cursor/feed or run enumeration. Query requires explicit run/digest/time interval; page order is event ID. | P4A-02B Gold integration and any production source cursor claiming no skipped observations. | Complete P4A-Pre verified-applied-prefix API/receipt and Stage A acceptance; owner approves API. Never checkpoint by event ID, CH timestamp or row count. |

## Conditional production activation blockers

| ID | Finding | Does not block | Blocks until resolved |
|---|---|---|---|
| B4A-02 | Current reader exposes no public sequence-page receipt; token is opaque and not a durable public receipt. Gold cannot rely on Silver private methods or decode token claims. | P4A-02A adapter baseline and controlled scans; generic SQLite schemas; scheduler mechanics. | P4A-02B/production source checkpoint and replay wait for the per-page `CanonicalBatchReceipt` and Stage A proof of exact applied-prefix coverage. |
| B4A-03 | `read_complete_scan()` accumulates all bundles in RAM and returns a tuple. | Bounded fixture scans with explicit bundle/byte/time caps and memory measurement. | Unbounded production scans; select temporary SQLite page staging behind a Silver page stream/receipt, or a measured scan scope budget. Configure stage quota and cleanup. |
| B4A-04 | No approved per-window expected-observation contract, cadence/coverage denominator or missing-sample rule. | Generic window identity, event membership, candidate scheduling, lifecycle capture and dirty/recompute mechanics. | Marking window completeness true and any publication eligibility based on coverage. Keep completeness UNKNOWN until contract exists. |
| B4A-05 | No approved lateness, finality, terminal-run or consumer correction policy; existing runtime constants are legacy. | Candidate scheduling, source tracking, work intent, dirty generation and recovery. | Any `FINALIZED`/publication transition or serving-visible revision. Finality remains disabled in 4A. |
| B4A-06 | No transaction spans Silver page validation, Bronze conflict/invalidation, Gold SQLite and ClickHouse. Source can advance immediately after a valid page/receipt. | Foundation metadata processing under a recorded receipt and eventual invalidation convergence. | Any claim of distributed snapshot consistency or safe serving. Require durable invalidation, dirty-generation CAS and race/crash acceptance; 4A publishes no marts. |
| B4A-07 | Production deployment thresholds, cutover and rollback are not approved. | Local runtime refactor, config/health development, isolated tests. | Production activation under P4A-07; approve one-service cutover, safe rollback and readiness thresholds. |

## Owner decisions by package

- **P4A-01:** close architecture register and assign Silver API/state owners; no technical blocker.
- **P4A-02A:** can proceed on current public API with controlled scope and no durable source cursor.
- **P4A-02B:** blocked by B4A-01/02; Option A Silver API is a cross-phase change request; Option B needs measured and approved scan budget.
- **P4A-03:** generic SQLite source binding/work/lease/dirty schema can proceed; advancing accepted-change cursor is blocked by receipt/feed contract. Invalidation cursor can be implemented from current public invalidation feed.
- **P4A-04:** identity, half-open membership, candidates and dirty mechanics can proceed once the identity/alignment version is recorded. B4A-04/05 block completeness/finality assertions only.
- **P4A-05:** generic dirty-tracking mechanics can proceed. Baseline-only tests may retain the existing invalidation page contract; production incremental consumption uses the new additive frozen-prefix method after Pre Stage A and preserves legacy `SNAPSHOT_STALE`. B4A-06 acceptance requires eventual convergence and forbids a distributed atomicity claim.
- **P4A-06:** isolated replay metadata and fake-source recovery can proceed; source-scoped production replay waits for receipt and incremental strategy.
- **P4A-07:** local refactor, health and observability development can proceed; production activation waits for B4A-01/02/03/06/07 gates. Business finality is not required while publication stays disabled.
- **P4A-08:** tests and evidence harness can be developed throughout; full production acceptance waits for applicable source, resource, race, cutover and integration gates.

P4-D34 adds a Core gate: V1 must use a physically distinct Gold checkpoint database and instance lock under the approved `gold_v1` path; preserve historical V2 state, migrate/configure/rollback explicitly, and run one active service. This blocks V1 state activation/cutover, not Phase 4A-Pre or unrelated adapter/scheduler design.

## Deferred business decisions (not Phase 4A blockers)

KPI formulas, mart grain/schema, business coverage threshold, output revision selection, serving queries, Spring Analytics API and Dashboard requirements remain in `deferred-phase-4b.md`. They do not block generic source adapter, state, window identity, invalidation, recovery or foundation acceptance. They block only their corresponding 4B outputs and publication contract.

## Non-blocking source limitations

- The unsigned Base64 pagination token is acceptable only as opaque, in-process continuity data; no cross-trust-boundary use is planned.
- Silver lifecycle projection separates registry state, archive/manifest mirror and freshness. Nullable `failure_evidence_json` does not prove producer failure evidence is populated; Gold must preserve unknown.
- Phase 3 runtime evidence is retained in its acceptance report; this plan revision does not independently rerun Docker/ClickHouse acceptance.
