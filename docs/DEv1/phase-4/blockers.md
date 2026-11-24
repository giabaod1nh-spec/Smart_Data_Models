# Phase 4A-Core blockers and gates

Status is reviewed at `9adc6fc7d4dd6ecada8b01b6d53b6ffe2d3b6a1b`. A resolved Pre dependency does not mean Gold Stage B, whole-pipeline capacity, or production readiness has passed.

## Resolved Phase 4A-Pre blockers

| ID | Status | Resolution and boundary |
|---|---|---|
| B4A-01 | RESOLVED — Silver contract implemented; Stage A accepted | `CanonicalSilverReader`/`CanonicalReadSession` expose `discover_canonical_source`, `read_canonical_changes_page`, `read_canonical_invalidations_page`. `de/silver/incremental_reader.py` verifies sequence/disposition evidence and frozen boundaries. Gold integration is still pending. |
| B4A-02 | RESOLVED — public receipt implemented; Stage A accepted | Frozen per-page receipts use the Pre contract's DTO/JCS/SHA-256 rules. Actual Gold receipt verification and durable local acknowledgment remain Stage B. |
| B4A-03 | RESOLVED for production source polling | The selected feed is bounded by entry/byte/time limits. Existing `read_complete_scan()` still materializes a tuple and is restricted to bounded fixture/compatibility/smoke uses; it is not a production progress path. |

## Remaining implementation and activation blockers

| ID | Status | Blocks | Resolution / owner |
|---|---|---|---|
| B4A-04 | OPEN — business contract deferred | Window completeness or publication based on expected coverage | Keep `window_completeness=UNKNOWN`; Phase 4B approves cadence, expected population, denominator, thresholds and missing-data semantics. Does not block Core candidate scheduling/membership. |
| B4A-05 | OPEN — business finality deferred | Any `FINALIZED` transition or business publication | Core fixes `publication_finality=DISABLED`. Phase 4B owns lateness, terminal-run and correction/revision policy. |
| B4A-06 | Architectural limit accepted; Stage B proof pending | Claim of distributed atomicity/snapshot or serving-safe output | Implement Gold-local atomicity, independent invalidation consumption, suppression and dirty-generation fencing; test real cross-store races and crashes. No distributed transaction/snapshot claim. |
| B4A-07 | OPEN — production deployment | Live cutover / operational-readiness PASS | Approve concrete one-owner maintenance/cutover/rollback procedure, readiness thresholds and resource/disk headroom after Stage B evidence. |
| B4A-08 | OPEN — Phase 4A-Core implementation | Actual durable Gold acknowledgment | Implement V1 state at approved separate paths, receipt validation, work obligations and transactional independent cursors. Pre's test SQLite harness is not Gold evidence. |
| B4A-09 | OPEN — capacity certification | Sustainable whole-pipeline capacity claim | `NOT MEASURED`; instrument and benchmark the actual end-to-end path at approved workload and resource limits after Core Stage B correctness. |
| B4A-10 | OPEN — window identity freeze | Durable production window keys | Decide numeric precision/normalization and run-boundary handling while retaining epoch-zero preferred alignment and pure 60s/300s tumbling. This does not block feed/state development. |

## Package gates

- P4A-01 can begin after owner approval of this revised plan; it freezes source/API, state, window and module contracts.
- P4A-03 builds isolated SQLite state and transactional primitives. For a new source, initialize both exclusive cursors at zero only after successful cold `discover_canonical_source`; advance beyond zero only with a verified receipt and complete local obligations.
- P4A-02A is limited to bounded fixtures/compatibility; P4A-02B is the only production source path and requires P4A-03 transaction integration.
- P4A-04 production integration requires P4A-02B/P4A-03 and the window precision/run-boundary resolution before durable IDs. Fixture-only membership can be developed earlier. Window completeness stays unknown and finality disabled.
- P4A-05 requires P4A-02B/P4A-03/P4A-04 and proves fixed-watermark invalidation-before-change acknowledgment, suppression and dirty fencing.
- P4A-06 requires durable state and invalidation semantics; production replay additionally freezes the accepted feed boundary and uses isolated state.
- P4A-07 in-place runtime refactor can proceed after contracts; live activation waits for all applicable Stage B, isolation, deployment and readiness gates.
- P4A-08 is actual Gold Stage B integration and acceptance. It must not be reported as passed from Pre Stage A evidence.

## Non-blocking limitations and historical failures

- Source discovery is journaled-run-only; an event-free run needs a separately approved run-registration source if it must be represented.
- Bronze journal retention/no-prune/no-rewind remains an operational assumption. Replacement/rewind/retention gap fails closed; no in-place cursor reset.
- The repository's 10 historical architecture failures listed in `pre/evidence/architecture-comparison.json` remain documented baseline failures. They are not evidence that the complete repository passes and must not be blanket-skipped or silently dropped from future regression reports.
- All business analytics, coverage, publication, API and dashboard requirements in Phase 4B are deferred and do not block Core foundation correctness.
