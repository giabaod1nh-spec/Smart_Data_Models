# Phase 4A-Core to Phase 4B handover

This handover describes the processing foundation available from the accepted Core implementation. [Stage B evidence](acceptance-result.md) records actual integration, the measured capacity shortfall and remaining activation gates. Phase 4B requires its own business decisions and approval.

## Core outputs Phase 4B may rely on after Stage B acceptance

- One in-place Gold runtime consumes the Silver public sequence feed and independent invalidation feed. No direct Gold reads from Bronze journals or physical Silver facts; Silver's internal fact reconciliation remains legitimate.
- A separate durable V1 SQLite database at `de/artifacts/gold_v1/checkpoint.sqlite3` and lock at `de/artifacts/gold_v1/instance.lock`. V2 state remains under `de/artifacts/gold/`, with migration 005 and historical rows unchanged. No silent V2 cursor/schema adoption, destructive reset or dual active owner.
- A validated receipt per bounded page: contract version, namespace, binding, requested exclusive cursor, returned interval and exact contiguous sequences, count, complete-envelope JCS byte length, SHA-256 digest/receipt ID, captured Bronze/Silver boundaries and safe invalidation watermark. Gold advances to the actual returned `through_sequence` only with all downstream obligations durable.
- Independent, durable change and invalidation cursors. Invalidation through the receipt's fixed safe `Pi=min(Hi,Si)` is acknowledged locally before the associated change receipt. A pending ClickHouse mirror does not cancel an authoritative Silver-local invalidation.
- Immutable source transition hashes and acknowledged receipt history preserved separately from boundary-dependent effective disposition and bundle hash. A legitimate later disposition change is handled as correction/suppression; a changed immutable hash under the same committed source identity fails closed.
- Durable event identity, suppression, membership, work intent, dirty generation, recompute obligations, retry/fault state and current-effective replay manifest. Indexed event-to-window mapping enables corrections without a history scan.
- The business-neutral input stream reads each indexed member from its exact public Silver sequence. It verifies the durable bundle and transition hashes, fixed invalidation high-water and current window generation. A caller completes the stream before using its input identity and discards partial work when the fence changes. This supports effective-state recomputation and depends on the retained Silver evidence. Gold retains receipt headers and hashes, with full canonical page bytes reserved for selected acceptance fixtures.
- Pure simulation-time Tumbling Windows at 60s and 300s; half-open `[start,end)`, deterministic versioned identity and approved precision/run-boundary behavior. Scenario is event-level provenance and is excluded from generic window identity. Both durations can receive the same observation.
- Independent window candidate eligibility, source-boundary state, completeness, processing state, dirty/recompute state, finality and run lifecycle. Completeness stays `UNKNOWN` absent a Phase 4B expected-coverage contract; publication finality stays `DISABLED` through Core.
- Current-effective-state replay with frozen source binding/boundary and isolated namespace, SQLite DB and lock. No arbitrary historical as-of claim or live cursor/work mutation.
- Operational evidence for Foundation health, per-stage backlogs, latency distributions, resource growth, recovery, retention headroom and actual Gold integration. Core correctness is not capacity or business-serving readiness.

## What 4B must not infer

Candidate membership is not evidence that expected observations exist. A COMPLETE run is not proof of window completeness. Source receipt or durable work metadata is not a business result. Core emits no business marts and does not make `FINALIZED` reachable. Current-effective Silver state is not arbitrary historical as-of state. Do not infer formulas or policy from V2.

## Consistency guarantee

Silver validates each returned page at its checked boundary; it does not create a cross-store snapshot. Gold makes each cursor atomic with its full local obligations. A source conflict just after the boundary may temporarily leave metadata for the earlier boundary; the durable invalidation feed eventually suppresses the event and dirties affected windows. Work creation checks durable suppression. In-flight work is fenced by dirty-generation CAS. A crash before a local acknowledgment replays from the previous cursor; after commit, cursor and complete local obligations recover together. This is **Gold-local atomicity and eventual convergence**, not a distributed transaction or snapshot isolation and not permission to serve stale output.

## Evidence required at handover

1. Assertion-level CORE, RESILIENCE and INTEGRATION acceptance, actual Gold plus actual Silver evidence, independent sequence/event/window oracle, and retained failures/resolution notes.
2. Source-binding verification; receipts/digests and both cursors; multi-run/late-event/duplicate/correction results; proof no ordinary full-history scan.
3. SQLite schema, indexes, transaction boundaries, idempotent migrations, V1/V2 physical isolation, migration-005 and V2 sentinel preservation, exactly-one-owner evidence.
4. Tumbling membership at both durations, boundary/precision/run rules, scenario preservation, completeness `UNKNOWN`, finality `DISABLED` and zero business-mart writes.
5. Invalidation-before/during/after-work, fixed-Pi ordering, suppression, dirty fencing, late accepted events, crash before/after both acknowledgments and eventual convergence snapshots.
6. Replay manifest and stable-boundary evidence, abort on boundary mismatch, retry transient outages, isolated replay state, and unchanged live-state snapshots.
7. Runtime configuration, credential/query boundary, deployment graph, foundation health, retry/backoff/fairness and graceful shutdown.
8. End-to-end benchmark evidence with workload, observed source rate and simulation/wall progression, per-stage backlog, latency distributions, CPU/RSS, SQLite/WAL, ClickHouse load, catch-up and disk/retention headroom. Distinguish tested capacity from maximum sustainable capacity.
9. Open architecture and operations decisions with named owner and approval state; Phase 4B requirement register remains explicitly deferred.

## Phase 4B entry gate

The owner must approve Phase 4B business requirements item-by-item in [`deferred-phase-4b.md`](deferred-phase-4b.md). Architecture then maps approved KPIs to reusable grains, canonical source fields, completeness/coverage semantics, window/finality and correction rules before any business mart, Spring API or Dashboard change. Use completed historical intervals under the selected Tumbling strategy. Rolling Window and Provisional Aggregation are excluded from the current roadmap. Do not design one table per chart.
