# Phase 4A handover contract

This is the planned contract between a future accepted Phase 4A foundation and Phase 4B. It is not evidence that either phase has been implemented.

The Silver API contract and Stage A acceptance must be complete before Phase 4A-Core Gold integration (Stage B); the Pre test-only cursor harness is not production Gold evidence. Whole-pipeline capacity is currently `NOT MEASURED`; finite Phase 2/3 catch-up evidence and its limits are recorded in [pre/capacity-and-backpressure.md](pre/capacity-and-backpressure.md).

## Phase 4A outputs Phase 4B may rely on after acceptance

- A single in-place Gold V1 runtime consuming canonical Silver bundles and invalidation feed; no V2 table or realtime API source path.
- Approved canonical adapter contract and typed error map; Gold-owned components never directly query physical Silver fact tables. Silver's internal canonical reader may query them to construct/reconcile valid bundles.
- Stable processing identity: namespace, simulation run ID, generation, topology digest, immutable manifest identity, Silver schema/source binding and Gold processor version.
- Public source-boundary receipt, if introduced, with Bronze change/invalidation high-waters, Silver applied cursors, reader/schema versions, query scope, full run/topology identity and whole-scan completion. Gold must not decode tokens, call private Silver methods, read Bronze journals or infer progress.
- Production incremental discovery contract, or explicit accepted full-rescan scope/resource limitations. Baseline `read_complete_scan()` accumulates O(N) bundles in memory and returns no public receipt; it cannot justify production-scale durable progress by itself.
- Durable exclusive source progress only for the approved Phase 4A-Pre per-page receipt/change API and independent invalidation sequence/binding cursor. Silver may deliver through `min(Bronze H, Silver applied cursor)` after per-event conflict/invalidation guards; it does not wait for live source catch-up. Gold acknowledges invalidations through the receipt's fixed safe applied invalidation watermark before acknowledging the change receipt. Gold SQLite transactions are atomic locally, but no cross-database snapshot exists.
- Bounded-memory limits and measured page/staging bytes, RSS, scan duration, quota, restart cleanup and stale-scan behavior. Temporary staging is non-authoritative until final receipt and Gold commit.
- Deterministic generic window identity: namespace, run ID, generation, topology digest, manifest identity, window-spec version, duration and boundaries. Scenario remains event-level; a scenario transition does not split or relabel the work window.
- Half-open simulation-time membership and approved alignment; source completeness, window completeness, publication finality and dirty/recompute dimensions remain separate. 4A never assumes a single scenario per window or marks `FINALIZED` without approval.
- Durable retry/fault/obligation and replay metadata, with isolated replay DB/lock/namespace and current-boundary refusal rules.
- Nullable source values, per-metric availability/quality and archived topology preserved in processing inputs; no invented zero or fixed direction population.
- Foundation health and lag/backlog visibility, with analytics readiness explicitly separate.
- Historical migration-005 objects/data preserved and before/after evidence retained.

## What 4B must not infer

Foundation readiness does not certify an approved KPI formula, business coverage threshold, finality policy, output mart grain/schema, latest revision selection, serving query, Spring contract or Dashboard behavior. Candidate window identity is not proof that every expected observation exists. A work candidate/receipt is not a finalized analytical result. Silver current-effective state does not provide arbitrary historical as-of reconstruction.

## Cross-database consistency limit

Silver guarantees page validity and source-boundary checks for the returned page/receipt. Gold can atomically commit its receipt reference, work intent, invalidation disposition, dirty generation and local cursor in one SQLite transaction. Bronze may publish a conflict after Silver's last boundary check and before that Gold commit; no final recheck prevents every such race. The durable invalidation feed is the convergence mechanism: Gold consumes it, suppresses the event, dirties affected work, and fences in-flight completion with dirty-generation CAS. A crash before feed acknowledgement causes replay from the last Gold cursor. This is eventual recovery correctness for processing metadata, not distributed snapshot isolation or a guarantee that derived state is safe to serve. Phase 4A publishes no business output.

## Handover evidence required

1. Accepted P4A acceptance matrix with assertion-level results and retained failure/resolution evidence.
2. Versioned canonical reader/invalidation adapter contract, typed errors and public receipt/boundary semantics (or explicit baseline-only limitation).
3. Gold runtime/state schema and migration inventory; proof that migration-005 V2 definitions/data are unchanged.
4. Documented window origin/durations, scenario-neutral work identity, completeness limitations and finality state. An approved lateness/revision policy is required only if finality is enabled; otherwise show finality disabled.
5. Replay manifest examples, stable-boundary evidence, crash recovery/cursor snapshots and live/replay isolation proof.
6. Operational configuration, credential grants, Compose dependency graph, health-state definitions and observed source-lag/recompute behavior.
7. Phase 4B requirements register with owner and approval state; no business semantics silently inherited from V2.
8. Measured bounded-memory limits, stage cleanup/restart evidence, resource budget and whether production uses incremental feed or bounded rescan.
9. Cross-database race timeline, dirty-generation fencing, crash convergence evidence and explicit no-distributed-snapshot statement.

## Phase 4B entry gate

The project owner approves business requirements item-by-item in `deferred-phase-4b.md`; architecture maps approved KPIs to reusable analytical grains, source fields, windows and revision policy; nullable/API consequences are reviewed before mart/API code. If 4A has not resolved incremental discovery, Phase 4B may not claim complete historical/current coverage from a scheduler cursor. The foundation must not be described as completed analytics. Unapproved finality, KPI, coverage and Dashboard decisions do not retroactively block an accepted scheduler foundation whose publication-finality state remains disabled.
