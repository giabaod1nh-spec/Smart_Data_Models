# Stage A planning handover

## Planning baseline and work done

Audit baseline: branch `codex/phase-1-v1-raw`, HEAD `addb59a88b2b62a6c97399925298cac20733f84c`, clean tree before document creation. This is exactly the requested source baseline. The only diff from Business Spec baseline `25d943fa3825523e14a596aab601bb4e38983853` is eight added Business Spec files; application source is unchanged across that range.

Created the Stage A planning package in `docs/DEv1/phase-4/phase4b-stage-a/`. No code, schema, migration, runtime, Phase 4A evidence, Business Spec or Master Plan was changed. No git add/commit/branch operation, test, benchmark or service startup was performed.

The repository ignores `docs/**` except `docs/shared/**`; these new files are local and do not appear in ordinary `git status`. They were intentionally not force-added or staged. The source worktree was clean at audit start; after document creation, Git still reports clean because the deliverables are ignored. Owner review must decide whether/how to track them.

## Findings to review first

1. Phase 4A Core correctness is accepted for tested cases; completeness is `UNKNOWN`, finality is `DISABLED`, performance capacity was insufficient in one scoped test, and operational readiness is not certified.
2. Performance minimum entry gate remains `ENVIRONMENT BLOCKED`; the latest paced attempt stopped on the memory safety floor and missed the full evidence boundary.
3. Expected Observation/coverage contract is not yet authoritative; strict 100% coverage cannot be applied with a denominator inferred from received rows.
4. E2 occupancy percent is incorrectly scaled at values ≤1 in source; lane fallback has a different raw unit, so shared source output is not trustworthy until corrected/versioned.
5. Queue method provenance, scenario window identity, and cross-store business publication require explicit decisions.
6. Master Plan governance text is a stale snapshot relative to current Phase 4A implementation; reconcile proposed Stage A/B/C split and gate ownership.

## Recommended next action

Owner review decisions D-A01..15, prioritizing governance reconciliation, G-COV, G-SRC/G-QUEUE, D-A06 window identity, G-METRIC/G-CAL and G-REV. Then authorize narrowly scoped SF-01/02/07 feasibility packages and independent oracle design. Keep optional throughput/waiting study separate. Do not approve physical marts or business writes until metric, expected population and publication gates are settled.

## Implementation prerequisites

- Explicit approval of Stage A scope and amendment path for the authoritative Master Plan / Performance Acceptance record.
- Approved source correctness and metric/definition versions for each adopted result.
- Independently computable expected observation slots and strict per-metric coverage policy.
- Resolved Core/business window identity and scenario provenance.
- Approved physical model, idempotent revision protocol, cross-mart visibility and invalidation suppression boundary.
- Independent arithmetic/source oracle, failure/replay suite and rollback path.
- Performance gate completion or explicit approved amendment with risk/scope/stop conditions.

## Readiness verdict

**SOURCE FEASIBILITY REQUIRED; REQUIREMENT APPROVAL REQUIRED; PERFORMANCE ENTRY GATE BLOCKED.** This package is ready for owner plan review. It is not ready for implementation, and owner approval of the plan alone would not clear unresolved mandatory gates.

