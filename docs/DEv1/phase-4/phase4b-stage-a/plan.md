# Stage A implementation plan

All packages below are proposed tasks, not authorization. No task becomes executable merely by appearing here. Design/research may start after owner review; production writes require G-COV and G-REV, and implementation requires plan approval plus resolution of the applicable entry gates.

## Dependency graph

```text
P4B-A01 requirements/governance reconciliation
  ├── P4B-A02 source feasibility (occupancy, queue, schedule, optional research)
  │    └── P4B-A03 expected population and coverage contract
  ├── P4B-A04 metric registry + independent oracles
  │    └── P4B-A05 pressure calibration (only after occupancy/queue quality)
  └── P4B-A06 Core/window identity compatibility decision
P4B-A03 + A04 + A06 + P4B-A05 (only for pressure) + G-REV
  └── P4B-A07 architecture and mart approval
       └── P4B-A08 pure aggregation engine
            └── P4B-A09 Core integration + staging/publication
                 ├── P4B-A10 Gold serving/query verification
                 └── P4B-A11 recovery/replay/failure acceptance
P4B-A08..11 + G-PERF ──> P4B-A12 Stage A correctness/performance closeout
```

## Work packages

### P4B-A01 — Requirements and governance reconciliation

- **Objective/requirements:** resolve B4B-01..15, Stage A/B/C proposal, Master Plan staleness, B4B-04 nested comparison and source-of-truth for gates.
- **Dependencies/scope:** owner review of `deferred-phase-4b.md`, Master Plan, eight Business Spec files; record approved/proposed/deferred decisions and AC mapping.
- **Out of scope:** no Master Plan/spec edits or implementation.
- **Existing/files:** `docs/DEv1/Master_plan_DE_V1_.md`, `phase-4/deferred-phase-4b.md`, `phase4b-business-spec/*`; proposed amendments recorded in Stage A decisions.
- **Risks/tests/evidence:** conflicting governance; review workshop and signed decision log; preserve current docs unchanged.
- **Acceptance/exit:** each B4B has owner/status/stage/AC; amendment decision and review evidence attached.
- **Rollback/approval:** documentation-only; owner approval required to change scope.

### P4B-A02 — Source feasibility and source correctness prerequisites

- **Objective/requirements:** prove source units/populations for M-01..07 and optional experiment data.
- **Dependencies/scope:** SUMO/Silver owners; SF-01..07 in `source-feasibility.md`; saved versioned fixtures and independent source oracle.
- **Out of scope:** no source fix, schema/contract or runtime change under this package until separately approved.
- **Files:** inspect `Visualize/observation/*`, `Visualize/simulation/backend.py`, `contracts/*`, `de/silver/*`, AI metrics; if later approved, changes isolated to identified source package and regression scope.
- **Tests/acceptance/evidence:** occupancy raw values and unit mapping, queue method compatibility, storage geometry, cadence capture; independent TraCI/TripInfo outputs and resource overhead.
- **Risks/exit:** old occupancy values cannot be safely transformed without provenance. Exit is explicit ADOPT/DEFER/REJECT per metric; source correction has its own approval/rollback.

### P4B-A03 — Expected Observation and Coverage Contract

- **Objective/requirements:** B4B-10 / Q-01, Q-02; define expected population/slots and per-metric strict coverage.
- **Dependencies/scope:** A01; A02 schedule/topology evidence; lifecycle/source owner.
- **Out of scope:** no official publication or cadence contract change.
- **Files:** proposed contract amendment only; inspect manifest, event schema, topology/archive, Core membership.
- **Tests/evidence:** independent schedule oracle, fractional timestamps, missing/delayed/duplicate/conflict, participant and terminal cases.
- **Acceptance/exit:** 100% meaning, denominator, applicability and lifecycle policy approved; expected slot identity computable independently.
- **Risk/rollback/approval:** if schedule unavailable, keep official results blocked; owner signs G-COV before output writes.

### P4B-A04 — Metric registry and independent oracle

- **Objective/requirements:** B4B-01..07; M-01..08 and AC-01..09.
- **Dependencies/scope:** A01; source fields identified by A02. Define formulas, precision, tie rules, null behavior and pure oracle fixture.
- **Out of scope:** no production aggregation and no official score calibration.
- **Files:** proposed `de/gold/business/*` design location after owner selection; tests only in approved implementation phase.
- **Acceptance/evidence:** independent hand-computable examples including nearest-rank, rollup and edge cases; metric registry IDs/version locked.
- **Exit/approval:** G-METRIC; P95 exact state budget specified. Pressure may remain separately blocked.

### P4B-A05 — Pressure calibration

- **Objective/requirements:** B4B-06/07; AC-05,24..27.
- **Dependencies/scope:** A02 occupancy/queue semantics corrected or versioned; A03 coverage; A04 candidate formula; domain owner ground-truth set and holdout protocol.
- **Out of scope:** do not calibrate from buggy occupancy or solely from pressure components; no threshold tuning after seeing acceptance holdout.
- **Evidence/acceptance:** empty/normal/red phase/peak/incident/spillback/directional cases; independent trajectories and owner-approved error targets; version/weights/thresholds signed.
- **Exit:** G-CAL, or mark pressure-dependent metrics unavailable/deferred. Physical metrics continue.

### P4B-A06 — Core compatibility and window identity decision

- **Objective/requirements:** B4B-04, Q-01, B4B-12; resolve scenario-neutral request versus current scenario-bearing Core identity.
- **Dependencies/scope:** inspect Core proof/membership behavior and test cross-scenario window examples; compare Core amendment vs business combining approach.
- **Out of scope:** no windowing behavior change in Stage A plan task.
- **Files:** `de/gold_runtime/processing_ledger.py`, `window_scheduler.py`, `processor.py`, `de/gold/windowing.py` inspected; amendment path separately approved if selected.
- **Acceptance/evidence:** deterministic membership and revision oracle across scenario switch and both durations.
- **Exit/approval:** one documented, backwards-compatible identity policy, with owner acceptance of any Core amendment.

### P4B-A07 — Aggregation/mart/revision design approval

- **Objective/requirements:** B4B-11/12 and Q-03/Q-04; choose Option A/B/C, physical model, lineage and atomic visibility.
- **Dependencies:** A03, A04, A06; A05 for pressure fields only; G-REV.
- **Scope/files:** architecture, ClickHouse/SQLite migrations design, query patterns; inspect existing migration and Core stores. No DDL yet.
- **Acceptance/evidence:** failure matrix, row/key model, ClickHouse ambiguity/idempotency proof plan, legacy V2 isolation and additive migration plan.
- **Exit:** owner approves model, current-effective semantics, retention and rollback.

### P4B-A08 — Pure aggregation engine

- **Objective:** deterministic metric computation over verified input model; AC-01..09,10..14.
- **Dependencies:** A03/A04/A06/A07 approval; A05 only for pressure.
- **Scope/files:** implement bounded accumulators and exact queue selection in approved Gold package; preserve Core proof interfaces.
- **Out of scope:** no database writes, API/UI, source contract change.
- **Tests/evidence:** independent oracle, property/fixture tests, both windows, null/zero/ties/rollups and precision; no production function as its own oracle.
- **Exit/rollback:** pure module can be disabled/reverted without touching Core/legacy V2; owner-authorized coding task required.

### P4B-A09 — Core integration, marts and publication

- **Objective:** use one verified traversal to derive/stage/verify/publish compatible business revisions.
- **Requirements/dependencies:** A08, A03, A07, G-REV and source performance entry conditions.
- **Files:** approved `de/gold_runtime/*`, migration and runtime config changes only under later explicit implementation approval.
- **Tests:** local/CH transaction, ambiguous write, partial rows, fence loss, generation race, invalidation after publication, one manifest across all grains.
- **Out of scope:** no Spring/React; no changes to Raw/Bronze/Silver behavior absent separate source package.
- **Exit/evidence:** read-back digest and cross-store crash matrix pass; rollback suppresses/removes only unpublished new namespace objects safely.

### P4B-A10 — Gold serving queries

- **Objective/requirements:** B4B-13; serving contract and AC-20..35,39.
- **Dependencies:** published schema/revision from A07/A09.
- **Scope:** bounded queries/views and selection contract; inspect existing Spring mappers only for compatibility.
- **Out of scope:** no Spring endpoint/DTO or dashboard code.
- **Acceptance:** historical runs offline from Orion, latest-effective selection, gaps, page/range guards, revision-consistent values, experiment fail-closed behavior.
- **Exit/evidence:** query plans and independent fixture outputs supplied to Stage B; rollback by disabling views/new query path.

### P4B-A11 — Recovery, replay and failure-path verification

- **Objective:** AC-15..19, restart/replay/legacy V2 protection.
- **Dependencies:** A09/A10; disposable isolated fixtures and resource budget.
- **Scope:** crash barriers at every publication point, ambiguous insert recovery, replay namespace and same-binding deterministic re-run.
- **Out of scope:** no use of preserved performance volumes/artifacts as mutable fixtures.
- **Acceptance/evidence:** independent SQLite + ClickHouse physical audit, old effective suppression, no mixed rows, V2 unchanged.
- **Exit:** all injected failures converge to one valid manifest or no publish; operational runbook reviewed.

### P4B-A12 — Stage A acceptance and performance closeout

- **Objective:** separate correctness/publication/recovery/serving/performance/operational verdicts; satisfy approved DoD.
- **Dependencies:** A08..11, all adopted metric gates, Performance Acceptance gate and owner operating envelope.
- **Scope:** stage tests, paced integrated proof, both durations, bounded drain and overhead evidence.
- **Out of scope:** API/UI final acceptance and Phase 5 cutover.
- **Acceptance/evidence:** `acceptance.md`, raw artifacts, independently verified result manifests and each separate verdict.
- **Exit/rollback:** no single PASS shorthand; unresolved dimension stays OPEN/INCONCLUSIVE/ENVIRONMENT BLOCKED. Owner accepts Stage A results before Stage B handoff.

