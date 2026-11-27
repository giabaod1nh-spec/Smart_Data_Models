# Phase 4B Stage A — Gold Business Layer

**Status:** Technical design and implementation plan for owner review. No business, schema, or architecture decision in this package is approved by this document.

**Baseline review:** local và remote `e57116e5bdda9fc6c39fdf84ae83c78dcd780cbc` trên `codex/phase-1-v1-raw`. Working tree và index sạch trước revision.

This package turns the Phase 4B review candidate into an evidence-linked Stage A plan. It does not change code, migrations, runtime configuration, Phase 4A records, or the business specification. The Phase 4B Stage A/B/C split remains proposed and must be reconciled with the Master Plan by the owner.


## Reading order

| Document | Responsibility |
|---|---|
| [audit.md](audit.md) | Repository baseline, source inventory, current behavior and discrepancies |
| [requirements.md](requirements.md) | B4B-01..15 disposition, scope, output inventory and traceability |
| [source-feasibility.md](source-feasibility.md) | Source-to-Silver metric feasibility and source research |
| [metric-contract.md](metric-contract.md) | Proposed definitions, sufficient statistics and versioning |
| [coverage-contract.md](coverage-contract.md) | Expected samples, applicability, quality and lifecycle |
| [architecture-design.md](architecture-design.md) | Reusable verified traversal and alternatives |
| [mart-design.md](mart-design.md) | Logical products, candidate physical model and access patterns |
| [publication-recovery-design.md](publication-recovery-design.md) | Revision, visibility, invalidation and crash recovery |
| [serving-contract.md](serving-contract.md) | Logical historical queries and experiment metadata |
| [performance-design.md](performance-design.md) | Existing capacity evidence and staged measurement plan |
| [plan.md](plan.md) | Ordered work packages and gates |
| [acceptance.md](acceptance.md) | Independent oracle plan and traceability to AC-01..40 |
| [decisions.md](decisions.md) | Unapproved decisions requiring owner action |
| [blockers.md](blockers.md) | Entry blockers, evidence and mitigation |
| [handover.md](handover.md) | Review directions and implementation prerequisites |
| [review-resolution.md](review-resolution.md) | RV-01…15, SR-01…04 và R01–R09 |
| [state-schema.md](state-schema.md) | SQLite schema, atomic handoff và migration |

## Current planning verdict

**READY FOR OWNER DESIGN REVIEW. SOURCE FEASIBILITY REQUIRED. IMPLEMENTATION ENTRY BLOCKED.** Xem [ma trận RV và SR](review-resolution.md). G-ENTRY và G-PERF có mục đích riêng. Gate hiện hành giữ nguyên.

## Final preparation candidate

**DESIGN CLOSURE INCOMPLETE** at reviewed baseline 0ccf2062c368def10b3601402215698b4a4e84ce. See [executed evidence](preparation-result.md), [resource sizing](resource-sizing.md), [final technical decisions](final-design-closure.md) and [implementation dependencies](plan.md). Earlier readiness and baseline sections above describe the prior review. G-ENTRY and G-PERF stay closed.
