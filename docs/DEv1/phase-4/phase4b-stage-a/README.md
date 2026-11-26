# Phase 4B Stage A — Gold Business Layer

**Status:** Technical design and implementation plan for owner review. No business, schema, or architecture decision in this package is approved by this document.

**Planning baseline:** branch `codex/phase-1-v1-raw`, HEAD `addb59a88b2b62a6c97399925298cac20733f84c`, clean working tree at audit start. Planning documents created against this baseline.

This package turns the Phase 4B review candidate into an evidence-linked Stage A plan. It does not change code, migrations, runtime configuration, Phase 4A records, or the business specification. The Phase 4B Stage A/B/C split remains proposed and must be reconciled with the Master Plan by the owner.

Repository `.gitignore` excludes `docs/**` except `docs/shared/**`. These files are present locally but ignored by Git; they were not force-added or staged, in line with the instruction against Git mutations. To include them in a future reviewed change, the owner must explicitly decide how this repository should track planning documents.

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

## Current planning verdict

**SOURCE FEASIBILITY REQUIRED; REQUIREMENT APPROVAL REQUIRED; PERFORMANCE ENTRY GATE BLOCKED.** Gold Core correctness has an accepted result, but its source coverage remains `UNKNOWN` and finality `DISABLED`. Do not interpret this planning package as implementation authorization or as a waiver of the Phase 4 Performance Acceptance gate.

