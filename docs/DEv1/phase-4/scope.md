# Phase 4 scope boundary

Status: planning baseline for owner review; implementation not authorized by this document.

## Phase 4A — Gold V1 processing foundation

**In scope:** refactor the existing `de/gold/` and `de/gold_runtime/` in place into one V1 active analytical runtime; consume only canonical Silver V1 bundles and invalidation feed; validate source binding, verified applied change prefix and independent safe invalidation prefix; track run/generation/topology/manifest identity; process simulation-time work windows; persist progress, work intent, dirty state, retry/fault and replay metadata; recover after crash/outage; expose foundation health and operational metrics; add only necessary infrastructure state migration; test real Silver/CH integration, isolation and V2 data preservation.

Phase 4A may persist source identities, hashes, window candidates, completeness/finality signals and recomputation obligations in Gold-owned SQLite. Such records are **processing metadata**, not analytical results and not consumer marts. It must not expose dashboard-ready results.

## Phase 4B — explicitly deferred

| Responsibility | Status / gate |
|---|---|
| KPI formulas, congestion/severity/priority/hotspot scoring, network metrics | Deferred until business definitions and oracle examples approved. |
| Intersection/direction/network mart grains and schemas | Deferred until analytical requirements and nullable/quality semantics approved. |
| Scenario comparison, traffic trends, signal-performance analytics | Deferred until comparison population, window, quality and formula decisions approved. |
| Coverage thresholds, quality labels, publication/finality policy | Deferred; cannot infer from scheduler or source completeness. |
| Gold CH marts/views, serving SQL/query contracts | Deferred until mart and revision semantics approved. No Phase 4A inserts to migration-005 tables. |
| Spring Analytics repository/DTO/OpenAPI and API | Deferred, requires approved query contract and null semantics. |
| Dashboard charts/tables/filters/interactions | Deferred, requires owner requirements and API readiness. |

## Ownership boundaries

| Component | Owns |
|---|---|
| Bronze V1 | Semantic event identity, revisions, ACCEPTED/CONFLICTED and durable change/invalidation journals. Gold cannot reclassify. |
| Silver V1 | Canonical validity, topology-aware facts, complete event bundle reader, source boundaries, invalidation feed and archive identity. Gold cannot use raw Silver fact scans as a substitute. |
| Gold V1 Phase 4A | Canonical adapter, work identity, scheduling, durable progress, dirty/recompute coordination, recovery/replay and foundation readiness. |
| Gold Phase 4B | Business semantics, marts and publication contract after approval. |
| Spring/Dashboard | Serving/API and presentation after Phase 4B contracts. |
| Realtime path (Orion-LD projector/API) | Operational realtime projection only; never an analytical source for Gold. |

## Non-goals and forbidden actions

- No parallel `de/gold_v1/`, `de/gold_runtime_v1/`, or second production Gold service.
- No V1 direct reads from `silver_fact_*`; no Orion-LD/projector/realtime API source.
- No constants for intersection IDs, direction names, cadence, or expected sample count.
- No conversion of absent/unavailable/null source metrics to zero.
- No speculative KPI calculation or business mart publication.
- No Spring or Dashboard changes in 4A.
- No destructive migration, rewrite or deletion of migration-005 objects or historical V2 rows.
- No arbitrary historical as-of replay claim; Silver replay/read boundary is current-effective-state only.
- No assertion that isolated page completeness means whole scan/window finality.

## Exit language

4A can exit as **foundation operationally ready** only when approved processing invariants and integration evidence pass. This says nothing about analytics output readiness, KPI correctness, API readiness or dashboard serving readiness; those are separate 4B/serving gates.
