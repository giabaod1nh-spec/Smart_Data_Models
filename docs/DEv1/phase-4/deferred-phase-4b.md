# Deferred Phase 4B business backlog

All items are **DEFERRED — OWNER APPROVAL REQUIRED**. This register asks what must be decided; it does not prescribe KPI formulas or one table per chart. Each item should map to reusable marts by approved analytical grain and semantics.

| ID / area | Business question and KPI definition | Grain / window | Candidate source fields (not formula approval) | Open semantics / shared mart | 4A dependency and approval gate |
|---|---|---|---|---|---|
| B4B-01 Intersection traffic | What describes traffic load/flow at each intersection? Define each KPI and handling of unavailable/partial inputs. | Intersection × run/scenario × window; compare 60/300 only if approved. | Per-direction vehicle count, PCU, speed, waiting count, queue, occupancy; event time, scenario, quality/availability. | Direction rollup and nullable intersection totals; coverage denominator, weighting, partial sample policy unresolved. Could share a window-grain traffic mart with direction views. | Needs canonical bundles, archived direction topology, window completeness and finality from 4A; owner approves formulas/grain/coverage. |
| B4B-02 Direction traffic | Which approach/direction needs intervention and how does it change? | Run × intersection × archived approach × window. | Direction key and traffic metrics, event identity, topology digest, availability. | Direction naming/order and missing-approach policy follow archived catalog; no N/S/E/W assumption. Shared traffic mart likely; confirm semantics. | Needs full source boundary and topology mapping; approve direction grain and nullable/quality contract. |
| B4B-03 Network-wide analytics | Is a network aggregate required and what does it mean when coverage varies by intersection? | Network × scenario × window, if needed. | Intersection/direction metrics plus source quality and topology population. | Network membership, weighting, duplicate aggregation, incomplete network behavior unresolved. Prefer reuse of lower-grain facts over separate chart-specific table. | Needs 4A population completeness and stable window identity; owner decides business question, grain and coverage. |
| B4B-04 60-second vs 300-second comparisons | Which time scales should be compared and what change should users interpret? | Same entity/scenario across nested or aligned windows. | Simulation time, metrics chosen by B4B-01/02, run identity. | Alignment origin, overlapping versus disjoint intervals, percentage/absolute delta, sparse data semantics unresolved. | Needs 4A approved window origin/finality; approve comparison semantics and source metric set. |
| B4B-05 Historical congestion trends | How should congestion evolve over a run or across runs? | Entity × ordered windows; possibly scenario/run cohort. | Approved traffic measures, event/window time, run/scenario and quality. | Trend smoothing, gaps, resets across runs, time alignment and history selection unresolved. Shared traffic mart may serve multiple charts. | Needs 4A stable window keys and corrections/revisions; owner defines trend question and retention/cohort policy. |
| B4B-06 Congestion/severity score | What does a congestion label/score mean operationally? | Usually intersection/direction × window; exact grain open. | Candidate traffic metrics and quality/coverage. | Formula, units, calibration, thresholds, labels, null/partial behavior and versioning all unresolved. Existing V2 formulas are inventory only. | 4A supplies reliable nullable inputs/finality; business owner and domain reviewer approve formula and oracle cases. |
| B4B-07 Priority/urgency ranking | Which locations should be handled first, and how should ties/uncertain data rank? | Ranked entity set per window/scenario. | Approved congestion/severity, queue/wait/speed and quality. | Ranking metric, tie-breakers, missing-value behavior, population, rank stability unresolved. Share score/grain if suitable; no dedicated table per visual. | Needs 4A deterministic revisions and topology population; owner approves ranking and tie behavior. |
| B4B-08 Scenario comparisons | How do Normal/Peak or other scenarios compare fairly? | Entity × scenario × comparable window/run cohort. | Scenario ID, run metadata, approved KPIs, topology/manifest identity. | Scenario labels, matching windows/topologies, normalization, unmatched runs and comparison cohorts unresolved. | Needs 4A immutable run/scenario identity and replay boundaries; approve cohort and comparability rules. |
| B4B-09 Signal performance | Which signal behaviors indicate performance or inefficiency? | Signal/intersection/approach × window. | Phase, color, phase duration/remaining, timing mode and timestamps where present. | Performance KPI, phase transition interpretation, missing signal coverage and relationship to traffic unresolved. | Needs 4A preserve signal availability/topology and window completeness; owner/domain expert approves metric semantics. |
| B4B-10 Coverage and quality publication | When may a metric be shown, and how are partial/unavailable values labeled? | Per KPI at its actual grain/window. | Availability map, quality flags, expected source population, lifecycle status. | Thresholds, denominator, `PARTIAL` vs unavailable labels, INCOMPLETE/STOPPED run handling unresolved. | Needs 4A expose quality and completeness separately; business owner approves per metric release criteria. |
| B4B-11 Mart grain and schema | What reusable stored analytical products support approved questions? | Determined from approved KPI dependencies, not chart count. | Selected canonical inputs and computed KPI version. | Grain, dimensions, keys, nullability, correction rows, partition/order and retention unresolved. | Needs 4A stable IDs/revision mechanics; data architect proposes shared marts after B4B-01..10 approval. |
| B4B-12 Output revision/version selection | Which correction is authoritative and how do consumers request as-of/latest? | Per business key/window and definition version. | Work generation, source boundary, input hash, invalidation/recompute identity. | Upsert/version history, supersession, audit, backdated correction and stale cache behavior unresolved. | Depends on 4A dirty/recompute and finality mechanics; owner approves consumer contract before writes. |
| B4B-13 Serving query contracts | What filters, sort/page, time range, freshness and null contract do consumers require? | API-specific projections over approved marts. | Approved mart fields plus run/scenario/window/quality. | Query names, limits, pagination, latest version, stale/fresh behavior and authorization unresolved. | Needs 4A health/source freshness but not raw runtime internals; approve API contract and query oracle. |
| B4B-14 Spring Analytics API | Which endpoints/DTOs expose approved analytics, including nullable fields? | API resource grain follows serving contract. | Approved marts and query contract. | DTO/OpenAPI/null compatibility, error mapping and versioning unresolved. | Depends on B4B-11..13 and 4A handover; Spring owner approval before modification. |
| B4B-15 Dashboard visualization/interactions | What user decisions should charts/tables enable? | UX-defined; may combine shared mart grains. | Approved API fields and quality/freshness metadata. | Chart/table choice, filters, drilldown, ranking, comparison controls, empty/stale states unresolved. | Depends on approved analytics/API and business workflow; project owner approves requirements before UI work. |

## Requirements template for each approval

```text
ID / owner / approval date:
Business question and user decision:
KPI name, unit, formula and worked examples/oracle:
Analytical grain and primary key:
Window durations, alignment and comparison semantics:
Required Silver fields and topology/run population:
Null, unavailable, partial, conflict and lifecycle behavior:
Coverage denominator and release threshold:
Late data, invalidation, correction and revision selection:
History/retention/as-of needs:
Shared mart candidates and reuse across use cases:
Serving filters, sort/page/freshness/error contract:
Spring DTO/OpenAPI impact:
Dashboard interactions and empty/stale presentation:
Dependencies on accepted 4A interfaces:
Approval evidence and acceptance oracle:
```

Do not start business mart schema or formula implementation until its owner has approved the corresponding semantics and oracle. One reusable mart may support many charts; chart count does not determine table count.
