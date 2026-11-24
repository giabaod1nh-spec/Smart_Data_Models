# Deferred Phase 4B business requirements

Status: **DEFERRED — OWNER APPROVAL REQUIRED** for every business definition below. Phase 4A-Core provides processing foundation only and publishes no business result.

## Accepted window foundation

Phase 4B uses the Core's pure simulation-time Tumbling Windows: 60-second and 300-second durations, deterministic half-open `[start,end)` membership and the approved versioned alignment/precision rules. A valid observation belongs to one window per duration; the same observation may belong to both. Generic work-window identity is scenario-neutral, while scenario and run provenance remain available at observation level. A window is not complete because its end passed or its run status is COMPLETE. Core exposes completeness as `UNKNOWN` until an expected-observation and coverage contract is approved, and keeps publication finality `DISABLED`.

Rolling Window, Sliding Window, Provisional Aggregation and provisional business KPI publication are outside the current roadmap. Analytics Dashboard is for historical/completed intervals; Realtime Dashboard owns current-state monitoring. Do not infer V1 business semantics from legacy Gold V2 and do not create a table per chart.

| ID / area | Approval question and open semantics | Candidate grain / input (not formula approval) | Gate/dependency |
|---|---|---|---|
| B4B-01 Intersection traffic | Define KPI, units, weighting, nullable/partial behavior and coverage denominator. | Intersection × run/scenario × completed tumbling window; traffic fields and quality/availability. | Owner-approved formulas, examples and oracle. |
| B4B-02 Direction traffic | Define approach identity/rollup and missing-approach behavior from archived topology; do not assume compass directions. | Run × intersection × archived approach × window. | Stable topology and source mapping from Core; owner-approved grain. |
| B4B-03 Network analytics | Decide whether a network aggregate is needed and how incomplete population/weighting behaves. | Network × scenario × window using approved lower-grain facts. | Coverage denominator, member population and aggregation oracle. |
| B4B-04 60s/300s comparison | Decide whether/how nested window scales should be compared and define sparse-data/delta semantics. | Same entity/run/scenario over both Tumbling durations. | Core alignment accepted; comparison formula and use case approved. |
| B4B-05 Historical trends | Define trend, gaps, run resets, smoothing and retention/cohort behavior. | Entity × ordered completed windows. | Approved KPI and correction/history selection. |
| B4B-06 Congestion/severity | Define score, units, calibration, thresholds, labels, versioning and missing-value behavior. | Intersection/direction × completed window. | Domain owner and worked oracle; V2 formula is not authority. |
| B4B-07 Priority/urgency | Define ranking metric, tie-breakers, uncertain-data behavior and population. | Ranked entity set per completed window/scenario if approved. | Approved score, grain and deterministic rank oracle. |
| B4B-08 Scenario comparison | Define scenario labels, matching runs/topologies/windows, normalization and unmatched-run behavior. | Entity × scenario × comparable run/window cohort. | Immutable Core scenario/run provenance; owner-approved cohort rules. |
| B4B-09 Signal performance | Define phase behavior measures and absent-signal coverage semantics. | Signal/intersection/approach × completed window. | Domain review of source fields and oracle. |
| B4B-10 Coverage and quality | Define expected population, denominator, thresholds, partial/unavailable labels and terminal-run treatment. | Per KPI at its selected grain/window. | Expected-observation contract is mandatory; until then Core state remains UNKNOWN. |
| B4B-11 Mart grain/schema | Select reusable stored products, keys, dimensions, nullability, corrections, partitioning and retention from approved requirements. | Business-requirement-driven reusable marts. | Approve B4B-01..10 dependencies first; no chart-per-table design. |
| B4B-12 Business revisions | Decide correction/history, latest vs as-of, supersession and consumer selection. | Per business key/window/definition version. | Core dirty/recompute metadata; owner policy before business writes. |
| B4B-13 Serving query | Define filters, pagination, freshness, null and error contract. | API projections over approved marts. | Mart schema and consumer query oracle. |
| B4B-14 Spring Analytics API | Define endpoints, DTO/OpenAPI, compatibility and error behavior. | Follows approved serving grains. | API owner approval after B4B-11..13. |
| B4B-15 Dashboard | Define user decisions, views, filtering, drill-down, stale/empty states and interactions. | Reuse approved serving data/grains. | Product owner approval and API readiness. |

## Approval template

```text
ID / owner / approval date:
Business question and user decision:
KPI name, unit, formula and worked examples/oracle:
Analytical grain and key:
60s/300s use, epoch alignment and comparison semantics:
Required Silver fields, topology/run population and scenario cohort:
Null, unavailable, partial, conflict and lifecycle behavior:
Coverage denominator and release threshold:
Late data, invalidation, business correction and revision selection:
History/retention/as-of needs:
Shared mart candidates and reuse across use cases:
Serving filters, sort/page/freshness/error contract:
Spring DTO/OpenAPI impact:
Dashboard interactions and empty/stale presentation:
Core dependency, approval evidence and acceptance oracle:
```

Do not start business mart schema, KPI implementation, serving API or Dashboard work until its requirements and independent oracle have owner approval. Core's eventual-consistency guarantee and current-effective source do not imply arbitrary as-of history.
