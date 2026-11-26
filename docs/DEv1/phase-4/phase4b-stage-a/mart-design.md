# Gold logical products and candidate physical model

**No migrations are created in this planning task.** Three products below are logical grains, not a mandate for three physical tables. Final storage and DDL require G-METRIC, G-COV and G-REV approval.

## Candidate reusable base model

Recommended initial comparison: immutable revisioned business result rows keyed by a stable result header and reusable grain-specific metric rows, or base marts with serving views. Prefer base marts + views if different nullable/quantile payloads make one wide nullable fact hard to govern. Avoid chart tables. Avoid defaulting to ReplacingMergeTree/`FINAL`; ClickHouse merges are asynchronous and do not implement a cross-mart publication fence.

| Logical product | Grain / business identity | Proposed fields |
|---|---|---|
| Direction × run × window | namespace, run ID, run generation, topology digest, intersection ID, archived direction ID, duration, start/end, metric definition set, business revision | Metric values; raw sufficient stats; exact P95; source method/version; scenario/control provenance summary; expected/valid/missing/invalid counts per metric; quality; proof IDs/hashes; result revision and publish manifest ID |
| Intersection × run × window | Same key without direction | Rollup sufficient stats; queue/pressure maxima and deterministic argmax IDs; complete-topology load; metric-level statuses; coverage and lineage |
| Network × run × window | Same key with network population identity | Network numerator/denominator metrics; high count only after calibration; participant count/digest; completeness; no unique-vehicle claim |
| Business publication manifest | result-set ID, generation, window, definition set, revision | expected product row count, deterministic output digest, staged/verified/published/suppressed state, current-effective fence, timestamps and Core proof identity |

## Keys and ClickHouse design questions

Use stable keys including namespace/replay ID, run generation, topology digest, entity grain/ID, duration, integer aligned window boundaries, metric-definition version and business revision. Preserve `start`/`end` half-open semantics and simulation-time precision. Include scenario as observation provenance or explicit mixed/profile metadata, not an accidental partition filter. Keep PCU profile, producer/source version, manifest identity, source binding and input proof lineage.

Candidate `PARTITION BY` should be coarse (e.g. run/time bucket after expected retention is known) and `ORDER BY` should support run + grain + duration + start + definition/revision current selection. High run cardinality, replay namespaces and multiple revisions must be costed before DDL. Define TTL/retention and immutable evidence window. Nullable values need explicit status columns; null must not coerce to zero. Use fixed precision/Decimal or documented Float64 comparison tolerance consistently.

Before schema approval, inspect actual `005_create_gold_m1.sql`, server mapper/query access patterns and ClickHouse version. Test additive migration on clean and existing V2 data; preserve old V2 views/tables and data. New migrations must be additive, restart-safe, namespace-isolated, and reversible by dropping only new objects after verifying no retained V1 evidence depends on them. No physical change is authorized here.

## Access pattern fit

- Run catalog and latest published windows filter by run/status and sort by end time.
- Selected window KPI loads one network row and matched intersection/direction rows.
- Ranking/heatmap scans bounded intersections for a selected run/duration/range.
- Trends/persistence scan bounded ordered windows for selected entity, metric definition and duration.
- Detail queries select one intersection's direction rows.
- Experiment query compares compatible immutable run rows and metadata on common interval.

If one base table serves these queries efficiently with explicit grain and status, use it. Otherwise materialize only reusable statistics/results with measured access justification. Do not introduce an experiment-specific mart or per-chart table until query evidence shows need.

