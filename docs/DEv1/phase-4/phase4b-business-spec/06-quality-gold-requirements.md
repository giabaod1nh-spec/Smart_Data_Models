# 06 — Window, Coverage, Publication & Gold Design Constraints

Status: proposed policies to approve before schema/implementation. This specification does not override Phase 4A Core invariants or formal performance gate [S4,S10].

## 1. Q-01 — Window identity and expected-observation contract

Use Core-approved simulation-time precision and alignment for deterministic half-open tumbling windows 60s/300s. Epoch 0 and membership `[start,end)` examples: t=59.99→[0,60), t=60→[60,120), t=300→[300,600) for 300s. Same observation belongs to one W per duration. No rolling/sliding/provisional. Do not infer official finality from source clock or run COMPLETE. Terminal partial windows outside crossed eligibility remain not publishable; no clipping or extrapolating. Run status/closure evidence and actual eligible boundary policy must be established from canonical lifecycle, not UI time.

Expected slots must be independent of observed data: archived participant/approach inventory, effective capture cadence, first capture alignment/offset, clock precision, start/stop eligibility and expected cycle identity. Current manifest does not establish a complete cadence/alignment contract; require G-COV approval before official publication. For source time 60.04 in fixture, do not round to 60 arbitrarily or assume sampling starts exactly 0; define how actual offset maps to expected slot and reject ambiguity. Freeze expected slot set from approved run metadata; do not shrink denominator to received count. Distinguish run starts at nonzero time, partial initial window, paused simulation, step changes, cadence changes, terminal incomplete window, cycle resets and conflicting samples; owner must approve exact behavior. Candidate: only aligned full windows with stable cadence and independently known expected capture schedule; otherwise not eligible/unknown until specified.

For uniform 1s cadence, complete aligned W and baseline population: intersection observations 300/1500, traffic direction slots 1020/5100, signal direction slots 1020/5100, intersection state slots 300/1500, for 60/300s. Per direction 60/300. These are examples not fixed runtime limits. Participant changes must use approved run generation/context; do not mix populations. Event membership alone does not prove expected slots complete.

## 2. Q-02 — Metric-level quality and applicability

Define expected applicable set E_m(g,W) per metric, grain, independent input dependency and approved applicability; observed unique valid slots V_m subset E_m. `coverage_m=|V_m|/|E_m|`. Track expected, received distinct, valid, invalid, missing, not applicable counts, reasons, expected set/version and input proof. For multi-input metrics use joint valid slots, not average of coverages. Coverage ratio undefined if no applicable slots; display N/A not 100% fabricated. Empty measured population is different from no applicable slots. Missing optional field does not decrease E; metric availability NOT_APPLICABLE must match approved independent rule. Required null with UNAVAILABLE → invalid; absent optional → unavailable; numeric 0 valid if semantics and quality prove measurement. Validate finite/ranges/integrality/cross-field consistency in business oracle. Quality VALID at Silver is not enough to prove metric completeness.

Proposed strict policy: official metric only if 100% of applicable slots valid and all required child groups available, known effective window and approved definition. With all vehicles stopped, moving speed metric valid input coverage but value N/A; ratio 100%. With empty traffic, speed/ratio N/A and measured queue/occupancy/load zero, score 0 only if validated empty consistency. A missing v in moving slot suppresses speed across its dependent groups, not independent queue. Missing direction pressure suppresses intersection max and network high count. Optional research metric absent in old run is unsupported/N/A, not universal full-dashboard failure. No zero filling, interpolation, stale carry-forward, cross-run mixing. A missing non-applicable signal approach cannot be assumed absent unless topology/rule says so.

Coverage quality distinguishes structural coverage (all expected members), metric validity and numerical result applicability. M-08 observed summary may be shown separately with v/e and explicitly incomplete, but official range summary withheld. Do not apply 100% to a weighted average of coverage percentages. For network scope use full population union; duplicates/revisions do not inflate counts. G-COV approval includes exact slot mapping, denominator, zero-applicable semantics, full-range/gap policy and oracle.

## 3. Q-03 — Business grain, logical products and identity

Three reusable logical outputs, names candidates (not SQL/migration approval):

| Product | Grain / fields | Consumers |
|---|---|---|
| Direction Traffic Window | Run context × W × intersection × approach × metric definition; V/H/M sums, vM sum, occupancy sum/count, q sum/n/P95/max, pressure, quality/provenance, optional storage/signal stats | Detail, diagnostic ranking |
| Intersection Traffic Window | Run context × W × intersection × definition; rollup sums, worst pressure/argmax, worst q95/argmax, scenario/mode sampled context, incidents/spillback quality, coverage | Ranking, heatmap, trends, persistence |
| Network Traffic Window | Run context × W × definition; network numerator/denominator, slot count, population, high count/IDs, worst q95/location, coverage | KPI, experiments |

Run context includes run identity/generation, topology/manifest binding, approved alignment/duration; W start/end in simulation time. Scenario remains per observation provenance, not generic window identity; network profile can contain different local scenarios. Keep definition version(s), input proof/generation, business publication revision, expected schedule identity. Decide physical tables/materialized query/partition/retention and transaction model in technical design, not here. Signal/storage may use separate logical products if justified, but not table per chart. Rollup sums of sufficient stats only with identical scope and compatible definitions. Use aligned slot denominator once for total load. No AVG of P95, no mean of means or ratios.

## 4. Q-04 — Revision and publication consistency

Core COMPLETE proof is necessary input evidence, not business completeness. Business aggregation must operate on verified effective canonical generation, not physical Silver queries, journal internals, old proof or provisional data. Reuse public `CanonicalReadSession`/Core input stream and global invalidation fence; preserve receipt and ack semantics. Technical design must decide where accumulation and publication occurs without weakening Core invariants. Candidate: accumulate all required stats during one verified traversal per work generation, stage results, publish only after proof/coverage/business checks and current-generation validation. If input generation changes, discard staged aggregate, recompute effective set. Do not publish partial work after crash or failed verification.

Publication must be logically atomic for compatible projections: all official results for W reference same input generation/definition and published manifest; metric-specific withheld states can coexist in same manifest. Do not claim distributed ACID across SQLite and ClickHouse. Design commit/fence/reconciliation protocol, idempotency and visibility rule so readers cannot see half-published marts or stale superseded generations. Store immutable revision lineage or owner-approved history policy, select current-effective explicitly (not simply highest insertion timestamp or arbitrary FINAL). On invalidation immediately suppress affected current result according to verified fence/serving contract, recompute new revision, update heatmap/persistence/comparison and any caches. Failed/unverified reprocessing returns unavailable, not old value labeled current. Exact atomic scope and visibility/revision protocol are G-REV design review items. Historical as-of queries beyond current-effective are out of scope until retention evidence and policy approved; revision lineage alone does not imply arbitrary historical snapshot support.

## 5. Q-05 — Serving contract requirements

API requests carry run context, duration, selected W or aligned range, optional intersection/approach and metric selector. Historical catalog independent of Orion current-run. Separate Overview selection, trend/heatmap range, detail, experiment metadata and comparison projections; no mandatory endpoint naming yet. Results include scope, window bounds, unit/definition, value nullable, coverage and reason, revision/manifest identity, service status and latest publication. Support bounded pagination/zoom/range, deterministic sorting; stale response from previous selection must not overwrite new view. One read session or stable publication token/consistent response group should ensure KPI/ranking/heatmap selected cell agree; retry if revisions shift. Caching keyed by run/context/W/definition/revision and invalidation-aware, not TTL-only official freshness. Error semantics distinguish not found, unavailable metric, processing, failed dependency, invalid filter, not comparable; explicit HTTP/DTO decisions in design. No cross-run fallback.

| UI state | Meaning |
|---|---|
| Published | Current-effective result with approved coverage and definition |
| Processing | Eligible closed window without completed business publication |
| Incomplete coverage | Known missing/invalid expected applicable slots; affected metric withheld |
| Reprocessing | Superseded generation suppressed, recompute pending |
| N/A | Valid empty population or no applicable metric, reason explicit |
| Unavailable / Unknown | Failed/missing inputs or unverified definition/evidence |
| Open / not eligible | Window not fully closed/eligible; no official result |
| Service unavailable / stale | Serving health or freshness issue; distinct from historical run ending |

UI states are not changes to existing Core enum. Actual API error/status vocabulary and publication consistency token require design approval. Realtime freshness and analytics publication lag are distinct; latest source wall capture-to-ACK is not dashboard freshness.

## 6. Gold Runtime cost & performance constraints

### 6.1. Baseline evidence

At baseline Phase 4A performance acceptance remains INCONCLUSIVE; `perf-w2-paced-02` ENVIRONMENT BLOCKED at 206.05 simulation seconds, safety floor 1 GiB, no complete 300s proof/independent drain audit. Retained stopped-source recovery completed 1535 source deliveries, 3070 memberships, six proofs with physical audit; 9.64 member reads/s in post-ACK tail, final 1500-member verification stream 136.20s, total drain 338.13s [S10]. These are fixture-specific observations, not sustainable integrated service rate. Earlier review distinguished 3.94 obs/s measured wall-clock aggregate over usable span vs about 5/s simulation-time density; this document does not remeasure raw time series. No new performance qualification. PA-01–12 open for full scope. Do not use stop-source throughput as proof recovery under continued arrivals.

### 6.2. Workload sizing (illustrative baseline)

At cadence 1s, 5 intersections, 17 approaches, source 5 observations/s at 1× pacing. Per second 17 traffic +17 signal +5 intersection fact rows =39 logical facts. Two W durations →10 membership obligations/s, up to 78 fact-row visits/s if each membership verification traverses its bundle; actual physical reads depend on verified API and reuse, not guaranteed counts. At 30 simulation minutes: 9000 observations, 18000 memberships, 30 closed 60s and six closed 300s network windows if complete/eligible. Across both durations potential 612 direction, 180 intersection, 36 network logical rows (17×36, 5×36, 36), excluding revisions, optional products and incomplete windows. Do not treat these as existing table sizes. At pace<1× arrival wall rate lower; measure actual wall clock separately.

| Work | Expected incremental complexity | Design guard |
|---|---|---|
| Speed/stopped/load/occupancy/pressure | O(n) per work, bounded counters per group | Reuse one verified input traversal, no per-metric read |
| Queue exact P95 | O(n) values, O(n log n) sort or equivalent exact selection | 5100 q samples for full 300s baseline; bound per work/generation, release on cancel |
| Storage P95 optional | Same complexity additional array | Only if scope approved, avoid repeated JSON parsing |
| Signal/context | O(n) counters and bounded sets | Align same bundle, no external query per fact |
| Ranking | O(I log I) on marts | Sort once per W projection |
| Heatmap/trend/persistence | O(I×K) bounded published range | Page/zoom, no event reread |
| Experiment | O(runs×I×K) range sums/max + metadata check | Reuse marts, no new canonical membership proof |
| Late correction | Recompute affected work generation | Preserve global fence; no stale result, bound retries/queues |

`n` = applicable samples in work, `I` participants, `K` windows selected. Complexity is conceptual not a benchmark. Actual memory includes Python objects, JSON, evidence, SQLite/ClickHouse and worker concurrency, not just raw floats. Gold work must not read/reverify separately for every chart, change ACK ordering, bypass Silver contract, cache stale proof, or weaken strict coverage for speed. Avoid unbounded batch across entire retained history. If additional canonical reads necessary, design must justify and benchmark.

### 6.3. Required instrumentation and acceptance inputs

Measure source actual pacing and payload, Raw/Bronze/Silver/Gold ACK throughput, eligible work arrival/completion, member verification throughput, aggregate CPU/wall time, publication latency after window eligibility, source-to-published lag, invalidation/recompute cost, backlog age/slope, API query latency/range and host memory/CPU/I/O. Include both durations and representative UI/API/Realtime coexistence. Propose operating envelope and thresholds from evidence, owner approval (no invented production SLO). Retain 1 GiB runtime floor and current approved startup reserve, no change without owner. Final integrated 30-minute demo rehearsal with actual workload, both durations, bounded drain, independent source-to-mart/API oracle; performance acceptance not claimed from short test. Source-only feasibility and small unit fixtures may precede full integrated qualification. A fresh paced gate is still required by repo until explicit owner change; planning is allowed without implementing business writes. Conditional deferral, if approved, leaves PA open, not PASS.
