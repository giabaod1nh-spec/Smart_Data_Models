# 07 — Acceptance, Validation & Planning Handoff

Status: proposed acceptance contract; tests not executed. Mỗi AC phải có independent expected result, fixture ID, source/definition version, actual result và evidence. Không đánh dấu PASS dựa trên mock screenshot hoặc production function tự tính expected. Scope này bổ sung, không thay thế Phase 1–4A correctness/PA obligations.

## 1. Bộ dữ liệu kiểm chứng độc lập

### F-01 — Small hand-calculated aggregation fixture

Dùng hai sampling slots đồng đều và hai approaches của một giao lộ; chỉ là unit fixture cho aggregation, không giả lập full 60s coverage. Tất cả fields available. Đơn vị speed km/h, q m, o %.

| t | approach | V | H | v | q | o |
|---:|---|---:|---:|---:|---:|---:|
| 0 | East | 10 | 0 | 40 | 10 | 20 |
| 1 | East | 20 | 10 | 20 | 30 | 60 |
| 0 | West | 5 | 5 | N/A (no moving) | 20 | 80 |
| 1 | West | 15 | 5 | 30 | 40 | 70 |

Expected East: Vsum=30, Hsum=10, Msum=20, speed numerator=600, speed=30, stopped=33.333333%, load=15, occupancy=40, qmean=20, qmax=q95=30, pressure candidate=36.666667. Expected West: Vsum=20, Hsum=10, Msum=10, speed numerator=300, speed=30, stopped=50%, load=10, occupancy=75, qmean=30, qmax=q95=40, pressure candidate=62.5. Intersection: speed numerator=900, Msum=30, speed=30; Vsum=50,Hsum=20, stopped=40%; load=(15+35)/2=25; worst pressure=62.5 West; worst q95=40 West. This fixture validates zero moving contribution, unequal population ratios and aligned load denominator. Separate fixture 10 moving×40 + 40 moving×20 validates speed=24 (not average 30). Production publication requires full expected W; these short unit cases never imply full-window publication.

### F-02 — Full-window quantile and pressure fixtures

- 60 slots per approach: 57 q=10 and 3 q=100, any permutation, P95=10, max=100, mean=14.5. 56 q=10 and 4 q=100 → P95=100; exact nearest-rank index 57. Repeat 300 slots with 285 q=10 and 15 q=100 → P95=10; 284 q=10 plus 16 q=100 →100. Include duplicates, zeros, rejected NaN/negative. Quantile computed by independent sorted-array oracle.
- Use canonical full 60s synthetic run of expected archived participants. Direction mean occupancy=88, stopped=76 with V/H integers (e.g. V=25,H=19 each slot) → pressure=82. Construct three approaches with pressure 82,28,42 → intersection 82. Build two separate worst IDs: East pressure higher but West queue larger. Verify rank/argmax independent. Use high count [82,74,52,24,19] →2/5 with candidate T=70; calibrated test regenerates expected from approved version.
- Same score after rounding near threshold: 69.95 stays below 70 before display. Tie stable by archived ID. Empty traffic V=H=0, o=q=0 → speed/ratio N/A, score=0 (approved empty rule), load/queue=0. All stopped V>0,H=V → ratio 100, speed N/A. Mismatched occupancy/empty scope must be investigated.

### F-03 — Full-window quality and identity fixture

Use independent expected schedule with 5 participants, 17 approaches, 1s cadence, approved aligned 60s and 300s windows. Expected 300/1500 intersection observations and 1020/5100 traffic direction samples. Build negative variants one at a time: missing speed when M>0, missing whole approach, missing entire intersection, duplicate semantic delivery, same slot conflicting revision, invalid H>V, absent optional storage, unknown signal color, changed topology, wrong run, wrong cadence, lifecycle incomplete, source at 60.04 mapping per approved offset policy. For one missing speed slot, valid speed coverage=1019/1020 if all slots applicable, official network speed withheld; q complete still published. Missing intersection cannot be hidden by reducing denominator. Measured empty and no-applicable metric distinct. Use fixture archived expected set, not received row counts.

### F-04 — Time series and navigation fixture

At 60s duration, source 680s, closed end 660, latest published end 600. Test auto follow, historical pin, incoming publication, invalidation of selected W, duration change, run change, range validation. Pressure sequence [High,High,missing,High,Low] yields h=3,v=4,e=5, longest observed streak=2, full-range official persistence withheld. For 15 valid high windows of 60s, duration of classified high windows=900s, not exact seconds of congestion. A local Incident at A (observed change), B/M remain Normal but later pressure rises; all participants retained. Control mode changes mid W → Mixed Mode, no pure strategy comparison. Marker inferred from sampling labeled observed interval.

### F-05 — Signal, storage and incident fixture

Signal 60 samples: 24 green,3 yellow,33 red,18 green with queue>0 → 40% green share and 75% green-with-queue. Missing q at green → second metric withheld only; zero green → ratio N/A. Storage lanes 40/50 and 60/100 → 80%, not max jam/max length=60%; `storage_utilization_pcu` nonzero with stopped=0 not a queue metric. Source fixture [S9] M East 0.4828→48.28% snapshot, A West .1405→14.05%; do not label window aggregates. Incident [] vs missing optional spillback false/unknown; true observed despite other missing slots only descriptive evidence, not official exposure rate.

### F-06 — Experiment fixture

Create separate baseline/candidate run IDs with identical topology/geometry, planned demand schedule and scenario schedule, same seed and same analysis range/duration/definition, full applicable coverage, strategy Fixed/Adaptive as only treatment. Include both improvement and degradation across intersections. Speed 25.4→31.6, stopped 48→32, worst window P95 44→30; expected deltas in 05. Negative variants different topology hash, same seed but demand differs, different warm-up, missing metadata, mixed mode, missing metric slot, old research metric absent, baseline zero. Range 0–1800 vs 0–1200 requires explicit common range selection, never compare whole runs. A paired run is descriptive evidence, not statistical proof.

### F-07 — Revision/restart and independent durable oracle

Build actual canonical fixture with verified expected slots, multiple change/invalidation receipts, duplicate transport deliveries, late event and conflict affecting one member. Record raw source bytes and expected semantics independently of aggregation code. Compute expected membership, per-direction sums/quantiles, rollups, coverage and result IDs from independent source oracle. Simulate interruption before stage, after stage, before publication, ambiguous response and during invalidation; assert old generation not current, no partial business result, no duplicate or mixed revision. Re-run same source with same definition in isolation; compare semantic results/digests using approved precision rules. Verify physical marts and API effective results, not only Core ACK or local SQLite quick_check. Include adversarial altered payload, revision, expected-slot set and result values; oracle must reject them.

## 2. Acceptance matrix (proposed)

| ID | Requirement | Fixture / proof | PASS condition |
|---|---|---|---|
| AC-01 | M-01 weighted speed | F-01 + unequal weights | 24 km/h for 10×40+40×20; no mean-of-means |
| AC-02 | M-02 ratio | F-01/F-02 | ΣH/ΣV, 100% all stopped, empty N/A |
| AC-03 | M-03 quantile | F-02 | exact nearest rank, 57/285 index, zero/duplicates |
| AC-04 | M-04 units | SUMO-only known raw occupancy | 0.8% remains 0.8%, no unit guessing; source version recorded |
| AC-05 | M-05 score and calibration | F-02 + G-CAL holdout | weights/threshold/version approved, independent reference cases, max approach correct |
| AC-06 | M-06 count | F-02/03 | correct 2/5, missing participant withheld, threshold pre-round |
| AC-07 | M-07 load | F-01/03 | aligned sum/slot, no unique network claim without scope audit |
| AC-08 | M-01/02/05 empty semantics | F-02/03 | N/A vs zero vs missing, no div-by-zero |
| AC-09 | Rollup invariants | F-01/02/07 | direction/intersection/network direct oracle equivalence |
| AC-10 | Q-01 window boundaries | F-03 | half-open, two durations, exact time precision and no clipping |
| AC-11 | Q-01 expected schedule | F-03 + real capture | independent population, cadence/offset/eligibility agreed; no hardcoded counts |
| AC-12 | Q-02 metric quality | F-03 | missing/invalid input suppress dependent output only; no denominator shrink |
| AC-13 | Q-02 network population | F-03 | missing participant cannot publish full network KPI |
| AC-14 | Topology and run context | F-03 | different archived directions supported, no cross-run mixing |
| AC-15 | Duplicate/replay | F-07 | no double count, exact semantic result on replay |
| AC-16 | Invalidation | F-07 | old effective result suppressed, new revision correct |
| AC-17 | Restart/ambiguous write | F-07 | no partial publication or duplicate current revision |
| AC-18 | Late observation/conflict | F-07 | dirty generation and recomputation, no stale proof |
| AC-19 | Q-04 visibility | F-07 | no half-published data across compatible projections, fence retained |
| AC-20 | O-01 default/filter | F-04 | 60s, one run, no global scenario/control command |
| AC-21 | O-01 follow/pin | F-04 | follow latest published, pin unaffected, invalidation state explicit |
| AC-22 | O-01 range/duration | F-04 | correct relative/custom range, deterministic duration mapping |
| AC-23 | O-02 scope/delta | F-01–04 | 4 cards same W, previous adjacent compatible baseline, units correct |
| AC-24 | O-03 rank | F-02/03 | deterministic rank and argmax; correct diagnostic direction |
| AC-25 | O-04 heatmap | F-04 | cells equal effective scores; gaps and click synchronize W |
| AC-26 | O-05 trends | F-04 | selected metric unit, correct grain, gaps, synchronized focus |
| AC-27 | O-06 persistence | F-04 | valid/expected counts, missing breaks streak, correct observed/full distinction |
| AC-28 | O-07 historical/quality | F-03/04/07 | no active run dependency; publication health not coverage; explicit states |
| AC-29 | D-01 direction detail | F-02/03 | topology rows, correct argmax, preserve run/window/range |
| AC-30 | D-02 storage | F-05 + geometry audit | ratio/p95 correct, optional missing withheld, no PCU substitution |
| AC-31 | D-03 signal | F-05 | sampled share and conditional q ratio, no phase duration overcount |
| AC-32 | D-04 local context | F-04/05 | scenario per intersection, network mode, observed evidence not exact cause |
| AC-33 | E-01 comparability | F-06 | fail-closed if required evidence missing/mismatch; no false verified badge |
| AC-34 | E-02 summary | F-06 | sufficient-stat rollup, zero baseline, valid queue scope |
| AC-35 | E-03/04 trends/delta | F-06 | matched boundaries, improvements and deteriorations, no fake missing curve |
| AC-36 | E-R1/R2 counts | SUMO independent arrivals/crossing oracle | one count per eligible completion/passage, exact interval boundaries/restart |
| AC-37 | E-R3/R4 waiting | known trajectories/TripInfo | completed population, zero waits, incomplete separately, correct units |
| AC-38 | Extended contract | versioned mixed-history fixture | old metric absent N/A, no break V1 compatibility, approved replay |
| AC-39 | Serving consistency | F-07 API/browser | stale responses ignored, current-effective revision and aligned projections |
| AC-40 | Gold overhead/operating envelope | representative integrated benchmark | owner-approved limits; both durations; stable resource/backlog/lag; bounded drain; independent oracle |

AC-30/31/32 depend on P1 adoption; AC-36–38 only mandatory for corresponding adopted research metric. AC-04/05/11/19 are release gates for affected results. Test count and pass numbers cannot be inferred from this matrix. Add regression of prior Core correctness, previous inherited failures/skips documented, no lowering performance guard.

## 3. Requirements traceability to existing Phase 4B IDs

| Existing requirement | Specification reference | Planning prerequisite |
|---|---|---|
| B4B-01 Intersection traffic | M-01–05,07; O-03–05 | formulas and rollup oracle |
| B4B-02 Direction traffic | M-01–05,07; D-01–04 | archived approach population |
| B4B-03 Network analytics | M-01–03,06,07; O-02 | strict population/slot rule |
| B4B-04 60s/300s comparison | O-01/05, Q-01 | default switch/scale support; dedicated nested comparison not committed |
| B4B-05 Historical trends | O-04–06, M-08 | effective revision/gaps |
| B4B-06 Congestion/severity | M-04/05/06 | occupancy verification, G-CAL |
| B4B-07 Priority/urgency | O-03, M-05/08 | deterministic rank/definition |
| B4B-08 Scenario comparison | E-01–04 | local scenario profile/cohort metadata |
| B4B-09 Signal performance | D-03 | sampled-signal/quality approval |
| B4B-10 Coverage/quality | Q-01/02, O-07 | G-COV |
| B4B-11 Mart grain/schema | Q-03/06 | approved business outputs + technical design |
| B4B-12 Business revisions | Q-04 | G-REV publication design and oracle |
| B4B-13 Serving queries | Q-05, O/D/E interactions | effective query/limit/cache/error contract |
| B4B-14 Spring Analytics API | Q-05 | API DTO/OpenAPI plan, consumer contract |
| B4B-15 Dashboard | O-01–07, D-01–04, E-01–04 | API acceptance, UI plan |

B4B-04: support choosing 60s or 300s is agreed; a dedicated nested scale comparison feature remains open. If adopted, compare same run/entity and common aligned end (e.g. [240,300) vs [0,300)), explicitly nested/not independent, no extra chart/mart by default; decide whether this deferred requirement is satisfied by switch or needs explicit comparison, owner approval. This avoids silently dropping a repository requirement.

## 4. Open decisions and gate register

| Gate | Owner decision / evidence needed | Blocks |
|---|---|---|
| G-ENTRY | Explicit decision to retain or amend minimum performance gate in repo; preserve open PA and correctness evidence | Implementation, not business design review |
| G-METRIC | approve M-01–09 definitions, exact quantile, precision/ties, scope, names and examples | affected business aggregation design |
| G-SRC | Verify occupancy units, moving population, E2/fallback and sensing geometry/coverage; fix/version if necessary under approved scope | affected metrics, especially pressure |
| G-CAL | independent calibration protocol and holdout, weights/threshold/version approved | pressure severity/high count/ranking/heatmap/persistence official publication |
| G-COV | approved effective sampling cadence/alignment/expected slots/applicability/full-range rules | any official KPI publication |
| G-SCOPE | approve P0/P1 boundaries, persistence UI, focus mode, nested comparison, signal/storage/extended adoption | final scope and plan |
| G-EXP | comparability metadata, matched offered demand, local scenario timeline, experiment protocol and required metric set | verified comparison |
| G-REV | business revision/visibility/recovery design, independent oracle | business writes/publication |
| G-API | serving schema, range budget, DTOs, error/revision contract | API/UI implementation |
| G-PERF | measured operating envelope, integrated workload, all relevant PA and demo acceptance | production/demo qualification |

Research SF-01 occupancy is P0 source correctness, not optional extension; SF-02 geometry/SF-03 retained coverage for storage; SF-04 crossing; SF-05 trip/approach waiting; SF-06 AI eval semantics; SF-07 interval/restart; SF-08 source overhead; SF-09 compatibility. For each attach evidence and explicit ADOPT/DEFER/REJECT. No contract changes based solely on this draft.

## 5. Handoff to future design & implementation plan

The next agent should read this whole set, exact current repository branch/commit, all Phase 4A contracts and performance evidence relevant to the design, then produce requirements disposition and dependency matrix. Reconcile source changes since baseline, distinguish source-supported/proposed/blocked. Do not start code yet. The future technical design must specify:

1. Approved requirements IDs, metric/version/coverage contract and calibration/source feasibility results.
2. Canonical input ownership; one verified traversal reuse; accumulator lifecycle, memory bounds, exact quantile policy, dirty-generation/invalidation fence, failure recovery and independent oracle.
3. Logical-to-physical mart choice and reuse; schema/migration, storage and revision visibility, idempotency/rollback and legacy coexistence without data loss.
4. Serving/query and metadata contracts; run catalog, scope/range validation, revision consistency, cache invalidation, API DTOs/OpenAPI, UI mapping; historical independent of Realtime.
5. Experiment metadata/controlled protocol; extended source change plan only if adopted and separately approved.
6. Work packages with dependencies, file-level impact after source audit, tests and acceptance IDs, resource budget, stop conditions, rollback, evidence artifacts. Separate correctness from performance release. Avoid chart-per-table and arbitrary new technologies.

Suggested review sequence (not approved implementation plan): business requirements review → source feasibility/occupancy and coverage contract → calibration protocol/results and metric approval → technical aggregation/revision/oracle design → plan approval and G-ENTRY resolution → implementation in dependency order → API/UI and experiments → integrated acceptance. Some independent feasibility research can proceed in parallel by owner approval. No autonomous git operations, no implementation before explicit authorization, no PA PASS by deferral. Any change from these requirements must be recorded with rationale, owner, date and traceable AC updates.
