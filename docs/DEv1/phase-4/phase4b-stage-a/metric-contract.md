# Proposed metric contract

**Status:** review candidate, not approved. Definitions follow `phase4b-business-spec/02-metric-specification.md`. No official result may rely on a candidate formula until G-METRIC and its source/coverage dependencies are approved.

## Registry and identity

Maintain a versioned registry keyed by immutable `metric_id` + `definition_version`. Each row records business name, unit, source fields and source contract version, eligibility/population, grain, formula, sufficient statistics, null/empty behavior, coverage policy, precision/rounding, oracle ID, approval status and effective date. A published output pins all metric versions; never reinterpret old rows by editing a definition in place.

Candidate version labels: `moving_speed_v1`, `stopped_ratio_v1`, `queue_stats_v1`, `occupancy_v1` (blocked pending source unit correction), `traffic_pressure_v1` (blocked pending calibration), `high_pressure_count_v1`, `observed_vehicle_load_v1`, `sampled_signal_v1`, `context_evidence_v1`. Names are proposals.

## Core formulas and rollup state

| Metric | Definition at direction/window | Persist/roll up | Empty / unavailable |
|---|---|---|---|
| Average Moving Speed (km/h) | `moving_count = Σ(V−H)`; `speed_numerator = Σ(v × (V−H))`; result = numerator / moving_count | Sum numerator and denominator through intersection/network; never average means | Zero moving vehicles with full coverage → N/A; missing speed where moving_count>0 → unavailable |
| Stopped Vehicle Ratio (%) | `stopped_numerator=ΣH`; `vehicle_denominator=ΣV`; `100 × numerator/denominator` | Sum counts; not average per-slot/direction ratio | Full coverage and denominator 0 → N/A; H>V is invalid |
| Queue mean/max/P95 (m) | Samples q by expected valid slot; exact nearest-rank P95 `sorted[ceil(.95n)-1]`; calculate per direction first | Preserve exact bounded sample set or an exact approved order-statistic state; max/mean cannot reconstruct P95; do not average P95s | Missing applicable queue sample makes strict official metric unavailable; measured zero is valid |
| Mean occupancy (%) | `Σoccupancy / valid expected samples` at direction grain | Sum + count; do not call density or capacity ratio | Blocked until source unit/method compatibility resolved |
| Pressure score (0–100) | Candidate `w_o×O + w_r×R`, weights sum 1; direction score then intersection max/argmax | Compute from compatible approved components; max/argmax deterministic by stable direction ID tie-break | No score if a component/required approach unavailable; zero-traffic rule needs approval; official use blocked by G-CAL |
| High-pressure intersection count | Count participant intersections with score ≥ approved high threshold; denominator is expected participant set | Count + population identity, only after every participant score is known | If any required participant missing or G-CAL open → unavailable, never divide by observed count |
| Observed vehicle load | Sum V over expected directions per slot, then average by applicable slots | Slot-aligned totals/sums + slot count; not unique vehicle count or throughput | Incomplete topology/slot → unavailable; complete empty slot may be zero |
| Sampled green share | green samples / valid signal samples; queue-while-green uses joint valid queue samples | Persist category counts and joint numerator/denominator; no sum of configured phase durations | G=0 → N/A; missing signal or queue suppresses dependent metric |
| Persistence | high-window count, valid/expected windows, longest contiguous high streak; missing breaks streak | Derived from effective published windows of one definition/duration | Missing window is a gap, not low pressure; no official full-range share under incomplete coverage |
| Estimated stopped exposure (research) | `Σ(H × Δt)` vehicle-seconds at approved fixed cadence | Additive only for same cadence/scope | Not trip delay, waiting time per vehicle, or unique vehicles |

## Worked examples for independent oracle

- Speed: populations 10 at 40 km/h and 40 at 20 km/h yield `(10×40+40×20)/50 = 24 km/h`, not 30.
- Stopped ratio: `(3,800/10,000)×100 = 38%`; groups `(10,10)` and `(90,0)` yield 10%, not 50%.
- Exact nearest-rank P95: 60 samples with 57 values 10m and 3 values 100m yield 10m at rank 57; max remains 100m. For 300 samples use rank 285.
- Rollup P95: direction P95 37m and 44m yields worst direction P95 44m with its ID; it does not produce a pooled intersection P95.
- Pressure candidate: O=88%, R=76%, equal weights gives 82, but remains illustrative pending source fix and owner calibration.
- Observed load: two slots with directional totals 15 and 35 yield average intersection load 25 vehicles.

## Precision, tie and definition decisions

Preserve unrounded sufficient statistics; round only at presentation. Define decimal precision/tolerance in the independent oracle. Threshold classification uses unrounded score. Argmax and rank ties require a stable key order. Range queue summary must be called “worst window P95” if computed as max of window P95s; exact range P95 requires the underlying distribution. B4B-04 scale switch alone is not proof of a nested-window comparison feature.

## Chuẩn state, provenance và P1 (RV-06)

Dùng registry version theo M-01…09 và source semantics version. Traffic facts cung cấp vehicle_count, waiting_vehicle_count (0 ≤ H ≤ V), average_speed_kmh, queue_length_m, occupancy_pct và direction_payload_json. Signal facts cung cấp signal_color. Intersection facts cung cấp control_mode, incidents_json, các cờ spillback và total_vehicle_count_quality. State tách structural slot ledger, expected/valid counts của từng metric và numerical denominator. Speed requires valid V/H và speed khi M>0. V=H hợp lệ đóng góp zero moving count mà không cần speed numeric nếu đo được V/H và xác nhận non-applicability. Stopped ratio có thể publish nếu speed unavailable. Load chia distinct aligned slots tại grain, không cộng denominator qua directions. P95 giữ sorted-array exact oracle và bounded samples, worst-ID tie sort stable by (intersection, archived approach ID). Áp precision/tolerance registry trước production, threshold trên giá trị chưa round.

| Metric / phiên bản ứng viên | Input, state và output | Oracle / cost |
|---|---|---|
| D-02 storage_lane_v1 | Valid storage_utilization_lane từ direction JSON, ratio 0–1; mean/nearest-rank P95/max, xuất percent ×100 nếu chọn unit %. Expected theo geometry và slots, missing optional không thay bằng PCU | F-05 40/50, 60/100 → .8 snapshot; O(samples) memory exact |
| D-03 sampled_signal_v1 | Count valid colors, green/yellow/red shares ×100; queue dependency chỉ green slots; green-with-queue % = 100 × joint count(q>0)/green count; G=0 N/A | 24/60=40%, 18/24=75%. Thiếu q trên green chỉ suppress conditional ratio. O(1) counters/direction |
| D-04 context_v1 | Count scenario theo intersection/slot, distinct >1 là Mixed Scenario. Network profile giữ vector local, heterogeneous profile ở cùng slot khác temporal mixed. Control mode theo network, distinct qua time >1 Mixed Mode; conflicting same-slot modes gắn invalid | F-04 local scenario và shuffled event IDs, O(slots × entities) bounded chronology |
| D-04 incident_spillback_v1 | Present vs absent vs unavailable theo valid field/quality; giữ observed positive samples và optional exposure share chỉ khi coverage đủ | F-05 false/unknown/partial, không suy ra duration hay causal inference |

Stream theo event ID. Buffer bounded context theo (slot ordinal, actual timestamp, stable event ID), sort khi finalize. First/last theo slot đã verify. Adjacent valid samples khác nhau tạo marker observed interval (previous time,current time], gap chỉ báo uncertainty, không giả exact event time. Previous window delta chỉ cùng run/duration/definition/source/population và adjacent eligible boundaries. Gaps break persistence. 50/50 pressure vẫn candidate, direct inputs chỉ occupancy + stopped ratio trên aligned population. Queue/trajectory/discharge/spillback là reference evidence, không thêm vào công thức. Holdout theo seeds, 60/300s, false-high khi normal red và missed bottleneck; owner duyệt error targets trước holdout.
## State trước khi fold

Reducer xử lý slot duplicate/conflict trước metric arithmetic theo [coverage contract](coverage-contract.md). Sau verified seal, compute worker fold chỉ slot UNIQUE hợp lệ theo deterministic order. Conflict invalidate cả slot dù một claim đến cuối stream. Store bounded computed payload theo [state schema](state-schema.md) để publication retry dùng lại kết quả.
