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

