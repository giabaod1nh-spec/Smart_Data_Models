# Source metric feasibility

## Data path and status

The intended path is TraCI → `SumoSnapshotProvider` → `capture_cycle`/V1 contract → Kafka Raw V1 → Bronze semantic identity → Silver V1 facts → public `CanonicalSilverReader` verified bundles → Gold. Presence in TraCI or the realtime Projector does not prove Silver retention or public canonical accessibility. For each metric, verify the complete path against real saved run evidence before adoption.

| Metric | Current source / canonical field | Silver/public availability | Limitation and required action | Disposition |
|---|---|---|---|---|
| Vehicle count V | `snapshot_provider.py` approach unique vehicle IDs; V1 direction payload `vehicle_count` | V1 Silver traffic fact and bundle | Per-snapshot observed exposure; approaches may overlap spatially. Prove identity and expected slot contract. | Candidate core |
| Waiting vehicle H | IDs below `HALTING_SPEED_MS`; `waiting_vehicle_count` | V1 traffic fact | Count of stopped vehicles in snapshot, not seconds or unique vehicles over a window. Verify speed threshold and V≤H invariant. | Candidate core |
| Moving speed | Mean speed among above-threshold IDs, km/h | V1 traffic fact | Mean must be weighted by corresponding moving count; prove numerator count and speed population match. No-moving zero is not measured 0 km/h. | Candidate core after oracle |
| Queue length | E2 jam by lane/movement when any jam is nonzero; otherwise stopped vehicle length + gap estimate; approach is max movement | V1 `queue_length_m` and optional movement JSON | Different methods/scopes can be mixed; E2 measured zero and fallback are ambiguous; provenance absent. Capture source method and quality or choose compatible policy. | Validate/block official P95 |
| Occupancy | E2 LaneAreaDetector value; lane fallback; both serialized as `occupancy_pct` | V1 traffic fact; quality may flag incomplete getter | E2 TraCI is percent, but `<=1` is multiplied by 100. Lane API is ratio and needs conversion. One rule is applied to two units. Confirm version/source boundary and correct in separate approved package. | Confirmed defect; block dependent score |
| Storage lane ratio | jam meters per sensing lane / lane length, max per approach | Optional direction JSON preserved by V1; emitted only with complete full-link inputs | Geometry/multi-edge and missing historical payload coverage unverified. It is stopped-space estimate, not general PCU load. | P1 feasibility |
| Storage PCU ratio | queued PCU / geometric PCU capacity | Optional JSON | Numerator may include moving vehicles; not queue storage utilization. | Do not use as queue utilization |
| Signal state | sampled color/phase/timing values in V1 direction/signal facts | Available from event; exact historical completeness not measured | Sampled green share is cadence-dependent and not exact green duration; timings are state fields, not additive per-snapshot durations. | P1 proposal |
| Scenario | `scenario_id` per observation/intersection | Silver context and canonical bundle | Can change within a window; retain sampled provenance and flag mixed. Do not filter membership by scenario without approved rule. | Candidate provenance |
| Control mode | `control_mode` from runtime | Silver intersection/common context | Current runtime mode is network-level. Do not infer mixed per-intersection controller. | Candidate run/window context |
| Incidents/spillback | incident records, spillback booleans/measurements | Optional and quality-aware fields | Absent optional field is unknown, not false; sampled evidence does not prove exact duration or cause. | Context only |
| Network trip arrivals | `backend._exited_total` via `getArrivedNumber()`; TripCollector arrival IDs | Not mapped into current canonical business facts | Requires durable interval deltas, restart/reset/teleport rules and independent TripInfo oracle. Not stop-line throughput. | Research |
| Stop-line throughput | E1 at configured offset; vehicle approach arrivals are ID deltas | No validated crossing event in canonical facts | E1 presence is not a one-time crossing count; SUMO transition tracking or native output needs feasibility experiment. | Research |
| Trip waiting seconds | TripCollector record field `waiting_time_sec=None`; active `getAccumulatedWaitingTime` side stats | Not available as canonical historical metric | Active waiting is not completed-trip average; `AI_Control_Traffic_Light/utils/metrics.py` labels waiting vehicle counts as avg_waiting. Do not expose as seconds. | Research |
| AI evaluation | `MetricsCollector` count-based waiting samples; reward uses arrived counter delta | CSV/evaluation path, not canonical truth | Semantics and cohort/reset alignment need AI owner review; no Gold authority. | Coordinate; no Stage A controller change |

## Required source experiments (separate, approval-gated work)

1. **SF-01 occupancy:** deterministic E2 and lane probes at 0%, 0.5%, 0.8%, 1%, 1.1%, 50%, 100%; record raw TraCI return, SUMO version, canonical payload, Silver values and flags. Correct only the source layer under a separately authorized package. Establish data version boundary and old-run treatment.
2. **SF-02 queue compatibility:** parallel E2 jam and trajectory/fallback values for empty, stationary, mixed-movement, mixed-lane and incomplete-detector cases; record method, length, scope and quality. Decide whether one method is canonical or methods remain separately typed.
3. **SF-03 storage geometry:** inspect `phase-2/fixtures/current-sumo-cycle.json`, generated topology and actual detector/lane geometry across approaches; prove units, denominators, multi-edge cases, optional omission, quality flags and historical availability.
4. **SF-04 throughput:** compare cumulative arrivals/TripInfo with independent trip completion; then evaluate E1 or junction crossing events for stop-line throughput. Include exact interval boundaries, teleport/removal and restart. Prefer offline SUMO output as oracle before adding per-step reads.
5. **SF-05 waiting:** compare TripInfo waiting definitions and known trajectories; define completed-trip cohort and unfinished reporting. Per-approach tracking is a later research option.
6. **SF-06 AI metrics:** AI owner verifies `avg_waiting`, throughput delta and evaluation call sites; document corrected meanings without modifying controller/reward in Stage A.
7. **SF-07 schedule capture:** record actual observation simulation times/steps over start, steady state, fractional step and terminal run. Reconcile cadence, alignment and participant set with manifest/topology.

Each experiment reports source/version, saved input/output, independent oracle, resource overhead, compatibility impact and ADOPT/DEFER/REJECT recommendation. Optional source extensions do not block nondependent core metrics.

