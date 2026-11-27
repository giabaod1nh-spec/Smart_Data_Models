# Source metric feasibility

## Data path and status

The intended path is TraCI → `SumoSnapshotProvider` → `capture_cycle`/V1 contract → Kafka Raw V1 → Bronze semantic identity → Silver V1 facts → public `CanonicalSilverReader` verified bundles → Gold. Presence in TraCI or the realtime Projector does not prove Silver retention or public canonical accessibility. For each metric, verify the complete path against real saved run evidence before adoption.

| Metric | Current source / canonical field | Silver/public availability | Limitation and required action | Disposition |
|---|---|---|---|---|
| Vehicle count V | `snapshot_provider.py` approach unique vehicle IDs; V1 direction payload `vehicle_count` | V1 Silver traffic fact and bundle | Per-snapshot observed exposure; approaches may overlap spatially. Prove identity and expected slot contract. | Candidate core |
| Waiting vehicle H | IDs below `HALTING_SPEED_MS`; `waiting_vehicle_count` | V1 traffic fact | Count of stopped vehicles in snapshot, not seconds or unique vehicles over a window. Verify speed threshold and 0 ≤ H ≤ V invariant. | Candidate core |
| Moving speed | Mean speed among above-threshold IDs, km/h | V1 traffic fact | Mean must be weighted by corresponding moving count; prove numerator count and speed population match. No-moving zero is not measured 0 km/h. | Candidate core after oracle |
| Queue length | E2 jam by lane/movement when any jam is nonzero; otherwise stopped vehicle length + gap estimate; approach is max movement | V1 `queue_length_m` and optional movement JSON | Different methods/scopes can be mixed; E2 measured zero and fallback are ambiguous; provenance absent. Capture source method and quality or choose compatible policy. | Validate/block official P95 |
| Occupancy | E2 LaneAreaDetector value; lane fallback; both serialized as `occupancy_pct` | V1 traffic fact; quality may flag incomplete getter | E2 TraCI is percent, but `<=1` is multiplied by 100. Installed lane Python API documents percent. Probe raw installed returns for both lane and E2 and version each path. Confirm version/source boundary and correct in separate approved package. | Confirmed defect; block dependent score |
| Storage lane ratio | jam meters per sensing lane / lane length, max per approach | Optional direction JSON preserved by V1; emitted only with complete full-link inputs | Geometry/multi-edge and missing historical payload coverage unverified. It is stopped-space estimate, not general PCU load. | P1 feasibility |
| Storage PCU ratio | queued PCU / geometric PCU capacity | Optional JSON | Numerator may include moving vehicles; not queue storage utilization. | Do not use as queue utilization |
| Signal state | sampled color/phase/timing values in V1 direction/signal facts | Available from event; exact historical completeness not measured | Sampled green share is cadence-dependent and not exact green duration; timings are state fields, not additive per-snapshot durations. | P1 proposal |
| Scenario | `scenario_id` per observation/intersection | Silver context and canonical bundle | Can change within a window; retain sampled provenance and flag mixed. Do not filter membership by scenario without approved rule. | Candidate provenance |
| Control mode | `control_mode` from runtime | Silver intersection/common context | Current runtime mode is network-level. Do not infer mixed per-intersection controller. | Candidate run/window context |
| Incidents/spillback | incident records, spillback booleans/measurements | Optional and quality-aware fields | Absent optional field is unknown, not false; sampled evidence does not prove exact duration or cause. | Context only |
| Network trip arrivals | `backend._exited_total` via `getArrivedNumber()`; TripCollector arrival IDs | Not mapped into current canonical business facts | Requires durable interval deltas, restart/reset/teleport rules and independent TripInfo oracle. Not stop-line throughput. | Research |
| Stop-line throughput | E1 at configured offset; vehicle approach arrivals are ID deltas | No validated crossing event in canonical facts | E1 presence is not a one-time crossing count; SUMO transition tracking or native output needs feasibility experiment. | Research |
| Trip waiting seconds | TripCollector record field `waiting_time_sec=None`; active `getAccumulatedWaitingTime` side stats | Not available as canonical historical metric | Active waiting is not completed-trip average; `AI_Control_Traffic_Light/utils/metrics.py` labels waiting vehicle counts as avg_waiting. Do not expose as seconds. | Research |
| AI evaluation | `MetricsCollector` count-based waiting samples; reward differences per-step arrival counts | CSV/evaluation path, not canonical truth | Semantics and cohort/reset alignment need AI owner review; no Gold authority. | Coordinate; no Stage A controller change |

## Required source experiments (separate, approval-gated work)

1. **SF-01 occupancy:** deterministic E2 and lane probes at 0%, 0.5%, 0.8%, 1%, 1.1%, 50%, 100%; record raw TraCI return, SUMO version, canonical payload, Silver values and flags. Correct only the source layer under a separately authorized package. Establish data version boundary and old-run treatment.
2. **SF-02/Q queue compatibility:** parallel E2 jam and trajectory/fallback values for empty, stationary, mixed-movement, mixed-lane and incomplete-detector cases; record method, length, scope and quality. Decide whether one method is canonical or methods remain separately typed.
3. **SF-02/SF-03 storage geometry and retained coverage:** inspect `phase-2/fixtures/current-sumo-cycle.json`, generated topology and actual detector/lane geometry across approaches; prove units, denominators, multi-edge cases, optional omission, quality flags and historical availability.
4. **SF-04 throughput:** compare cumulative arrivals/TripInfo with independent trip completion; then evaluate E1 or junction crossing events for stop-line throughput. Include exact interval boundaries, teleport/removal and restart. Prefer offline SUMO output as oracle before adding per-step reads.
5. **SF-05 waiting:** compare TripInfo waiting definitions and known trajectories; define completed-trip cohort and unfinished reporting. Per-approach tracking is a later research option.
6. **SF-06 AI metrics:** AI owner verifies `avg_waiting`, throughput delta and evaluation call sites; document corrected meanings without modifying controller/reward in Stage A.
7. **SF-07 interval/restart + Q-01 schedule capture:** record actual observation simulation times/steps over start, steady state, fractional step and terminal run. Reconcile cadence, alignment and participant set with manifest/topology.

Each experiment reports source/version, saved input/output, independent oracle, resource overhead, compatibility impact and ADOPT/DEFER/REJECT recommendation. Optional source extensions do not block nondependent core metrics.

## SF-01 occupancy theo từng đường đo (RV-02)

Máy local có SUMO 1.27.1 theo `sumo --version`. `D:/SUMO/tools/traci/_lane.py:getLastStepOccupancy` ghi percent và gọi wire getter. [Lane Value Retrieval](https://sumo.dlr.de/docs/TraCI/Lane_Value_Retrieval.html) mô tả phép chia hình học, còn [Python TraCI](https://sumo.dlr.de/pydoc/traci/_lane.html) mô tả API. [E2 retrieval](https://sumo.dlr.de/docs/TraCI/Lane_Area_Detector_Value_Retrieval.html) ghi percent. Đây là static proof, raw getter values và deployment client/server version cần capture.

| Path và source | Expected khi raw percent | Hiện tại nếu raw 0.8 | Getter failure / impact |
|---|---|---|---|
| A inbound E2, detector_manager.py:118–139 | p percent | 80 | Partial E2 có quality flag, toàn bộ fail chuyển lane fallback. Gold occupancy/pressure, canonical và Projector occupancyRate |
| B inbound lane, snapshot_provider.py:_build_direction | p percent, kiểm tra installed wire | 80 | Missing lane getters đánh dấu incomplete; canonical có thể chuyển null/unavailable |
| C outbound E2, detector_manager.py:141–168 | p/100 ratio | 0.8 thay vì 0.008 | Missing detector → lane path hoặc spillback_unavailable. Threshold ảnh hưởng downstream fullness, spillback/box flags, canonical và Projector hasSpillback |
| C fallback outbound lane, snapshot_provider.py:_spillback_metrics | p/100 ratio nếu percent | 0.8 | Getter failure/empty evidence đánh dấu unavailable. Đường này cần probe riêng, cùng regression downstream |

Probe theo vector percent 0, 0.5, 0.8, 1, 1.1, 50, 100. Expected ratio tương ứng 0, .005, .008, .01, .011, .5, 1. Hiện tại nếu percent thì hai inbound path cho 0,50,80,100,1.1,50,100 trước aggregate rounding. Outbound cho 0,.5,.8,1,.011,.5,1 trước rounding. Capture raw, version, length geometry, canonical flags/null và downstream projections. AI reward đọc occupancy và spillback từ snapshot trong evaluation path (reward_calculator.spillback_penalty), effect trên active model/controller chưa được định lượng. Source fix riêng sau approval: scale theo API unit và phương pháp đã xác nhận, preserve fail-closed quality và ghi source semantics/version. Historical rows thiếu provenance không thể sửa bằng chia 100. Regression thêm all getters fail và partial getter.

## SF-02/Q queue và trace IDs (RV-03)

Giữ Worst Approach P95 trong scope. DetectorManager cộng jam theo lanes cùng movement rồi snapshot lấy max movement. E2 scope là configured sensing lanes gần stop line (`Visualize/Visualize/detectors.add.xml`), khác full-link. Mixed lane khiến E2 trả empty mapping. E2 all-zero cũng đi sang fallback, có thể trả số dương. Fallback nhóm stopped vehicles theo inferred movement, cộng length+1.5m, lấy max, không phải đo contiguous jam. Getter errors tạo cờ partial, canonical_cycle loại dependent values theo quality. Silver bảo toàn primary value và direction JSON, chưa có method per sample đáng tin cậy.

| Alternative | Coverage/risk |
|---|---|
| A E2-only | Đồng nhất detector scope. Cần chứng minh mapping all 17, mixed movement có thể không hỗ trợ. |
| B estimate-only | Có thể bao phủ mixed lanes, phụ thuộc lane/vehicle/route getters và scope. |
| C method-qualified | Ưu tiên đánh giá. Lưu method/geometry/version khi đủ evidence, chỉ so compatible populations. Cần contract approval. |
| D mixed current | Chỉ chấp nhận nếu độc lập chứng minh equivalence và quality trong từng stratum. |

Dựng 17-approach matrix từ archived topology, lane bindings và XML, chạy cases E2 nonzero, all zero + stopped estimate positive, mixed lane, missing detector, partial getter, empty và full-link spillback. Oracle tự tính detector sums/max từ raw readings và estimate từ independently captured trajectories/map. Đối soát canonical quality, Silver retention, method-specific coverage/quantile và signed tolerances trước owner decision. Kết quả per-approach hiện chưa đo.

Đối chiếu ID Business Spec: SF-02 geometry và SF-03 retained coverage giữ cho storage (queue branch được ghi SF-02/Q riêng), SF-04 crossing, SF-05 waiting, SF-06 AI, SF-07 interval/restart, SF-08 overhead, SF-09 compatibility. Moving population kiểm tra riêng trong A-P02 bằng sampled vehicle IDs, halting threshold và paired counts. AgentManager gọi reward `network_throughput_delta` lấy hiệu giữa hai giá trị per-step `getArrivedNumber()` và clamp, khác cumulative `backend._exited_total`. CSV đó chưa chứng minh completed trips. Ưu tiên TripInfo offline cho completion/waiting, crossing cần oracle E1/trajectory riêng.
## Evidence để chốt R02 và sizing

SF-07 cần raw run lifecycle cho start/pause/resume/reset/end, cardinality theo archived topology/config, actual step/cycle/time, source capture failures, checked watermark và bounded public metadata plan. Capture fidelity official và diagnostic tách riêng. Bằng chứng SF-07 vẫn cần cho A-P03. Không derive slot population từ received rows.
