# 05 — Experiments & Comparative Analytics

Status: Review candidate. Fixed-Time vs Adaptive là yêu cầu bắt buộc của đề tài; kết quả nghiên cứu extended metrics chưa được cam kết. Đây là trang riêng, không phải tab điều khiển trên Analytics Overview.

## 1. E-01 — Experiment design & run catalog (P0)

**Business question:** Hai run có điều kiện đầu vào tương đương để đánh giá tác động của chiến lược điều khiển không?  
**Input:** archived run identity, topology digest, participant set, detector geometry/config, SUMO/producer/controller version, initial state, seeds, routes, demand schedule, local scenario schedule, effective control mode timeline, warm-up, analysis period, run status, metric definition/coverage/revisions. `run_manifest.py` hiện có nhiều metadata (seed, hashes, profile, topology, control mode); chưa chứng minh đầy đủ effective sampling/demand history hoặc experiment cohort. Không giả định run catalog API đã hoàn chỉnh.

**Grain:** experiment group × independent run × analysis interval; one baseline and one or more candidate runs. Fixed-Time và Adaptive là hai lần chạy độc lập có common input design, không phải hai outputs cùng snapshot. So sánh cùng simulation time không yêu cầu cùng vị trí xe sau khi strategy khác nhau; đó là kết quả cần đo.

**Design:** record demand schedule cố định từ trước hoặc chứng minh equivalent offered demand; cùng seed riêng không đủ khi spawning phụ thuộc trạng thái. Ghi planned departures, inserted/completed/unfinished và depart delay nếu có. Với strategy comparison, chỉ strategy intervention và controller-specific parameters được thay đổi; không thay scenario/demand/net/measurement definitions. Local incident scenario lưu per-intersection schedule, không gắn một label global duy nhất. Với scenario comparison, cố định strategy và các biến không chủ đích, khai báo biến thay đổi (ví dụ Incident tại A ở t=300) và các giao lộ còn Normal. Không loại neighbors Normal khỏi scope. Adaptive có thể làm insertion time thay đổi; đó là outcome cần báo, không ép realized vehicle counts giống nhau.

**Comparability rule (proposed):** tất cả mandatory conditions phải được chứng minh bằng metadata/evidence; thiếu thông tin → Unverified/Not Comparable, không green check. Chọn matched scenario profile, same analysis bounds và duration, same archived topology/participant/sensing scopes, cadence, metric versions, valid coverage. Nếu control mode thay đổi giữa range hoặc manual/preemption intervention không nằm trong experiment protocol, exclude pure-strategy comparison. Cho phép protocol riêng cho paired interventions nếu được duyệt. Không so full run 1800s với run 1200s; có thể explicit chọn common compatible range 0–1200s, hiển thị phạm vi mới. Chỉ so published closed windows; ongoing run có thể xem interim historical comparison trên common published range được xác minh, ghi “in progress”, không gọi final experiment. Final conclusions dùng completed planned experiment và independent oracle.

**Visualization:** two tabs Control Strategy / Scenario; run selectors từ experiment catalog (ID, status, metadata), baseline/candidate, scope All Intersections hoặc one intersection, duration, common analysis range. Comparability panel liệt kê từng check với PASS/FAIL/UNKNOWN và evidence; không xem cả hai run là comparable chỉ vì UI chọn cùng scenario/window. Nếu backend chưa đủ metadata, có thể nhập run ID để tra cứu nhưng vẫn fail-closed.

**Interaction:** chọn baseline/candidate → validate trước query result; excluded run hiển thị reason và no aggregate/delta; scope/range change revalidate; không start/stop SUMO hoặc đổi control mode. Link về run Analytics Overview giữ context. 

**Data quality / acceptance:** same seed + different demand schedule → reject; same network name + different digest → reject; missing scenario history → unknown; mixed mode range → reject pure comparison; compatible local scenario profiles → allow; all checks verified → permit. Revision invalidation revalidates affected comparison. Cost: read small metadata and effective marts, no canonical scan, no separate pipeline.

## 2. E-02 — Performance Summary (P0)

**Business question:** Trong cùng khoảng thực nghiệm, phương án nào có kết quả tốt hơn ở từng metric?  
**Input/formula/grain:** non-overlapping published windows cùng duration/range/scope của mỗi run; use M-01 speed Σnumerator/Σdenominator, M-02 ratio ΣH/ΣV, M-07 load Σslot exposure totals/Σslots. Queue metric `worst_window_p95_queue_m=max_W max_d P95_d,W` (max across window-level worst approach P95). Không gọi là P95 toàn run; nếu cần exact full-range quantile phải duyệt thêm distribution strategy. Optional high-pressure window share/persistence có thể thêm sau calibration, không dùng mean score làm kết luận hiệu quả.

| Metric | Unit | Interpretation | Change |
|---|---|---|---|
| Average Moving Speed | km/h | Thường cao hơn là mobility tốt hơn, cần đọc cùng stopping/load | absolute + relative % |
| Stopped Vehicle Ratio | % | Thường thấp hơn; không đồng nhất delay | percentage points; optional relative |
| Worst Window P95 Queue | m | Thường thấp hơn, cần xét location/storage | absolute + relative % |
| Average Observed Vehicle Load | veh | Context, không có chiều tốt/xấu mặc định | absolute/relative, neutral |
| Network Trip Completion Rate | veh/h | Research, completed trips per analysis time | chỉ khi source được duyệt |
| Average Trip Waiting Time | s/veh | Research, completed trip cohort | chỉ khi source được duyệt |
| Stop-line Throughput / Approach Waiting | veh/h, s/veh | Research, khác scope network trips | chỉ khi source được duyệt |

`delta=B−A`, `relative_pct=100(B−A)/A` khi A≠0; baseline 0 → absolute only. Round cuối; improvement color tùy direction metric, không all green; N/A nếu denominator 0 hoặc missing. Không mặc nhiên Adaptive tốt hơn; có thể improve A, worsen B. Multiple metrics trade-off cần báo đầy đủ, không cherry-pick.

**Visualization:** comparison table với values, units, delta, coverage/cohort; compact per-row bars normalized trong cùng metric (không chung Y scale mixed units). Show baseline/candidate IDs, actual common analysis range, excluded runs. 

**Interaction:** metric row chọn comparative trend E-03; click candidate mở Analytics run; switch scope recompute summary with correct grain.

**Data quality:** 100% metric-specific coverage toàn comparable range và both runs. Một metric thiếu không tự làm những metric độc lập mất hiệu lực, nhưng nếu experiment comparability metadata fail thì không publish comparison chính thức. Official study scope phải nêu required metric set trước; thiếu optional research không tạo false complete experiment. No average of averages/P95s.

**Acceptance:** speed 25.4→31.6 km/h, delta+6.2, +24.41%; stopped 48→32%, −16pp; worst window q 44→30m, −14m, −31.82%; load neutral; baseline zero no division; all moving absent N/A; missing W not silently excluded. Cost: sums/max over marts O(K), no new proof per comparison.

## 3. E-03 — Comparative Performance Trends (P0)

**Business question:** Sự khác biệt giữa phương án xuất hiện từ thời điểm nào và duy trì ra sao?  
**Input/formula/grain:** same metric at same window boundaries, run × scope × W, validated common range. M-01/02/03/07; pressure only compatible calibrated version. One line per run/strategy; no alignment by wall clock, no averaging two durations.  
**Visualization:** selector, two lines gray/cyan, per-window tooltip absolute/delta, event markers only verified, gap if missing, planned endpoint vs available endpoint. Do not extrapolate adaptive curve.  
**Interaction:** select metric from summary; click W opens historical detail of corresponding run; zoom common range revalidates summary.  
**Quality/acceptance:** no other run data if missing; no interpolation; two lines aligned 60/300 boundaries; out-of-range points excluded; missing either result delta withheld. Cost O(K×runs) published rows, bounded range/page.

## 4. E-04 — Change by Intersection (Proposed P0)

**Business question:** Cải thiện toàn mạng có che khuất suy giảm ở một số giao lộ?  
**Input/formula/grain:** same intersection scope across matched runs, metric range rollup E-02; `delta_i=B_i−A_i`; optional relative change when baseline nonzero.  
**Visualization:** diverging horizontal bars với zero center, positive/negative units and meaning, show both baseline/candidate values; sort stable, highlight worsening and improvement, not always positive. Không gọi improvement nếu metric load.  
**Interaction:** click intersection → both matched trends or respective historical details.  
**Quality/acceptance:** full matching participant set, one missing intersection no numeric delta; a test includes one improved and one worsened intersection; correct units/zero baseline. Cost O(I×K), no physical mart per chart.

## 5. E-05 — Repeated trials & interpretation (Proposed research protocol)

Một cặp run là minh họa một thí nghiệm, không đủ kết luận generalizable. Đề xuất nhiều paired seeds với paired demand schedules; định nghĩa trước sample size, warm-up, metric set và evaluation horizon; ghi mọi lần chạy kể cả thất bại, không chọn riêng best Adaptive. Tính per-pair metric deltas, báo distribution/mean hoặc median, uncertainty phù hợp với sample size và method đã duyệt. Không báo statistical significance nếu chưa thực hiện test hợp lệ. Scenario comparison là sensitivity/descriptive evaluation, không mặc nhiên causal superiority. Before/after trong cùng run là observational intervention analysis, tách khỏi controlled experiment.

## 6. Extended metrics feasibility (Research; no automatic contract change)

### E-R1 — Network Trip Completion Rate

`Visualize/simulation/backend.py` có cumulative `_exited_total` từ `getArrivedNumber()` mỗi step; `TripCollector` có arrived/departed IDs. Đây là network trip arrivals, không phải stop-line crossings. Cần durable count theo interval/run identity và phân biệt genuine arrivals vs removals/teleports, reset/restart; count once per completed trip. `rate=3600×N_completed/D`, units completed trips/h. Denominator exposure time là simulation seconds. Nếu count=0 với complete interval →0; missing interval withheld. So sánh cùng planned demand, unfinished population và depart delays. Source candidate có thể dùng cumulative counter deltas với epoch/reset and interval rules hoặc TripInfo arrivals (offline oracle). Không cộng cùng global total từ 5 intersection events. Acceptance: 30 arrivals in 60s→1800/h; zero valid; restart no duplicate, event at boundary exactly once; count agrees independent arrivals/TripInfo with defined eligibility.

### E-R2 — Stop-line Throughput

Source E1 detectors đặt trước lane end mặc định 20m [S7], arrival-rate PCU/s là ID-set delta giữa snapshots, không crossing. Không dùng `getLastStepVehicleNumber` sum làm count nếu vehicle detector presence kéo dài nhiều steps. Nghiên cứu E1 đúng vị trí và passage event semantics hoặc junction transitions theo movement. Cần `crossing_count` với intersection/approach, unique traversal/passage identity hoặc exact interval count, interval bounds và quality. `throughput=3600×Σcrossing_count/D`, veh/h; network sum = total junction passages (xe có thể nhiều lượt), không unique trips. Capture ở step đủ chính xác; aggregate interval trước publish nếu an toàn, không tạo Kafka event/vehicle mặc định. Boundary assignment cần chứng minh (capture timestamp không mặc nhiên interval end thuộc W hiện tại); teleport, lane change, repeated pass, short lanes, restart. Oracle trajectory/independent stop-line passage count. Scope comparison throughput high không luôn đồng nghĩa demand served nếu demand khác nhau.

### E-R3 — Average Trip Waiting Time

`TripCollector` hiện có waiting_time_sec placeholder None; active waiting stats là active-only accumulated waiting memory, không completed-trip duration. Nghiên cứu SUMO TripInfo [R5] `waitingTime` (trip-level semantics), duration, departure delay, arrived, unfinished; confirm SUMO version/config và waiting threshold/intentional stops. `avg_trip_wait_s=Σcompleted_trip_wait_s/N_completed`, including zero-wait trips. Cohort theo trip arrival time W hoặc explicit completed-trip range. N=0 with complete source → N/A; missing records withheld. Báo unfinished vehicles và not-inserted demand cùng metric để tránh survivorship bias. TripInfo offline có thể phục vụ experiment oracle trước khi quyết định durable pipeline capture; không tuyên bố đã tích hợp Gold. Acceptance five trip waits [12,8,0,20,10]→10s/trip; unfinished excluded from mean but reported; complete vs truncated record; duplicated trip not counted twice.

### E-R4 — Average Approach Waiting Time (P2 candidate)

Cần tracking traversal vào sensing approach tới verified crossing; accumulate stopped duration trên mỗi traversal theo approved halt threshold, movement and interval semantics. `avg_approach_wait=Σwait_duration_completed/N_completed_traversals`, s/veh-passage. Assign full duration to W of completion, có thể bắt đầu trước W. Exclude incomplete traversal from official mean, report incomplete/active separately; teleport/remove/reenter/restart identity rules, no inferred waiting from snapshots. Không gọi control delay. Acceptance: entry=50, stopped 60–76, crossing=85 →16s contributes [60,120), zero-wait included; incomplete never completed. Source overhead may be substantial; prioritize network metrics first, not per-step all-vehicle polling until measured.

### E-R5 — AI evaluation consistency

Audit `AI_Control_Traffic_Light/utils/metrics.py` avg_waiting currently derives waiting_vehicle_count (veh), not seconds; `reward_calculator.py` throughput delta uses difference of per-step arrivals, not cumulative count, as found in preceding source audit. Verify call sites and runtime version before relying on results. AI evaluation CSV is not automatically canonical evidence. Do not change reward/controller in Phase 4B without separate approval. Research deliverable must propose corrected metric names/semantics and compare with independent SUMO oracle.

### Feasibility gate

For each research metric, record source/version, scope, exact counting rules, required evidence, boundary/restart tests, compatible contract evolution, old-run N/A, measured SUMO overhead and payload/DE impact, decision ADOPT/DEFER/REJECT. No source modification by this specification. If source unavailable, keep research out of official KPI/experiment results; do not block independent P0 traffic marts. Prioritize E-R1/E-R3 for required control evaluation, E-R2 next, E-R4 last (proposed priority, owner to approve).
