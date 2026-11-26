# 04 — Intersection Analytics Detail & Source Feasibility

Status: proposed scope; không phải implementation. Reuse cùng run/duration/window/range và canonical metrics của Overview. Mục tiêu chẩn đoán direction, không nhân bản network dashboard. P0 direction breakdown; P1 storage/signal/context sau review nguồn.

## 1. D-01 — Direction Performance & Pressure Breakdown (P0)

**Business question:** Hướng nào đóng góp áp lực lớn nhất và vì đại lượng nào?  
**Input/formula/grain:** archived approaches × W: M-01 speed, M-02 stopped ratio, M-03 q mean/P95/max, M-04 occupancy, M-05 score, M-07 observed load. Mỗi hàng một hướng; không tự tạo North/South/East/West cho mọi giao lộ. Score bar max 100, tooltip components; show worst-pressure and worst-queue markers independently.  
**Visualization:** table + bars hoặc compact direction comparison, no duplicated score cards. Queue distribution có thể hiển thị mean/P95/max bằng dot/range plot, không gọi là boxplot nếu chưa có quartiles.  
**Interaction:** chọn direction → focus historical trends (same metrics, direction grain), preserve context khi back.  
**Quality:** strict per metric; missing direction hiện row unknown, không drop. Calibration pending giữ physical metrics, suppress official severity.  
**Acceptance:** topology A có 3 rows, M có 4; queue argmax khác score argmax; exact rollup khớp Overview; empty vs missing đúng. Gold cost reuse direction accumulators.

## 2. D-02 — Queue Storage Utilization (P1; source already present)

**Business question:** Hàng đợi có chiếm phần lớn không gian chứa của sensing lane, dù queue metres không lớn?  
**Input:** optional `direction_payload_json.storage_utilization_lane` (ratio), `full_link_jam_length_m`, `approach_length_m` và quality/method metadata; source đã tính ở `_full_link_queue_metrics()` [S6]. Silver giữ JSON [S2]. Không cần source contract mới chỉ để đọc giá trị hiện hữu; feasibility vẫn cần xác minh geometry/coverage.

**Source semantics:** mỗi sensing lane l, `J_l=Σ_halted(length_vehicle+1.5m)`; `U_l=min(1,J_l/L_l)` nếu valid positive length; `U_d=max_l U_l`. Đây là ước lượng occupied storage của stopped vehicles, không phải continuous queue tail hoặc tổng capacity tuyến. `full_link_jam_length_m=max_l J_l` và `approach_length_m=max_l L_l` có thể đến từ hai làn khác nhau, không chia chúng để tái tạo U. `storage_utilization_pcu` dùng PCU hiện diện (cả moving) so với nominal geometry, không phải stopped queue ratio. Một số sensing approaches chứa nhiều đoạn; phải ghi scope rõ. Contract description có wording sum trong khi provider returns max lane; cần reconcile documentation, không tự đổi formula Gold.

**Formula/grain:** direction×W: `mean_storage_pct=100ΣU/n`, `p95_storage_pct=100×nearest_rank(U,0.95)`, `peak_storage_pct=100max(U)`. Optional high-storage sample share = count(U≥approved threshold)/n×100; threshold chưa duyệt, không default 80% cảnh báo thật. Có thể roll up max P95 across directions nếu có nhu cầu, không avg ratios giữa lanes hoặc khẳng định spillback từ score alone.

**Visualization:** sorted approach bars P95 storage %, tooltip mean/max, measured lane scope; đặt cùng P95 queue m để đối chiếu nhưng riêng units. Ví dụ lane 40m/50m=80%, lane 60m/100m=60%; hướng 80%, dù max jam là 60m. Fixture [S9] M East 3.5m jam, U=0.4828; A West 7m, U=.1405 tại 60.04s; đây là snapshot, không P95. 

**Interaction:** click storage bar focus approach, mở geometry/evidence nếu có. Không suy ra lane ID worst nếu payload chỉ lưu max ratio không argmax.

**Data quality:** 100% applicable slots, numeric finite in [0,1], missing optional/failed full_link read → unavailable. Không clamp invalid payload trong Gold. Feasibility kiểm tra all sensing lane lengths/IDs, lane segmentation, fallback/method changes và source full_link_incomplete. Lịch sử không có đủ method evidence không được certified official chỉ vì field numeric.

**Acceptance:** lane example above; fixture ratio conversion 48.28%; P95 exact nearest rank; optional absent withheld; storage_pcu>0 with no stopped vehicles không làm queue storage>0; true ratio 0 valid; geometry denominator 0/unknown rejected. Chi phí O(n) arrays + O(n log n) exact sort (tối đa 5100 direction samples trên baseline 300s), reuse verified input. Owner gate SF-02/03 trước phát hành.

## 3. D-03 — Sampled Signal Operation (P1)

**Business question:** Tín hiệu được phân bổ ra sao và hàng đợi có còn tồn tại trong các mẫu đèn xanh?  
**Input:** same-event same-direction canonical traffic & signal facts: `signal_color`, `phase`, `timing_mode`, optional timing, `queue_length_m`, `waiting_vehicle_count` [S2]. Không join theo gần đúng wall clock. Signal color direction-level không đảm bảo mọi movement đều green; cần audit mapping.

**Formula:** G = count valid green samples; Y,R tương tự; `sampled_green_share_pct=100G/n_signal` (sampled red/yellow tương tự, unknown/other explicit). `green_with_queue_ratio_pct=100×count(green AND q>0)/G` với joint valid inputs theo approved applicability; cần đủ signal slots và q tại mọi applicable green slot. Queue>0 là candidate rule cần source semantics review. Nếu G=0 → N/A No green samples. Không dùng configured `green_duration_seconds` cộng qua samples. Có thể bổ sung mean queue during green (Σ q_green/G), không gọi split failure hoặc served volume.

**Grain:** direction×W, không tự avg green shares qua directions thành network green time (có thể green đồng thời). Range merge counts theo non-overlap windows. 

**Visualization:** stacked 100% sampled color shares per direction, separate bar green-with-queue; tooltip G/n, q method và no green. Thời gian phase chính xác, residual queue cuối green, served vehicles/cycle là research cần phase transitions/crossing events.

**Interaction:** select direction hoặc green-with-queue → focus queue/signal context cùng time range; không gửi thay đổi đèn.

**Quality:** signal unknown, missing joint samples withheld; measured no queue valid; sampled state có aliasing khi phase ngắn hơn cadence. Không khẳng định causal “green too short” từ ratio. 

**Acceptance:** 60 signal samples (24 green, 3 yellow, 33 red), 18 green with q>0 → green share 40%, green-with-queue 75%; missing q tại một green → withheld second, first valid; G=0→N/A; signal direction mismatch rejected. Gold cost O(n) counters and same-bundle matching; no per-chart join scan.

## 4. D-04 — Incident, Spillback & Local Context (P1)

**Business question:** Trong window có evidence sự cố, thay đổi scenario hoặc spillback không?  
**Input:** `scenario_id`, intersection `incidents_json`, optional `spillback_detected`, `intersection_box_blocked`, `downstream_edge_full`, `spillback_pressure`, `control_mode` [S2,S5]. `waiting_reason_counts` chỉ là heuristic dựa màu đèn, không ground truth nguyên nhân. 

**Formula/grain:** intersection×W: incident_observed=any(valid nonempty incidents), incident_sample_share=count(nonempty)/expected valid slots×100 khi đủ coverage; spillback analogous true/false/unknown. Positive observed evidence có thể ghi nhận với partial coverage như evidence note, không publish official share/absence khi thiếu slots. Scenario/mode distinct set + sampled counts; changes between adjacent slots = observed change marker, không exact event start. Nếu hai snapshots khác nhau nhưng có gap, chỉ biết khoảng thay đổi chưa rõ. 

**Visualization:** context strip + small historical event list/marker, source timestamp/observed status; không tạo unsupported exact duration hoặc nguyên nhân. 

**Interaction:** focus before/after aligned windows, highlight network heatmap/trends; no causal claim. 

**Quality/acceptance:** [] là none at capture, optional absent unknown; one true+missing → observed yes, rate withheld; scenario changes mid W→Mixed Scenario; control mode network-wide at baseline; exact timestamp chỉ có nếu nguồn event đã được xác minh. Chi phí O(n) counters/sets, bounded metadata; sampled transition history query từ marts cần giữ first/last/context summaries hoặc design event evidence riêng nếu chính xác hơn.

## 5. D-05 — Extended source metric investigation

Không phát hành throughput/waiting như số liệu chính thức chỉ vì mock đã hiển thị. Xem Experiment spec 05 cho công thức và feasibility. P95 storage có đường truyền V1, throughput/approach waiting chưa có canonical inputs. Ưu tiên nguồn SUMO-only diagnostic nhỏ, independent trajectories/TripInfo, đo overhead trước khi contract change. Không thêm per-vehicle polling ở step 0.01s mà chưa profile. Nghiên cứu không bắt buộc phát sinh implementation nếu kết quả không khả thi; report explicit exclusion. Source correction occupancy là P0 dependency, không gộp vào research optional.
