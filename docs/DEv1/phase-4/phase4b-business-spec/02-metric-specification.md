# 02 — Metric Dictionary & Aggregation Specification

Status: Proposed formulas for owner review; phạm vi 4 KPI đã thống nhất. Nguồn chuẩn: V1 canonical Silver bundles [S1–S5], không sử dụng legacy Gold V2 làm định nghĩa. Tất cả ví dụ là dữ liệu kiểm chứng tổng hợp, không phải kết quả thực nghiệm.

## 1. Ký hiệu, grain và source mapping

- `W = [a,b)`, `D = b-a ∈ {60,300}` giây mô phỏng; `t` là expected sampling slot (không phải wall-clock), `i` là intersection, `d` là archived approach thuộc `i`.
- `V`, `H`, `M=V−H`, `v`, `q`, `o` lần lượt là vehicle count, waiting count, moving count, moving speed km/h, queue metres và occupancy percent tại `(i,d,t)`.
- `A(i)` và `I(run)` lấy từ archived context; không hardcode. Tập slots áp dụng phải xác định độc lập theo expected-observation contract (06).
- Với cadence đều, cộng các samples có cùng trọng số thời gian. Nếu thay cadence hoặc sampling không đều, cần versioned weighting/applicability contract; không tự dùng công thức hiện tại cho workload khác.
- Scope network nghĩa là tổng hợp vùng quan sát của các giao lộ tham gia, không nhất thiết toàn bộ SUMO network hoặc số phương tiện duy nhất. Nếu sensing scopes chồng lấn thì vehicle exposures có thể lặp giữa approaches; cần xác minh trước khi gọi là unique vehicles.

| Alias | Silver V1 fact / field | Semantics và validation |
|---|---|---|
| V | traffic `vehicle_count` | Unique IDs trong một approach snapshot; số nguyên ≥0 |
| H | traffic `waiting_vehicle_count` | Xe dưới halting threshold; 0≤H≤V; không phải giây |
| v | traffic `average_speed_kmh` | Moving-only mean; finite, nonnegative; xác minh count và speed cùng population |
| q | traffic `queue_length_m` | Max theo movement, E2 hoặc fallback estimate; finite ≥0; cần audit method compatibility |
| o | traffic `occupancy_pct` | E2 hoặc mean lane occupancy, 0–100; unit issue cần live verify |
| load_i | intersection `total_vehicle_count`, `total_vehicle_count_quality` | Chỉ dùng COMPLETE_TOPOLOGY_SUM; không sum intersection + direction cùng lúc |
| signal | signal `signal_color`, `phase`, `timing_mode`, durations/remaining | Sampled states; không cộng configured durations mỗi snapshot |
| storage | traffic `direction_payload_json.storage_utilization_lane` | Optional, ratio 0–1, lấy max tỷ lệ từng sensing lane; [S6–S8] |
| context | common `scenario_id`; intersection `control_mode`, `incidents_json`, optional spillback flags | Context theo observation; absent optional khác false |
| quality | traffic `metric_availability_json`, `quality_status`, `quality_flags_json` | Không chỉ dựa vào overall VALID; kiểm tra input dependency cụ thể |
| identity | `simulation_run_id`, `run_generation`, `topology_digest`, `manifest_identity`, `event_id`, `cycle_sequence`, `simulation_time_seconds`, `intersection_id`, `source_direction`, Bronze revision/hash | Kiểm tra membership, provenance và current-effective state |

Fact families thực tế: `silver_traffic_observation_fact_v1`, `silver_signal_state_fact_v1`, `silver_intersection_state_fact_v1` [S2,S3]. JSON fields ở trên là đường dẫn logic khi giải mã payload, không phải cột SQL nested đã có. Gold đọc verified bundles qua public Silver contract, không đọc physical Silver trực tiếp.

## 2. M-01 — Average Moving Speed (km/h)

**Business question:** Các xe còn di chuyển có đi nhanh hay chậm trong vùng quan sát?  
**Input:** V, H, v và quality; cần xác minh cùng tập unique IDs và cùng ngưỡng dừng.  
**Formula:** Với một group G của approach-time samples, `N_move = Σ(V−H)`, `S_speed = Σ(v×(V−H))`, `speed = S_speed/N_move` khi N_move>0. Tại slot V=H, đóng góp trọng số 0; không nhân NULL rồi làm mất tổng. Nếu missing không được suy ra N_move.  
**Grain:** Direction → Intersection → Network × run × duration × window; cộng numerator/denominator của nhóm con, không AVG các mean. Range nhiều windows không chồng lấn dùng cùng phép cộng.  
**Visualization:** KPI toàn vùng, bảng worst-pressure approach, trend intersection, detail direction, experiment range. Đơn vị km/h, tooltip moving-only và delta với previous compatible window.  
**Interaction:** chọn window/approach cập nhật scope; mode compare vs focus dùng cùng definition.  
**Data quality:** V,H hợp lệ và v có measurement hợp lệ khi M>0. Khi M=0, chỉ coi v không áp dụng nếu rule độc lập xác nhận, không bỏ slot có getter failure. Full coverage nhưng N_move=0 → N/A No moving vehicles, không phải speed=0. Nếu V=0 thì H=0 phải được xác minh.  
**Acceptance:** 10 moving vehicles×40 + 40×20 → 24 km/h; một nhóm 0 moving không đổi kết quả; thiếu v với M>0 → withheld; unequal direction populations rollup trùng oracle trực tiếp.  
**Gold cost:** O(n) cộng 2 statistics mỗi group; O(1) state mỗi group; không thêm canonical scan.

## 3. M-02 — Stopped Vehicle Ratio (%)

**Business question:** Bao nhiêu phần trăm lượt quan sát phương tiện bị dừng?  
**Input:** V,H cùng scope/slot.  
**Formula:** `N_obs=ΣV`, `N_stop=ΣH`, `R=100×N_stop/N_obs` nếu N_obs>0. Đây là exposure ratio; xe xuất hiện nhiều lần đóng góp nhiều lần, không phải tỷ lệ xe duy nhất đã dừng.  
**Grain:** Direction, intersection, network và compatible range, cộng raw sufficient statistics.  
**Visualization:** KPI %, delta theo điểm phần trăm (pp); ranking worst-pressure approach, trends, comparison.  
**Interaction:** Khi speed cao nhưng R cao, chuyển focus xem queue/load/signal.  
**Data quality:** H≤V, đủ expected applicable slots; nếu N_obs=0 → N/A No observed vehicles. Không dùng mean per-slot percentages hoặc mean direction percentages.  
**Acceptance:** 3.800/10.000=38%; nhóm V=10,H=10 và V=90,H=0 cho 10%, không 50%; all stopped →100%, empty →N/A; H>V từ chối.  
**Gold cost:** O(n), 2 counters; dùng chung với M-01 và M-05.

## 4. M-03 — Queue Length Statistics & Worst Queue (P95)

**Business question:** Hướng nào thường xuyên có hàng đợi ở mức cao, thay vì chỉ một spike?  
**Input:** `queue_length_m` q; đo E2 hoặc fallback estimate, max movement trong approach. Source ưu tiên E2 có jam khác 0, fallback có thể được dùng khi E2 toàn 0; cần kiểm chứng measurement semantics và method provenance trước khi dùng chính thức. Không gọi q là full-link queue.  
**Formula:** Direction: `queue_mean=Σq/n`; `queue_max=max(q)`; `queue_p95=sort(q)[ceil(0.95×n)]` (index 1-based, nearest-rank). P95 là giá trị có ít nhất 95% samples ≤ nó; không chứng minh chúng liên tục. Intersection: `intersection_worst_approach_p95_queue_m = max_d Q95_d`; Network: `worst_approach_p95_queue_m = max_i max_d Q95_d`. Giữ argmax ID (intersection,approach); tie-break theo ID/direction key ổn định đã duyệt. Worst pressure approach không nhất thiết là worst queue approach.

**Grain:** Direction thống kê queue distribution theo W; intersection/network worst direction theo cùng W. Range comparison dùng `worst_window_p95_queue_m = max_W Qworst_W` (không gọi là P95 toàn run). Nếu cần range P95 đúng nghĩa, phải merge raw distribution/exact histogram hoặc đọc tập samples thích hợp theo thiết kế được duyệt; không AVG P95.  
**Visualization:** KPI ngắn “Worst Queue (P95)” + location + tooltip đầy đủ; ranking hiển thị q95 của worst-pressure approach; trend hiển thị worst P95 của intersection; detail mean/P95/max.  
**Interaction:** click KPI mở detail của worst-queue approach, không đổi ranking mặc định theo pressure.  
**Data quality:** 100% q samples theo scope; thiếu một hướng làm worst network không đủ population. Empty queue measured 0 vẫn hợp lệ. Không cap q tùy ý theo `approach_length_m` vì phạm vi/movement khác nhau.  
**Acceptance:** 60 samples: 57×10m + 3×100m → P95=10, max=100, mean=14.5; 300 samples index 285; `[0,0,...]` →0; tie deterministic; hai hướng P95 37 và 44 → worst 44 đúng ID, không pooled percentile.  
**Gold cost:** exact quantile cần O(n) sample storage và O(n log n) sort (hoặc exact selection tương đương). Với 17 directions và 300 slots, 5100 giá trị q cho một window toàn vùng, có thể giữ bounded per work generation; không giữ cả lịch sử trong RAM. Không xây dựng thêm full scan cho queue.

## 5. M-04 — Mean Occupancy (%)

**Business question:** Vùng detector/lane đang bị chiếm dụng đến mức nào?  
**Input:** `occupancy_pct`; E2 mean hoặc lane fallback [S1,S6,S7].  
**Formula:** `O_d=Σo/n` tại direction; không đồng nhất với mật độ hay capacity ratio.  
**Grain:** Direction × W. Không dùng mean occupancy network làm KPI; dùng để giải thích pressure trong tooltip/detail. Nếu sau này cần network occupancy, phải xác định detector weighting/length/population mới.  
**Visualization:** Occupancy tooltip và direction comparison.  
**Interaction:** click pressure bar hiển thị hai components.  
**Data quality:** 0–100, đủ slots, method compatible. Quan trọng: source có logic `if occ <= 1.0: occ *= 100.0` ở E2 và fallback; TraCI mô tả occupancy theo phần trăm [R4]. 0.8% có thể bị thành 80%. Cần kiểm chứng phiên bản SUMO và sửa/version metric semantics nếu lỗi được xác nhận. Không tự sửa giá trị Gold lịch sử bằng chia 100 (không phân biệt được 0.8 thật và dữ liệu đã biến đổi).  
**Acceptance:** live/raw fixture với 0%,0.8%,1%,1.2%,50%,100%; kiểm tra canonical output; failed getter không thành 0; E2/fallback consistency; version boundary không trộn.  
**Gold cost:** O(n) sum/count; nguồn correctness là dependency, không xử lý bằng calibration.

## 6. M-05 — Traffic Pressure Score (0–100)

**Business question:** Hướng nào cần ưu tiên do đồng thời bị chiếm dụng và có nhiều lượt xe dừng?  
**Input:** M-02, M-04 tính trên cùng applicable slots; strict joint input coverage.  
**Candidate formula:** `S_d=w_o×O_d+w_r×R_d`, w_o+w_r=1, weights ≥0. Khởi đầu 0.5/0.5; `S_i=max_d S_d` và `worst_pressure_approach=argmax_d S_d`. Phân loại ứng viên: Low [0,40), Moderate [40,70), High [70,100]. Không áp dụng threshold lên giá trị đã làm tròn. Khi V=0 mọi slots và O=0, score 0 theo empty-traffic rule; nếu V=0 nhưng O>0, cần điều tra consistency/measurement scopes, không ép score.  
**Grain:** Direction → Intersection × W; không đề xuất average pressure toàn mạng.  
**Visualization:** Ranking, heatmap, pressure trend selector (P1), persistence, detail. Tooltip nêu components, definition version và label.  
**Interaction:** cùng scale và definition giữa các panels; selected cell chọn window.  
**Data quality:** invalid or unknown component → no score; thiếu bất cứ expected approach → không công bố intersection max như đầy đủ; high count withheld nếu thiếu intersection score. Calibration pending → official classification/high KPI withheld (prototype chỉ nhãn experimental mock).  
**Acceptance:** O=88,R=76,w=.5 →82; max(82,28,42)=82; score 69.95 phân loại Moderate dù display rounding gần 70; empty rule và unknown khác nhau.  
**Gold cost:** O(n) reuse accumulators và O(approaches) max; không thêm scan.  
**Limitation:** occupancy và stopped ratio có tương quan; xe dừng bình thường ở đèn đỏ cũng tăng score. Score không phải LOS/HCM, không chứng minh congestion hoặc spillback.

### 6.1. Calibration protocol (mandatory gate G-CAL)

1. Resolve occupancy unit finding; xác minh queue methods, sensing scopes và input quality. Nếu semantics thay đổi, khóa definition/source version và không trộn lịch sử không tương thích.
2. Tạo fixtures empty, normal qua nhiều chu kỳ, peak, overload, incident cục bộ, ít xe dừng đèn đỏ, hướng tắc riêng, detector unavailable. Chọn ground truth độc lập dựa trên trajectories, queue discharge/residual, spillback observations và kiểm tra trực quan; không dùng chính hai input làm nhãn duy nhất.
3. Đánh giá candidate weights 25/75, 50/50, 75/25 trên calibration data; kiểm tra false-high khi normal red, missed persistent bottleneck và ranking consistency. Đánh giá riêng 60s/300s, holdout seeds/scenarios; định lượng error/confusion, nêu cases bất đồng. Chưa có ngưỡng accuracy được owner duyệt; agent phải đề xuất trước khi đánh giá chứ không chọn hậu nghiệm để PASS.
4. Owner duyệt weights, thresholds, applicability, label, tolerance/tie rule và `pressure_definition_version`. Nếu không đạt, report blocked; giữ metrics vật lý, không tự đổi KPI/score thành standard hoặc tự phát hành high labels.

## 7. M-06 — High-Pressure Intersections

**Business question:** Vấn đề ảnh hưởng bao nhiêu giao lộ?  
**Input:** M-05 intersection scores và expected participant set.  
**Formula:** `high_pressure_intersection_count=Σ_i 1[S_i≥T_high]`; denominator=`|I(run)|`; giữ ID set.  
**Grain:** Network × W, cùng duration/definition.  
**Visualization:** `2/5 intersections`, IDs, previous-window absolute delta.  
**Interaction:** click card highlight/filter high-pressure rows trong ranking (có nút clear); không đổi population của KPI/heatmap/trends.  
**Data quality:** G-CAL pass và mọi expected intersection có valid effective score; nếu thiếu, official count=N/A; có thể ghi riêng unknown count, không gọi partial count là full KPI.  
**Acceptance:** A/M=82/74, B/C/D=52/24/19 →2/5; missing D → withheld, không 2/4 hoặc 2/5; prior high count 1 →+1.  
**Gold cost:** O(intersections), derived từ M-05; không scan mới.

## 8. M-07 — Average Observed Vehicle Load (veh)

**Business question:** Số xe hiện diện trong vùng quan sát thay đổi ra sao, có liên hệ với queue/speed không?  
**Input:** V, intersection total complete topology sum.  
**Formula:** `load_d=Σ_t V_d,t/nslots`; `V_i,t=Σ_d V_d,t`; `load_i=Σ_t V_i,t/nslots`; `load_net=Σ_t Σ_i V_i,t/nslots`. Cộng theo aligned slots trước khi tính mean, không lấy mean các hướng khi muốn total load. Trong range dùng sum exposures/slot count phù hợp (không cộng slot count theo hướng).  
**Grain:** Direction, intersection, network × W; range compatible.  
**Visualization:** Trends selector “Observed Vehicle Load”; detail; comparison context. Không thêm KPI thứ 5.  
**Interaction:** focus so load, stopped, queue, speed trên cùng trục thời gian nhưng riêng trục Y.  
**Data quality:** full topology mỗi slot; empty traffic 0 hợp lệ; scope overlap phải được công bố. Không gọi là throughput/unique network volume.  
**Acceptance:** 2 directions [10,20], [5,15] ở hai slots → mean intersection load (15+35)/2=25 veh; thiếu direction withheld.  
**Gold cost:** reuse V sums; slot alignment state bounded.

## 9. M-08 — Persistence & Change

**Business question:** Điểm nghẽn nhất thời hay kéo dài?  
**Input:** effective published M-05 intersection scores của một run, cùng duration/definition, expected closed window sequence trong range.  
**Formula:** high count `h`; valid score windows `v`; expected eligible windows `e`; `high_share=100h/v` chỉ với v>0; hiển thị `v/e`. Longest high streak = số adjacent valid high windows liên tiếp lớn nhất; missing/invalid breaks streak (không nối qua gap). Current streak tại selected window: đếm ngược đến non-high/gap/range boundary. First high start, peak score, latest delta=`S_W−S_previous` nếu liền kề cùng definition; không leap qua gap. `streak_seconds=count×D` chỉ là tổng độ dài các windows classified High.  
**Grain:** Intersection × run × duration × analysis range; selected streak ghi rõ lower bound nếu bị cắt bởi range start.  
**Visualization:** compact table/bar high count, share, longest streak, peak; ranking column Δ và streak (tooltip).  
**Interaction:** click row focus giao lộ/heatmap; click first-high hoặc peak jump đúng window.  
**Data quality:** v<e → observed summary chỉ mang tính coverage-qualified, hiển thị h/v và v/e; không công bố official full-range share/streak khi thiếu dữ liệu theo strict policy. v=0 → N/A. Missing gap không phải Low.  
**Acceptance:** [High,High,missing,High,Low] → h=3,v=4,e=5, observed share=75%, longest=2 windows, full-range official withheld. Prev=missing →delta N/A.  
**Gold cost:** O(intersections×range windows) trên published marts, không đọc canonical events; pagination/limit cho dài range. Không cần mart riêng ban đầu.

## 10. M-09 — Estimated Stopped Vehicle Time (research optional)

`ΣH×Δt` vehicle-seconds trong cùng sampling scope. Đây là stopped exposure estimate, không phải delay, waiting time s/veh hoặc total unique network stop time. Có thể dùng giải thích experiment khi chưa có completed-trip waiting; chỉ công bố nếu owner duyệt definition/cadence/scope. Reuse stopped sum, không thêm input. Ví dụ H=[3,5,8,7,4] với Δt=1s →27 vehicle-seconds.

## 11. Quy tắc chung về delta, precision và rollup

Delta speed/queue = absolute unit difference, stopped ratio = percentage points, score = score points, high count = intersections. Relative percentage change chỉ khi baseline≠0 và hai kết quả compatible; không dùng dấu tốt/xấu cho load. Round ở presentation; tolerance cho float arithmetic phải duyệt bằng oracle, không dùng display rounding để phân loại. Không cộng hai duration chồng lấn; 300s statistics tính từ membership gốc hoặc equivalent verified sufficient stats khi được thiết kế chứng minh. P95 và max score không phải additive; không rollup bằng mean các quantile/score con. Mọi metric cần provenance, definition version, coverage và effective revision (06).
