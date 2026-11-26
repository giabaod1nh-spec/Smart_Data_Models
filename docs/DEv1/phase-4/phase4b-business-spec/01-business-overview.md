# 01 — Business Overview & Design Rationale

**Dự án:** Smart Traffic Streaming Data & Analytics Platform  
**Giai đoạn:** Phase 4B — Business Specification  
**Phiên bản tài liệu:** 0.1 — Review candidate, 26/11/2026  
**Đối tượng:** Product owner, SUMO/AI developer, Data Engineer, Backend, Frontend, QA và người đọc báo cáo.  
**Baseline:** `codex/phase-1-v1-raw` tại commit `25d943fa3825523e14a596aab601bb4e38983853`.

## 1. Mục đích và cách sử dụng

Bộ đặc tả xác định hệ thống cần cung cấp thông tin gì, vì sao thông tin có giá trị, cách tính và cách kiểm chứng trước khi thiết kế Gold marts. Đây là đầu vào của requirements review, source feasibility, technical design và implementation plan; không phải implementation plan hoặc quyết định schema vật lý. Không được coi tên trường Gold đề xuất là cột đã tồn tại, hay coi các yêu cầu trong tài liệu là test đã PASS.

Quy ước trạng thái: **Agreed** = lựa chọn nghiệp vụ đã thống nhất trong trao đổi; **Proposed** = thiết kế/công thức chi tiết cần owner duyệt; **Research** = chưa đủ chứng cứ nguồn hoặc cần thử nghiệm; **Blocked** = không được công bố kết quả chính thức trước khi điều kiện tương ứng được giải quyết. Các yêu cầu có mã ổn định để trace sang thiết kế và test. P0/P1/P2 là độ ưu tiên nghiệp vụ, không phải thứ tự tự động thực thi.

## 2. Mục tiêu nghiệp vụ

Hệ thống phân tích giao thông trên mạng SUMO trích xuất từ OSM, hiện có các giao lộ A, B, C, D, M. Từ dữ liệu lịch sử theo simulation time, người dùng cần xác định mức độ áp lực giao thông, điểm nghẽn, diễn biến và các yếu tố liên quan; từ các thí nghiệm có kiểm soát, đánh giá Fixed-Time và Adaptive. Hệ thống không tự chứng minh nguyên nhân ùn tắc chỉ từ sự đồng biến của các chỉ số.

| Mục tiêu | Câu hỏi quyết định | Sản phẩm thông tin |
|---|---|---|
| Assess | Phạm vi và mức độ vấn đề trong một window? | 4 KPI toàn vùng quan sát |
| Locate | Giao lộ, hướng nào cần kiểm tra trước? | Priority & Diagnosis |
| Analyze | Vấn đề xuất hiện khi nào, kéo dài hay lan rộng? | Pressure Heatmap, Persistence Summary |
| Diagnose | Tải, tốc độ, hàng đợi, tín hiệu thay đổi ra sao? | Performance Trends, Intersection Detail |
| Evaluate | Phương án điều khiển hoặc kịch bản nào cho kết quả khác biệt? | Experiments với run comparison |

Căn cứ lựa chọn: FHWA Traffic Signal Management Plans, chương 2, gắn performance measures với mục tiêu vận hành [R1]; FHWA ASCT Validation Guidance, chương 5, phân biệt mobility, throughput, access equity, reliability và nguồn đo tương ứng [R2]. P95 queue được chọn làm đại lượng mô tả hàng đợi cao theo mẫu; không đồng nhất với P95 queue theo chu kỳ trong tài liệu chuyên ngành [R3]. Các tài liệu này không định nghĩa Traffic Pressure Score của dự án.

## 3. Ranh giới trách nhiệm các màn hình

| Màn hình | Phạm vi | Trách nhiệm | Không thực hiện |
|---|---|---|---|
| Realtime Monitoring & Control | Active run, trạng thái hiện tại | Theo dõi, điều khiển, xác nhận lệnh | Không dùng dữ liệu lịch sử Gold để giả lập trạng thái hiện tại |
| Analytics Overview | Một run được chọn; 60/300s | Tổng quan, xếp hạng, xu hướng, persistence | Không gửi lệnh, không so sánh hai thuật toán trực tiếp |
| Intersection Analytics Detail | Một giao lộ trong cùng run/window/range | Chẩn đoán hướng, queue, storage, tín hiệu, sự cố | Không khẳng định nguyên nhân nếu thiếu evidence |
| Experiments | Hai hoặc nhiều runs đủ điều kiện | So sánh scenario profile và chiến lược điều khiển | Không tạo kết quả counterfactual giả, không cần active run |

Tách Experiments vì đơn vị phân tích là nhiều run với metadata thực nghiệm, khác với selected window của một run. Hai màn hình lịch sử sử dụng lại Gold products; không yêu cầu chạy hai SUMO instances đồng thời. Realtime không nằm trong phạm vi redesign của bộ này.

## 4. Quyết định thiết kế Analytics Overview

| ID | Phương án chọn | Phương án loại và lý do | Trạng thái |
|---|---|---|---|
| BR-01 | 4 KPI: High-Pressure Intersections, Average Moving Speed, Stopped Vehicle Ratio, Worst Queue (P95) | Total Intersections là metadata; average score che khuất điểm nghẽn; queue alert với ngưỡng mét tùy ý khó so sánh | Agreed P0 |
| BR-02 | Ranking table + horizontal bar | Map chỉ lặp score, không đủ thông tin chẩn đoán với 5 giao lộ | Agreed P0 |
| BR-03 | Pressure Heatmap | Pie chart hoặc ranking thứ hai không thể hiện thời gian | Agreed P0 |
| BR-04 | Trends có metric selector và focus | Nhiều chart cố định gây lặp nội dung; trục Y chung cho nhiều đơn vị dễ hiểu sai | Agreed P0; focus detail Proposed |
| BR-05 | Bottleneck Persistence Summary nhỏ | Findings văn bản lặp lại heatmap; ưu tiên số liệu tổng hợp có thể kiểm chứng | Proposed P0 |
| BR-06 | Window navigator + Follow Latest; historical range presets/custom | Dropdown hàng nghìn windows, range cố định 0–900/900–1800 không mở rộng | Agreed direction; rules Proposed |
| BR-07 | Scenario là provenance từng giao lộ, không global filter mặc định | Lọc Incident sẽ loại các giao lộ Normal bị ảnh hưởng gián tiếp | Agreed P0 |
| BR-08 | Control mode metadata toàn mạng; không toggle điều khiển trên Analytics | Source hiện có `self.control_mode` toàn runtime, chưa hỗ trợ mix theo giao lộ | Source-confirmed; P0 |
| BR-09 | Separate Experiments page | Cùng run không thể quan sát đồng thời hai chiến lược thay thế | Agreed P0 |
| BR-10 | Drill-down Intersection Detail giữ context | Chuyển thẳng Realtime làm mất historical context | Agreed direction; scope P0/P1 |
| BR-11 | 100% applicable coverage theo metric/grain | Partial KPI dễ gây kết luận sai; zero không thay missing | Agreed principle; coverage contract Proposed |
| BR-12 | Event context để phân tích thay đổi cục bộ | Không tạo chart lan truyền riêng nếu heatmap/trends đáp ứng; không suy luận nhân quả tự động | Proposed P1 |
| BR-13 | Nghiên cứu throughput, waiting, storage | Không dùng vehicle snapshots để giả throughput hoặc waiting seconds | Research; chi tiết tài liệu 04–05 |

### 4.1. Bố cục và phạm vi

```text
Simulation Run | Window Duration 60s / 300s | Publication / Coverage
Selected Window Navigator | Follow Latest / Historical
[High Pressure] [Moving Speed] [Stopped Ratio] [Worst Queue P95]
Intersection Priority & Diagnosis (full width)
Historical Analysis Range: Last 5m / 15m / 30m / Full run / Custom
[Traffic Pressure Heatmap] [Traffic Performance Trends]
Bottleneck Persistence Summary (compact)
Expandable Data Quality & Publication Information
```

Không tăng số chart chỉ để đủ số lượng. Dữ liệu lặp lại có chủ đích nếu khác grain/time scope: speed toàn vùng ở card, speed worst-pressure approach ở ranking, speed intersection qua nhiều windows ở trend. Không hiển thị ba bản sao cùng grain, cùng window, cùng mục đích. Mỗi chart phải có unit, run, duration, time scope, trạng thái chất lượng, metric definition và liên kết đến evidence.

## 5. Source boundary và topology

Baseline topology có 17 hướng: A (East, South, West); B (North, South, East); C (North, East, South, West); D (North, East, West); M (North, East, South, West). Đây là fixture/topology hiện hành, không phải hằng số của nghiệp vụ. Population và ID phải từ archived run topology. Định nghĩa Approach = hướng/nhóm sensing lanes trong canonical observation; không tự coi là một làn, một movement hoặc toàn tuyến đường.

Source thực tế: `scenario_id` theo observation từng giao lộ; `control_mode` theo runtime. Mode/tình trạng từng đèn và timing có thể khác nhau; không suy ra mode điều khiển độc lập từ `timing_mode` nếu không có contract. Window có scenario thay đổi theo thời gian tại một giao lộ được gắn Mixed Scenario; mạng có nhiều scenario đồng thời là heterogeneous scenario profile. Hai khái niệm này không giống nhau. Giữ window identity scenario-neutral, không tách membership theo scenario.

## 6. Điều kiện phát hành và quản trị scope

- Phase 4A Core cung cấp canonical receipt, durable ACK, membership, invalidation, recompute và proof. Không cung cấp business KPI; completeness UNKNOWN, finality DISABLED tại baseline.
- Business specification có thể được review/lập plan trước khi performance hoàn tất. Repository vẫn yêu cầu minimum gate tại `acceptance-result.md`; conditional development chỉ có hiệu lực khi owner sửa/phê duyệt gate rõ ràng. Không tự chuyển PA thành PASS.
- P0: core traffic analytics, strict publication, direction drill-down cơ bản, comparison Fixed-Time/Adaptive là yêu cầu đề tài. P1: sampled signal, storage sau feasibility, event context, scenario comparison nếu cohort hợp lệ; P1 Research: completion/throughput/waiting. P2: per-approach traversal tracking nếu chi phí và scope cho phép. Không đánh đồng research với chức năng cam kết.
- Không dùng rolling/sliding/provisional aggregates. Không mở rộng contract, thay đổi controller, schema hoặc runtime trước khi design và plan được duyệt. Không tự stage/commit/push.

## 7. Tài liệu nguồn

Tham chiếu [S1–S12, R1–R5] trong README. Source review ở baseline là static; không phải xác nhận toàn bộ runtime hoặc historical coverage. Notion là bản ghi nghiệp vụ trước đó, không phải contract kỹ thuật đã được phê duyệt. Phần ảnh công thức nhúng trong Notion chưa được kiểm chứng trực tiếp; các công thức trong bộ này được viết lại từ định nghĩa đã thảo luận và source đã đọc.
