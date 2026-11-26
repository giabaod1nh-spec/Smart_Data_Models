# 03 — Analytics Overview: Component & Interaction Specification

Status: Review candidate. Đây là giao diện đọc lịch sử của một simulation run. Không gọi Control API và không sử dụng current-run Orion làm điều kiện xem lịch sử. Thiết kế không phụ thuộc Lovable mock data.

## 1. O-01 — Header, selection và navigation

**Business question:** Đang xem run nào, mức thời gian nào, dữ liệu đã được publish đến đâu?

**Input/grain:** run catalog từ archived metadata và Gold publication metadata; selected run identity (run ID + generation/topology/manifest nếu cần disambiguation), window duration, latest effective published interval, latest known source simulation time nếu có. Scenario/control tại selected window là context, không phải input gửi lệnh.

**Visualization:** Global filters chỉ có Run và Window Duration (60s mặc định, 300s tùy chọn). Hiển thị participant count, source time và latest published window end riêng, quality và service status. Không sử dụng một scenario duy nhất làm filter toàn mạng; không có Fixed-Time/Adaptive toggle. Run không có historical evidence hiển thị Empty/Processing, không tự chuyển sang run khác.

**Window Navigator (trước KPI/Ranking):** [previous] [start,end) [next], jump tới simulation time, first/last, Jump to Latest. Không load dropdown toàn bộ window IDs. Mặc định Follow Latest; người dùng chọn lịch sử, click heatmap, jump hoặc previous thì pin Historical. Next ở historical không tự bật follow; Jump to Latest bật follow. Navigator dựa trên aligned closed-window timeline, cho phép xem gap và trạng thái processing thay vì bỏ qua âm thầm.

**Analysis Range (trước Heatmap/Trends):** Last 5m, Last 15m (default đề xuất), Last 30m, Full Run, Custom start/end; các phút là simulation minutes. Phạm vi mặc định bám latest published end L. Last X phút = [max(run aligned start,L−X),L); Full Run = toàn bộ closed aligned history đến L, gồm cả gaps. Custom dùng boundaries hợp lệ, hiển thị adjustment rõ ràng nếu snap; không cắt partial windows rồi aggregate. Relative range dịch theo L khi Follow Latest; khi Historical, giữ anchor/range cố định, cho phép pan/jump hoặc mở rộng range. Nếu selected window nằm ngoài range, hiển thị thông báo/marker outside range; không âm thầm đổi selection/range. Range giới hạn/phân trang theo serving budget; full run có thể cần zoom/page, không silently sample mất peaks/gaps.

**Formula:** Mapping time `t` vào W = approved Core alignment; ví dụ epoch 0, `[floor(t/D)×D, (floor(t/D)+1)×D)`. Khi đổi duration ở Historical, dùng start của W cũ để chọn window chứa nó; nếu window mới chưa publish, giữ đúng W và trạng thái, không tự chọn mốc khác. Follow Latest chọn latest của duration mới. Khi đổi run reset selection/range theo policy mặc định, giữ duration nếu được hỗ trợ, không tái dùng results của run cũ.

**Data quality:** Latest published = latest effective publication metadata tại duration/scope, không phải latest closed time hoặc Core ACK. Policy đề xuất: latest window có publication manifest hiện hành, từng metric có trạng thái riêng; không ghép KPI nhiều windows. Reprocessing/suppression phải cập nhật trạng thái; historical pin không tự nhảy. Follow Latest không âm thầm lùi sang window cũ nếu latest bị invalidation: hiển thị reprocessing hoặc ghi rõ fallback window khác nếu được owner duyệt. Service offline khác historical run completed; cache nếu có phải ghi rõ as-of/stale, không coi là current-effective sau invalidation không kiểm tra được.

**Acceptance:** source=680s, latest closed=660, latest published=600 → default [540,600), không hiện [600,660) KPI chưa publish. Publish [600,660) → follow update, pinned [180,240) không đổi. Click heatmap 60s tại [1320,1380), đổi 300s → [1200,1500) hoặc Processing. Với latest 1800→2100, Last15m 900–1800→1200–2100 ở follow; custom 300–900 giữ nguyên. 24h 60s có 1440 windows, không render dropdown 1440 options hoặc load all raw events.

## 2. O-02 — Network KPI Summary

| Card | Business question | Metric, formula | Grain & input | Visualization / action |
|---|---|---|---|---|
| High-Pressure Intersections | Phạm vi ảnh hưởng? | M-06: count score≥T | Network×W, M-05 all participants | 2/5; IDs; delta count; highlight high rows |
| Average Moving Speed | Xe đang chạy di chuyển tốt không? | M-01: ΣvM/ΣM | Network×W, V/H/v | km/h; delta km/h; tooltip moving-only |
| Stopped Vehicle Ratio | Mức độ dừng xe? | M-02: 100ΣH/ΣV | Network×W, V/H | %; delta pp; focus stopped trend |
| Worst Queue (P95) | Hướng có queue cao nhất theo P95? | M-03: max approach percentile | Network×W, q | m + intersection/approach; click đúng worst-queue detail |

**Rationale:** Bốn chỉ số lần lượt phản ánh extent, mobility, stopping, bottleneck. Speed moving-only không phản ánh xe đứng yên; ratio bổ sung. P95 giảm ảnh hưởng spike so với max, không thay max khi kiểm tra spillback. High count phản ánh distribution qua giao lộ, không thay bằng average pressure. Không thêm total intersections, load hoặc average score làm card mới.

**Data quality:** Metric-specific 100%, full expected network population, common effective W. Missing metric chỉ suppress metric phụ thuộc; không lấy window cũ để bù. Empty ≠ missing. High count chờ calibration. Delta chỉ với window ngay trước và definition/context compatible; nếu scenario/mode thay đổi ghi context change và không gọi delta là improvement do thuật toán.

**Acceptance:** bộ dữ liệu AC-01–09 tài liệu 07; assert scope thống nhất, delta, tooltip, no accidental zero, click action không đổi run. No mixed revisions across panel projections. Chi phí: network result reuse lower-grain sufficient statistics, không scan raw/Silver riêng mỗi card.

## 3. O-03 — Intersection Priority & Diagnosis

**Business question:** Giao lộ nào cần kiểm tra trước, hướng nào giải thích score và tình trạng đó ra sao?

**Input:** intersection M-05 score/argmax, direction M-01/02/03/04, M-08 previous/streak, archived topology, scenario context, incident/spillback evidence. Control mode chung ở header; nếu cần row-level context chỉ hiển thị sampled mode thực tế (không tạo mix control độc lập giả).

**Formula/grain:** Một row mỗi intersection trong selected W, sort descending unrounded score, tie by canonical intersection ID; direction tie by stable archived approach key. Unknown rows cuối, không numeric rank như đầy đủ; rank known subset phải ghi rõ nếu population thiếu. Không sort “urgency” theo arbitrary combined score. Các cột diagnostic luôn là metrics của worst-pressure approach; worst-queue argmax riêng.

| Field | Definition / display |
|---|---|
| Rank / Intersection | Stable rank, archived ID/name, số approaches |
| Scenario | Single scenario nếu mọi expected samples cùng label; Mixed nếu thay đổi, tooltip sampled distribution; Unknown nếu context thiếu |
| Pressure Score / Level | M-05 unrounded classification; bar 0–100, 1 decimal; không tô xanh Unknown |
| Worst Approach | argmax pressure; tooltip direction identity |
| Moving Speed | M-01 tại hướng đó (km/h) |
| Stopped Ratio | M-02 tại hướng đó (%) |
| P95 Queue | M-03 direction (m) |
| Occupancy | M-04, tooltip/expanded row nếu bảng rộng |
| Δ Pressure / Streak | previous adjacent delta, current high streak tại W, range lower bound |
| Incident / Spillback | Observed nếu có true evidence; no evidence/unknown phân biệt absent với false |
| Coverage / View Analysis | metric availability indicators; link historical detail |

**Visualization:** full-width table + horizontal bar, header group “Worst-pressure approach metrics”, sortable columns. Default rank remains pressure; alternate sort không đổi pressure rank. Không map hoặc bảng rank thứ hai. 0.1 điểm khác biệt không chứng minh khác biệt vận hành có ý nghĩa; không gắn critical urgency chỉ từ rank.

**Interaction:** click row focus intersection ở trends; View Analysis giữ run, duration, W, range, revision context; quay lại giữ filter/scroll. Optional filter High từ card chỉ đổi visible rows, không đổi KPI population. Drill-down không chuyển current realtime. 

**Data quality:** nếu pressure thiếu, vẫn có thể hiển thị các metric vật lý hợp lệ với scope rõ (intersection/detail), không chọn worst direction tùy ý; unknown rank/argmax. Nếu speed tại worst direction thiếu nhưng score đủ thì giữ row/score, speed N/A. Incident observed trong valid sample là positive evidence; absence của optional field không kết luận false.

**Acceptance:** scores M=73.7,A=73.6,B=51.6,C=28.1,D=24.2 → order M,A,B,C,D; all five rows; A East hợp lệ, A North không tồn tại trong baseline; worst pressure East nhưng worst queue West → không trộn; tie stable; missing direction suppress intersection score; selected scenario local Incident không lọc mất Normal neighbors. Chi phí O(I×A) mart projection/sort, không thêm aggregation scan.

## 4. O-04 — Traffic Pressure Heatmap

**Business question:** Giao lộ nào chịu áp lực cao khi nào, có kéo dài hoặc xuất hiện sau sự cố ở giao lộ khác không?

**Input/formula:** M-05 intersection score per W, effective revision/quality, context and event marker evidence. Cell=(run,intersection,duration,start,end,definition); dùng chính score ranking, không tính lại score riêng. X=ordered expected closed windows trong range, Y=stable archived intersection ordering, color=calibrated classes. Include gaps. No map/geographic inference.

**Visualization:** categorical heatmap Low/Moderate/High/Unknown; score text khi đủ chỗ, tooltip decimals, exact W, components tại worst direction, coverage, scenario, control, revision. Missing/reprocessing/open tail có pattern/status riêng, không green. Với range dài pan/zoom/page, label sparse nhưng không bỏ dữ liệu mà không thông báo. Marker scenario/incident/control changes đặt trên simulation time, ghi observed_at nếu suy từ sample; source exact command effective time chỉ khi durable evidence tồn tại.

**Interaction:** click cell → pin historical W, highlight selected intersection, đồng bộ KPI/Ranking và trend time cursor; không đổi range. Click marker → xem event context, trước/sau aligned windows; không gửi lệnh.

**Data quality:** only effective valid score has pressure color. Invalidation suppress cell và cập nhật persistence, không nối gap. Calibration/definition changes phải phân tách hoặc cảnh báo incompatibility. Cùng run không đồng nghĩa cùng scenario trên mọi giao lộ.

**Acceptance:** 5×15 grid cho 900s ở 60s; missing window đúng vị trí, click [600,660) cập nhật O-02/O-03; score 69.95 Moderate theo threshold candidate; marker source sample change không giả exact action time; một local event không chứng minh network causal propagation. Chi phí O(I×K) rows từ mart, không re-read verified events.

## 5. O-05 — Traffic Performance Trends

**Business question:** Đại lượng vật lý nào thay đổi theo thời gian, ở đâu, trước/sau sự kiện nào?

**Input/formula/grain:** intersection × published W × selected range: M-01 Moving Speed, M-02 Stopped Ratio, M-03 Intersection Worst Queue P95, M-07 Observed Vehicle Load; M-05 Pressure Score bổ sung selector P1 để phân tích network context. Không lấy metrics của worst-pressure approach làm intersection trend (khác scope). Mỗi điểm tại window end có tooltip interval. Không smoothing/rolling.

**Visualization mode A — Compare Intersections:** một line chart, một metric và một Y unit, toggle A/B/C/D/M từ topology, stable identity colors; default speed. Mode B — Focus Intersection (Proposed): small multiples synchronized, riêng Y axes cho load/queue/stopped/speed; chỉ khi focus, không tạo bốn charts cố định trên Overview. Pressure/occupancy có thể xem thêm theo selector/detail. Event marker dùng cùng convention O-04.

**Interaction:** hover/click time chọn W, crosshair đồng bộ mode focus; chọn row/heatmap focus intersection; clear focus về compare mode. Range shared với heatmap/persistence. Khi 60→300, không lấy 5 điểm 60s average để tạo 300s; query đúng aggregates. 

**Data quality:** gaps khi missing, không interpolation/carry-forward; measured zero hiển thị đúng, N/A speed no moving vehicles khác missing. Không nối run boundary, không trộn versions. Khi query range dài cần giới hạn/page/zoom, nếu downsample được duyệt phải ghi rõ method và preserve gaps/peaks, không đổi KPI calculation.

**Acceptance:** 2 groups unequal weights không avg means; queue trend 44m của worst queue approach dù ranking pressure khác; thiếu window ở giữa tạo gap; chọn metric đổi unit/tooltip và đúng data; click time đồng bộ W; mode focus giữ x-range. Chi phí query O(I×K) mart points per metric, no canonical reread; bounded response size.

## 6. O-06 — Bottleneck Persistence Summary

**Business question:** Vấn đề có lặp lại hay kéo dài đủ để ưu tiên điều tra? 

**Input/formula/grain:** M-08 per intersection × range, same run/duration/definition; expected/valid/high count, high share, longest/current streak, first high start, peak score, latest adjacent delta. Không tạo “persistent/emerging/recovered” labels chính thức trước khi threshold/rules được duyệt. Ưu tiên hiển thị số liệu, không tạo hệ thống alert riêng.

**Visualization:** compact table hoặc horizontal bar high share với denominator h/v và coverage v/e; longest streak count và seconds, peak window link. Khi range incomplete, ghi observed summary + coverage; official full-range classification withheld. Không dành panel lớn lặp nguyên heatmap hoặc sinh text suy diễn.

**Interaction:** click row highlight heatmap/trends, click first/peak jump W. Nếu range pinned không tự mở rộng. Streak trong ranking là selected-window streak, summary longest trong range khác nhau; tooltip phân biệt.

**Acceptance:** [High,High,missing,High,Low] → h=3,v=4,e=5, longest 2, official range withheld; không nối qua gap. 15/15 high windows×60s → 900s of classified windows, không khẳng định every second congested. Nếu no valid score windows → N/A. Chi phí tính O(K) trên mỗi intersection mart sequence, không thêm physical mart nếu query budget đáp ứng.

## 7. O-07 — Compact Quality & Publication

Hiển thị selected W, metric coverage, latest published W, current-effective publication revision, definition version và service status. Expand details cho expected/received/valid/applicable counts theo metric/grain, missing intervals/reasons, manifest/run scope. Không dùng run average coverage thay selected-window coverage. Optional metric research không làm whole dashboard blocked. UI labels và serving state mapping theo tài liệu 06, không sửa Core enum tùy ý. AC: published queue với speed withheld; stale service vẫn phân biệt historical immutable data; invalidate và reprocess update all dependent panels; no active run vẫn xem historical run; service Healthy không đảm bảo coverage. Chi phí projection metadata, không scan raw for UI.
