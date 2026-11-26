# Expected Observation và coverage (RV-04/05)

## Evidence và lựa chọn

`Visualize/simulation/backend.py:417,449–536` đọc clock sau step, lần đầu quan sát ngay sau step vì last < 0, sau đó dùng hiệu float với interval mặc định 1s. Sequence tăng khi attempt bắt đầu, last time cập nhật kể cả khi generate trả lỗi. Pause không tạo simulation steps. Restart/terminal cần bằng chứng riêng về generation và clock tiến tới đâu. Snapshot realtime làm tròn hai chữ số. `canonical_cycle.capture_cycle` ghi đè timestamp bằng clock truyền vào và V1 yêu cầu đủ participant khi tạo cycle. Fixture `phase-2/fixtures/current-sumo-cycle.json` có 60.04s. Một fixture hoặc cadence setting không chứng minh lịch kỳ vọng.

`traffic_observation.build_observation` ghi simulationStep, cycleSequence và simulationTimeSeconds vào envelope. Raw giữ envelope, Bronze giữ observation và lineage. `de/silver/fact_builders.py:build_v1_fact_rows` ánh xạ cycle và time vào facts, không ánh xạ step. Public changes trả bundle facts đã kiểm chứng và run identity, invalidation có cycle/time. Gold không giả định step, raw envelope, expected schedule hoặc terminal status có trong bundle.

| Phương án | Evidence, thay đổi và recovery |
|---|---|
| A. Tái dựng scheduler hiện có | Ưu tiên nếu authoritative step/clock/config và deterministic execution tái dựng được cùng kết quả qua failure/restart. Ít thay đổi, nhưng lần đầu, float drift và terminal chưa được chứng minh. Historical chỉ đủ điều kiện nếu còn đủ provenance. |
| B. Lịch deterministic tại producer | Khuyến nghị nếu A/C không chứng minh được. Producer ghi lịch trước capture, định danh slot riêng, giữ actual clock, cần extension tối thiểu và versioning. Chi phí metadata theo slot/range, recovery từ durable schedule. |
| C. Durable attempt/outcome ledger | Ghi attempted/failed/committed, idempotent theo run/slot. Hữu ích bổ trợ B. Chỉ đủ độc lập nếu có planned schedule hoặc counter evidence phát hiện cả attempt chưa chạy. Đo write overhead, crash giữa capture và Outbox. |
| D. Đếm records đã nhận | Loại vì không phát hiện được slot hay cả window vắng mặt. |

## Contract ứng viên B

Schedule bất biến theo (run ID, generation, topology digest, manifest identity, schedule version). Digest bao gồm participant/approach set, time unit (đề xuất integer microseconds để biểu diễn lịch), origin, period, first slot ordinal, step rule, capture tolerance, producer version. Slot ordinal k có due time = origin + k × period và key (run context, intersection, schedule ID, k). Lưu actual simulation timestamp nguyên giá trị canonical, step/cycle nếu được expose, planned due time và outcome. Timestamp tolerance cần owner chọn từ capture. Slot và actual time phải thuộc cùng Core window ở cả hai durations, nếu lệch boundary hoặc ngoài tolerance thì báo mismatch, giữ nguyên Core membership và không cấp coverage credit. Cycle/step phải khớp schedule mapping đã phê duyệt.

Lifecycle evidence có start, observed-through simulation time, terminal reason/cutoff và last eligible slot. Pause giữ nguyên tiến độ. Resume cùng generation chỉ khi chứng minh continuity. Restart thiếu continuity cần generation mới hoặc đánh dấu unsupported. Failed/missed capture giữ nguyên denominator. Chỉ lấy lịch và linkage qua contract public đã verify hoặc qua extension Silver có owner approval. Legacy thiếu evidence trả UNSUPPORTED_SCHEDULE. Không suy đoán metadata từ journal riêng.

## Accounting và timeline

Tính distinct expected keys độc lập, classify từng key VALID, MISSING, UNAVAILABLE, INVALID/CONFLICTED hoặc contract-defined NOT_APPLICABLE. Received là số slot có claim. Structural coverage xét danh tính và participant. Metric input coverage xét từng field/joint input. Numerical applicability xét denominator sau khi đủ coverage. Hai event ID cùng claim slot làm slot conflicted, cả hai bị loại khỏi metric state và yêu cầu xử lý có lineage. Transport retry cùng event là idempotent. Late valid claim cập nhật current generation/revision, không cộng thêm slot. Không hạ denominator vì thiếu dữ liệu. Full coverage với V=H có thể cho speed N/A.

Serving dựng bounded aligned timeline từ schedule và lifecycle rồi đối chiếu Core work và publication. Interval đã eligible mà không có observation là MISSING_WINDOW ngay cả khi không có core_window. Window có dữ liệu đo đầy đủ và không xe là measured empty. Trước cutoff là NOT_YET_ELIGIBLE. Work chưa xong là PROCESSING, correction đang chạy là REPROCESSING. Terminal partial là TERMINAL_PARTIAL, không extrapolate. Thiếu lifecycle là UNKNOWN. Không tạo rows zero để lấp gap. Chỉ PUBLISHED khi manifest hiện hành và metric đạt strict applicable coverage. Các số 300/1020 và 1500/5100 chỉ áp dụng topology 5/17 và 1Hz đã chứng minh.

Gate G-COV cần SF-07 có start, 59.99/60.04/299.99/300, missing entire window, duplicate claim, pause/resume, failure và terminal, đối chiếu độc lập với producer clock/steps và archive.
