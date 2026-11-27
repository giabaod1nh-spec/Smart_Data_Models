# Kết quả preparation tại baseline 0ccf206

Local và remote khớp 0ccf2062c368def10b3601402215698b4a4e84ce trên codex/phase-1-v1-raw. Index và working tree sạch, không có delta sau reviewed baseline. Ngày báo cáo 27/11/2026 (Asia/Bangkok).

| Package | Kết quả |
|---|---|
| A-P01 | Hoàn thành scope candidate và traceability. Owner còn quyết định Master Plan boundary và metric adoption. |
| A-P02 | Đã chạy E-01…03 và sizing trong phạm vi isolated. Evidence không bao phủ producer restart/canonical serialization/DE runtime. |
| A-P03 | Đã chọn contract ứng viên range + sparse evidence. BLOCKED cho closure do upstream chưa expose lifecycle và schedule. |
| A-P04 | Đã kiểm thử moving population, nearest-rank và empty/conflict oracle. Occupancy/queue/pressure/experiment còn gate. |
| A-P05 | Đã lượng hóa core shape của projection và SQLite, xác định reader/transaction semantics. Chưa có full-size payload, ClickHouse hoặc integrated envelope. |

## E-01 — SUMO và TraCI thật, source-only

Chạy SUMO 1.27.1, TraCI protocol 22 từ D:/SUMO/tools, một process headless trên bản sao tạm của Visualize/Visualize/{intersection.sumocfg,intersection.net.xml,intersection.rou.xml,detectors.add.xml,viewsettings.xml}. Git blob tương ứng bốn input đầu: d775cde6fa4d5f55a8549558360da3430372083c, 34808726ebf5889a59c3fb9be45667f28319db3a, 2cf436bb0b3a10e5a577e6816a8c93963b795961, a2e6b75b5d31dcc0fcd5995e179a57b441c8c11b. Giữ seed 42 và step 0.01s. Đầu tiên sandbox từ chối localhost; lần chạy có quyền loopback thành công và process kết thúc. Startup guard 2.5 GB available và 500 MB scratch; kiểm tra lại mỗi 1,000 steps, hard simulation cap 30,200 steps, time check 58 wall-s. Available memory thấp nhất đọc được 26,552,881,152 B trong run, scratch preflight khoảng 6.93 GB free. Wall time 14.02 s. Không đo isolated process peak RSS hay end-to-end throughput.

Recipe: copy input files vào disposable directory; start sumo -c intersection.sumocfg với loopback TraCI, gọi simulationStep 30,200 lần; đọc getTime rồi tái hiện predicate backend `last < 0 or actual-last >= 1.0`, lưu step và actual clock; tính số captures bằng partition half-open độc lập. Gọi SumoSnapshotProvider._build_direction tại captures 1,31,61,151,299 cho mọi approach. Oracle lấy union lane vehicle IDs và dùng math.fsum trên raw speeds ≥0.1 m/s. Poll E2/lane raw occupancy tại capture points. Script tạm tên phase4b-source-feasibility-0ccf206.py, không phải source/test production.

| Finding | Actual vs oracle | Verdict |
|---|---|---|
| SF-07 nominal grid | 302 captures tới 302 sim-s; 0.01, 1.01, 2.02, 3.02 đầu run; 59.04 → 60.04, 299.05 → 300.05. Max drift so nominal 0.01+i là 0.04s. Đếm 60 captures cho từng [0,60)…[240,300), 300 cho [0,300), hai captures trong tail. | VERIFIED in probe: float drift, boundaries; PARTIALLY VERIFIED for canonical scheduling vì probe chỉ tái hiện predicate. Producer pause/resume/reset, failed capture, canonical linkage và authoritative cutoff chưa chạy. |
| SF-01 raw getters | 46 E2 detectors. Inbound E2 0.890378 và outbound E2 0.610941 là ví dụ raw % nhỏ. Lane raw 0.020475 xuất hiện ở outbound edge -694973742#5_0; installed Python getter mô tả %. Dưới documented units, branch inbound nhân 100 và outbound bỏ chia 100 làm sai scale. | PARTIALLY VERIFIED: live getter + E-02 branch fixture. Không assert canonical/Silver runtime payload hoặc calibrated pressure. |
| SF-02/Q actual method | Cả 17 approach có lane được app config phân loại mixed; ở 85 provider calls, E2 movement mapping rỗng 85/85. Điều này giải thích fallback dù detector tồn tại. | VERIFIED cho configured path trên sample. Chưa chứng minh estimate tương đương E2 hoặc giao thông khác. |
| Moving population | 85/85 triples V,H,average km/h khớp lane-ID union oracle, 73 empty, 7 có moving vehicles. | VERIFIED trong sample. Missing speeds, overlapping detectors/approaches và canonical quality chưa được chứng minh runtime. |

## E-02 — Detector và arithmetic fixtures

Gọi trực tiếp DetectorManager trên process Python riêng, fake getters với 0, 0.5, 0.8, 1, 1.1, 50, 100 percent. Compare independent oracle inbound p và outbound p/100. Actual inbound E2 [0,50,80,100,1.1,50,100], outbound E2 [0,.5,.8,1,.011,.5,1]. Fault getter trả None và occupancy_detector_incomplete. Synthetic forced straight (one lane, only process-local monkey patch) cho E2 jam 4.5 m, measured zero 0, failed getter 0 với queue_detector_incomplete=true; mixed/missing detector → empty mapping. Source review: snapshot fallback when no nonzero E2, with separate stopped-length estimate. Lane fallback normalization được đối chiếu từ code và arithmetic fixture, chưa đưa cả bảy giá trị qua live provider/canonical/Silver.

Independent slot/time fixture: duplicate zero giữ zero, hai event IDs slot 1 conflict, slot 2 thiếu, slot 3 valid. Expected và actual [0,null,null,3] ở cả hai input orders. [0.01,59.99,60,60.04,299.99,300] map 60s starts [0,0,60,60,240,300] và 300s [0,0,0,0,0,300]. VERIFIED fixture scope. Hai probe Python tạm chạy tuần tự, no server/database shared. Exact queue ground-truth trajectory, replay and crash injection chưa chạy.

## Evidence inventory và giới hạn

Đường source đối chiếu: Visualize/simulation/backend.py, observation/{detector_manager,snapshot_provider,canonical_cycle}.py, runtime/run_manifest.py, contracts/traffic_observation.py, phase-2 fixture, de/kafka_raw V1, de/bronze semantic handling, de/silver/{fact_builders,incremental_contract,incremental_reader}.py, de/gold_runtime/{main,processor,processing_ledger,checkpoint_store,replay,repositories}.py. Core hiện có lifecycle UNKNOWN và mỗi indexed member requalification qua public reader. Legacy repository không phải V1 publication authority. Governance: Master Plan, Stage A RV/SR/R matrices, Phase 4A Pre contract, Core acceptance, Business Spec metric/acceptance, Performance Plan và checkpoint-02. Audit chọn các path và sections liên quan; không tuyên bố live integration coverage hay exhaustively verify retained volume. Benchmark artifact không được thay đổi. ClickHouse component, authoritative lifecycle and full-fidelity integrated acceptance NOT EXECUTED. [Sizing](resource-sizing.md).
