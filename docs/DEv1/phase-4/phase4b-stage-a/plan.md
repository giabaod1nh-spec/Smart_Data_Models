# Roadmap Stage A sau technical review

## Preparation & Design Closure

| ID / mapping cũ | Scope và đường dẫn tác động dự kiến | Phụ thuộc, rủi ro và rollback | Evidence, AC và exit |
|---|---|---|---|
| A-P01 (A01) | Reconcile B4B-01…15, Stage A/B/C, D-A07 và governance trong bộ Markdown Stage A | Owner chọn scope. Amendment Master Plan/PA theo change riêng. Revert riêng proposed wording nếu chưa chốt | Decisions có owner/action và AC coverage |
| A-P02 (A02) | SF-01…09 scope riêng, kiểm tra Visualize/observation/detector_manager.py, snapshot_provider.py, canonical_cycle.py, Visualize/simulation/backend.py, phase-2/fixtures/current-sumo-cycle.json, AI evaluation source | Authorization cho từng fixture. Không dùng retained volumes làm mutable input. Optional study chạy song song. Dừng fixture theo budget | Raw read vs canonical/Gold-facing evidence và independent expected values. AC-04/11, SA-05…08/19 |
| A-P03 (A03) | Closure Q-01/02: lịch, slot claims, lifecycle, compatibility. Proposed impact Visualize/runtime/run_manifest.py, contracts/traffic_observation.py, de/silver/incremental_contract.py nếu extension cần thiết | A-P02 schedule proof. Upstream correction/extension là change riêng, có source owner approval, tests và rollback version-aware, preserve old readers | Signed G-COV, authoritative population usable, AC-10…14, SA-02…04 |
| A-P04 (A04/A05) | Registry, precision, exact quantile, fixtures và pressure calibration protocol ở metric-contract.md. Candidate fixtures de/tests/gold_runtime/ sau authorization | A-P01, evidence A-P02 và population A-P03. Pressure holdout riêng sau source correctness. Revoke candidate version nếu calibration không đạt | G-METRIC per metric, G-CAL riêng, AC-01…09 |
| A-P05 (A06/A07) | Design closure cho Core compatibility, SQLite obligation, ClickHouse marts, read protocol, rebuild, experiment metadata và performance. Inspect de/gold_runtime/processor.py, processing_ledger.py, checkpoint_store.py, de/migrations/005_create_gold_m1.sql | A-P01 và A-P02…04 theo áp dụng. D-A06 đã xác minh. Review bounded transaction/payload, interface serving và DDL. Nếu sizing fail quay lại alternatives | Signed designs G-MART/G-REV/G-EXP theo scope, failure matrix, SA-01/09…18 |

## G-ENTRY

A-P01…05 là preparation có thể cấp quyền riêng, tuỳ scope read-only hoặc isolated fixture. Production Gold business code chờ applicable approvals và G-ENTRY authority. Nếu owner duyệt amendment conditional entry thì điều kiện của amendment phải được ghi nhận trước task đầu tiên. G-PERF kiểm chứng performance/operational sau khi có workload phù hợp. Xem D-A14.

## Business Implementation

Các file hiện hữu bên dưới là điểm tích hợp. Tên file mới và migration version chốt trong A-P05. Chỉ tác động source sau explicit authorization.

| ID / mapping cũ | Scope và file impact | Dependencies, risk và rollback | Tests, evidence, exit |
|---|---|---|---|
| A-I01 (A08) | Pure accumulators trong candidate de/gold/business, input qua canonical contracts | A-P03/04/05 theo metric + G-ENTRY. No persistence; disable module nếu oracle sai | AC-01…14, SA-02…08/11/12, raw fixture + independent output hashes |
| A-I02 (A09 phần Core) | de/gold_runtime/processor.py, processing_ledger.py, checkpoint_store.py. Bounded consumption + durable obligation cùng finish CAS | A-I01. Preserve Core proof hash, scheduler, cursors, namespace. Disable business integration, preserve durable obligations for recovery | AC-15/18, SA-01/09/11/12, crash barriers và Core regressions |
| A-I03 (A09 phần writes) | Candidate additive de/migrations and Gold coordinator. Exact staged rows, digests, manifest pointer | A-I02 + G-MART/G-REV. Disable publication, keep evidence, suppress incompatible revisions | AC-16…19, SA-17, physical ClickHouse/SQLite independent audit |
| A-I04 (A10) | Gold reader candidate dưới de/gold_runtime, pinned manifest set, range timeline, query limits | A-I03, design adapter G-API; no Spring/React. Disable new reader nếu stale/mixed results | AC-20…32/39, SA-13/14/18, return digest, query plans, stale/cancel fixtures |
| A-I05 (tách từ A10) | Experiment metadata validation và paired Gold query, candidate reader module dưới de/gold_runtime | A-I04 và G-EXP + controlled independent run metadata. Không chờ optional throughput/waiting. Metadata không đủ thì return UNKNOWN, disable comparisons only | AC-33…35, SA-19, signed demand/controller/geometry + paired oracle |
| A-I06 (A11) | de/gold_runtime/replay.py và coordinator: historical backfill cursor, replay, rebuild và recovery | A-I02…04, source retention and compatibility. Pause live-independent rebuild, preserve history and V2 | AC-15…19/38 theo adoption, SA-09/10/15…18, replay proof, no live mutation |
| A-I07 (A12) | Stage A acceptance và evidence package từ de/tests/gold_runtime, representative approved runtime fixture | A-I01…06 theo scope, G-PERF. Safety stop giữ artifacts. B/C và cutover không nằm trong package | AC-40 + toàn bộ adopted AC; ba grain, hai durations, oracle, restart, query and scope verdicts |

Critical path A-P01 → A-P02 required paths → A-P03 + A-P04 → A-P05 → G-ENTRY → A-I01…04 → A-I06/07. A-I05 là nhánh bắt buộc cho experiment acceptance; có thể chuẩn bị metadata song song. Source correctness implementation, nếu cần, phải được phê duyệt riêng trước dependent outputs. Pressure và queue gates chỉ chặn outputs/qualification phụ thuộc. Đóng tất cả bốn KPI cần cả pressure và queue. Optional research không nằm trên critical path.

Stage A exit theo acceptance.md, ghi riêng business correctness, publication, recovery/replay, serving, performance và operational verdicts. Phase 5 vẫn chịu integrated verification/cutover.
