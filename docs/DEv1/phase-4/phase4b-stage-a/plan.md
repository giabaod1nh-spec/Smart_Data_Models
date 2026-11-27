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

## Implementation package mapping

ID ổn định A-I01…07 tương ứng P4B-A08, A09 phần Core, A09 phần publication, A10 serving, A10 experiments, A11 và A12. Bảng bên dưới mô tả scope cập nhật.

## Sequencing refinement sau final review

| Work package | Input → output, impact và scope | Prerequisite, invariant và acceptance |
|---|---|---|
| A-P03 contract closure | SF-07 và archived manifest → approved source lifecycle, schedule IDs và Gold applicability. Chỉ đề xuất upstream change riêng | A-P02 source evidence + owner source approval, Q-01/02, FA-01…07 |
| A-P05 state and lane design approval | Approved identities → schema v2, writer lease, bounds, backpressure, freeze/replay/retention, CH/reader protocols. Review state-schema và lane design | A-P03 + metric definitions theo scope. No migration execution. G-MART/G-REV và budgets approvals, FA-08…30 planned |
| A-I01 projection + compute | Public verified member fields và aggregate specs → bounded reducer và deterministic payload builder trong candidate de/gold/business | A-P03/04/05 + G-ENTRY. No per-KPI reread; exact P95 and slot oracle, FA-01…07 |
| A-I02 state + atomic handoff | Core store v1 → additive schema + proof/obligation/projection, scheduler admission trong processor.py, processing_ledger.py, checkpoint_store.py | A-P05 + G-ENTRY, schema migration có thể implement song song pure reducer, integration chờ A-I01. Preserve ACK/lease/fences và verify rollback, FA-08…14/21/25 |
| A-I03 publication | Verified durable projection → compute+result state, CH schema/verify/write lane and pointer | A-I02, G-MART/G-REV. No live enablement under conditional entry, FA-13/15…19/22/24 |
| A-I04 serving | SQLite pointer/epoch + CH manifest → bounded read protocol | A-I03, FA-20/29, không viết Spring/React |
| A-I05 experiment | approved pair/metadata + reader → comparability results | A-I04/G-EXP, AC-33…35, optional source metrics riêng |
| A-I06 recovery/replay | Frozen namespace/expected work set và durable results → restart/rebuild and separate completion | A-I02…04, retained evidence/frozen source support, FA-23/25…28/30 |
| A-I07 acceptance | Stage fixtures + workload envelopes → independent correctness and performance verdicts | A-I01…06, G-PERF và operational approvals. FA-01…30 theo scope, prior PA gate còn hiệu lực |

Dependency chính là authoritative population → state/atomic contract + projection design → separately authorized implementation → publication/reader/replay → integration acceptance. Benchmarked numeric limits cần setup diagnostics được duyệt. Chúng không tự thay thế G-ENTRY, cũng không dùng kết quả tương lai làm điều kiện để duyệt read-only design.
