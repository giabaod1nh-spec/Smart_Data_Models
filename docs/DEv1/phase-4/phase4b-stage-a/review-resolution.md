# Ma trận xử lý technical review

Baseline local/remote 035e88e26151951dbee407934751550ae7bfc381. So với baseline Business Spec addb59a… chỉ có 16 Markdown plan. Các verdict dưới đây phân biệt factual correction, thiết kế ứng viên và acceptance cần chạy sau.

| ID / severity | Claim cũ | Evidence | Verdict | Resolution | Documents | Còn lại |
|---|---|---|---|---|---|---|
| RV-01 Critical | Core key chứa scenario | GoldCoreProcessor, core_window_identity, SQLite unique, Core tests | INCORRECT | Rút blocker và giữ Core key | audit, architecture, blockers, decisions, plan, acceptance, requirements, handover | Regression SA-01 |
| RV-02 Critical | Lane ratio và chỉ inbound lỗi | detector_manager, snapshot_provider, SUMO 1.27.1 installed client | PARTIALLY CORRECT | Mở SF-01 tới cả outbound lane fallback | audit, source-feasibility, metric-contract, blockers | Raw getter probe và versioned correction |
| RV-03 High | Mixed queue có thể dùng chung | E2 nonzero/zero/mixed cases, canonical cycle, Silver fact builder | CONFIRMED | Bốn options, per-approach independent fixture | source-feasibility, acceptance | SF-02/Q và owner chọn method |
| RV-04 Critical | Schedule thiếu exact contract | backend clock/attempt sequence, manifest, public feed | CONFIRMED | Bốn options và exact candidate B | coverage-contract, plan | Schedule proof hoặc approved extension |
| RV-05 Critical | Gaps và double claims chưa xử lý | Core membership + fact metadata | RESOLVED IN DESIGN | Expected timeline, slot ledger, conflict rules | coverage-contract, serving-contract, acceptance | G-COV và runtime fixture |
| RV-06 High | Thiếu field/state/temporal rules | Silver facts, CoreInputStream.members, Business Spec | CONFIRMED | Metric state, P1, sort by slot, pressure gate | source-feasibility, metric-contract, acceptance | Owner definition + calibration |
| RV-07 Critical | COMPLETE có thể mất obligation | _work discards bundle, finish CAS, claim RUNNING/PENDING | CONFIRMED | Atomic finish + durable obligation và resource guards | architecture-design, publication-recovery-design, plan | G-REV và crash oracle |
| RV-08 High | Physical model chưa cụ thể | Legacy migration 005 và reading patterns | RESOLVED IN DESIGN | Ba candidate grain marts, fields/keys/engines/cost | mart-design, plan | G-MART, budget và DDL closure |
| RV-09 Critical | CH-only read có thể lộ wrong revision | Gold-local authority, CH stage visibility | RESOLVED IN DESIGN | Verify exact rows, pointer CAS, fenced reader và stale cursor | publication-recovery-design, serving-contract, acceptance | Approve reader integration và race tests |
| RV-10 High | Proof cũ chưa có rebuild policy | Core proof work ID, replay and feed bounds | RESOLVED IN DESIGN | Bounded historical obligation/rebuild và version policy | publication-recovery-design, plan | Retention/source compatibility |
| RV-11 High | Experiment bị gộp serving | Business Spec E-01…04, manifest/controller call paths | CONFIRMED | A-I05 independent, fail-closed pair rules | requirements, serving-contract, plan, source-feasibility | G-EXP và control protocol |
| RV-12 High | Cost model chưa định lượng | checkpoint-02 final-attribution, drain, paced stop | CONFIRMED | Sizing theo simulation/wall, stage-specific attribution | performance-design | Paced representative workload |
| RV-13 High | Entry và acceptance bị trộn | Authoritative PA record và Business Spec gates | OWNER DECISION REQUIRED | Conditional-entry proposal, G-ENTRY và G-PERF riêng | decisions, blockers, plan, performance-design | Owner amendment hoặc satisfy current gate |
| RV-14 High | Trace AC theo range | AC-01…40 và F-01…07 | RESOLVED IN DESIGN | Mỗi AC một row, 19 supplemental cases | acceptance | Oracle artifacts chưa chạy |
| RV-15 High | Research/design và implementation trộn lẫn | Review dependency graph | RESOLVED IN DESIGN | Preparation / implementation, old-to-new IDs | plan, handover | Approvals theo phạm vi |
| SR-01 High | Outbound lane fallback cũng heuristic | snapshot_provider.py:_spillback_metrics | CONFIRMED | Thêm SF-01 path C fallback | source-feasibility | Wire probe |
| SR-02 Medium | Git ignore mô tả sai trạng thái hiện tại | 16 plan files tracked tại reviewed commit | INCORRECT | Sửa baseline/delivery note | README, audit, handover | New document cần explicit force-add |
| SR-03 High | AI delta được mô tả mơ hồ | AgentManager → RewardCalculator đọc per-step getArrivedNumber, backend cumulative riêng | CONFIRMED | Evidence này không dùng làm completed-trip count | source-feasibility | SF-06 |
| SR-04 Medium | Snapshot time bị dùng làm canonical evidence | capture_cycle ghi đè từ backend clock | INCORRECT | Tách timestamp lineage | audit, coverage-contract | SF-07 |

## Consistency review

Truy vết D-A06 → A-I02/SA-01, G-COV → slot ledger/metrics/timeline, G-REV → SQLite obligation/pointer + fenced read, G-EXP → A-I05 và AC-33…35. Optional study giữ gate riêng. Giữ Phase 5 integrated acceptance. Các thiết kế trong package là proposal, runtime acceptance vẫn open.
