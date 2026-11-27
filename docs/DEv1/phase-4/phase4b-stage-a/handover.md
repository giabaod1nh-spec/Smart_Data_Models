# Bàn giao Phase 4B Stage A

Baseline review local và remote `e57116e5bdda9fc6c39fdf84ae83c78dcd780cbc`, branch `codex/phase-1-v1-raw`. So với Business Spec `addb59a…`, chỉ có 17 Markdown Stage A trong Plan V2. Xem [scope và evidence](audit.md), [RV/SR và R01–R09](review-resolution.md) và [decisions](decisions.md).

**READY FOR OWNER DESIGN REVIEW. SOURCE FEASIBILITY REQUIRED. IMPLEMENTATION ENTRY BLOCKED.**

- D-A06 đã đóng factual finding. Active Core giữ scenario-neutral key và chỉ lấy scenario làm provenance. Đưa regression SA-01 vào A-I02.
- Duyệt trước A-P01 (scope Stage A/B/C và B4B-04 duration switch) và A-P02 (isolated SF-01, SF-02/Q, moving population, SF-07). Optional research có authorization riêng.
- Dùng feasibility evidence để đóng expected slots và source version, metric registry và calibration, bounded state/retention, rebuild, publication và serving interface. Cần design closure review trước implementation.
- G-ENTRY chờ fresh paced minimum gate hoặc owner amendment theo D-A14. G-PERF vẫn mở. Gold publication cần G-COV/G-REV. Pressure, queue và comparisons có gates riêng.
- Stage B nhận logical Gold serving contract sau Stage A acceptance. Stage C và Phase 5 giữ nguyên trách nhiệm UI và cutover.
## Sau final architecture review

Ưu tiên A-P03 để chốt lifecycle/schedule authority rồi A-P05 để chốt atomic state, migration và resource envelope. [State schema](state-schema.md) và [publication/replay](publication-recovery-design.md) là source cho implementation design, cần owner phê duyệt. Kế hoạch chưa sẵn sàng implementation khi G-ENTRY, G-COV và các design approvals còn mở. FA-01–30 đã lập kế hoạch và chưa chạy.
## Preparation handoff

Current reviewed baseline 0ccf2062c368def10b3601402215698b4a4e84ce. Source-only SUMO and isolated sizing executed. DESIGN CLOSURE INCOMPLETE. Owner to review UP-01 schedule/lifecycle authority, UP-02 source version and queue method, isolated D-A14 amendment, SQLite/CH reader budgets and replay retention. No implementation started. [Final candidate](final-design-closure.md).
