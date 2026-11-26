# Stage A performance design

## Baseline and current verdict

Phase 4A Core correctness and bounded-resource checks passed their scoped tests. Phase 4A Performance Acceptance remains **ENVIRONMENT BLOCKED**. On 2026-11-25, the owner-approved normal-paced minimum-gate run stopped at 206.05 simulation seconds after host available memory crossed the existing 1 GiB floor; it generated 1,035 observations. It did not reach 300 simulation seconds, prove full 60s/300s eligible work, complete bounded post-source drain, or run the independent durable oracle. Do not label it PASS or semantic failure. Earlier stopped-source recovery and retained replay evidence are targeted subcases, not sustained capacity.

Performance acceptance is a separate Phase 4 gate and still applies before Stage A runtime qualification. Owner may review design/research planning, but cannot treat Stage A implementation or publication as cleared to bypass the gate. No SLA or throughput target is approved.

## Workload model (conditional example)

At 5 intersections, 17 directions and verified 1Hz sampling: one 60s window has 300 intersection observations / 1,020 direction samples; one 300s window has 1,500 / 5,100. Thirty simulation minutes has 9,000 / 30,600. Each observation has one membership for each duration, so up to 18,000 memberships. These counts are examples only until manifest/topology/cadence/slot alignment are verified. Actual computation grows O(N) for additive statistics and O(N log N) for per-direction exact P95 sorting (or equivalent exact selection); retained result reads depend on windows/entities queried. Memory should be bounded to one work quantum plus exact eligible window sample state; recompute can revisit O(window members) canonical reads under current Core behavior. Two durations and invalidation may repeat work.

Measure separately: verified canonical member reads and read amplification; JSON decode/validation; accumulator CPU; queue exact-state memory; candidate/backlog age by stage; eligible-work backlog; CH insert rows/bytes and insert/merge cost; staging read-back time; publication latency; invalidation recompute cost; SQLite DB/WAL growth; CPU/RSS/disk and bounded drain. Keep simulation duration and wall-clock duration separate in every report.

## Verification sequence

Each benchmark records objective, exact fixture/source, rate, pace, duration, resource limits, versions, workload, correctness oracle, before/after backlog, stop limits, raw evidence directory and acceptance rule approved by owner.

1. Pure aggregation fixture and independent oracle (both durations, weighted means, quantile, null and tie cases).
2. Deterministic sized fixture benchmark; no live services.
3. Public canonical-bundle integration on bounded fixtures; count read amplification and memory.
4. Gold-only isolated materialization and ClickHouse insert/read-back diagnostics, if permitted by Core/performance gate.
5. Short paced source-to-Gold run after prerequisite gates; report all stage backlogs and resource envelope.
6. Both-duration proof and atomic publication oracle; measure publication latency.
7. Invalidation/recompute, restart, ambiguous insert and isolated replay cost.
8. Fresh normal-paced minimum gate, sustained qualification and coexistence only under separate approved performance execution plan; preserve stop conditions and source artifacts.

Do not add throughput targets, concurrency, batch size, infra, 30-minute soak or performance waiver by assumption. API/dashboard latency and final cutover belong to later gates.

## Sizing và gate tách biệt (RV-12/13)

Ở pace 1×, ví dụ 1Hz/5 intersections tạo 5 obs/wall-s và khoảng 10 membership obligations/wall-s. 30 simulation minutes có 30 windows 60s + 6 windows 300s tiềm năng và 612 direction + 180 intersection + 36 network rows nếu tất cả eligible, chưa tính revisions, stages, metadata. Baseline ở `performance/profiling-result.md` và checkpoint-02 stopped-source: 2338 reads/242.43 wall-s, 9.64 reads/wall-s sau ACK, stream cuối 1500 members/136.20 wall-s, last proof ở 338.13 wall-s từ readiness. Fresh run `perf-w2-paced-02` dừng ở 206.05 simulation-s, thiếu proof 300s/drain/oracle. Hai profile khác nhau, headroom chưa xác định. Không lấy 10−9.64 làm tốc độ backlog.

Tách baseline Raw/Bronze/Silver source progress, Core ACK, eligible wait, member verification, accumulator CPU/heap, exact queue sample state O(direction slots), staged bytes/rows và read-back, SQLite WAL/manifest growth, publication lag, retry/cancel do fence, range queries và comparative range reads. Đo Core-only và Core+business cùng isolated workload đã duyệt, từng wall/simulation clock, CPU/RSS/disk, tail latency và backlog age. Không tăng concurrency hay budget dựa trên stopped-source evidence. G-ENTRY cấp scope implementation. G-PERF đánh giá runtime/operational envelope riêng. Safety guards giữ nguyên 1 GiB runtime floor/2.5 GB startup reserve theo acceptance.
