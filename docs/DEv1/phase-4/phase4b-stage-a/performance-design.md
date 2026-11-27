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
## Toàn pipeline, overload và drain (R01/R09)

Current Core có một public requalification read cho từng indexed member và không tính Business hay insert marts. Proposed operational path giữ lượt đó, thêm projected state, CPU compute từ sealed bytes và CH write/read-back theo immutable output. Publication retry tái dùng result. Restart trước atomic seal có thể phải đọc lại Core work. Historical rebuild dùng bounded extra traversal. Khối lượng CH tăng theo revisions và partial insert reconciliation, đo rows/bytes mỗi batch, queries per publication, reader verification và CPU. Không có evidence speedup.

| Stage | Arrival và capacity theo wall time | Theo dõi và overload action |
|---|---|---|
| Core eligible | eligible windows và member bytes/wall-s theo durations, service verified proofs/wall-s | age, raw/member reads, quota, ACK lag, invalidation lag, Core tick service. Defer finish khi admission thất bại |
| Obligations | atomic handoffs/wall-s và compute starts/completions | pending/oldest age, projection bytes + reservations, runnable vs blocked, mailbox/in-flight, retry amplification, CPU/RSS |
| Durable results | compute output rows/bytes/wall-s, publisher service rate | append size, SQLite writer/transaction p95/p99, WAL/checkpoint pressure, result age và pending bytes |
| CH publication | completed results và rows/wall-s, retry/reconcile load | insert timeout, missing/divergent keys, attempts/result, batch size, CH CPU/disk and read-back queries, stale write attempts |
| Serving | publish/suppression updates, requests/wall-s và range shape | visibility lag, pinned set size, retry/fence conflicts, stale cursor, CH exact-read cost and cache revalidation |

Với arrivals A và durable service C trong cùng cửa sổ wall time, backlog đổi theo A−C và disposition hợp lệ. Theo dõi cả slopes và oldest age; baseline không suy ra sustained C. Temporary backlog drain khi service dư. Sustained overload dừng historical admission, throttle Business admission và báo degraded. CH outage giữ durable payload, backoff/circuit breaker theo dependency. Poison giữ BLOCKED riêng, không chặn runnable queue. Retry storm dùng due-time credits, per-item và global budgets. Restart với backlog dùng indexed reconciliation, không load hết vào RAM. Khi protected storage reserve sắp cạn, dừng ở committed cursor, điều phối upstream retention/activation với owner. Resume theo low watermark và measured service/drain. Availability trade-off là Core eligible work có thể chậm dù ACK còn tiến trong envelope. Không tự thay fidelity/cadence/retention.

Benchmark đối chứng Core-only và Core+Business với official SUMO fidelity, live experiment, reader traffic, replay pause/resume, outage/drain và realtime regression. Đo simulation step/cadence thực tế, successful current-effective publications, per-lane service/tail/age, reserve/SQLite/CH/RSS/CPU/I/O, Raw through Silver lag và TraCI/Projector latency. Diagnostic 1s step không đại diện 0.01s normal config. G-ENTRY và G-PERF giữ nguyên, PA còn open. Dùng approved safety stops và owner duyệt budgets/acceptance thresholds từ evidence.

## Reuse existing evidence and optimization decisions

PERF-04 grouped frozen direction obligations per event/target and avoided fixed waits while a verified prefix has work. PERF-05 reuses bounded physical inventory and archive context within entry, keeping final source/identity validation. Bronze frozen point-read reuses first exact mirror query with final SQLite identity check. Archive catalog digest memo is fingerprint keyed and fresh manifests/registry are still validated. See [optimization decisions](../performance/optimization-decisions.md), [implementation status](../performance/implementation-status.md). Retained probes: 250 → 96 CH content queries for 32 reads in an earlier comparison, then direct bounded inventory reuse removed 96 extra fact queries and 32 redundant resolutions per pass; not end-to-end throughput. Core per-member public read remains. 2,338 reads in 242.43 wall-s after stopped-source ACK, final 300s stream 136.20 wall-s. This confirms work-lane cost in that phase, not what dominates live operation. Silver obligation ACK lock contention was observed (21.35s / 524 calls including failures); attribution amid live coexistence still open. JSON decode/hashing, Core SQLite fsync and shared realtime costs need matched profiling. Partial paced attempt stopped on host pressure, no proof of leak or insufficient sustained throughput. Current host preflight for E-01 is not evidence that the retained memory incident is resolved.

## OPT-01 bounded batch and OPT-02 duration reuse

| Option | Correctness, resources and decision |
|---|---|
| Per-window traversal | Keep in Stage A. Normal path reads each member once per duration; preserves exact Core proof and unchanged contract. No per-KPI verification. |
| OPT-01 contiguous batch | Public request already supports bounded max entries/encoded bytes and contiguous verified prefix. Indexed work membership is event-ID ordered and sequences across windows/runs may be sparse. Batch across arbitrary gaps risks extra reads, wrong proof order and invalidation drift. A future implementation may coalesce adjacent sequences only, pin same binding/receipt fences, enumerate canonical proof in existing event-ID order via bounded buffer, reject partial/invalid pages without advancing materialization proof, never touch live ACK cursor. Arbitrary multi-key public API would need separate Silver proposal. Measure coverage density, CH queries/member, retry and memory expansion before adoption. |
| OPT-02 generation-aware reuse | Both durations see the same event but differ in membership/generation. Cache would need bounded entries and key of binding+change seq+revision+transition/bundle hash, verified final fence and dependency invalidation for both windows. Additive V,H and speed numerators can combine only under identical complete slot/source definitions. Exact P95 needs underlying values or exact equivalent distribution, cannot derive from subwindow P95s. Temporal context, alignments and supersession require independent proof. Defer until evidence of bottleneck and approved cross-duration invalidation/replay oracle. |

No new point-read API, data retention cache or 60→300 shortcut in A-I scope. Regression for either optimization includes sparse/interleaved runs, partial receipt, duplicate/conflict, changed high-water during work, generation restart, both proof hashes and same outputs under restored order. Preserve frozen replay and Core scheduling. Quantitative fixture costs in [resource sizing](resource-sizing.md).
