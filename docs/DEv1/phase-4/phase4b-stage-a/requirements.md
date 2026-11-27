# Requirements and output traceability

All B4B items below are repository requirements with open semantics from `phase-4/deferred-phase-4b.md`. Business Spec choices marked agreed/proposed/research keep those statuses; this plan does not approve them. Stage A owns Gold data and logical query behavior; Stage B owns Spring implementation; Stage C owns dashboard; Phase 5 owns cutover and integrated acceptance.

| ID | Objective / proposed Stage A output | Source and acceptance dependency | Disposition / owner action |
|---|---|---|---|
| B4B-01 | Intersection metrics by run × topology-derived intersection × duration × window | M-01..08; canonical traffic/signal/context; independent rollup | `PROPOSED`; approve formulas, population, coverage and statuses. |
| B4B-02 | Direction/approach metrics on archived approach IDs | M-01..05,07; topology digest and archived catalog | `SOURCE_SUPPORTED` at field level; queue/occupancy semantics need validation. |
| B4B-03 | Network aggregate over the declared participant set | M-01,02,06,07; complete participant coverage | `PROPOSED`; clarify region is observed participants, not necessarily whole SUMO network. |
| B4B-04 | 60s/300s historical scale selection and possibly nested comparison | Core two durations; Q-01; AC-10,22 | `PROPOSED`; switch is feasible, distinct nested comparison remains owner decision. |
| B4B-05 | Trends, gaps, history and persistence | Effective published revisions; M-08; AC-26..28 | `PROPOSED`; approve range, missing gaps, retention and corrected revision visibility. |
| B4B-06 | Severity/pressure definitions, thresholds and labels | Occupancy correction, M-05 calibration; AC-04/05 | `BLOCKED` for official score/labels until source and G-CAL approval. Physical metrics may proceed independently. |
| B4B-07 | Deterministic priority ranking and diagnostic direction | M-05 + stable ties and data qualification; AC-24 | `BLOCKED` on calibrated pressure if pressure drives official ranking. |
| B4B-08 | Scenario comparison among independent runs | Experiment metadata/comparability contract; AC-33..35 | `RESEARCH_REQUIRED`; no verified cohort contract yet. |
| B4B-09 | Sampled signal measures | Signal facts and joint queue coverage; AC-31 | `PROPOSED/P1`; sample state is not exact phase duration. |
| B4B-10 | Per-metric expected population and quality | Coverage contract and topology/run schedule; AC-11..13 | `BLOCKED`; mandatory G-COV. |
| B4B-11 | Reusable business marts and lineage | Approved grains/definitions/revisions; AC-09/19 | `PROPOSED`; physical DDL only after approval. |
| B4B-12 | Business correction, revision and current-effective result | Core generation/invalidation and publication protocol; AC-15..19 | `BLOCKED`; G-REV required before writes/publication. |
| B4B-13 | Bounded Gold read contract for historic consumers | Serving contract and revision/quality states; AC-20..35/39 | `PROPOSED`; Stage A logical query design, no Spring code. |
| B4B-14 | Spring Analytics APIs | Gold serving contract; existing Spring consumer audit | `DEFERRED Stage B`; not complete because deferred. |
| B4B-15 | Dashboard analytics integration | Stage B contract and Stage C UI acceptance | `DEFERRED Stage C`; not complete because deferred. |

## Business output inventory

| Product | Grain | Required fields/uses | Current disposition |
|---|---|---|---|
| Direction window | run + run generation + topology + direction + duration + window + definition + revision | Weighted speed stats, stopped/vehicle counts, queue distribution evidence and exact P95, occupancy, pressure only after G-CAL, optional storage/signal/context, coverage and provenance | Proposed reusable base product |
| Intersection window | Same window keys, intersection | Rollup numerators/denominators, intersection P95, worst pressure and worst queue approach IDs, observed vehicle load, quality and coverage | Proposed reusable aggregate |
| Network window | Same window keys, network/participant scope | Four KPI inputs, high-pressure count only when calibrated, population identity and completeness | Proposed; network completeness strict policy requires approval |
| Published result manifest | logical result/run/window + revision | Input proof identity, generation, definition set, expected population digest, staged output digest, status, effective fence | Required design; physical structure open |
| Run/experiment metadata | run or experiment cohort | manifest identity, topology/demand/scenario/control/cadence/controller/warm-up/range/metric versions | Minimal catalog contract needed; no dedicated mart by default |

### Scope boundaries

Stage A includes feasibility, metric/coverage decisions, independent oracles, Gold aggregation, proposed physical storage, revision/publication and Gold serving query design/verification. It does not implement Spring Controllers/Repositories/DTOs, React components, dashboard integration or final cutover. Throughput and Waiting Time remain research and do not block independent core traffic metrics. No chart-specific tables or unsupported extra KPI are authorized.

## Ranh giới sau review

B4B-04 đề xuất duration switch 60/300. Nếu owner xác nhận nested comparison riêng thì dùng cùng aligned end, ghi rõ samples chồng lấn, không tạo mart riêng. B4B-01/02/03 giữ scenario-neutral Core membership và context sampled. B4B-08 cùng E-01…04 có data support/comparability ở A-I05. End-to-end UI riêng ở Stage C. B4B-12 phụ thuộc durable obligation và read protocol. Pressure trì hoãn riêng không đóng đủ 4 KPI.
## R02/R05: eligibility và identity

Q-01/Q-02 và B4B-10 là dependency A-P03. Gold tính finalized KPI cho full tumbling windows có authoritative horizon và strict applicable coverage. Partial first/last, missing toàn window và unsupported schedule trả trạng thái. Traffic zero đo được giữ numeric meaning của từng metric. Window identity Core giữ scenario-neutral, source sequence là proof coordinate và replay/order boundary. Business result identity gắn Core proof/generation cùng source semantics, schedule/population, definition và revision. B4B-12/13 áp dụng fence chung cho publication và serving.
## A-P01 scope disposition

- Required core candidate: B4B-01/02/03 direction/intersection/network, B4B-04 60/300 switch, B4B-05 historical gaps/trends, B4B-10 per metric quality, B4B-11/12/13 mart/revision/Gold reader. Required design includes deterministic recovery and rebuild.
- Conditional: B4B-08 experiment data support P0 với G-EXP, candidate metadata/pair rule. B4B-09 sampled signals only if adopted. Nested 60/300 comparison chờ D-A07. UP-01 là external prerequisite cho official coverage.
- Blocked source/calibration: B4B-06/07 pressure-dependent outputs, queue method slice trong B4B-02/03. Independent metrics có thể dùng approved fixtures sau conditional entry.
- Deferred optional: storage, trip throughput/waiting/crossing. B4B-14/15 giữ Stage B/C. Phase 5 giữ runtime coexistence/cutover. D-A01 owner amendment nếu cần, không sửa Master Plan.
## Frozen scope

The owner freezes direction/intersection/network marts and finalized 60/300 tumbling windows with existing Core invariants, as described in [freeze closure](final-design-closure.md). Initial isolated metric set is weighted moving speed, stopped ratio, observed load, coverage/slot dispositions and exact fixture P95. Official scope and conditional metrics retain independent prerequisites. Stage B API, Stage C UI and Phase 5 activation remain separate.
