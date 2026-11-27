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
## Final candidate implementation entry and exit

Existing A-I paths are proposals. UP-01 and UP-02 are separately owner-authorized external prerequisites, never hidden in A-I tasks. Keep per-window traversal. OPT-01/02 stay deferred.

| Work | Entry / dependency | Mechanism, durability and resource envelope | Exit, rollback, exclusions |
|---|---|---|---|
| A-I01 metric engine | A-P03/04/05 proposed contract accepted, G-ENTRY or effective D-A14 amendment with scoped authorization. Synthetic authoritative slots allowed only as fixture data | From verified projected slots emit deterministic payload, no state on slot conflict except bounded reason. Cap projected slots and exact queue samples; use approved registry | FA-01…07, AC definitions, E-03 reference. Disable feature before durable integration. No upstream correction/API |
| A-I02 atomic integration | A-I01 reducer + reviewed state design; schema rehearsal can start independently under conditional scope | Core processor/ledger/checkpoint store reserve both BLOBs, upgrade scratch schema, seal proof+obligation+projection atomically, release stale token on rollback. Short writer transaction | FA-08…14/21/25; recover migrated DB and unchanged cursors. Disable Business with schema-aware binary; owner-approved restoration only. No production migration under conditional |
| A-I03 result/publication | A-I02, G-REV and G-MART, approved CH scratch access | Compute durable result/manifest then bounded CH writer/reconcile, exact verification, pointer CAS; no rerun on ambiguous write. Retain orphan evidence | FA-13/15…19/22/24, exact row oracle and WAL/queue metrics. Disable writes and retain fence/durable results. No live publication |
| A-I04 serving | A-I03, approved read contract/deployment design | Gold reader pin/revalidate exact revision set, capped range and retries. Fail closed on authority loss | FA-20/29 and serving AC. Disable adapter. No Spring API or UI |
| A-I05 experiment pairs | A-I04 and G-EXP with pinned controller/source metadata | Compare matched eligible ranges/definitions/demand controls; limit pair/range reads | AC-33…35, SUPPORTED and incompatible cases. Disable comparisons only. No pressure/crossover inference beyond approved methods |
| A-I06 replay/recovery | A-I02…04 and approved retention/frozen source scope | Partial restart by durable stage, immutable replay namespace/expected set, live-first quota | FA-23/25…28/30 and both windows. Pause scope and preserve evidence. Concurrent replay deferred without frozen source |
| A-I07 acceptance | A-I01…06 applicable slices, owner-approved operating budget and authority | L4 measured integrated comparison, L5 operational retention/restore, both durations and independent durable oracle | Remain blocked until gate evidence. Halt at guards and retain raw evidence. No cutover or relaxing Phase 4A gates |

Isolated-ready after conditional amendment and work-specific approvals: A-I01, A-I02 and bounded failure-path A-I03 slices only. Full paths still blocked by UP-01/G-COV, affected UP-02 and performance evidence. See [final closure](final-design-closure.md).
## Frozen execution plan — 09ae5dd baseline

This section controls future package handoff. Owner approved the architectural principles and selected D-A14. No implementation authorization is effective until the [authoritative amendment](conditional-entry-proposal.md) and package-specific approvals. All runtime checks remain planned.

```
Records: D-A14 effective + work-specific authorization
     |-- Track 1: UP-01 U1-A -> U1-B -> U1-C -> approved U1-D integration
     |            UP-02A -> occupancy eligible -> separate pressure calibration
     |            UP-02B -> queue oracle and method adoption -> official queue
     |-- Track 2: A-I01a pure fixtures -> A-I02 isolated migration/atomic handoff
     |                                   -> A-I03 isolated failure paths
     |      A-I02 migration rehearsal may start in parallel with A-I01a
     '-- Full integration gate + prerequisites + verified runtime budgets
             -> A-I01–03 integration (per adopted metric)
             -> A-I04 -> A-I05
                    '--> A-I06 -> A-I07 integrated/operational acceptance
```

Track 1 and Track 2 can progress independently under approved fixture identities. Do not run resource-heavy experiments concurrently. UP-01 is required for official coverage, UP-02A for occupancy/pressure and UP-02B for queue; fixture P95 does not require source approval. Optimizations OPT-01/02 and unsupported concurrent replay deferred.

| Package and entry | Exact mechanism/input/output and components | Verification, exit and rollback |
|---|---|---|
| UP-01, UP-02A/B | Separate owner authorization and exact specs below; source, Outbox/archive, Raw/Bronze/Silver/public reader, affected consumers | Versioned receipts/provenance, fault/continuity/source oracle and gates; disable feature/emission for new runs, preserve artifacts. [UP-01](up-01-implementation-plan.md), [UP-02](up-02-implementation-plan.md) |
| **A-I01a first** | Pure module under de/gold/business candidate path, de/tests/ Gold fixture location to match existing test layout. Inputs: frozen approved synthetic slot/population and projected member fixtures for both durations; output deterministic versioned metric payload with quality, no DB. Implement bounded NONE/UNIQUE/CONFLICT reducer and approved independent formulas, sort exact P95. Enforce fixture caps before allocation. | FA-01…07, FZ-01/06/10 applicable slices and AC metric oracle. Exit same result/hash on permutations, near boundary, equality and reject malformed input. Drop/discontinue isolated feature; no Core change or source-dependent official metrics. |
| **A-I02** after A-I01a for integration, isolated v2 rehearsal can start earlier | de/gold_runtime/checkpoint_store.py, processing_ledger.py and processor.py. Add schema/version, lease-scoped preclaim reservation, bounded selection and atomic finish + projection + obligation. Existing public receipts/prefix unchanged. Synthetic approved metadata fixture for isolated mode; U1-D real metadata integration gated separately. | FA-08…14/21/25, FZ-06…09. Verify no loss/head-of-line starvation, preserve Core rows/ACKs and restore sealed payload. Disable with v2-aware binary, restore disposable snapshot only. No production migration. |
| **A-I03** after A-I02 and G-MART/G-REV design sign-off | Gold coordinator + bounded compute/publication lane and candidate CH schema in disposable store. Durable payload/revision and required exact row manifest; result reuse, exact reconciliation, pointer CAS, suppression and WAL reserve. | FA-13/15…19/22/24, FZ-11. Independently verify duplicate/ambiguous/partial/stale cases and same hash/revision after restart. Disable scratch writer, retain evidence. No production use. |
| **A-I04** after full integration approvals/A-I03 | de/gold_runtime Gold reader boundary, one logical query port, exact pinned rows/epoch and bounded retries. Approve CH consistency, adapter transport and credentials before integration. | FA-20/29 and AC serving, stale cursor/availability; disable adapter. Stage B Spring implementation excluded. |
| **A-I05** after A-I04/G-EXP | Gold pair queries and official comparable metadata, pin both runs before metric deltas. | AC-33…35, reject incompatible or unknown. Disable comparison only. No new KPI adoption. |
| **A-I06** after A-I02…04 and retained source approval | de/gold_runtime/replay.py, coordinator/rebuild, durable expected-set digest, separate proof/compute/publish predicates and namespace. Live-first bounded budget. | FA-23/25…28/30. Restart and deterministic replay; pause isolated work, preserve payloads. Concurrent historical replay deferred pending frozen source. |
| **A-I07** after integrated scope and user-approved environment | Matched Core-only/Core+Business both durations, official 0.01s profile, end-to-end stages, realtime coexistence, drain/recovery and retained independent oracle. | L4/5 and G-PERF, approved numeric operating limits; stop safely/preserve snapshots. No production cutover. |

The smallest scoped prerequisite now is the authoritative amendment and A-I01a fixture/oracle sign-off. No extra architecture review or new live benchmark required to request that approval.
