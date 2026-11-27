# Repository audit

## Baseline

| Item | Observed value |
|---|---|
| Branch | `codex/phase-1-v1-raw` |
| HEAD local và GitHub trước review | `e57116e5bdda9fc6c39fdf84ae83c78dcd780cbc` |
| Working tree | Clean (`git status --short` empty) |
| Diff từ reviewed baseline | 0. So với addb59a… chỉ thay đổi 17 Markdown Stage A. Plan V2 có review-resolution.md. |
| Diff from Business Spec stated baseline | Eight added specification files, 761 lines; no source changes between `25d943fa3825523e14a596aab601bb4e38983853` and HEAD |

No branch switch, pull, runtime, benchmark, or test was performed. The Business Spec's source baseline is older, but the audited application source has no delta from it. The planning files in that newer commit must still be reconciled with current phase governance.

## Architecture and implementation state

The active historical path is SUMO/TraCI → canonical V1 observation → durable SQLite Outbox → Kafka → Raw V1 → Bronze V1 → Silver V1 → Gold Core. The realtime Projector/Orion path is separate and is not a historical source. Phase 4A Core is implemented and its acceptance report records Stage B correctness PASS and tested-envelope resource boundedness PASS. That same report says completeness `UNKNOWN`, finality `DISABLED`, capacity insufficient at the measured representative rate, and operational readiness not certified. See `phase-4/acceptance-result.md`.

Phase 4B business aggregation and marts are absent. Existing `de/gold/` contains legacy V2-oriented aggregation, builders, network/priority calculators and serving views; the accepted V1 Gold Core path is in `de/gold_runtime/` and verifies Silver canonical bundles but currently materializes Core proofs, not approved business publication. Do not reuse V2 formulas as V1 authority.

The Master Plan is a historical governance snapshot: its header says its audit basis was `803c6e4…` and local source `e4c2a25…`, and its body describes DE V1 Raw/Bronze/Silver/Gold as missing/V2. At current HEAD those layers and Phase 4A are implemented. This is a confirmed documentation staleness/discrepancy, not a reason to silently edit the Master Plan in this planning task. Owner reconciliation is required before stage ownership or gates are treated as authoritative.

## Source reading inventory

The inventory distinguishes file existence from depth of review. Core contract/design and acceptance evidence were read; listed runtime modules were read for relevant definitions and call paths, with function/field targeted inspection. The full Phase 4A performance acceptance result and the cited retained evidence summaries were read; not every supporting Markdown document or retained evidence file was inspected line by line, and retained runs were not replayed. No live system was opened. Do not interpret this as a byte-for-byte review of all repository files.

| Path / area | Review status and role | Main finding / requirement |
|---|---|---|
| `docs/DEv1/Master_plan_DE_V1_.md` | Read; governance history | Stale relative to current V1 implementation and Phase 4A acceptance; reconcile Stage A/B/C proposal. |
| `docs/DEv1/phase-4/deferred-phase-4b.md` | Read; B4B-01..15 and constraints | Business terms/formulas remain open; coverage and late-data publication are mandatory decisions. |
| `docs/DEv1/phase-4/plan.md`, `scope.md`, `audit.md`, `decisions.md`, `blockers.md`, `implementation-status.md`, `acceptance.md`, `acceptance-result.md`, `handover.md` | Read, with detailed inspection of plan/acceptance/status and targeted review of registers | Core preserves simulation-time tumbling membership, durable receipts, generation fencing, replay; business finality absent. |
| `docs/DEv1/phase-4/pre/plan.md`, `contract-design.md`, `decisions.md`, `implementation-status.md`, `acceptance.md`, `acceptance-result.md`, `capacity-and-backpressure.md` | Read contract/acceptance and key plan/capacity sections; not every line of every evidence appendix | Public canonical API is additive; bounded pages, receipts and independent invalidation cursor. Pre correctness passed; whole-path capacity is distinct. |
| `docs/DEv1/phase-4/performance/acceptance-result.md`, `acceptance.md`, `baseline.md`, `audit.md`, `blockers.md`, `decisions.md`, `design.md`, `handover.md`, `implementation-status.md`, `instrumentation.md`, `optimization-decisions.md`, `plan.md`, `profiling-result.md`, `requirements.md`, `execution-handoff.md` | Acceptance result read fully; remaining documents reviewed selectively for gate/baseline/status and cited sections; not exhaustive line-by-line audit | PA remains open; latest minimum-gate attempt ENVIRONMENT BLOCKED. |
| `docs/DEv1/phase-4/performance/evidence/checkpoint-02/` | Selected cited evidence reviewed via acceptance report; not every file opened | Stopped-source recovery is scoped evidence, not sustained paced acceptance. No artifacts changed or reused. |
| `docs/DEv1/phase-4/phase4b-business-spec/{README,01..07}.md` | Read as review-candidate requirements | 4 KPI direction, 40 proposed AC, formulas and gates; formulas and proposed split still need approval. |
| `Visualize/observation/{detector_manager,snapshot_provider,canonical_cycle,trip_collector,metrics_derivation}.py` | Read relevant functions | E2 occupancy unit heuristic; queue switches between E2 and vehicle estimate; full-link storage fields optional; TripCollector waiting fields are placeholders. |
| `Visualize/simulation/backend.py`, `Visualize/runtime/run_manifest.py`, `Visualize/configuration/*`, `Visualize/generated/network_topology_catalog.json`, `phase-2/fixtures/current-sumo-cycle.json` | Targeted source/fixture review | Configurable cadence defaults 1s, but observation schedule alignment/expected slot identity is not an authoritative contract; run/topology context is present. |
| `contracts/observation/V1_METRIC_CONTRACT.md`, `contracts/events/intersection-observation-v1.schema.json`, `contracts/traffic_observation.py`, topology/archive contracts | Read relevant schemas and mappings | V1 permits unavailable/null semantics and optional storage fields; additive contract changes require explicit approval. |
| `de/kafka_raw/`, `de/bronze/`, `de/silver/`, `de/migrations/006..008` | Source inventory and metric-field mapping review | V1 stages preserve identity, quality and payload; Silver facts include traffic, signal and intersection families. |
| `de/silver/{incremental_contract,incremental_reader,canonical_reader,fact_builders}.py` | Read public contract and builder paths | Gold must use `CanonicalSilverReader` public verified feed; do not query physical Silver tables. |
| `de/gold_runtime/{processor,silver_readers,processing_ledger,window_scheduler,revisions,replay,checkpoint_store}.py`, `de/gold/windowing.py` | Read relevant identity and work paths | One member requalification uses bounded single-entry reads; work is generation/fence guarded; active Core key excludes scenario. |
| `de/migrations/005_create_gold_m1.sql`, `de/gold/*` | Inventory and targeted review | Existing marts/views and legacy calculators are not V1 business publication authority. New DDL stays proposed only. |
| `server/src/main/java/com/traffic/server/analytics/**`, `server/src/main/resources/application-analytics.properties` | Consumer contract inventory; selected controllers/services/mappers | Existing analytics reads fixed Gold objects; nullable/revision semantics need contract review before API compatibility claims. |
| `frontend/src/pages/AnalyticsOverviewPage.tsx`, `frontend/src/components/charts/AnalyticsCharts.tsx`, `frontend/src/hooks/useAnalytics.ts` | Consumer inventory | Existing screens suggest query needs; do not let current charts drive physical tables. |
| `AI_Control_Traffic_Light/utils/metrics.py`, `environment/reward_calculator.py` | Read relevant functions | `avg_waiting` is count-valued; network throughput delta differences consecutive per-step arrived values. These are AI evaluation semantics, not accepted Gold metrics. |
| Compose files, detector XML and SUMO config | Existence and relevant settings inspected | Deployment cadence/resources and detector geometry must be recorded per run; no runtime validation performed. |

## Findings by severity

### Critical gates

1. **Expected observations cannot yet be computed from an approved source contract.** `Visualize/simulation/backend.py` defaults `OBSERVATION_INTERVAL_SEC` to 1.0 and captures after elapsed simulation time meets the interval; the schedule is not tied to a documented epoch slot ID. Realtime snapshot time is rounded to two decimals; capture_cycle overwrites it with actual backend simulation time. Manifest/topology population is available, but exact phase/start alignment, sampling slots and terminal eligibility are not proven. This blocks official strict coverage (Q-01/Q-02; G-COV).
2. **Performance Entry Gate remains ENVIRONMENT BLOCKED.** `performance/acceptance-result.md` records stop at 206.05 simulation seconds when memory crossed the 1 GiB floor, with only 1,035 emitted observations. Required fully paced both-duration proof, bounded drain and independent cross-layer oracle did not run. No gate waiver exists.
3. **RV-01: finding cũ INCORRECT.** main.py:28 khởi tạo GoldCoreProcessor. processing_ledger.py:356–365 dùng source namespace, run/generation, topology/manifest, spec/duration/start/end. checkpoint_store.py:641 unique (run_key,spec,duration,start,end). Scenario lưu ở event provenance. test_core_foundation.py:239 và test_core_stage_b.py:132 bao phủ PEAK late member cùng window. Legacy make_window_identity/batch_identity dùng scenario, thuộc đường khác.

### High

4. **RV-02: inbound/outbound E2 có lỗi normalization theo đơn vị documented percent.** Lane fallback cũng có heuristic theo độ lớn. Installed TraCI docstring mô tả percent, cần live capture trước khi chứng nhận payload thực tế. Xem SF-01.
5. **Queue methods may be mixed.** `snapshot_provider.py` prefers E2 lane-area jam meters when any movement has nonzero jam, else uses a vehicle-based stopped length estimate. Zero E2 can mean measured empty, but also the code's fallback branch; per-method provenance is not part of the primary queue value. Strict P95 must first establish compatible sensing scope and missing behavior.
6. **Storage is optional and geometry-sensitive.** `storage_utilization_lane` is lane jam meters / lane length; `storage_utilization_pcu` is queued PCU over geometric capacity. The latter can include moving population in its numerator and must not be called queue utilization. `canonical_cycle.py` omits full-link fields when the aggregate is incomplete; historical Silver coverage is unverified.
7. **Scenario/control metadata granularity differs.** Scenario is per intersection observation; `control_mode` is runtime/network-level. Do not claim per-intersection mixed controller mode. No validated experiment comparability catalog/protocol is established.
8. **Waiting-time Gold source is unavailable.** `TripCollector` writes `waiting_time_sec=None`; active waiting is snapshot accumulated waiting; AI metric `avg_waiting` sums `waiting_vehicle_count` (vehicles). Do not expose it as seconds.

### Medium

9. Master Plan governance text is stale versus current source/acceptance. Business Spec says its source baseline is `25d943f…`; the current audit source is identical, but its documents are new. Keep discrepancy explicit pending owner amendment.
10. B4B-04 is ambiguous: selecting 60s or 300s is supported by Core, but a distinct nested-scale comparison feature is not defined. Clarify before claiming full requirement satisfaction.
11. Existing Spring/React analytics are consumers, not authoritative business definitions. API compatibility belongs to later phase gates.

No live SUMO source experiment, detector calibration, queue-method capture, database read, benchmark or test was run. Some contract, phase-governance and performance supporting documents/evidence were inspected by relevant sections rather than exhaustively read; this remains an audit completeness limitation. Those items are plan prerequisites, not completed evidence.

## Final architecture review evidence

Baseline remote xác minh bằng exact ref. Origin fetch refspec mặc định chỉ theo dõi final, nên dùng explicit target refspec và đối chiếu FETCH_HEAD/ls-remote. Không có application delta từ Plan V2.

Source call chain cập nhật: CoreWorkLedger._suppress/_advance_work tác động tối đa hai Core memberships và giữ completed proof cũ immutable. processor._changes chờ required invalidation prefix, _work dùng one-member stream và finish CAS. GoldCoreStore.open kiểm tra schema v1/application guard; transaction BEGIN IMMEDIATE giữ lease. CoreReplayManifest.check reject moving authority, run_cycle hiện dừng ở terminal Core replay. Legacy repositories._insert/find_existing và migration 005 không phải active V1 writer. Xem [kiến trúc và đối chiếu source](architecture-design.md) và [R01–R09](review-resolution.md). Đây là static review, chưa có runtime test mới.
