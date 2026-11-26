# Phase 4B — Traffic Analytics Business Specification

**Version 0.1 | Review candidate | 26/11/2026**

Bộ tài liệu đầu vào để review nghiệp vụ, thiết kế kỹ thuật và lập Phase 4B implementation plan cho Smart Traffic Streaming Data & Analytics Platform. Viết cho toàn nhóm, không chỉ cho người thiết kế giao diện. Không phải kết quả implementation, không mở gate Phase 4B, không thay đổi repository hay Notion.

## Reading order

| File | Phạm vi |
|---|---|
| [01-business-overview.md](01-business-overview.md) | Mục tiêu, scope, lựa chọn thiết kế và trách nhiệm màn hình |
| [02-metric-specification.md](02-metric-specification.md) | Metric dictionary, source mapping, công thức, grain, rollup, calibration |
| [03-analytics-overview.md](03-analytics-overview.md) | 4 KPI, Ranking, Heatmap, Trends, Persistence, filters/time navigation |
| [04-intersection-detail.md](04-intersection-detail.md) | Direction metrics, storage utilization, sampled signal, incident context |
| [05-experiments-comparison.md](05-experiments-comparison.md) | Fixed-Time/Adaptive, scenario, comparability, throughput/waiting research |
| [06-quality-gold-requirements.md](06-quality-gold-requirements.md) | Coverage, identity, publication/revision, serving, Gold runtime cost |
| [07-acceptance-and-planning-handoff.md](07-acceptance-and-planning-handoff.md) | 7 fixture groups, 40 AC, traceability B4B-01..15, gates, plan handoff |

Suggested repository placement after owner review: `docs/DEv1/phase-4/business/` (proposed, not created in remote repo). Existing `deferred-phase-4b.md` and performance decision documents remain authoritative until owner reconciles and approves changes. Do not auto-update source or declare performance gate cleared.

## Source references and evidence boundary

Baseline HEAD verified on branch `codex/phase-1-v1-raw`: [`25d943fa3825523e14a596aab601bb4e38983853`](https://github.com/giabaod1nh-spec/Smart_Data_Models/commit/25d943fa3825523e14a596aab601bb4e38983853). Pin links to commit; recheck HEAD/source before planning if repository changes. The current review re-read S1, S2, S4, excerpts of S5–S7, S10 and S11; the other source details rely on preceding audit in this conversation at the same commit, not a new exhaustive repository audit. No live SUMO/ClickHouse, tests, source calibration or performance measurements were run. Notion text re-read; embedded images not inspected directly. Examples in this specification are synthetic except explicitly identified retained fixture/evidence values.

- N1. [Analytics overview (Notion)](https://app.notion.com/p/420e135975bc809dbf8aed57b96ae272) — prior business decisions; updated separation of Experiments and source scope from subsequent discussions.
- S1. [V1 metric contract](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/contracts/observation/V1_METRIC_CONTRACT.md) — measurement semantics, required/optional/availability.
- S2. [Silver V1 fact builders](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/de/silver/fact_builders.py) — canonical bundles, fields, payload preservation, quality.
- S3. [Silver V1 schema](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/de/migrations/008_create_silver_v1.sql) — physical baseline facts.
- S4. [Deferred Phase 4B requirements](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/docs/DEv1/phase-4/deferred-phase-4b.md) — owner approval, B4B IDs and core window rules.
- S5. [SUMO backend](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/simulation/backend.py) — per-node scenario, global control mode, trip counter.
- S6. [Snapshot provider](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/observation/snapshot_provider.py) — queue, occupancy, moving speed, storage geometry.
- S7. [Detector manager](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/observation/detector_manager.py) — detector reads and occupancy finding; [detector XML](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/Visualize/detectors.add.xml).
- S8. [Canonical cycle](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/observation/canonical_cycle.py), [event mapper](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/contracts/traffic_observation.py), [JSON Schema](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/contracts/events/intersection-observation-v1.schema.json) — canonical availability/optional storage.
- S9. [Current SUMO cycle fixture](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/phase-2/fixtures/current-sumo-cycle.json), [topology catalog](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/generated/network_topology_catalog.json) — observed sample and participant scope.
- S10. [Performance acceptance](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/docs/DEv1/phase-4/performance/acceptance-result.md), [checkpoint-02 evidence](https://github.com/giabaod1nh-spec/Smart_Data_Models/tree/25d943fa3825523e14a596aab601bb4e38983853/docs/DEv1/phase-4/performance/evidence/checkpoint-02) — open PA, stopped-source proof and environment blocker.
- S11. [Run manifest](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/runtime/run_manifest.py) — metadata and expected schedule gap.
- S12. [Trip collector](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/Visualize/observation/trip_collector.py), [AI metrics](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/AI_Control_Traffic_Light/utils/metrics.py), [reward calculator](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/AI_Control_Traffic_Light/environment/reward_calculator.py), [Gold processor](https://github.com/giabaod1nh-spec/Smart_Data_Models/blob/25d943fa3825523e14a596aab601bb4e38983853/de/gold_runtime/processor.py) — source feasibility and verified input boundary, prior audit.

External sources (reviewed 26/11/2026; guidance, not project contract):

- R1. FHWA, [Traffic Signal Management Plans — Operational Objectives and Performance](https://ops.fhwa.dot.gov/publications/fhwahop15038/ch2.htm).
- R2. FHWA, [Measures of Effectiveness and Validation Guidance for Adaptive Signal Control Technologies — Chapter 5](https://ops.fhwa.dot.gov/publications/fhwahop13031/chap5.htm).
- R3. FHWA, [Traffic Signal Timing Manual — Chapter 2](https://ops.fhwa.dot.gov/publications/fhwahop08024/chapter2.htm) (archived edition; background for queue/performance metrics, not claim of current standard).
- R4. Eclipse SUMO, [Lane Area Detector Value Retrieval](https://sumo.dlr.de/docs/TraCI/Lane_Area_Detector_Value_Retrieval.html) — detector occupancy semantics; installed SUMO version still needs live verification.
- R5. Eclipse SUMO, [TripInfo Output](https://sumo.dlr.de/docs/Simulation/Output/TripInfo.html) — trip-level waiting/arrival evidence; source integration not assumed.
