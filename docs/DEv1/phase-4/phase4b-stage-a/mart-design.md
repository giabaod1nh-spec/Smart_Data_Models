# Gold logical products and candidate physical model

**No migrations are created in this planning task.** Three products below are logical grains, not a mandate for three physical tables. Final storage and DDL require G-METRIC, G-COV and G-REV approval.

## Candidate reusable base model

Recommended initial comparison: immutable revisioned business result rows keyed by a stable result header and reusable grain-specific metric rows, or base marts with serving views. Prefer base marts + views if different nullable/quantile payloads make one wide nullable fact hard to govern. Avoid chart tables. Avoid defaulting to ReplacingMergeTree/`FINAL`; ClickHouse merges are asynchronous and do not implement a cross-mart publication fence.

| Logical product | Grain / business identity | Proposed fields |
|---|---|---|
| Direction × run × window | namespace, run ID, run generation, topology digest, intersection ID, archived direction ID, duration, start/end, metric definition set, business revision | Metric values; raw sufficient stats; exact P95; source method/version; scenario/control provenance summary; expected/valid/missing/invalid counts per metric; quality; proof IDs/hashes; result revision and publish manifest ID |
| Intersection × run × window | Same key without direction | Rollup sufficient stats; queue/pressure maxima and deterministic argmax IDs; complete-topology load; metric-level statuses; coverage and lineage |
| Network × run × window | Same key with network population identity | Network numerator/denominator metrics; high count only after calibration; participant count/digest; completeness; no unique-vehicle claim |
| Business publication manifest | result-set ID, generation, window, definition set, revision | expected product row count, deterministic output digest, staged/verified/published/suppressed state, current-effective fence, timestamps and Core proof identity |

## Keys and ClickHouse design questions

Use stable keys including namespace/replay ID, run generation, topology digest, entity grain/ID, duration, integer aligned window boundaries, metric-definition version and business revision. Preserve `start`/`end` half-open semantics and simulation-time precision. Include scenario as observation provenance or explicit mixed/profile metadata, not an accidental partition filter. Keep PCU profile, producer/source version, manifest identity, source binding and input proof lineage.

Candidate `PARTITION BY` should be coarse (e.g. run/time bucket after expected retention is known) and `ORDER BY` should support run + grain + duration + start + definition/revision current selection. High run cardinality, replay namespaces and multiple revisions must be costed before DDL. Define TTL/retention and immutable evidence window. Nullable values need explicit status columns; null must not coerce to zero. Use fixed precision/Decimal or documented Float64 comparison tolerance consistently.

Before schema approval, inspect actual `005_create_gold_m1.sql`, server mapper/query access patterns and ClickHouse version. Test additive migration on clean and existing V2 data; preserve old V2 views/tables and data. New migrations must be additive, restart-safe, namespace-isolated, and reversible by dropping only new objects after verifying no retained V1 evidence depends on them. No physical change is authorized here.

## Access pattern fit

- Run catalog and latest published windows filter by run/status and sort by end time.
- Selected window KPI loads one network row and matched intersection/direction rows.
- Ranking/heatmap scans bounded intersections for a selected run/duration/range.
- Trends/persistence scan bounded ordered windows for selected entity, metric definition and duration.
- Detail queries select one intersection's direction rows.
- Experiment query compares compatible immutable run rows and metadata on common interval.

If one base table serves these queries efficiently with explicit grain and status, use it. Otherwise materialize only reusable statistics/results with measured access justification. Do not introduce an experiment-specific mart or per-chart table until query evidence shows need.

## Candidate physical contract (RV-08)

Khuyến nghị ba base marts `gold_v1_direction_window`, `gold_v1_intersection_window`, `gold_v1_network_window` trên MergeTree, dùng reusable projections/queries. Unified wide fact giảm inserts nhưng nhiều nullable fields và khó ràng buộc grain. Metric-per-row linh hoạt nhưng nhân rows, thêm joins và read-back. Chỉ giữ reusable sufficient stats cần range rollup, đo I/O trước khi materialize projection khác.

| Cột ứng viên | Kiểu và ý nghĩa |
|---|---|
| namespace, run_id, topology_digest, manifest_identity, source_binding_id | String; replay và V1 tách riêng |
| run_generation, core_generation, business_revision | UInt64; revision độc lập với generation |
| duration_s, start_s, end_s | UInt16, UInt64, UInt64; simulation seconds theo Core |
| definition_set_id, source_semantics_id, population_digest, core_work_id, input_hash, publication_manifest_id, result_id, row_hash | String; lineage bất biến |
| intersection_id, approach_id, participant_digest | String theo grain; direction có intersection+approach, intersection chỉ intersection, network dùng population digest |
| speed_sum_weighted, moving_n, stopped_n, vehicle_n, exposure_n, aligned_slot_n | Float64 và UInt64 cho counts; raw stats |
| mean_speed_kmh, stopped_ratio_pct, load_veh, queue_mean_m, queue_p95_m, queue_max_m, occupancy_pct, pressure_score | Nullable(Float64); áp dụng theo grain; null kèm reason |
| metric_quality | Map(String, Tuple(UInt64, UInt64, UInt64, UInt64, UInt64, UInt64, String)); expected/valid/missing/unavailable/invalid/not-applicable/status; structural received riêng |
| queue_n, queue_sum_m, occupancy_n, occupancy_sum_pct, signal_green_n, signal_valid_n, joint_green_queue_n | UInt64 counts, Float64 sums; direction, signal chỉ nếu adopt |
| worst_queue_intersection_id, worst_queue_approach_id, worst_pressure_approach_id, high_pressure_n | Nullable(String) cho IDs, Nullable(UInt32) cho count; theo grain |
| scenario_counts, control_counts, observed_markers | Map(String, UInt64), Map(String, UInt64), bounded Array(Tuple(Float64,String,String)); theo provenance scope |

Full logical key là namespace+run context+duration/endpoints+grain entity key+definition set+source semantics+population+revision. ORDER BY (namespace, run_id, run_generation, topology_digest, duration_s, start_s, definition_set_id, result_id, grain entity keys). Candidate PARTITION BY tháng UTC immutable của run registration, có thể chọn unpartitioned nếu workload nhỏ, không partition theo từng run. Retention chưa có số được duyệt. Exact P95 giữ result/count/sum/max, bỏ raw samples sau work; rebuild exact distribution qua verified input nếu giữ đủ lịch sử. Idempotency xác minh physical rows theo result ID và row hash như publication protocol. V1 additive bootstrap, chạy thử schema cả clean và V2 đã có, preserve V2. Cleanup chỉ xóa V1 orphan/expired đã release authority.

Ví dụ logical select: WHERE namespace = :ns AND run_id = :run AND duration_s = :duration AND result_id IN :pinned_ids ORDER BY start_s, intersection_id. Ranking đọc intersection projection cộng direction đúng manifest. Range speed dùng Σspeed_sum_weighted / Σmoving_n. Không dùng AVG(queue_p95_m). Schedule/source/method field layout còn phụ thuộc G-COV/G-SRC, precision và partition phải qua mart design closure.
## Liên kết với durable state và publication

SQLite giữ sealed projection, computed payload bounded, row manifest và current authority theo [state schema](state-schema.md). ClickHouse là analytical storage cho immutable result rows. Writer retry đọc payload SQLite và reconcile đúng result ID. Views chỉ làm projection sau khi reader pin IDs được quyền đọc. Current resolved revision phải được xác minh ở SQLite. State migration và mart DDL là hai bước riêng cùng approval, cleanup xét fences và replay references.
## Payload sizing và field disposition

E-03 đo 23 synthetic result rows/window. Numeric sizes chưa gồm full lineage/identity, coverage status, optional context hoặc per-version fields. Giữ metric method/source provenance columns, nullable queue/occupancy khi conditional; không đưa research metrics vào required row manifest. Batch count/byte limits còn phải đo trên CH disposable scope. [Sizing](resource-sizing.md).
