# Logical Gold serving contract

This is a Stage A logical contract, not Spring endpoints/DTOs. Every response should include selected run ID/generation, topology digest, window duration and `[start,end)`, metric definition versions, business revision/publication ID, per-metric status/coverage, source freshness/finality status and lineage reference. Historical queries read Gold only and work with no active simulation, Orion or Projector.

## Query inventory

| Query | Inputs and grain | Output and selection | Empty/error/cost contract |
|---|---|---|---|
| Historical Run Catalog | optional scenario/profile/status; bounded limit + cursor | run metadata, participant/topology/manifest, available published range and quality summary | no active-run dependency; metadata unknown remains unknown; indexed bounded read |
| Latest Published Window | run, duration, optional entity | latest eligible manifest/window, not latest Core completion | no eligible result → explicit NOT_READY/NO_PUBLISHED_WINDOW; current-effective fence required |
| Selected Window KPI | run, generation or current run binding, duration, exact start or end | network KPI set, metric-specific N/A/unavailable, coverage and revision | Validate aligned bounds; no fallback to prior incompatible result |
| Intersection Ranking | run/window/metric definition, sort field/order, page | deterministic rank with stable ID tie-break and diagnostic approach IDs | bounded participant set/page; ties stable; metric unavailable rows explicit |
| Pressure Heatmap | run, duration, bounded time range, participant IDs optional | one intersection × published window cell, score/status and gaps | score only under approved calibration; no interpolation; range cap required |
| Historical Trends | run, grain/entity, metric, duration, from/to, cursor/limit | ordered effective points and explicit missing-window gaps | validate range and metric applicability; no smoothing by default |
| Persistence Summary | run, intersection set, metric definition, duration, range | high/valid/expected counts, share, longest streak, peak and missing gaps | no connection across a missing window; full-range official summary withheld if coverage fails |
| Intersection Detail | run/window or range, intersection ID | direction rows, speed/stops/queue stats, load, optional storage/signal/context and quality | topology-backed directions; unsupported optional field is unavailable, not zero |
| Experiment Run Comparison | experiment ID or run IDs, metric set, duration, common range, scope | comparability checks with PASS/FAIL/UNKNOWN evidence, paired values/deltas | fail-closed for required metadata, version mismatch, topology/scope mismatch or incomplete coverage |

All queries use explicit maximum time-range, page-size, run-count and experiment-size limits approved with API owners. Query contracts define error identifiers separately for invalid arguments, no result, incomplete source, reprocessing, incompatible experiment, source unavailable and internal integrity fault. Never return a partial official KPI without its status and denominator. Cache keys include manifest/revision and are invalidated on suppression/publication change.

## Experiment metadata minimum

At minimum retain immutable run manifest identity; topology/catalog digest and participant/detector geometry; source/producer/SUMO versions; simulation step and effective observation schedule; seed plus planned demand schedule/routes and initial conditions; local scenario timeline per intersection; effective network control mode and controller version/config; warm-up and analysis interval; run terminal status; metric-definition set; expected coverage and publication revision. Same seed alone is insufficient. Comparability checks show each required item as verified, mismatch or unknown, with evidence pointer. No “comparable” badge when a mandatory item is unknown.

Stage A provides reusable Gold run/window data and metadata support. Stage B implements API orchestration and compatibility; Stage C visualizes. Fixed-Time vs Adaptive remains an experiment between independent runs and needs a controlled protocol; no performance claim is inferred from a single run pair.

## Revision token và Experiment Data Support (RV-09/11)

Dùng [read protocol](publication-recovery-design.md) cho mọi query kể cả latest, ranking, gaps, export và experiment. Range pagination pin manifest set và timeline. Các truy vấn KPI/ranking/heatmap dùng selection token, pin nguyên kết quả hoặc trả STALE_CURSOR khi token đổi. Range sử dụng revision riêng theo window với cùng definition/source policy, không bắt buộc revision number giống nhau. A-I05 sở hữu controlled pair comparability và query-time aggregation. Dedicated experiment mart chỉ xét nếu query-time cost đo được vượt envelope và có thiết kế suppression riêng.

Experiment catalog ghi pair ID, baseline/candidate roles, planned offered-demand digest (routes, departures, demand profile), hashes topology/detector/participant/initial conditions, local scenario schedule, controller build/config và effective mode, step/cadence, warm-up, aligned common analysis range, required metric and coverage versions. So sánh cả experiment context và pinned manifest sets của hai run. Khác giá trị bắt buộc → INCOMPATIBLE kèm field. Thiếu evidence → UNKNOWN kèm field. Mode khác nhau đúng treatment thì hợp lệ, mixed intervention ngoài protocol thì fail. Dùng sum numerators/denominators, max window worst P95 và paired intersection deltas, zero baseline chỉ absolute delta. Một cặp chỉ là mô tả của pair đó. Stage B điều phối API, Stage C trình bày. Optional trip completion, waiting, crossing, storage và signal có contract/approval riêng.
