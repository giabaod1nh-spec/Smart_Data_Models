# Silver-to-Gold analytical contract

## Dual path and ownership

The realtime path is `SUMO → Kafka → Orion → realtime service → realtime dashboard`.
The analytical path is `SUMO → Kafka → Raw → Bronze → Silver → Gold → analytics service →
analytics dashboard`. They are independent: Orion is never a Gold source, and Gold is never
the authority for current operational state.

Gold marts are historical/windowed analytical products. Their names do not use `realtime`,
`live`, `current-state`, or `current_state`.

## Allowed Silver sources

Gold may read these nine main Silver objects only:

1. `silver_fact_traffic_observation`
2. `silver_fact_intersection_state`
3. `silver_fact_signal_state`
4. `silver_fact_camera_observation` (corroboration only, not an M1 primary measure source)
5. `silver_fact_run_event`
6. `silver_dim_run`
7. `silver_dim_scenario`
8. `silver_dim_intersection`
9. `silver_dim_approach`

Silver control, replay, quarantine, and ledger tables are not analytical sources.

## Window and metric semantics

Gold windows are exactly 60 or 300 simulation seconds and are half-open:
`[window_start_sim_sec, window_end_sim_sec)`. `window_id` is SHA-256 of canonical JSON
containing run, scenario, size, start, and end. The component fields remain stored with the
hash.

| Silver metric kind | Legal Gold window outputs | Forbidden |
|---|---|---|
| Snapshot/gauge (`vehicle_count`, PCU, queue, occupancy) | `avg_*`, `max_*`, `latest_*` | SUM across snapshots |
| Pre-averaged speed/rate | AVG (as defined), MAX/MIN/LATEST | SUM as cumulative traffic/arrivals |
| State flags | occurrence counts, MAX/OR, latest, ratios | SUM as event count without the approved meaning |
| Categorical traffic/signal state | latest or distribution | numeric AVG/SUM |

Gold fact measures always encode their aggregation operator. In particular, ambiguous bare
`vehicle_count` is forbidden on Gold traffic-window facts.

## Direction vocabulary (`direction-v1`)

Gold canonical direction keys are exactly `N`, `S`, `E`, `W`, and `UNKNOWN`.
Canonicalization runs in the Gold layer (not by rewriting Silver):

| Source (trim + upper) | Canonical |
|---|---|
| `N`, `NORTH`, `NORTHBOUND` | `N` |
| `S`, `SOUTH`, `SOUTHBOUND` | `S` |
| `E`, `EAST`, `EASTBOUND` | `E` |
| `W`, `WEST`, `WESTBOUND` | `W` |
| any other | `UNKNOWN` + quality flag `NON_CANONICAL_DIRECTION` |

Facts that carry direction store both `direction` (canonical grain key) and
`source_direction` (unchanged lineage). Mapping version column:
`direction_mapping_version = direction-v1`.

## Current-row / revision ordering

Serving views emit at most one row per business identity. Ordering is physical and
never lexicographic on `v1.0` / `v10.0` strings:

1. `definition_major DESC`
2. `definition_minor DESC`
3. `revision_seq DESC`
4. `computed_at DESC`
5. `source_set_hash DESC`

`definition_version` / `metric_version` remain display labels (e.g. `v1.0`).

## Gold inventory

Dimensions: `gold_dim_run`, `gold_dim_scenario`, `gold_dim_intersection`,
`gold_dim_approach`, `gold_dim_window`, `gold_dim_metric_definition`.

Facts: `gold_fact_traffic_window`, `gold_fact_intersection_window`,
`gold_fact_traffic_comparison`, `gold_fact_signal_operation_window`,
`gold_fact_kpi_result`.

Control: `gold_processing_ledger`.

Marts: `gold_mart_network_window_overview`, `gold_mart_intersection_window_summary`,
`gold_mart_direction_window_summary`, `gold_mart_congestion_window`,
`gold_mart_priority_window_ranking`, `gold_mart_signal_operation_window`.

All Gold facts carry source-set lineage, source row/offset bounds, source tables, quality,
analytical freshness, computation timestamp, schema version, and definition version.

## Approved business definitions

- BD-1: `CONGESTION_SCORE_WINDOW` (`bd1_congestion_window_v1`).
- BD-2: `SIGNAL_OPERATION_SUMMARY_WINDOW` (`bd2_signal_operation_window_v1`).
- BD-3: `INTERSECTION_PRIORITY_WINDOW` (`bd3_priority_window_v1`).

The definitions, weights, units, quality behavior, and formula versions are locked in
`docs/gold/GOLD_1_BUSINESS_CONTRACT_SCHEMA_PLAN.md`. Gold 1 records only contracts and
schemas; it does not calculate these values.

## Explicit prohibitions

Do not source Gold from Orion. Do not create countdown/remaining-duration Gold fields. Do not
use Gold for realtime/current-state serving. Do not expose ambiguous `vehicle_count` on Gold
facts.
