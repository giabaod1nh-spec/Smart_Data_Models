# Gold Analytics Consumer Contract (Model A)

**Status:** Gold4B consumer-ready handoff  
**Scope:** logical query contract, SQL templates, sample payload shape, ownership matrices  
**Out of scope:** Business Service HTTP routes, Dashboard implementation, KPI recalculation

Gold does not expose HTTP. Downstream path:

```text
Gold facts/marts/views (ClickHouse, migration 005)
  → Business Service: SELECT / FILTER / ORDER / PAGINATE / DTO-map / JSON / authorize
  → Dashboard widgets
```

Realtime remains `Dashboard → Server → Orion`. Analytics must never use Orion for historical Gold values. Dashboard must never query ClickHouse/Gold directly.

## 1. Mandatory query filters

Every live analytical query template MUST:

1. Filter `namespace = 'live'`
2. Preserve Gold definition version (`definition_version = 'v1.0'`, major/minor when present)
3. Apply migration-005 current-row ordering (do not invent a second “latest” rule)
4. Filter explicit `simulation_run_id` / `scenario_id` when serving a run-scoped UI
5. Filter exact `window_size_sec` and/or `window_id` when serving a window card
6. Order by stable business identity columns (see templates)
7. Bound pagination (`LIMIT` / `OFFSET` or keyset)
8. Never aggregate, recalculate KPI, rank, congestion, priority or explanation JSON
9. Treat replay as `namespace = 'replay:<id>'` only; never mix with live

## 2. Consumer ownership matrix

| Consumer | Reads | Must not read |
|---|---|---|
| Realtime Server | Orion / realtime endpoints | Gold historical facts/marts |
| Analytical Business Service | approved Gold marts/views | Orion for historical analytics; Silver raw facts from Dashboard path |
| Dashboard | Business Service APIs only | Gold/ClickHouse directly; Orion for historical analytics |
| Operators | `/health`, `/ready`, metrics, runbook queries | mutation of Gold formulas/schema |

## 3. Analytical capability matrix (`CONTRACT_REQUIRED` routes)

| Analytical capability | Gold source | API route/method | Dashboard widget | Owner |
|---|---|---|---|---|
| network overview | `gold_mart_network_window_overview` | `CONTRACT_REQUIRED` | network KPI cards/trend | Business Service/Dashboard |
| intersection summary | `gold_mart_intersection_window_summary` | `CONTRACT_REQUIRED` | intersection detail | Business Service/Dashboard |
| direction summary | `gold_mart_direction_window_summary` | `CONTRACT_REQUIRED` | N/S/E/W panel | Business Service/Dashboard |
| congestion | `gold_mart_congestion_window` | `CONTRACT_REQUIRED` | congestion score | Business Service/Dashboard |
| priority ranking | `gold_mart_priority_window_ranking` | `CONTRACT_REQUIRED` | priority table/map | Business Service/Dashboard |
| signal operation | `gold_mart_signal_operation_window` | `CONTRACT_REQUIRED` | signal summary | Business Service/Dashboard |
| freshness/quality/lineage | all approved marts | `CONTRACT_REQUIRED` | freshness/quality badges | Business Service/Dashboard |

No HTTP route, DTO name or auth policy is invented here.

## 4. Dashboard analytical handoff matrix

| Widget capability | Source mart | Required fields | Units/quality | Owner |
|---|---|---|---|---|
| network overview | network overview mart **when populated** | vehicle count, speed/status, freshness, quality | Gold units + badges | Business Service/Dashboard |
| intersection summary | intersection summary mart | metrics, score, explanation, comparison | Gold units + previous/current | Business Service/Dashboard |
| direction panel | direction summary mart | N/S/E/W metrics, coverage, trend | direction-v1 + quality | Business Service/Dashboard |
| congestion/priority | congestion/priority marts | score, rank, explanation, revision | 0–100/ordinal + quality | Business Service/Dashboard |
| signal operation | signal mart | operation summary and quality | composite summary | Business Service/Dashboard |
| realtime status | Server realtime API / Orion | current entity state | realtime payload | Orion → Server → Dashboard |

### Architecture limitations (do not claim solved)

| ID | Limitation | Consumer rule |
|---|---|---|
| G3-P0-011 | migration-005 views may expose physical rows before terminal ledger | Prefer terminal ledger / work-unit evidence, or accept documented partial visibility |
| Network scaffold | `gold_mart_network_window_overview` currently `WHERE 0` | Do not fabricate rows; keep capability in handoff limitation until Gold1 change-control |

## 5. Field ownership matrix

| Field class | Authoritative owner | Transformation allowed |
|---|---|---|
| KPI score/value | Gold2 / Gold facts | API serialization only |
| metric code/version/unit | Gold metric definition | none |
| quality/coverage/freshness | Gold fact/mart | API pass-through |
| source lineage/hash | Gold fact/mart | API pass-through / redaction policy |
| authentication/user | Business Service | service policy |
| widget labels/layout | Dashboard | presentation only |
| realtime entity state | Orion via Server | never merged into historical Gold |

## 6. Logical SQL templates (validate against ClickHouse)

Templates live under `docs/gold/gold4_query_templates/`. Business Service may only wrap them with SELECT/FILTER/ORDER/PAGINATION/DTO/JSON/auth.

## 7. Sample payload shape (preserve Gold fields)

Every analytical JSON object MUST preserve at minimum:

- metric code / version / unit (when present)
- window identity (`window_id`, `window_size_sec`, start/end sim time when present)
- `namespace`, `simulation_run_id`, `scenario_id`
- quality / coverage / freshness fields present on the mart/fact
- previous/current comparison semantics when the source is a comparison or summary that includes them
- lineage / explanation JSON when present on the row
- `revision_seq` when present

Gold4 validates templates → ClickHouse → expected field presence. It does not call an unimplemented HTTP service.
