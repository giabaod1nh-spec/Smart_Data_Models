# Acceptance strategy and traceability

The production aggregation under test must not calculate its own expected value. Oracle inputs are frozen source observations, archived run/topology, approved expected-slot schedule and metric registry. A separate, simple reference implementation or hand-calculated fixture derives memberships, populations, sufficient statistics, quantiles, statuses and output identities. Verify physical ClickHouse rows, SQLite publication fences and serving selection.

## Business Spec AC mapping

| AC range | Scope | Stage/owner | Independent evidence |
|---|---|---|---|
| AC-01..09 | speed, stopped ratio, exact P95, occupancy, calibrated pressure, high count, load, empties, rollups | Stage A; occupancy/calibration/metric approvals | F-01/F-02 independent arithmetic and source unit fixture |
| AC-10..14 | 60/300 membership, expected schedule, quality, network population, topology/run isolation | Stage A; G-COV | F-03 independent manifest+archive slot oracle |
| AC-15..19 | duplicates, invalidation, restart/ambiguous insert, late change, cross-mart fence | Stage A; G-REV | F-07 durable source oracle, injected crash points, physical audit |
| AC-20..28 | overview selection, latest/pinned, range, cards, ranking, heatmap/trends, persistence, historical quality | Stage B/C; Stage A data support | F-04 fixture queries; API/UI owner acceptance later |
| AC-29..32 | detail, optional storage/signal/context | Stage A data / Stage C UI; only if adopted | F-05 geometry and signal joint-coverage oracle |
| AC-33..35 | experiment comparability, summaries, trends and intersection changes | Stage A data + source experiment protocol; Stage B/C query/UI | F-06 matched/mismatched independent-run fixture |
| AC-36..38 | trip throughput/waiting and extended contract | Research owner; only if metric adopted | independent SUMO arrival/TripInfo oracle; old-run N/A fixture |
| AC-39 | serving consistency/stale selection | Stage B and Phase 5 integration, Gold prerequisite Stage A | manifest/revision fixture plus API response verification |
| AC-40 | Gold overhead/operating envelope | Stage A + Phase 4 PA owner | both durations, backlog/resource/latency instrumentation and approved limits |

Deferred ACs remain traceable and open; they are not implicitly accepted by this stage.

## Required test matrix

| Category | Cases and oracle result |
|---|---|
| Metric correctness | Unequal speed populations; H/V ratio; exact nearest-rank 60/300; occupancy raw/normalized units; approved pressure only; ties/argmax; invalid H>V; empty, null, unavailable; direct and rolled-up results; Decimal/float tolerance |
| Window/population | `[start,end)` boundaries, fractional values, source cadence/slot alignment, missing sample/participant, invalid approach, topology/run isolation, scenario/control changes, incomplete terminal run |
| Revision/publication | duplicate delivery, late event, conflicting event, post-publication invalidation, work-generation bump, failed stage, ambiguous CH insert, crashes before/after every durable step, all-grain manifest consistency |
| Recovery/replay | restart and idempotent resume; deterministic same-source output; replay namespace isolation/no live mutation; legacy V2 preservation |
| Serving | selected historical run; latest published; metric-specific unavailable; stable rank ties; time gaps; persistence streak breaks; comparable/incompatible runs; range/page limits; revision visibility and cache invalidation |
| Performance | both durations; change/invalidation and eligible work backlog; read amplification; source-to-ACK vs business publication latency; memory/CPU/SQLite/ClickHouse writes; bounded recovery/drain; wall time vs simulation time |

## Definition of Done proposals

Require approved requirements and metric versions; authoritative expected population and strict coverage; public canonical source only; one verified Core traversal for approved aggregations; output parity with independent oracle at Direction/Intersection/Network grains; no zero substitution; generation invalidation/recompute/revision proof; atomic family visibility; restart/replay evidence; bounded serving contract; separate performance verdict under approved envelope; outstanding requirements/stage ownership recorded; Stage B has stable data and query contract.

Report six separate verdicts: **business correctness**, **publication correctness**, **recovery/replay**, **serving readiness**, **performance acceptance**, **operational readiness**. Valid values include PASS, FAIL, OPEN, INCONCLUSIVE, ENVIRONMENT BLOCKED, NOT MEASURED as appropriate; never compress them to one PASS.

