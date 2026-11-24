# Phase 4 scope boundary

Status: revised planning baseline after accepted Phase 4A-Pre Stage A. This document defines the requested Core scope; it does not claim Gold implementation or authorize it.

## Phase 4A-Core — Gold V1 processing foundation

Refactor the existing `de/gold/` and `de/gold_runtime/` in place into one active Gold runtime. Implement the Silver public incremental adapter, source-binding/version checks, receipt verification, independent change and invalidation cursors, Gold-owned durable state, event idempotency/suppression, canonical event-to-window membership, work intent, dirty generations, recovery, current-effective-state isolated replay, runtime lifecycle, foundation health, and performance instrumentation.

The only production source contract is the Silver V1 API implemented by Phase 4A-Pre. Every returned change sequence receives exactly one verified disposition. Gold commits only a contiguous returned interval, and advances only to the actual `through_sequence` after its full local obligations are durable. It acknowledges invalidations through the receipt's fixed safe watermark before change acknowledgment. It must not access Bronze journals or Silver physical facts directly.

Gold state is SQLite at `de/artifacts/gold_v1/checkpoint.sqlite3` and its single-owner lock at `de/artifacts/gold_v1/instance.lock`. Existing V2 state remains under `de/artifacts/gold/`; no silent schema/cursor adoption, destructive reset, complete Silver fact warehouse copy, or second active Gold owner. Migrations are additive, versioned, idempotent and transactional. Index event identity and invalidation-to-window impact. Measure database/WAL growth.

The logical SQLite state must cover: immutable source binding and contract/runtime versions; independent exclusive change and invalidation cursors; immutable receipt and transition history; effective disposition/correction reference; event identity and bundle hash; durable suppression; versioned window membership; work intent/state and retry/fault disposition; dirty generation and recompute obligation; replay manifest and isolated replay namespace identity; schema migration and owner lease. Enforce unique binding+sequence transition identity, unique receipt+interval, unique event identity per source generation, unique window membership, and unique deterministic work identity. Index source sequence, event identity, work status/creation order, and event-to-window membership. One SQLite `BEGIN IMMEDIATE` transaction commits each receipt, corresponding sequence entries, required membership/suppression/work/dirty obligations and cursor CAS. A separate invalidation transaction commits the invalidation receipt, suppression, impacted-window dirty/recompute state and invalidation cursor CAS. A change receipt transaction is not allowed until Gold has committed invalidations through its fixed safe `Pi`. No transaction spans Bronze, Silver, Gold and ClickHouse.

### Window behavior

Use pure simulation-time Tumbling Windows with durations 60s and 300s, aligned preferably to epoch zero, half-open `[start,end)`. An event may join one window at each duration. Apply deterministic precision normalization before persisting IDs; tests cover exact boundaries, fractional times, out-of-order data, restart and run bounds. The generic identity is:

```text
source_namespace, simulation_run_id, run_generation, topology_digest,
manifest_identity, window_spec_version, window_duration_sec,
start_simulation_time, end_simulation_time
```

Source schema and processor versions bind the relevant processing identity, input-set hash and work identity; they are not extra generic window-key fields. `scenario_id` stays on each observation and input-set hash; it is not part of generic window identity. Preserve run lifecycle independently. No fixed intersection IDs, direction names, cadence or expected sample count.

Keep window discovery/candidate eligibility, source-boundary status, window completeness, processing state, dirty generation/recompute, publication finality and run lifecycle as independent state dimensions. During Core, completeness is `UNKNOWN` without an approved expected-observation contract and publication finality is `DISABLED`; `FINALIZED` is unreachable. A window ending or a run becoming COMPLETE proves neither completeness nor finality. Continue ingesting observations for open windows; never wait for their end before Silver consumption or Gold metadata/membership work. Do not publish provisional or completed business results from Core.

### Invalidation, convergence and replay

Map each invalidation deterministically using verified event identity and simulation-time coordinates to all affected windows (including both `[120,180)` 60s and `[0,300)` 300s for an event at 135s). Use indexed event-to-membership lookup; do not scan all retained history. Suppression before work creation excludes the event; during-work invalidation increments generation and fences completion CAS; post-completion invalidation creates recompute obligations without rewriting immutable receipts. Late accepted events can also alter an existing window's effective input set and dirty it. Duplicate delivery is idempotent.

Guarantee Gold-local atomicity and eventual convergence via the durable feed only. A source change after Silver's final check may temporarily leave earlier boundary metadata in Gold; later invalidation suppresses/dirties it. This is neither a distributed transaction nor distributed snapshot isolation. Never claim that processing metadata is safe to serve.

Replay is current-effective-state only. Freeze binding, boundary, run/topology/manifest identity, source contract/schema and processor/work versions, scope and replay ID. Use an isolated namespace, SQLite DB and lock; never mutate live cursors/work or write historical V2 marts. A changed frozen source boundary aborts that replay ID; a transient authority outage is retryable. Resume incomplete work without manual cursor reset. Arbitrary as-of reconstruction is unsupported.

## Explicitly excluded from Core

- Average Speed, Queue Length, Congestion Score, KPI formulas/weights, priority ranking and hotspot logic.
- Business Gold facts/marts, serving views, result revision selection and business publication/finality.
- Rolling Windows, Sliding Windows, Provisional Aggregation or provisional business KPI publication.
- Spring Analytics APIs, Dashboard changes, chart contracts and serving-latency claims.
- Rewriting Raw/Bronze/Silver or changing producer pacing/source workload to create a capacity pass.
- A new Gold ClickHouse migration absent a concrete separately approved infrastructure need.

These responsibilities are deferred to Phase 4B and later serving work in [`deferred-phase-4b.md`](deferred-phase-4b.md). The Analytics Dashboard targets historical/completed intervals; the Realtime Dashboard owns current-state monitoring. Any future Gold business publication is only under a separately approved completed-window policy.

## Acceptance boundaries

Pre Stage A correctness and focused Silver-feed/test-consumer capacity have passed as documented in `pre/acceptance-result.md`; actual Gold Stage B integration is pending. Whole `SUMO → Kafka → Raw → Bronze → Silver → Gold Runtime` capacity is `NOT MEASURED`, and production operational readiness is not established. Report correctness, capacity and operational readiness independently. Ten inherited architecture failures remain tracked in `pre/evidence/architecture-comparison.json`.
