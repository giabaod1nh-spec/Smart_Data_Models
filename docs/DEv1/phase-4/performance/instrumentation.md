# Instrumentation and profiling design

No instrumentation was added by this plan. Implement metrics in a separate package, retain current health/contract surfaces unless changes are approved, and bound all in-process samples/buffers.

## Correlation model

Use sampled trace/correlation fields: `simulation_run_id`, cycle sequence, event ID, Kafka topic/partition/offset, Raw ingestion identity, Bronze semantic change sequence, Silver obligation/change/invalidation sequence, feed receipt ID and Gold work ID/generation. Metrics must not use these IDs as labels. Add monotonic span IDs and carry context across process boundaries only through logs/traces, never mutate canonical event payloads for profiling.

Each span has `stage`, `operation`, `start_monotonic`, `duration`, `outcome`, bounded error code, input/output count and byte count, batch ID where applicable, and commit/reconciliation boundary. Record wall timestamps separately for end-to-end latency; monotonic clock for duration. Sample payload hashes/IDs only under secure acceptance artifact controls.

### Offered-load and hidden-backlog accounting

At source expose expected scheduled capture cycles, actual successful cycles, completed observation entities, explicit failed/invalid captures, durable Outbox appends, Outbox pending, Kafka ACK and bytes. Record SUMO steps/s, simulation seconds/wall second, configured publish/cache interval and `publish_asap` count/reason. Kafka ACK is not offered load during outage. If intended pacing slows, the trial is invalid for W2 capacity claims.

At each stage expose received, buffered/read, validated, pending durable obligations, awaiting reconciliation, committed outputs, cursor/offset committed, fault-blocked and explicitly disposed states. State each unit and reconciliation equation. Include all in-memory and durable queues. `input = output + pending + explicit dispositions` is checked in the same unit; never equate a stage's `processed` counter with downstream completion.

## Required metrics by stage

| Stage | Counters/gauges | Histograms/spans |
|---|---|---|
| SUMO/capture | steps, cycles, observations, bytes, scenario-triggered extra cycles, capture queue depth/drops (must stay 0), source simulation/wall clock | TraCI step, vehicle/detector reads, snapshot derive/build, validation, serialization, Outbox append and Orion/projector capture delay |
| Outbox | rows by status, queued bytes, oldest pending age, retries, disk free, DB/WAL/SHM bytes, fault/degraded state | append lock wait/insert/commit, dispatch batch, delivery callback, ACK/retry state transaction, cleanup and WAL checkpoint |
| Kafka | broker log end/low, committed and safe offsets per partition, lag and oldest age, bytes, producer queue, request errors, rebalances | produce-to-ACK, poll, batch formation, broker request, retry delay, partition assignment/recovery |
| Raw | received/pending/persisted/quarantine/conflict by topic/partition, offset lag/age, CH bytes/rows, ledger size | poll wait, decode/validate, batch flush, CH insert, ambiguity reconcile, ledger commit, broker offset commit |
| Bronze | Raw backlog by partition/age, intent counts/status/age, dispositions, change/invalidation sequence rates, invalidation mirror age, outputs/obligation per disposition, faults | read batch, payload validation/hash, semantic identity lookup, intent freeze txn, output insert/reconcile by table, invalidation publish, checkpoint CAS |
| Silver | Bronze fixed high-water, change/invalidation cursor/backlog/age, validity and obligation state/age, facts and obligations per event, lifecycle freshness, archive hits/misses, readiness | pass queue wait, selected lane counts, each accepted/conflict/invalidation/recovery span; Bronze point/page read, archive file/fingerprint, validator, dimension/fact build, freeze txn, each CH reconcile/insert/visibility, final source read, atomic finalize |
| Silver feed | binding, frozen Bronze/Silver boundary, per-lane lag, page count/entries/bytes, dispositions, prefix blocked count, error class | discovery, each evidence query category, archive and bundle verification, JCS/hash serialization, page build, end-boundary check, time/byte budget exhausted |
| Gold ACK | change/invalidation cursor, receipt interval, lag/age, pending mirror safe Pi, local metadata rows/bytes/WAL, DB disk free | source session, DTO parse/JCS/hash, query by method, lane schedule, SQLite receipt/invalidation commit and restart recovery |
| Gold work | OPEN/not eligible; newly eligible; current-generation PENDING/RUNNING/COMPLETE; superseded/historical rows; dirty/recompute; oldest eligible age; 60s/300s complete; members; restart reasons; discarded reads/bytes; cache hits/misses/evictions; stale finish rejections | promotion; membership; per-member feed read/verification; stream hash; completion CAS; restart/rework; generation coalescing tx |
| Shared deps | per container CPU/memory/I/O/network/restarts; CH active/queued queries, query-log identity, parts/merges, disk; Kafka request/bytes/disk/partitions | per-query type and `query_id`, sanitized stage/session/query comment, execution time/read rows/read bytes/memory; client wait and server time separated |

## Tracing and query attribution

Create a bounded query attribution map:

`query_id → component/service, operation, batch size, source sequence range or partition range, receipt/work ID in controlled trace, attempt, success/fault`.

Where the installed `clickhouse-connect` driver/server 24.3 support query IDs/comments, set a per-operation `query_id` or a sanitized stable component/operation comment and capture server query ID. Verify driver/version support in an L0 instrumentation test before relying on it. If custom attribution is unsupported, exact per-query attribution is **UNAVAILABLE**; use approximate correlation by client monotonic span, server start/end, CH user and query log, record confidence and do not present approximation as exact. Capture completed/failed query counts, elapsed, read_rows/read_bytes, memory, tables/user/query ID and `system.parts`/merge/mutation pressure. Separate Raw, Bronze, Silver apply, Silver feed, Gold ACK, Gold work and diagnostic queries. Do not assign shared 49,761 query count to one stage. Measure attribution overhead and bound trace retention.

## Profiling methods

1. **Low-overhead production counters:** monotonic counters/gauges/histograms and periodic resource snapshots; bounded cardinality, fixed-size reservoirs or streaming quantiles.
2. **Sampled distributed spans:** sample deterministic fraction and 100% errors/slow operations, retaining exact identities in acceptance artifacts only.
3. **Silver controlled profile:** warm archive/cache, then run one isolated single-owner Silver worker against frozen verified Bronze boundary and disposable CH; compare accepted, conflict, invalidation and recovery work; capture operation spans and source evidence unchanged. Use CPU profiler only in separate profile run and report instrumentation overhead.
4. **Gold G-A local ceiling:** fixture-local source isolates receipt verification, SQLite transaction, membership, suppression, scheduling and generation behavior; label component ceiling only.
5. **Gold G-B actual canonical reads:** real Silver source authority, retained Bronze/Silver/CH evidence and public API; profile page ACK and 60/300 member work, proof restart/rework, exact verification and conflict/invalidation. A pre-materialized source may isolate read efficiency only if verification cost remains real.
6. **Gold G-C full E2E:** SUMO→Outbox→Kafka→Raw→Bronze→Silver→Gold completed work. This is the only profile establishing whole-path capacity; G-A cannot pass PA-11 alone, and a fast mock feed cannot pass G-B/C.
7. **Resource profile:** concurrently sample process/container CPU, RSS, IO, network, disk free, SQLite files/WAL, CH query load/merges and Kafka lag at fixed cadence; preserve raw series.

## Instrumentation acceptance

- Every stage counter has explicit unit, monotonic behavior, reset/restart meaning and source of truth.
- `input = output + durable_pending + explicit dispositions` reconciles in the stage's own unit; exception classes are explicit.
- Latency spans begin/end at named durable boundaries. No mixing wall and monotonic time.
- Metrics sample does not add unbounded memory, sync full payload or block TraCI.
- At least 99% of completed CH work in isolated profile has attributable service+operation, or missing attribution is quantified before proceeding; set final threshold after pilot overhead measurement.
- Overhead measured by matched instrumented/uninstrumented diagnostic trials; if overhead materially alters rates or pacing, reduce sample rate and retain detailed profile run.
- Health/readiness remains truthful during metrics/backend outage; observability failure does not advance cursors or drop data.
