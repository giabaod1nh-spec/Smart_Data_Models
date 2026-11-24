# Instrumentation and profiling design

No instrumentation was added by this plan. Implement metrics in a separate package, retain current health/contract surfaces unless changes are approved, and bound all in-process samples/buffers.

## Correlation model

Use sampled trace/correlation fields: `simulation_run_id`, cycle sequence, event ID, Kafka topic/partition/offset, Raw ingestion identity, Bronze semantic change sequence, Silver obligation/change/invalidation sequence, feed receipt ID and Gold work ID/generation. Metrics must not use these IDs as labels. Add monotonic span IDs and carry context across process boundaries only through logs/traces, never mutate canonical event payloads for profiling.

Each span has `stage`, `operation`, `start_monotonic`, `duration`, `outcome`, bounded error code, input/output count and byte count, batch ID where applicable, and commit/reconciliation boundary. Record wall timestamps separately for end-to-end latency; monotonic clock for duration. Sample payload hashes/IDs only under secure acceptance artifact controls.

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
| Gold work | PENDING/RUNNING/OPEN/COMPLETE/dirty counts, oldest eligible age, 60s/300s eligible/completed/revised, members, proof restarts/reasons, backlog bytes | candidate promotion, membership reads, per-member public read/verify, stream hash, computation, durable completion CAS, interruption/restart |
| Shared deps | per container CPU/memory/I/O/network/restarts; CH active/queued queries, query-log identity, parts/merges, disk; Kafka request/bytes/disk/partitions | per-query type and `query_id`, sanitized stage/session/query comment, execution time/read rows/read bytes/memory; client wait and server time separated |

## Tracing and query attribution

Create a bounded query attribution map:

`query_id → component/service, operation, batch size, source sequence range or partition range, receipt/work ID in controlled trace, attempt, success/fault`.

Where driver supports `query_id` or query comments, inject stable component/operation plus bounded build tag; record server query ID and sanitize arguments. For drivers that cannot propagate custom metadata, use server query log timing correlated to client span by start/end and user, and mark attribution confidence. Do not claim the 49,761 shared upstream queries belong to Silver. Capture `system.query_log` completed/failed counts, elapsed, read_rows/read_bytes, memory, tables, user and query id; also `system.parts`, merges and mutations. Define log retention and avoid storing credentials/payloads.

## Profiling methods

1. **Low-overhead production counters:** monotonic counters/gauges/histograms and periodic resource snapshots; bounded cardinality, fixed-size reservoirs or streaming quantiles.
2. **Sampled distributed spans:** sample deterministic fraction and 100% errors/slow operations, retaining exact identities in acceptance artifacts only.
3. **Silver controlled profile:** warm archive/cache, then run one isolated single-owner Silver worker against frozen verified Bronze boundary and disposable CH; compare accepted, conflict, invalidation and recovery work; capture operation spans and source evidence unchanged. Use CPU profiler only in separate profile run and report instrumentation overhead.
4. **Gold source profile:** feed a repeatable independent canonical sequence/disposition workload at several controlled rates, then run 60/300 window work and invalidation scenarios. Label this as isolated component capacity, then confirm full-pipeline performance.
5. **Resource profile:** concurrently sample process and container CPU, RSS, IO, network, disk free, SQLite files/WAL, CH query load/merges and Kafka lag at fixed cadence; preserve raw time series.

## Instrumentation acceptance

- Every stage counter has explicit unit, monotonic behavior, reset/restart meaning and source of truth.
- `input = output + durable_pending + explicit dispositions` reconciles in the stage's own unit; exception classes are explicit.
- Latency spans begin/end at named durable boundaries. No mixing wall and monotonic time.
- Metrics sample does not add unbounded memory, sync full payload or block TraCI.
- At least 99% of completed CH work in isolated profile has attributable service+operation, or missing attribution is quantified before proceeding; set final threshold after pilot overhead measurement.
- Overhead measured by matched instrumented/uninstrumented diagnostic trials; if overhead materially alters rates or pacing, reduce sample rate and retain detailed profile run.
- Health/readiness remains truthful during metrics/backend outage; observability failure does not advance cursors or drop data.
