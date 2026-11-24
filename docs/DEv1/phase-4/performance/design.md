# End-to-end optimization design

## Recommendation

Use an evidence-gated, bottleneck-first program with a unified measurement model. First instrument the full chain and profile Silver under a backlogged, controlled source. Then tune the confirmed Silver cap/poll behavior and measured dominant operations without changing semantic authority. In parallel, build a Gold window-work capacity test against a controlled high-rate canonical source; do not assume the 0.96 ACK rate describes Gold's intrinsic capacity. Re-run Raw/Bronze capacity and then the integrated pipeline after every change because backlog and query pressure can move downstream or upstream.

Do not preselect parallel workers, bulk semantics, caches, ClickHouse schema changes, producer cadence reductions or data-path redesign before root-cause evidence. Any optimization must preserve the current accepted Bronze/Silver semantic decisions, complete source lineage, frozen obligations and independent invalidation semantics.

## Alternatives

| Approach | Correctness | Liveness/capacity | Resource cost | Recovery/contract | Recommendation |
|---|---|---|---|---|---|
| A. Instrument then bounded tuning in existing single-owner state machines | Strongest compatibility; existing CAS, immutable obligations and bounded reads remain the gates. Every batch optimization can be checked against original oracle. | Targets measured cost and current Silver cap; batch-aware polling removes artificial idle time only under backlog. May be incremental and expose next bottleneck quickly. | Low to moderate; fewer redundant calls/commits possible, bounded CPU and memory. | Existing durable boundaries retained; rollback is parameter/code revert. Public contract unchanged unless profiling proves it is needed. | **Preferred first.** Lowest semantic risk and directly targets measured queue. |
| B. Increase concurrency / shard or multi-worker processing | Can be correct only after ownership, per-event ordering, SQLite single-writer, invalidation sequence and output idempotency are redesigned/proved. | Potentially raises service rate; can amplify CH contention and cause Bronze/Silver invalidation reordering. | Higher worker memory, query amplification, lock contention and container CPU. | Requires explicit partitioning and cross-worker recovery protocol; likely schema/contract work. | Defer until measured profile proves CPU parallelism is the limiter and local optimization cannot meet approved headroom. |
| C. Larger batches or cross-entry reconciliation | Preserves semantics if each frozen identity/output is reconciled and cursor only advances through exact complete prefix. | Amortizes request/commit overhead; can worsen tail latency, memory, and a blocked poison entry; can starve invalidations. | More bytes and working set per pass; fewer round trips if DB supports batch. | Must retain bounded batch size, durable per-entry progress, exact response reconciliation. | Evaluate as measured tuning with fixed upper limits and comparative tests. |
| D. Cache archive/dimension/static lookups | Safe only for immutable, versioned inputs with validated archive fingerprint and explicit bounded eviction. | Can reduce repeated CPU/file reads; no benefit if CH round trips dominate. | Small bounded memory/cache invalidation complexity. | Must fail closed on source binding/fingerprint/version changes. | Candidate after profile; immutable content key only. |
| E. Expand Silver public batch-read contract | Existing feed is correct but evidence-heavy per sequence; grouped public reads may reduce query amplification. | Could accelerate Gold ACK and materialization; may transfer work to Silver/CH and introduce boundary-proof complexity. | Lower call count but larger response and memory; may change CH query pressure. | Requires additive contract version, frozen boundary, contiguous sequence/disposition proof, stable binding and end verification. | Deferred until canonical feed/member reads are shown to limit Gold with a faster source. |
| F. Gold materialization batching/coalescing | Could avoid repeated source reads and collapse obsolete generations, provided exact current generation and invalidation fence remain. | Directly targets window queue; may reduce proof restart and duplicate reads. | Higher per-work set memory/SQLite rows; may increase latency for small work. | Must fence `generation`, source binding, fixed Bronze invalidation high-water and suppression. | Profile first, then bounded coalescing/stream read API proposal. |
| G. Reduce producer cadence/payload or drop work | Changes source fidelity and accepted contract. | Makes queues appear stable by lowering λ. | Lower resource use. | Breaks semantics or at-least-once/accounting contract. | **Rejected** absent explicit owner-approved product contract change. |
| H. Add Flink/Redis/new queue or relax authority/reconciliation | Broad architecture change, duplicated state authority or weaker guarantees. | May increase throughput but no evidence current technology is intrinsically insufficient. | High operational footprint. | New checkpoint, exactly-once/replay/consistency burdens. | Not justified by evidence; outside current scope. |

## Target operating architecture

Keep the same data path and ownership:

1. SUMO captures canonical observations at approved cadence; immutable cycle is committed to FULL/WAL Outbox before async Kafka delivery.
2. Kafka consumer uses bounded batches and Raw physical reconciliation; only confirmed contiguous offsets are committed.
3. Bronze remains semantic authority for accepted/duplicate/conflict and change/invalidation sequence. Frozen outputs reconcile before checkpoint advancement.
4. Silver remains the only Bronze→canonical transformation authority. Its worker uses bounded, fair change/invalidation/recovery scheduling; while backlog exists, no avoidable idle sleep; configured budgets are tuned from measured service/queue age. Archive and immutable inputs can be reused only with explicit version/content verification.
5. Silver feed remains contiguous, source-bound, bounded and end-verified. Contract change is a later separately approved design only if per-entry evidence calls dominate Gold capacity.
6. Gold retains independent durable ACKs and invalidations; work scheduler completes 60s/300s inputs under generation and invalidation fences. Batch/coalesce/read optimizations remain bounded and independently oracle-checked.
7. Compose allocates resources so analytics cannot starve TraCI, projector, Orion or serving services. Set budgets only after host/container profile and operator approval.

No cross-database atomicity is assumed. Raw CH rows, Raw SQLite, Bronze SQLite/CH, Silver SQLite/CH, Gold SQLite and Kafka each have separate commit/reconciliation boundaries. Recovery repeats and reconciles, never assumes a distributed transaction.

## Resource accounting

Measure CPU wall/process time, RSS high-water, Python allocation samples, SQLite main/WAL/SHM sizes, commit wait and disk throughput/latency, free space, CH query count/duration/rows/bytes/read_bytes/memory/merge activity, Kafka broker disk/requests/lag, network bytes and per-container throttling. Keep memory bounded by max entries, encoded bytes, processing deadlines and member streaming. Keep metrics cardinality bounded: service, stage, lane, disposition, partition bucket, outcome; do not label by event ID or run ID in metrics. Put high-cardinality identity only in sampled traces/logs with retention limits.

## Rollout and rollback

Every optimization ships behind bounded configuration or a reversible implementation change. Keep known-good image/tag and state backup procedure; do not rewind authoritative cursors or drop history. Use isolated staging data, canary only if operationally authorized, and phased enablement. On invariant mismatch, integrity fault, unexpected backlog slope, realtime regression, resource breach or increased error/retry rate beyond approved thresholds: stop the changed worker safely, preserve durable state/evidence, roll back code/config while preserving source and checkpoint compatibility, reconcile after restart, and compare independent oracles before resuming. Schema change requires additive migration and forward/backward operational compatibility plan; no automatic destructive rollback.

## Phase boundary

The performance endpoint is completed Gold foundation work (receipt ACK plus eligible work proof), not business KPI/mart/API/UI. Phase 4B stays blocked until requirements and PERF-07 gates in `acceptance.md` pass with owner-reviewed evidence. Later Gold KPI→mart→Spring API→Dashboard freshness requires its own separate performance plan.
