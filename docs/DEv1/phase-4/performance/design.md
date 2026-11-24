# End-to-end optimization design

## Recommendation

Use an evidence-gated, bottleneck-first program with a unified measurement model. Freeze initial workload/compatibility at G0, harden the reviewed Core replay path, instrument the complete chain, then profile producer/Raw/Bronze, Silver apply and Gold ACK/work early. Given current data, Silver is the first confirmed queue; Gold must be profiled before choosing a Silver optimization that could only move the bottleneck. Freeze numerical performance objectives only after those profiles at G1. Optimize Silver first unless profiling shows an upstream limitation blocks a valid measurement; upstream code changes remain conditional on measured need. After each significant optimization, run correctness regression, operation/stage profile, complete-path measurement, all queue review and bottleneck-migration review.

Do not preselect parallel workers, bulk semantics, caches, ClickHouse schema changes, producer cadence reductions or data-path redesign before root-cause evidence. Any optimization must preserve the current accepted Bronze/Silver semantic decisions, complete source lineage, frozen obligations and independent invalidation semantics.

Default dependency order is PERF-00A/G0 → PERF-00B → PERF-01 and all-stage profiling (including early Gold G-A/G-B) → G1 → PERF-04 Silver and PERF-05 canonical/Gold by evidence, with PERF-02/03 implementation conditional on measured upstream need → PERF-06 → PERF-07 → owner evidence approval. If upstream limits prevent a valid downstream profile, move only that conditional package earlier. This order is synchronized with `plan.md`, `blockers.md` and `handover.md`.

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

## Silver bounded grouped-reconciliation candidate

This candidate is evidence-gated: source shows per-obligation `reconcile → optional one-row insert → reconcile` in `recover_v1_obligations`, but no profile proves it dominates time. First record query count/time and input/output bytes by operation.

```text
select bounded pending frozen obligations ordered by source sequence
validate each frozen JSON, target allowlist, business key and expected content hash
group only compatible target tables / key projection / namespace / replay mode
for each bounded group:
    grouped SELECT exact identity/business-key evidence for requested keys
    map returned physical rows to each obligation without dropping duplicates
    classify each obligation independently: MATCHED | MISSING | CONFLICT | AMBIGUOUS
    fail closed on conflicting content, duplicate incompatible physical evidence,
        target mismatch, missing required lineage, or unclassifiable response
    insert only MISSING rows in bounded multi-row insert
    grouped visibility/content read for inserted keys
    set exact per-obligation durable status only for MATCHED/verified rows
re-read durable obligation statuses in source order
finalize event validity only after all mandatory obligations for that event match
advance source cursor only through exact verified contiguous prefix
```

Batch ceilings are independent `max_entries`, encoded bytes, query/result bytes and execution deadline; start from measured driver/CH envelope, never unbounded. If a 100-row group confirms 99, persist the exact 99 outcomes and leave the missing/ambiguous entry pending; no cursor may advance across that sequence. Crash before status commit repeats idempotent reconciliation; crash after status commit resumes from frozen status. Ambiguous insert response must run exact content reconciliation before retry; never blind insert. Conflicting physical output becomes durable fault. Retry only retryable dependency/timeout outcomes with bounded backoff. Local SQLite and CH remain separate transactions; CH output is verified before local status/cursor transaction. Preserve final Bronze current-state point read and atomic Silver VALID+cursor CAS after obligations reconcile.

Fallback: isolate the single unresolved obligation and use current single-row reconcile/insert/reconcile path. Grouped failure must not erase known confirmed outcomes. Profile query reduction, batch latency/tail, CH memory/read bytes, SQLite statements/transactions/WAL, peak Python memory, poison-row behavior and downstream lag. Tests cover partial write (99/100), lost response after commit, conflicting output, missing row, retry, crashes before/after ACK, and earlier incomplete sequence blocking later advancement.

Separate evidence-gated candidates: bounded archive/run/topology cache keyed by source binding + immutable manifest/catalog fingerprints + schema/builder version; static dimension computation reuse; skip unchanged dimension writes only with exact content check; indexed pending obligations; reduce redundant SQLite commits only if transactional atomicity unchanged; backlog-aware polling. Cache hit never bypasses archive hash/fingerprint validation lifecycle; cache eviction/restart/source replacement falls back to authoritative verification. Cache limits, eviction, invalidation, memory and cold/warm recovery must be measured.

## Silver scheduler fairness and dependency rules

Preserve current mandatory order: startup recovery reconciles pending frozen obligations before new journal work; within a verified fixed boundary, the entire invalidation prefix is processed before the change prefix; a `CONFLICTED` change can advance only after local invalidation state is durable. Sequence pages remain exact and contiguous. Do not round-robin a change ahead of a related invalidation or an unresolved obligation. Any fair scheduler can interleave only independently safe bounded lanes after proving dependency identity; always give independent invalidation progress when a change prefix is blocked, while preventing unrelated invalidation floods from starving safe changes/recovery. Freeze each lane boundary, keep byte/entry/time budgets, avoid retry busy loops with backoff, and distinguish idle delay from backlog continuation. Test continuous arrivals, related/unrelated invalidations, recovery backlog, append during processing and outage/restart.

## Gold canonical materialization alternatives A–D

Measured input demand indicates why Gold needs early component and canonical profiling: at an illustrative five observations/s, 60s/300s full windows may hold 300/1,500 members; each event contributes to both, yielding up to ~10 member retrievals/s before recomputation. The benchmark's 440 reads, 9.12MB and one 219.96s proof is insufficient for a capacity claim.

| Option | Design and correctness | Resource/history/recovery trade-off | Gate and recommendation |
|---|---|---|---|
| A — Optimize existing public incremental feed | Keep exact one-member API, batch internal Silver evidence queries, reuse fixed binding/boundary proofs where equivalent, reduce redundant CH round trips. Every member still verifies sequence, disposition, transition, bundle and invalidation boundary. | No new contract or durable cache. Query amplification may remain O(members); query locality depends on noncontiguous global sequences/run interleaving. Existing replay/restart behavior preserved. | First experiment. Select if measured query cost falls enough and G-B completes work target; simplest rollback. |
| B — Silver-owned bounded canonical batch materialization API | Additive request with source namespace/binding, requested sequence set (possibly sparse), strict member/byte/evidence/time limits, deterministic ordering, exact requested-set coverage, disposition/missing/conflict/superseded rules, transition+bundle hashes, captured Bronze/Silver source boundaries and invalidation fence, receipt, errors/retryability. Append during read is above frozen boundary; conflicts/invalidation must be reflected by fence or cause retry/block. This API is not a change-cursor advancement mechanism; existing feed unchanged. | Fewer queries per member/window possible; larger response/memory and extra Silver CH load may shift bottleneck. Needs versioning, independent sparse-coverage oracle, crash/replay semantics and API compatibility approval. | Consider only if A profiling proves feed/member query amplification is limiting and measurable headroom is needed. Separate architecture approval before implementation. |
| C — Bounded Gold-owned working representation | Silver remains source authority; Gold owns a derived cache of already fully verified bundles/inputs, never an authority. Explicitly compare volatile-memory cache (lost on restart; refetch all misses) with durable SQLite cache (schema/version, transaction, backup/restore and WAL effects). Freeze max records and bytes per namespace/window plus TTL/eviction; key with binding, sequence, transition/bundle hash, run identity, schema/processor versions. Invalidation suppresses immediately and evicts/fences affected items; every cache hit checks current generation and durable invalidation state. A dirty old window after eviction rereads public Silver canonical evidence; cache miss/fingerprint mismatch fails closed. Replay cache paths/namespaces remain isolated. Never duplicate all Silver history. | Reduces repeated active-window reads only when verified hit rate is high; consumes bounded memory or storage/WAL, introduces stale evidence/eviction and compatibility risks. Restart cost differs between memory/durable mode. | Evaluate after A profile. Choose memory-only first if it meets G1; durable only if restart cost proves material. Require cache hit/read savings, bounded storage, restart/invalidate/evict/source-replacement oracle and old-dirty recomputation before selection. |
| D — Hybrid | Bounded verified active-window working set + Silver grouped API on misses/old windows/recompute. | Best potential locality, highest interaction complexity and multiple fallback paths; API + cache state/compatibility burden. | Last option, only if A/C independently fail G1-derived objectives and B has approved contract. |

Compare all four for canonical authority/current validity, immutable evidence, invalidation safety, restart/replay, long history/old dirty windows, reads/member and per completed window, CPU/memory/SQLite/CH pressure, growth, latency/throughput, Phase 4B fit, complexity and rollback. No option bypasses the public Silver authority.

| Comparison dimension | A — current feed optimized | B — Silver batch API | C — bounded Gold working set | D — hybrid |
|---|---|---|---|---|
| Correctness / current validity | Existing per-member exact checks; keeps current Silver truth. | Must cover exact requested sparse set at captured boundary; superseded/conflicted status explicit. | Retained bundle revalidated against binding, transition, suppression and generation before use. | Cache hits and API misses need identical proof semantics. |
| Evidence / invalidation | Existing receipt and namespace-global invalidation fence retained. | Batch receipt hashes and fence cover every requested member; no cursor advancement. | Persist input identity/hash; discard/fence active proof on invalidation; eviction falls back to Silver. | Same receipt/fence in both paths and no stale cache fallback. |
| Restart / replay / history | Existing recovery and source rereads; low added state. | API operation is retryable/idempotent by request identity; replay-bound source scope and boundary explicit. | Durable cache needs versioned migration/restore; memory cache warms after restart; old dirty windows reread after eviction. | Highest recovery matrix and state/version complexity. |
| Work amplification / resources | O(members) calls may remain; limited extra storage, current query/CPU costs measured. | May reduce CH round trips/member; response bytes, Silver CPU/CH and memory rise with bounded request. | Can reduce active-window reads; bounded cache consumes Gold RAM/SQLite/WAL/disk. | Potentially lowest hot-window reads but highest memory, storage, query routing and operational complexity. |
| Long history / old dirty work | Historical member reread cost persists; public source is authority. | Requested sparse members may avoid scanning unrelated global sequences; capped request/result. | Requires eviction; old dirty work can be expensive after eviction, never fail open. | Batch API handles cache misses but carries both API and retention burden. |
| Compatibility / rollback | No public contract or persistent schema change; easiest revert. | Additive versioned contract; existing API stays intact; separate owner approval. | State compatibility matrix and retention policy; old code/new state support explicitly decided. | Requires both API and persistence approval, highest rollback risk. |

Selection gate: instrument A and use G-B to measure member/query cost first. Select A if it meets G1; C only if its bounded hit rate reduces measured active-window cost without breaking eviction/restart; B only if residual public-read amplification prevents G1 and Silver can afford added load; D only if independently justified savings exceed combined complexity. Compare all with matched G-B and full G-C; do not select using component peak rate alone.

### Global invalidation fence and work-generation coalescing

Keep current namespace-global Bronze invalidation high-water until an alternative proves equivalent correctness. Measure proof restarts and discarded member reads/bytes/query costs, restart reason, related/unrelated/other-run invalidation, age/fairness across run and duration. Exercise invalidation within/outside window, another run and final check during a 300s proof. A narrower fence is a separate design decision requiring exact impacted-window lineage and independent oracle.

Work ledger distinguishes **input generation**, **durable current obligation**, **execution history**, and **immutable completed history**. Candidate coalescing is state-specific: OPEN can advance fencing generation without a row for every never-runnable version if current source receipt is retained; PENDING can atomically supersede obsolete unstarted work and keep latest durable obligation; RUNNING input change invalidates/fences old execution and ensures latest work obligation; COMPLETE remains immutable and a later change creates a new generation; DIRTY always has a durable current recompute obligation. One local Gold SQLite transaction must atomically update generation/current work; receipt ACK transaction boundaries remain unchanged. Never delete completed history or allow stale completion. Test crash before/after coalescing, concurrent membership/invalidation, multiple pending changes, late observations, restart, stale completion; measure rows/observation, superseded growth, write statements, transactions, DB/WAL, scheduler lookup, completion rate.

## Cross-stage impact analysis

| Candidate | Immediate expected effect | Potential transferred bottleneck | Required net evidence |
|---|---|---|---|
| Silver faster pass / grouped reconcile | Raises Silver applied sequence rate; fewer CH calls/idle wait if proven | Gold ACK, Gold member reads/work, CH shared query/merge, SQLite write lock | Complete-path queue slopes plus Gold eligible completion and shared resources after each change |
| Feed batch API or cache | Fewer Gold public reads | More Silver CPU/response bytes/CH query demand, memory, stale evidence risk | G-B and G-C member exactness, query attribution, resource and all-lane lag |
| Gold coalescing/cache | Less obsolete recompute or repeated read | Added SQLite/WAL, cache memory, eviction misses and invalidation restart work | Current-generation backlog, durable history, hit/miss, invalidation and G-B/G-C completion |
| Raw/Bronze batching | Amortizes upstream operations | Faster Silver arrival and CH burst, larger queues or tails | Upstream stage capacity plus downstream queue growth and full E2E after batch change |

Component peak rates are not integrated sustainable capacity. After each optimization run affected operation and stage checks plus a matched end-to-end comparison and inspect *every* queue/resource.

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
