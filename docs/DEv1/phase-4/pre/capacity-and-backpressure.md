# Phase 4A-Pre — End-to-end capacity, backpressure and throughput design

**Status:** source audit and benchmark plan; no production capacity result claimed.  
**Repository evidence:** local HEAD `443f40e3ddde0f9cf657785b3fbf1851281897ea`, branch `codex/phase-1-v1-raw`.  
**Scope:** SUMO → durable Outbox → Kafka → Raw V1 → Bronze V1 → Silver V1 → Gold V1 processing foundation. Gold marts, serving API and dashboard freshness remain Phase 4B.

## 1. Verdict from current evidence

**Whole-path sustained capacity verdict: NOT MEASURED.** Retained reports do include useful bounded evidence: the Phase 2 Raw/Bronze sustained acceptance ran 120 simulation seconds / 600 observations / 120 cycles and reported 12.322 seconds catch-up, 42.259 seconds total, with 14 periodic metric/resource samples; its result is scoped to that workload and stack. The Phase 3 acceptance result records a real 600-event A01 producer/Raw/Bronze oracle and Silver materialization, and a later source-boundary snapshot at 2,200 with Silver cursor 2,200. Chronological runtime samples show a lag of 588 at 08:07, 356 at 08:11 (Asia/Bangkok), and cursor 2,200 by the last progress update at 01:17:39 UTC / 08:17:39 local (final snapshot 08:24:54 local). This is evidence that a finite backlog was eventually caught up, not a controlled λ/μ, burst, or continuous capacity benchmark. The report explicitly says no throughput improvement/capacity claim was inferred. One Docker point sample reported Silver at 58.74 MiB and 8.83% CPU; this is not a representative resource profile. The Phase 3 acceptance report describes a later dirty worktree/HEAD than the current requested baseline, so treat it as historical runtime evidence, not proof of exact current-commit performance.

No retained test measures the entire SUMO→Kafka→Raw→Bronze→Silver→Gold path with a production Gold feed consumer. Hence no sustained supported workload, end-to-end capacity, backlog-stability objective or end-to-end latency SLO is certified. Phase 2 catch-up and Phase 3 finite backlog recovery are evidence inputs for the benchmark design below, not production claims.

V1 producer publication is disabled by default in `Visualize/configuration/config.py` (`V1_PUBLISH_ENABLED=false`, `KAFKA_OUTBOX_ENABLED=false`). V1 stack Compose/test profiles enable it explicitly. Therefore a continuously running default SUMO process is not, by default, the production V1 feed whose capacity can be inferred. Even when enabled, the current output rate depends on participating nodes, mapped observation contents, publish interval, SUMO wall-clock pacing, scenario-triggered extra publishes, and host performance. Do not infer event rate from a count of intersections alone.

The chain is sustainable only if each stage's measured service rate `μᵢ` exceeds its long-run arrival rate `λᵢ` for its actual input units, with sufficient durable capacity for bursts/outages. A persistent `λᵢ > μᵢ` grows that stage's backlog approximately by `(λᵢ-μᵢ) × elapsed_time`; buffers defer overload but do not fix it. End-to-end supported rate is bounded by the slowest required stage and downstream fan-out.

## 2. Architecture and boundary controls

```text
SUMO/TraCI producer
  | emits observation cycles; never waits for all downstream transformations
  | overload: current V1 publish path can fail-stop on Outbox append failure;
  v
SQLite durable Outbox
  | authority for locally captured, pending Kafka deliveries; ACK/status rows
  | backlog: rows/bytes/oldest age + disk free; async delivery max-in-flight
  | overload: no configured pending-row/byte quota; low free disk degrades/faults
  v
Kafka topic traffic.observations.v1 (3 partitions, RF=1, local topic retention)
  | durable broker buffer; source cursor is partition offset / consumer group
  | backlog: consumer lag/oldest retained offset; retention can expire history
  v
Raw V1 consumer and ledger + ClickHouse raw facts
  | Raw SQLite ledger + contiguous persisted/committed partition prefix
  | bounded receive/flush configuration; consumer commits only after persistence
  v
Bronze V1 semantic worker
  | Bronze SQLite identity/intents/change journal/invalidation journal authority
  | raw partition offsets and separate semantic sequences/cursors
  | backlog: raw offset lag; Bronze journal high-water minus Silver cursor
  v
Silver V1 worker and local checkpoint + ClickHouse canonical facts
  | Silver obligations/current validity and independent applied change/invalidation cursors
  | source verification, archive/lineage/quality and ClickHouse reconciliation
  | source-confirmed cap: at most 10 journal rows per outer pass in live V1 loop
  v
Silver-owned Verified Applied Prefix API (proposed by Phase 4A-Pre)
  | exact global namespace coverage; bounded page/bytes/time; versioned receipt
  | Gold reads only safe prefix; Silver/producer may continue above captured H
  v
Gold V1 processing foundation (Phase 4A-Core integration)
  | Gold SQLite durable receipt/work/cursors; independent invalidation cursor
  | future feed backlog + window scheduling/dirty/recompute queue
  v
Phase 4B marts → Spring API → dashboard [DEFERRED]
```

This is asynchronous processing with independent durable progress. The architecture is not a synchronous RPC chain from SUMO to Gold.

| Boundary | Durable authority/buffer | Progress and measurement | Correctness barrier | Main resource risk / overload response |
|---|---|---|---|---|
| SUMO → Outbox | Local SQLite WAL Outbox, atomic complete-cycle insert | Pending rows/bytes, oldest age, append p95/p99/max, free disk | V1 cycle is appended durably before Kafka-only producer advances its publish marker; exact event IDs and hashes | Pending queue has no row/byte ceiling. Disk warning/fault thresholds exist. Append failure raises/fail-stops the V1 Kafka-only loop; it does not dynamically pace simulation. |
| Outbox → Kafka | Outbox rows/status + Kafka broker | In-flight delivery, pending rows/bytes/age, ACK/retry/permanent-fail counts | Kafka callback ACK recorded durably; replay is at-least-once, dedup by stable event identity | `max_in_flight` bounds producer work; failed rows remain durable. Broker unavailable grows local DB until disk fault. |
| Kafka → Raw | Kafka topic/offset retention; Raw SQLite ledger; ClickHouse Raw rows | Per-partition consumer lag; received/persisted/committed rates; batch/flush latency | No auto commit/store; offsets follow contiguous persisted Raw prefix and synchronous commit | Consumer stall moves backlog to Kafka; retention is finite (V1 topic configured 7 days/1 GiB); detect retention headroom before offsets expire. |
| Raw → Bronze | Raw ClickHouse table + Raw ledger; Bronze SQLite checkpoint/intents | Offset lag by partition, batch size/duration, pending intents, CH operations, journal sequence rate | Resolve contiguous source offsets; prepare intent, reconcile output, then advance durable checkpoint | Poll-cycle and batch caps bound per pass, not total stored backlog. One worker/single-instance guard in live stack. No evidence of automatic SUMO pacing. |
| Bronze → Silver | Bronze SQLite append-only journals + Silver SQLite obligations/checkpoint | `Hc-Sc`, `Hi-Si`, oldest lag age, pending obligations, per-sequence processing duration | Bronze authority preserved; Silver advances each independent cursor only after obligations/reconciliation; invalidations handled before changes in current pass | Silver 10-row cap plus 2 s wait is a source-confirmed scheduling limit. Bronze keeps appending independently. Disk/journal growth lacks a pruning protocol in this phase. |
| Silver → Gold | Silver-owned public feed (proposed); Gold durable SQLite (Phase 4A-Core) | Safe-prefix distance, feed page/bytes, ack rate, invalidation lag, work/window backlog | Silver emits only verified prefix and receipt; Gold commits receipt/work/cursor atomically and consumes invalidations independently | Feed doesn't regulate SUMO or Bronze. If Gold slower than Silver, durable source journals remain the buffer under the no-prune assumption; readiness degrades before storage risk. |
| Gold → 4B | Gold processing state; later mart/storage contract | Separate work/window/mart rates and backlog | Gold ingestion ACK does not imply window completion or mart publication | Per-event contribution to multiple windows and later KPI/mart compute may be slower than feed. Not certified by Phase 4A-Pre. |

## 3. Source-grounded pipeline audit

### SUMO producer and Outbox

- `Visualize/simulation/backend.py` refreshes observation caches at `OBSERVATION_INTERVAL_SEC`, default one **simulation** second. `Visualize/app/traci_runner.py` publishes when `PUBLISH_INTERVAL` is reached (default one simulation second) or an explicit `publish_asap` event occurs. `Visualize/configuration/model_parameters.yaml` and the SUMO config default to a 0.01-second simulation step. This is simulation cadence, not wall-clock event rate. Real-time pacing is optional; accelerated/unpaced execution can emit faster than wall time.
- V1 row count per cycle depends on the event mapper and participating nodes/observations. The only retained exact examples are the Phase 3 five-node 3-cycle and two-node 1-cycle tests. No universal “one event per intersection” rate assumption is used here.
- The durable Kafka producer sets `acks=all`, idempotence enabled, LZ4 compression, default 10 ms linger, 30 s delivery timeout and 10 s request timeout. These settings improve delivery/retry behavior but do not imply an event rate. The topic remains RF=1 locally, so `acks=all` is not a multi-replica durability guarantee.
- `OutboxStore.append_v1_cycle` stores the complete frozen cycle atomically using a SQLite `BEGIN IMMEDIATE`/commit path. Store runs WAL + `synchronous=FULL`; the Outbox publisher is a separate worker, has configured max in-flight (default 32), polls Kafka, retries retryable delivery failures, records ACKs, and cleans acknowledged rows after configured retention (default seven days).
- Pending row/byte/oldest-age and append latency metrics exist. Free disk warning/fault thresholds default to 512 MiB/64 MiB; they are free-space thresholds, not a maximum Outbox size. There is no configured pending row/byte quota. Kafka-only V1 raises on cycle append failure, so fault is visible and fail-stop; no rate-control callback to SUMO based on downstream lag was found.
- Class: **SOURCE-CONFIRMED LIMIT** for finite free-disk fault behavior and unbounded-by-row-count pending backlog; **POTENTIAL BOTTLENECK** for FULL-synchronous cycle append/write-gate under high event volume; append timing metrics exist but no representative runtime samples were found.

### Kafka and Raw V1

- `docker/kafka-init.sh` creates `traffic.observations.v1` with three partitions, replication factor one, seven-day retention and 1 GiB `retention.bytes` configuration. This local single-broker configuration is not high availability. Confirm actual broker topic configuration at benchmark time; environment/runtime overrides can differ.
- Raw V1 settings default to batch size 500, flush interval 500 ms, max buffered bytes 32 MiB, fixed topic/group. Consumer disables automatic commit/offset store, uses `auto.offset.reset=error`, read-committed isolation and a fixed local group instance. On flush it inserts missing data into ClickHouse, moves ledger records through persistence states, computes each partition's contiguous prefix and commits offsets synchronously only after its safe boundary is durable. Restart refuses topic UUID changes, expired history and broker commit ahead of local persisted confirmation.
- The topic has three partitions but current local Raw runtime is single consumer owner; adding same-group processes is not automatically useful and partition ordering/ownership must be reviewed. Actual V1 runtime batch/flush and per-record validation/ClickHouse insert cost require load measurement. Kafka theoretical throughput is not evidence.
- Class: **SOURCE-CONFIRMED DESIGN** for offset safety and three partitions; **POTENTIAL BOTTLENECK** for ClickHouse insertion/ledger flush and one consumer process; **NOT MEASURED** for sustainable rate and outage catch-up. Kafka retention can remove data before a prolonged outage is recovered; fail closed on retention gap.

### Bronze V1

- V1 observation settings default to 500 records per batch and one-second poll. Each worker pass snapshots available source offsets per configured partition, resolves a bounded batch, classifies semantic changes, reconciles ClickHouse outputs, and advances a contiguous checkpoint only after the local operation is durable. V1 uses SQLite identity, sequence allocators, intents, change/invalidation journals, conflicts and recovery state. Raw V1 and Bronze V1 have separate databases/checkpoints.
- The V1 processor is sequential over configured partitions within its single worker loop. Compose includes a single-instance/single-worker setting for the Bronze service; adding parallel workers against SQLite must not be presumed safe. Classification, multiple target writes, conflict processing, retries and recovery costs vary by accepted/duplicate/conflict/quarantine paths.
- Class: **SOURCE-CONFIRMED LIMIT** for bounded batches/poll and one live owner; **POTENTIAL BOTTLENECK** for per-event semantic/CH reconciliation and partition-serial service; **NOT MEASURED** for capacity under continuous SUMO input. Existing `de/tools/bronze_sustained_acceptance.py` is a short acceptance workload (120 simulation seconds) but no retained report establishes a sustainable rate or burst drain objective.

### Silver V1

- Compose sets `SILVER_BATCH_SIZE=500`, `SILVER_POLL_INTERVAL_SEC=2.0`, a single worker. The live loop calls `run_v1_journal_pass(batch_size=min(settings.batch_size,10), max_entries=10)`. `max_entries=10` counts both invalidation and change entries because invalidations are processed first within the same `processed` budget. It is an explicit hard per-pass cap; source and comments do not establish that it is required for semantic correctness. Its original rationale is **UNVERIFIED**.
- Every outer pass waits `poll_interval_sec` after success even if a full ten-entry batch was returned and backlog remains. The idealized sustained upper bound from count/sleep alone is approximately `10/2 = 5` journal entries/s before processing overhead; actual rate can be lower, and change entries/s can be lower because invalidations share the cap. This is arithmetic from config, not a measured rate or production SLO.
- Before new journal work, `_initialize_v1` recovers pending obligations in batches until none remain. Inside a pass, source binding is reverified without full-history mode, frozen boundaries are checked, pages are sequence-checked and events are processed sequentially. Invalidation stream is exhausted to captured highwater before change stream; this protects correctness, but under a continuous invalidation backlog it can consume each shared 10-entry budget and starve changes. That starvation scenario is a **POTENTIAL LIVENESS RISK**, not an observed production failure.
- Per accepted sequence, source/journal identity is read, archive context is resolved, fact/dimension obligations are created/reconciled through ClickHouse repository, final Bronze point read is performed, then Silver local validity/cursor are finalized. Invalidation reads local provenance and updates local/CH suppression obligations. The exact number of SQL/CH round trips varies by event and repository implementation and is not instrumented as a per-event counter.
- Canonical page read currently requires `Sc == Hc` and `Si == Hi` at its boundary. Each event is then rechecked against Bronze current identity, reconciled via `read_valid_observation`, resolves archive context, fetches three ClickHouse fact families, fetches frozen obligation JSON, reconciles each expected row, rechecks local validity, and finally rechecks the global boundary. Potential N+1 operations include two current-validity reads per event, a Bronze SQLite/CH point reconciliation, three CH event-row fetches, per-target SQLite obligation queries, and per-expected-row CH reconciliation. Archive resolver may be repeatedly invoked; cache behavior must be inspected/measured before changing it. A Silver batch API can share verified static binding and captured boundary checks per page, but must keep per-event identity/hash/obligation/evidence checks and repeat a final relevant-identity/binding guard. No broad cache or bulk verification is approved without a race-equivalence proof.
- **Index evidence:** Bronze `v1_identity` has primary key `(namespace,event_id)`; `v1_changes` primary key `(namespace,change_sequence)`, unique `(namespace,event_id,revision)`, and `v1_change_event` index on that event/revision key; `v1_invalidations` has unique `(namespace,invalidation_sequence)` and primary key `(namespace,invalidation_id)`. Silver `silver_v1_obligation` primary key is `(namespace,source_kind,source_sequence,target_table,row_key)` with a partial pending index; `silver_v1_event_validity` primary key is `(namespace,event_id)`. Bronze page range and event lookups align with these SQLite keys. Existing canonical reader's validity query additionally filters run/digest/simulation-time and orders by event ID; no composite index for that combination is declared in Silver checkpoint DDL, so query-plan measurement is required before reusing it for feed enumeration. Silver fact `fetch_v1_event_rows` filters by run/digest/event, which matches leading ClickHouse MergeTree sort-key fields in migrations 008; still measure actual query latency/parts/rows. ArchiveResolver loads and validates manifest and topology files on each `resolve` call; no cache is visible in its implementation. Repeated file hashing is therefore source-confirmed work, but whether it is material is unmeasured.
- **Per accepted sequence call graph (structural, not fixed query-count promise):** journal range read by sequence → current Bronze identity point lookup → `read_valid_observation` SQLite identity + completed-intent lookup and Bronze ClickHouse observation/output reconciliation → two archive file loads/hash and topology/event validation → three Silver ClickHouse fact-family fetches → three Silver SQLite expected-obligation queries → per expected fact-row ClickHouse reconciliation (one intersection plus one per direction for traffic and signal) → final Silver current-validity point lookup and Bronze current point-read in processor finalization. Invalidation path has its own page/provenance/current identity/obligation/local-validity checks. This is a plausible query-amplification bottleneck; actual per-event calls and time are to be captured by instrumentation, not assumed.
- Class: **SOURCE-CONFIRMED LIMIT** for 10 total journal entries and post-pass 2 s wait; **POTENTIAL BOTTLENECK** for sequential multi-store query amplification; **NOT MEASURED** for sustainable Silver applied rate.

### Gold V1 processing foundation

- Gold runtime sources currently show a two-second poll, Silver-fetch ceiling 500 and `max_windows_per_cycle=1`. Its existing `run_cycle` snapshots source bounds, polls the current reader, discovers runs, creates candidate windows and processes up to the configured window cap. This current code is not yet the proposed Silver canonical sequence-feed consumer; the Gold source-feed adapter/checkpoint integration remains a Phase 4A-Core dependency.
- Feed-read throughput, durable Gold acknowledgments, window scheduling throughput, dirty/recompute throughput and future mart compute throughput are distinct capacities. Current `WINDOW_SIZES_SEC` is `(60,300)`; one observation can affect multiple durations. The `max_windows_per_cycle=1` cap plus fixed poll can limit window scheduling independently of page read rate. Future 4B formulas/joins and serving are unknown and unmeasured.
- Class: **SOURCE-CONFIRMED LIMIT** for current bounded windows-per-cycle and poll settings; **POTENTIAL BOTTLENECK** for repeated run/source polling and window fan-out; **NOT MEASURED** for source-feed capacity or end-to-end Gold throughput.

## 4. Safe worker scheduling and throughput optimization

Do not simply increase Silver's batch 10 cap or reduce its poll interval. First measure time by operation, change/invalidation mix, obligations recovered, query count, batch duration, source lag, and lock/IO pressure.

Candidate separate remediation: backlog-aware scheduling that immediately runs another bounded pass when verified backlog remains and dependencies are healthy; waits only when caught up; applies bounded backoff under dependency failure. Keep a shutdown check and CPU/IO fairness yield. Preserve obligation recovery before any newly dependent work. Preserve exact source binding/sequence checks.

The current “all invalidations through H first, then changes” policy needs a safe fairness redesign before using immediate continuation. If invalidations continuously occupy all 10 slots, changes can starve. A separate change budget is safe only if a change entry's live current identity is checked and the consumer does not emit a bundle if an unapplied conflict exists; sequences with unresolved conflict must stop the contiguous change prefix. A bounded interleave or dynamically reserved slots requires tests proving conflicts, invalidation durability and no starvation of either stream. Keep invalidation priority and local invalidation obligations; do not mark an event valid because its invalidation work is waiting. Backlog-aware loop is a **liveness/latency improvement**; increasing useful batch work after profiling is a **throughput candidate**. Both require separate Phase 3 pipeline-remediation approval, not Phase 4A-Pre implementation.

Canonical batch optimizations, in increasing risk order:

1. Share one static source-binding verification and one frozen allocator/cursor read per bounded page; do not repeat full-history verification per item. Existing source binding already has a `full_history=False` path for polling/resume and full continuity scan for cold bootstrap/new binding or explicit rebootstrap.
2. Instrument SQLite/ClickHouse call count, query type, rows/bytes, latency, archive resolution, and object allocation per entry. Add measurements before caching or broad batching.
3. If N+1 cost is demonstrated, introduce bounded chunked reads that retain the same exact keys/hashes and current-identity checks; verify raw database rows against independent per-event reconciliation oracle. Final boundary check and per-event after-read guard remain.
4. Never cache current validity across source mutations; never omit fact/obligation reconciliation to improve a benchmark.

Producer pacing is a later approved control-runtime decision. Prefer measuring buffering and processing first. If sustainable arrival exceeds capacity, slowing simulation's **wall-clock execution** may be considered only through explicit TraCI control that holds simulation steps while retaining the same step length, observation cadence in simulation time, and every event. Changing SUMO step length changes simulation semantics; reducing observation frequency changes data semantics; dropping observations is prohibited. Kafka publish throttling alone shifts backlog into Outbox and is not end-to-end pacing. Controlled degraded/fail-stop operation is required if no buffer has sufficient headroom and no safe pacing is enabled.

Future Gold should use only new feed sequences, persist work intents and local cursor atomically, track independent invalidations, schedule affected windows incrementally and dirty only impacted work. This can eliminate repeated full-source scans, but does not prove the window worker or 4B mart compute keeps up. Those rates have separate gates.

### Optimization classification register

| Change | Classification | Evidence / status | Root-cause mechanism and alternative | Correctness/resource gate | Workstream |
|---|---|---|---|---|---|
| Verified applied prefix in new Silver API | Correctness + liveness | Current reader blocks on full source catch-up; `Sc/Hc` source cursors exist. | Read only `min(Hc,Sc)` and truncate before unresolved conflict; alternative wait for all Bronze, which can starve under continuous arrival. | Race tests for current conflicted identity, invalidation lag, append and final guard. Bounded memory. | Phase 4A-Pre |
| Backlog-aware Silver pass scheduling | Liveness + latency; possible throughput | Current loop sleeps 2 s after every pass; 10 entries cap. Source-confirmed limit, impact not measured. | Continue while safely backlogged; alternative lower fixed poll, which adds idle polling but doesn't remove per-pass wait behavior. | Fairness, shutdown, dependency backoff, transaction/IO limits, no invalidation starvation. | Separate Phase 3 pipeline remediation approval |
| Increase Silver batch cap after profiling | Throughput candidate | 10 is hard limit, reason undocumented; no performance profile. | Raise only if query batching/overhead proves lower batch size limiting. | Peak memory, SQLite busy, CH load, rollback/recovery duration, conflicts. | Separate pipeline remediation approval |
| Share page boundary/static verification | Resource reduction | Existing canonical scan has repeated global boundary checks per page and per-event checks. | Boundary checks once per batch; keep event evidence checks. | Frozen source identity race tests and exact independent oracle. | Phase 4A-Pre |
| Chunked bounded source evidence queries | Throughput/resource candidate | Potential N+1 in `_bundle`; impact unmeasured. | Batch indexed lookups by bounded event IDs after profiling. | Same rows/hashes and per-event ownership checks; compare query trace and semantic output. | Pre design; implementation separately gated after evidence |
| More consumer processes | Throughput candidate | Raw topic has 3 partitions; SQLite authorities and single-owner locks exist. | Could use partition parallelism only where state owner and ordering allow. | Do not parallelize Bronze/Silver SQLite sequence writers without serializable allocator/checkpoint design. | Separate infrastructure/runtime approval |
| SUMO real-time pacing | Overload control | Optional realtime pacing path exists; default capacity/profile not measured. | Hold wall-clock step progression without changing simulation step/observation semantics. | Validate command/control support, Outbox and resume behavior. | Separate producer/control-runtime approval |
| Gold incremental work/windows | Resource + throughput | Current Gold polls current fact sources and caps windows; proposed canonical API absent. | Durable sequence work avoids repeat full rescans; per-window compute separately benchmarked. | Gold Core acceptance and 4B workload tests. | Phase 4A-Core / Phase 4B |

## 5. Backlog and overload policy by stage

Policy priority is **preserve all source semantics, report lag early, buffer temporary overload within measured disk limits, recover/catch up when measured `μ>λ`, and fail/degrade visibly before a durable limit is exhausted**. Do not claim a queue feeds backpressure unless an explicit control link exists.

| Stage | Buffer + measurement | Recovery/limit | Actual pacing capability and recommendation |
|---|---|---|---|
| SUMO → Outbox | SQLite Outbox pending row/byte counts, age, append latency; filesystem free bytes | ACK rows cleaned after retention; pending has no configured max size; warn/fault at free-disk thresholds; fail-stop if append fails | Current code does not feed downstream lag back to SUMO. On low disk/outbox fault raise degraded/stop, preserve durable rows. Consider real-time pacing only in separate producer work. |
| Outbox → Kafka | Outbox status, in-flight (max 32 default), oldest pending, permanent failures | Retry; ACK cleanup; pending stays durable; alert before disk threshold | Outbox is a buffer, not an automatic rate limiter. Kafka outage accumulates rows until disk pressure. |
| Kafka → Raw | Broker offsets and Raw partition ledger; consumer lag + retention headroom | Kafka retention configured 7 days/1 GiB for V1 topic; Raw retries/restarts from durable offsets; retention gap faults, never reset | Stop/slow Raw consumer when ClickHouse/ledger unavailable. Broker buffering finite; alert and restore before retention expiry. |
| Raw → Bronze | Raw ClickHouse offsets/ledger and Bronze checkpoints/pending intents | Retry gaps, recover intents, batch size configured 500; source retention/storage policy must be measured | Bronze polls independently; no automatic SUMO backpressure. Degrade if lag age rises; inspect ClickHouse/local disk before increasing workers. |
| Bronze → Silver | Bronze journals and Silver local obligations/cursors; sequence lag and age | No prune/rewind assumed for Phase 4A-Pre; file size/WAL/backup growth must be measured and budgeted | Silver can process verified prefix; backlog-aware Silver worker is separate gated remediation. On persistent μ<λ alert/degrade and retain source. |
| Silver → Gold | Bronze journal retained; Silver API page; Gold local receipt/work DB | bounded page/byte/time; Gold ack rate and cursor lag; no pruning protocol | Silver API will not block producer. Gold must throttle its own work, apply local durable backpressure and mark degraded before storage limits. No unbounded in-RAM staging. |

**Recommended initial overload mode:** Strategy A, durable buffer and catch-up for temporary overload, combined with Strategy D, visible degraded/non-ready status and operator response at sustained overload/storage risk. Keep complete events; never drop. Do not add Redis/Flink/Spark/new Kafka topics. Evaluate Strategy B only after pipeline capacity is measured and control semantics approved. Strategy C changes are targeted to measured root causes and preserve SQLite single-writer constraints.

## 6. Bootstrap, polling, recovery and retention cost

1. **Cold bootstrap:** new binding only; scan the retained SQLite journals once in bounded pages from sequence 1, verify allocator/count/linkage, reconcile required Silver obligations as specified, discover journaled runs, and initialize consumer cursor at zero only after verification. Measure bootstrap entries/s, total CPU/IO, elapsed wall time, peak RSS and DB/WAL size.
2. **Normal polling:** verify static binding and allocator/cursor relation; read `min(Bronze H, Silver applied cursor)` safe prefix, capped by bytes/time/count; no full-history scan.
3. **Restart, unchanged binding:** read persisted cursor, verify it is in range and the next expected sequence plus static source identity; resume. Do not repeat full history. A checkpoint ahead of source, missing next row or binding mismatch faults.
4. **Source replacement:** stop and require operator-created new namespace/bootstrap generation plus reconciliation. Never reset an old consumer cursor in place.
5. **Historical backfill/reconciliation:** separate, explicitly scoped operation with isolated progress/receipts and resource budget; do not block normal cursor with a fake latest snapshot.

No journal pruning is introduced. Because Silver/Gold feed progress and conflict history rely on retained Bronze sequence rows, storage, backup, restore and bootstrap time grow with retained history. Inventory Bronze SQLite DB/WAL growth, checkpoint backups and restore time at representative years/event volumes before production retention promises. A pruning/compaction protocol would need signed retention floors, consumer registrations/leases, tombstones/anchors, source-binding versioning, bootstrap checkpoint and independent test plan; it is deferred.

## 7. Latency taxonomy

Never report one vague “end-to-end latency” from polling configuration alone.

| Metric family | Start → end | Phase ownership / notes |
|---|---|---|
| Event transport latency | SUMO event capture wall-clock → Kafka publish ACK → Raw persistence | Producer/Outbox/Kafka/Raw. Instrument timestamps and clock source; simulation time is not wall-clock. |
| Semantic latency | Raw persisted → Bronze ACCEPTED/CONFLICTED → Silver durable obligations and applied cursor | Bronze/Silver. Report per event and sequence lag/age. |
| Incremental consumer latency | Silver applied safe prefix → Gold feed page → Gold durable local acknowledgment | Pre API + Gold Core; test-only harness can measure Silver feed, not claim Gold runtime. |
| Window latency | Event simulation-time interval → eligibility → scheduled/completed Gold window | Gold Core, and future KPI semantics. Not the same as feed lag. |
| UI freshness | Gold mart publication → Spring API query → Dashboard refetch/render | Deferred Phase 4B. TanStack `staleTime` is cache freshness, not pipeline throughput or refetch cadence. |

Polling interval is an upper-level wake schedule component, not a service rate. Simulation-cycle cadence differs from wall-clock event rate. Silver applied cursor is not Gold acknowledged cursor. A run's current lifecycle is not full historical namespace coverage. No end-to-end freshness SLO is set before whole-path trace evidence and owner approval.

## 8. Reproducible capacity benchmark suite

Use actual SUMO observations or a replay generated from a documented real traffic capture with identical schema, size distribution and semantic paths. Record repository HEAD, hardware (CPU model/cores, RAM, storage, filesystem), ClickHouse/Kafka settings, Compose/profile, service resource limits, seeds/topology, producer controls, event counts/bytes and every dataset reduction. Run independent correctness oracle throughout. Separate throughput and correctness outcomes; a mock-only consumer is clearly labeled test-only.

| Scenario | Procedure | Required evidence / gate |
|---|---|---|
| A — baseline | Normal SUMO wall-clock pacing and current supported topology/population. | Measured producer events/sec and bytes/sec; Raw/Bronze/Silver rates and lags; CPU/RAM/disk; exact sequence/data reconciliation. |
| B — accelerated | Candidate 1x/2x/5x/10x pace or controlled producer rates; these are experiments, not promised support modes. | Actual events/sec (not multiplier label), first saturated stage, lag slope and resource profile. |
| C — sustained | Continue long enough that steady behavior, RSS, SQLite WAL and ClickHouse pressure are visible; duration chosen from retention/steady-state rationale. | No producer pause; rates and backlog time series; errors/retries; correctness oracle. Short fixture batches cannot pass this gate. |
| D — burst/catch-up | Raise λ above measured μ temporarily, then return to baseline. | Backlog grows in burst, all sequences retained, then drains by owner-approved catch-up target without manual cursor reset. |
| E — dependency outage | Stop/fault Kafka, Raw/CH, Bronze dependency, Silver dependency, then restore one at a time. | Durable buffer growth, explicit fault/health state, no silent loss, bounded recovery and no expired retention. |
| F — continuous conflicts | Inject representative accepted-then-conflicted events while new events arrive. | Change/invalidation progress, no stale valid output, no permanent stream starvation, independent oracle exact. |
| G — bootstrap/restart | New test consumer from zero; separately restart from saved durable cursor after partial and completed acknowledgment. | Cold bootstrap cost separated from normal resume; no full rescan on ordinary restart; no manual reset. |
| H — local resource limit | Run supported service subset under declared CPU/RAM/disk constraints on project-local hardware. | No unlimited-resource assumption; cap/backpressure/failure behavior and bounded RSS/WAL verified. |

Minimum metrics: `producer_events_per_sec`, producer bytes/sec, Raw ingested events/sec, Bronze semantic changes/sec, Silver applied change and invalidation entries/sec, test Gold consumer entries/sec and durable ack/sec; Kafka consumer lag; Bronze raw-offset lag; Bronze→Silver change/invalidation lag; Gold change/invalidation lag; source→Silver and Silver→Gold p50/p95/p99; batch duration distribution; catch-up duration; CPU; peak RSS; SQLite WAL and checkpoint DB sizes; ClickHouse query count/latency/rows; retries; stale boundaries; source churn; Outbox pending rows/bytes/age; disk free.

### Capacity worksheet (owner completes before load gate)

| Parameter | Required value/evidence |
|---|---|
| SUMO emission profile | Observed observations per cycle and bytes/cycle for each approved topology/run mode |
| Wall-clock producer rate | Measured events/s and bytes/s with pacing/accelerated mode identified |
| Simulation speed relation | Simulation seconds per wall-clock second, measured |
| Topology population | Actual intersections/participants and supported population range |
| Event size | Average, p95 and maximum serialized bytes; canonical bundle size distribution |
| Conflict/invalidation rate | Measured typical profile plus injected stress profile |
| Retained history | Journal entries/DB bytes/age at bootstrap and target retention horizon |
| Available resources | CPU cores/limits, RAM/container limits, disk throughput/capacity, ClickHouse/Kafka limits |
| Required steady-state rate | Owner-approved events/s and semantic changes/s with headroom assumption documented |
| Burst profile | Owner-approved λ/μ ratio and burst duration |
| Catch-up objective | Owner-approved time to drain defined burst/outage backlog |
| Freshness objective | Measured source→Silver and Silver→Gold distributions; later owner-approved target |
| Resource ceilings | Owner-approved RSS, DB/WAL, disk free and query pressure ceilings |

## 9. Verdict and false-pass prevention

Each benchmark reports separate outcomes:

- **CORRECTNESS PASS/FAIL:** independent semantic coverage, validity, checkpoint, invalidation and replay oracle.
- **CAPACITY verdict:** `SUSTAINABLE AT TESTED WORKLOAD`, `BURST RECOVERY VERIFIED`, `CAPACITY INSUFFICIENT`, `INCONCLUSIVE`, or `NOT MEASURED`.
- **OPERATIONAL READINESS PASS/FAIL:** buffers, quotas/limits, lag/fault reporting, retention headroom, recovery and operator actions.

Never pass capacity with producer paused, fewer events, skipped slow/invalid records, cursor-only progress, disabled reconciliation, a short run with no backlog, mean-only latency, mock-only integration, or buffer size presented as processing capacity. A zero queue is not proof that downstream obligations or invalidations completed. Correctness can pass while capacity fails; keep those verdicts separate.

## 10. Evidence classes and limitations

- **SOURCE-CONFIRMED LIMIT:** Silver's `max_entries=10`, 2 s poll, per-pass delay; Raw batch/flush/buffer settings; Gold current window cap and poll; topic partitions/retention; Outbox disk thresholds/max-in-flight and ACK cleanup; default V1 publication is disabled.
- **POTENTIAL BOTTLENECK:** sequential Silver per-event multi-store reconciliation; current global invalidation-first shared budget; SQLite FULL sync on cycle append; Raw/CH flush path; Bronze sequential partition processing; Gold window fan-out and one-window scheduling.
- **UNVERIFIED HYPOTHESIS:** any actual service with μ below λ; exact event rate for arbitrary SUMO topology; whether query N+1 dominates CPU/IO; whether larger batches improve net rate; whether local SSD/CH is resource-saturated.
- **CONFIRMED BOTTLENECK:** none established by retained performance evidence for the complete V1 path.

The primary `docker-compose.yml` contains no explicit CPU/memory limits for the relevant pipeline services. Therefore local deployment resource ceilings must be declared by the benchmark environment; no code default establishes a safe supported hardware profile.

## 11. Phase gates

- **Phase 4A-Pre:** freeze exact Silver API, verified applied prefix, DTO/receipt/error semantics, source evidence batching rules, and Stage A public-contract correctness/capacity test design. Pre acceptance uses real public Silver API and real Bronze/Silver fixtures (including real ClickHouse integration where required), independent journal oracle, and a minimal test-only durable consumer harness. It proves Silver feed capacity only; no Gold runtime claim.
- **Phase 4A-Core:** implement actual Gold adapter, durable Gold SQLite checkpoint/work intents, independent invalidation coordination, dirty-generation fencing, window scheduling, replay isolation and recovery. Stage B integration acceptance runs only after this exists; Pre is not circularly dependent on it.
- **Pipeline performance remediation:** worker scheduling/batch/query/Outbox pacing changes in Raw/Bronze/Silver/producer are separate gated work items; do not silently broaden accepted Phase 1–3 contracts.
- **Phase 4B:** KPI calculation, marts, serving API and dashboard freshness/latency are deferred and need their own capacity tests.
