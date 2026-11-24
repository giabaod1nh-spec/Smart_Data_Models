# Performance requirements and operating workload

**Status:** proposed acceptance framework; numerical service objectives require owner approval. No production workload or latency SLO is inferred from the diagnostic Core trial.

## End-to-end model

For stage `i`, define actual arrival rate `λᵢ` and sustainable service rate `μᵢ` in that stage's own unit. A persistent `μᵢ < λᵢ` grows backlog at approximately `(λᵢ-μᵢ)t` only when the same unit and scope are counted at both ends. Fan-out and semantic disposition require explicit conversion; observation, delivery, change, fact, obligation, membership and completed-window rates are distinct.

| Stage | Arrival/work unit | Service/output unit | Queue and durable boundary | Required metrics |
|---|---|---|---|---|
| SUMO/capture | simulation steps and scheduled cycles | validated observation cycles and bytes | simulation schedule; capture queue if configured | step/capture rate, step lateness, cycle duration, observations/cycle, bytes/cycle |
| Outbox | frozen events/cycle and bytes | Kafka-ACKed delivery rows | SQLite Outbox transaction and rows/bytes | append/ACK rate, tx/commit duration, queue rows/bytes/oldest age, retries, disk/WAL |
| Kafka | topic deliveries per partition | Raw persisted and committed offsets | broker retention and consumer group | input rate, broker lag/oldest age, partition skew, retention headroom, retries |
| Raw | physical deliveries | confirmed Raw deliveries or quarantine | CH evidence + SQLite ledger before offset commit | poll/batch/flush/insert/reconcile/commit rates, bytes, partition backlog age |
| Bronze | Raw deliveries and semantic identity decisions | journal changes, accepted observations, conflict/quarantine dispositions, output obligations | intent then CH reconciliation then SQLite journal/checkpoint | deliveries/s, changes/s by disposition, fan-out, intents pending/age, CH calls/latency, checkpoint lag |
| Silver journal | Bronze change/invalidation sequences by lane | fully reconciled Silver applied sequences and facts/obligations | frozen obligations and source-bound cursor/validity transaction | arrival/service by lane, age, facts/sequence, obligations/sequence, nested op time, retries |
| Silver public feed | applied contiguous sequences and canonical bytes | verified bounded receipts and disposition records | fixed read boundary and source end verification | page count/entries/bytes, source-query count/cost, retries, boundary lag, p95/p99 |
| Gold ACK | verified change/invalidation receipts | durable Gold sequences | Gold SQLite independent lane transactions | sequences/s, page bytes, ACK delay, invalidation lag, receipt and DB size |
| Gold windows | eligible memberships / dirty generations | completed proofs for 60s and 300s windows | local PENDING/RUNNING/COMPLETE work generations | eligible work/s, memberships/s, oldest age, reads/member, rework, restart rate, CPU/CH/SQLite |

## Required outcomes before performance acceptance

1. **Correctness preservation:** no data dropping, sampling, lower capture rate, weakened authority, skipped validation, relaxed source retention, unsafe offset/cursor advancement, or changed availability semantics.
2. **Approved representative workload:** freeze normal SUMO step/capture cadence, topology/participants, scenarios, payload distribution, source bytes/s, semantic conflict/invalidation mix, and duration. Phase 1 15.57 deliveries/s is historical Raw output; Core 4.95 obs/s is diagnostic offered rate; neither alone is the future requirement.
3. **Stable queues:** for every required stage, early/middle/late steady-state slopes are statistically compatible with bounded/cyclic backlog within measurement uncertainty. Report rows/bytes/age, not only a final zero counter. Test each unit separately.
4. **Recovery headroom:** define a finite approved disturbance backlog `B` (in each relevant stage's native unit) and maximum recovery time `T`. Required service rate under resumed normal arrivals is `μ ≥ λ + B/T`; derive each target after owner supplies `B` and `T`. A near-equal average rate is not sufficient. Verify backlog drains while normal production continues.
5. **Gold work:** independently show sustainable eligible-window completion, 60s and 300s separately, plus membership throughput and dirty-window recomputation. Zero Silver-to-Gold cursor lag alone cannot pass.
6. **Latency:** define source-to-durable-Gold-ACK and eligible-to-completed-work percentile objectives from product/operation needs after timestamp instrumentation. Current Core latency is an observed baseline, not an accepted objective.
7. **Resources:** owner sets per-process/container CPU, RSS, storage/WAL and CH/Kafka envelope on specified hardware. Show safe behavior under an approved constrained profile; no claim of indefinite outage buffering.
8. **History:** demonstrate indexed access, startup, replay, SQLite metadata and CH merge behavior across expected retained history. Specify retention and disk headroom before live deployment.
9. **Realtime coexistence:** run full normal app stack and prove no unacceptable SUMO pacing, Projector/Orion or Dashboard-serving regression against matched baseline.

## Workload profiles and gates

| Profile | Frozen meaning | Use |
|---|---|---|
| W1 Historical Comparable | Core tool profile: `SUMO_STEP_LENGTH=1.0`, 600 simulation seconds, five-node participant set, exact archived topology/payload/scenario/seed/pacing and isolated stack. | Reproduce historical before/after; not intended-operation certification. |
| W2 Intended Operation | Normal YAML step 0.01s; approved capture/cache/publish cadence, nodes and scenario triggers; actual normal application stack. Current ~5 observations/wall-second is an estimate at 1x pacing, to verify from scheduled and completed capture counters. | Operating workload after G0, with rate/bytes distribution evidence. |
| W3 Controlled Stress | Declared controlled offered-load sweep using accelerated SUMO and/or deterministic component fixtures; distinguish source type and preserve event semantics/payload. | Saturation, headroom and overload behavior. Fixture-only results do not certify canonical or E2E rates. |

All profiles record commit, full config/env hash, SUMO step and simulation/wall pacing, cache/capture/publish interval, expected schedule, completed captures, entities/cycle, bytes/s, participants/topology, scenario/seed, versions, initial retained history/backlog, resources and contention. Do not reduce fidelity/cadence or permit slow simulation to lower offered work.

### G0 — initial workload and compatibility freeze

Before implementation, owner approves W1/W2/W3 bounds, intended SUMO config and capture semantics, participant topology/scenario, complete path endpoint, existing authority/durability/recovery invariants, available hardware, initial safe diagnostic resource limits, isolated benchmark/fault approach, and prohibition on source reduction. G0 authorizes PERF-00B hardening, instrumentation and controlled profiling. It does **not** require final numeric latency, throughput or recovery SLOs.

### G1 — evidence-based performance objective freeze

After PERF-01 and component/whole-path profiling, engineering proposes normal offered throughput, source-to-ACK and eligible-work completion latency distributions, stage backlog/age envelope, burst/outage demand and global recovery deadline, resource/disk/retention limits, realtime regression tolerance, and benchmark duration. Every proposal includes measured evidence, product/operation rationale, hardware impact, trade-offs and validation method. Owner approves G1 before optimization acceptance targets are treated as pass/fail. Do not invent ten-second latency or per-stage rates before evidence.

## Tiered duration methodology

Use L0–L4: L0 operation/batch microprofile; L1 2–5 minute smoke; L2 10–15 minute matched iteration; L3 30–60 minute integrated qualification with multiple 300s windows; L4 2–4 hours or longer where history/resource drift warrants. These are duration recommendations, not objectives. Do not run L4 before shorter levels show correctness and a plausible stable operating point. Define warm-up, measurement and early-stop rules per level in `acceptance.md`. Select final duration using W2/W3, completed work proofs, backlog trend confidence, repeated-run variance, resource/history growth and machine envelope. Retain all early termination evidence as FAIL or INCONCLUSIVE.

Sampling must yield at least 30 observations per analysis segment where practical; account for autocorrelation, workload phases and measurement overhead. Warm-up is predefined and retained separately. Four-hour endurance is reserved for final sustained/history confidence where L1–L3 and observed growth justify it, not every optimization iteration.

## Backlog and headroom method

For each edge, sample source high-water and durable consumer cursor/queue in the matching unit, plus age buckets and bytes. Fit backlog slope separately over early/middle/late steady intervals, report interval estimates and compare against zero. Use rolling rates and bounded batch-cycle phase alignment to distinguish cyclic queue from drift. A stage passes only if no positive sustained trend beyond agreed noise margin, max queue/storage remains within approved budget, oldest age meets an approved objective, and finite burst backlog drains with source continuing. Derive headroom from `μ_target ≥ λ_normal + B/T_recovery`, use a controlled offered rate sweep to establish margin, and repeat runs to expose variance.

This formula is a planning estimate, not proof of global recovery. Draining an upstream queue can temporarily raise downstream arrival rates; include all downstream queues and Gold eligible work. Define recovery start as end of genuine finite burst or restored dependency; recovery end is when **all** queues return to the approved steady envelope while W2 production continues.

Global recovery accounting must include Outbox pending rows/age; Kafka lag; Raw pending deliveries; Bronze unresolved intents/source backlog; Silver change and invalidation lag plus pending reconciliation obligations; Gold change/invalidation lag; current-generation eligible work and dirty/recompute work; retries/resource pressure; and independent observation accounting. Distinguish OPEN, not-yet-eligible windows from incomplete eligible work. Report each stage recovery, maximum downstream backlog after upstream drain, oldest event/work age, total elapsed global recovery and correctness. No stage-only catch-up is a global recovery pass.

Gold's event-to-window work demand is at least two membership contributions per eligible event for the fixed 60s and 300s specs; unique windows per event vary by event time/run and boundary. Conflicts can invalidate and regenerate multiple window obligations. Measure membership reads per completed proof and invalidation impact cardinality rather than deriving total work from observation count alone.

## Overload and buffering rules

Keep bounded in-memory queues and durable Outbox/Kafka/Raw/Bronze/Silver evidence. Publish visible degraded/non-ready signals before approved storage/retention limits. Producer backpressure may pause/fault only through existing explicitly safe behavior; no dropped/overwritten evidence. Calculate retention runway from broker low/high watermarks, durable queues, source history and disk; define operator action before runway is exhausted. No additional distributed queue/database is proposed until measurements show existing safe architecture cannot meet approved targets.
