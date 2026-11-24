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

## Workload profiles

Maintain two comparisons:

- **Historical comparable profile:** reproduce Core benchmark settings (including one-second diagnostic step) solely for before/after comparability; same topology, payloads, seeds/scenario, container versions, isolated state and offered workload.
- **Intended operation profile:** use the current approved SUMO configuration and actual normal stack with exact run manifest, topology, capture schedule and scenario triggers. Do not use one-second capture if normal semantics specify another cadence.

Both profiles record commit, config hash, step length, capture cadence, participants, topology digest, scenario/seed, event size distribution, rate/bytes, pacing, initial data/history/backlog, service image/runtime/database versions, CPU/RAM/disk/filesystem, Docker limits, external contention and test times. Before/after runs require equivalent inputs and isolated state.

## Provisional duration methodology

**Recommended starting design, pending owner approval:** at least a four-hour uninterrupted representative steady run after a declared warm-up, followed by measured finite backlog recovery under ongoing normal input. Four hours is a test-design recommendation, not an SLO: it yields 48 non-overlapping 300-second simulation windows at 1x pacing, supports early/mid/late comparisons, and exposes multi-hour WAL/storage/merge drift better than the existing 545-second interval. Extend the run if change-point analysis, resource trend or retained-history growth has not stabilized; a short run may be accepted only as a diagnostic, never final certification. Include additional duration for current normal SUMO pacing if 1x simulation takes longer.

Choose sampling interval from expected queue dynamics and ensure at least 30 samples in each early/middle/late analysis segment. Report time-series and confidence method; account for autocorrelation and workload phases. Exclude warm-up only by predefined time/rule and retain its samples. The owner must approve duration, workload and confidence method before PERF-07.

## Backlog and headroom method

For each edge, sample source high-water and durable consumer cursor/queue in the matching unit, plus age buckets and bytes. Fit backlog slope separately over early/middle/late steady intervals, report interval estimates and compare against zero. Use rolling rates and bounded batch-cycle phase alignment to distinguish cyclic queue from drift. A stage passes only if no positive sustained trend beyond agreed noise margin, max queue/storage remains within approved budget, oldest age meets an approved objective, and finite burst backlog drains with source continuing. Derive headroom from `μ_target ≥ λ_normal + B/T_recovery`, use a controlled offered rate sweep to establish margin, and repeat runs to expose variance.

Gold's event-to-window work demand is at least two membership contributions per eligible event for the fixed 60s and 300s specs; unique windows per event vary by event time/run and boundary. Conflicts can invalidate and regenerate multiple window obligations. Measure membership reads per completed proof and invalidation impact cardinality rather than deriving total work from observation count alone.

## Overload and buffering rules

Keep bounded in-memory queues and durable Outbox/Kafka/Raw/Bronze/Silver evidence. Publish visible degraded/non-ready signals before approved storage/retention limits. Producer backpressure may pause/fault only through existing explicitly safe behavior; no dropped/overwritten evidence. Calculate retention runway from broker low/high watermarks, durable queues, source history and disk; define operator action before runway is exhausted. No additional distributed queue/database is proposed until measurements show existing safe architecture cannot meet approved targets.
