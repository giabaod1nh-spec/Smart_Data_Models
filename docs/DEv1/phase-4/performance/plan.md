# End-to-end optimization implementation plan

**Execution remains unauthorized until owner approves the requirements, design and unresolved decisions. This document is planning only.** Work packages are dependency-ordered, but profiling evidence can reorder PERF-02..05. Every source change requires package-specific tests plus applicable Phase 1–4A regressions and full-path oracle.

## PERF-00 — Owner objectives and benchmark freeze

- **Objective:** approve target workload, step/capture semantics, source rate, recovery backlog/time, latency goals, resource envelope, retention and test duration.
- **Evidence/root cause:** current 4.95 obs/s diagnostic is insufficient but is not an approved operating target; no SLO/resource objectives exist.
- **Prerequisite:** review `requirements.md`, baseline, source configuration and warning/fidelity status.
- **Changes:** likely docs/config fixture only; no production code until target freezes. Benchmark manifest records commit/config/topology/pacing/seed/image/DB versions/state/backlog/contention.
- **Correctness/contract:** no source cadence/payload or availability change. Public contract unchanged.
- **Tests/DoD:** validate manifest reproducibility; repeat baseline trial sufficiently to estimate variation; preserve raw samples. Owner signs workload and thresholds.
- **Failure/rollback/approval:** if intended workload cannot be represented, stop optimization acceptance and resolve workload contract; preserve diagnostic results.
- **Phase 3 compatibility:** none; benchmark uses Phase 3 behavior as immutable oracle.

## PERF-01 — End-to-end instrumentation and independent stage accounting

- **Objective:** establish trustworthy per-operation profiles, queue ages, rates, resource use and trace attribution before tuning.
- **Existing evidence:** 229 Core samples and Gold reader query summary; no per-op Silver profile, stage CH attribution or full app resource series.
- **Files to inspect/change:** producer runner/backend/snapshot/provider; Outbox store/worker/publisher; Raw V1 consumer/repository/ledger/metrics; Bronze V1 processor/repository/checkpoint/metrics; Silver processor/repository/checkpoint/archive/feed; Gold processor/store/reader/ledger/metrics; health APIs, Compose, benchmark tool and support tests. Prefer additive metrics modules where they match repository conventions; no schema migration unless durable sampling needs are justified (prefer external evidence files).
- **Steps:** add bounded counters/timers/spans; query IDs/context; stage-specific backlog age/bytes and resource sampler; workload manifest and raw time-series writer; verify `input = output + pending + disposition` oracle.
- **Dependencies:** PERF-00 workload snapshot; no authority or public DTO changes.
- **Correctness:** metrics are observational and never determine disposition, cursor, output or retry behavior.
- **Tests/acceptance:** unit test monotonic counters/errors/cardinality; instrumentation overhead A/B; real integration path stage-accounting oracle; resource samples align across services.
- **Failure modes:** high-cardinality memory growth, slow metrics endpoint, wall-clock jumps, query labeling leak. Roll back instrumentation independently, preserve acceptance artifacts.
- **DoD:** all required stages attributable per `instrumentation.md`; missing attribution quantified; raw samples and manifests reproducible.
- **Phase 3 compatibility:** add-only metrics/health fields; no change to validity, invalidation, archive, cursor or readiness semantics.

## PERF-02 — Profile and tune producer, Outbox, Kafka and Raw

- **Objective:** establish whether producer/durable transport/Raw has headroom at the approved workload and optimize only measured dominant stage(s).
- **Hypotheses:** TraCI capture or FULL/WAL cycle append can consume simulation budget; Raw insert/reconcile/commit could be near offered rate. Neither is yet a confirmed ceiling.
- **Files:** `Visualize/app/traci_runner.py`, simulation/backend and observation packages, Outbox/publisher/worker, `de/kafka_raw/v1_consumer.py`, Raw repo/ledger/config, migrations only if evidence supports additive index/schema.
- **Steps:** capture same payload profile; controlled batch/flush/linger/in-flight sweep, partition skew, outage/backpressure and disk runway. Keep exact event bytes, cycle identity and Raw persistence-before-offset commit.
- **Alternatives:** tune batch vs partitions vs query/index work; do not relax acknowledgments, remove Outbox or reduce source fidelity.
- **Tests:** contract byte equality; duplicate/retry/ambiguous insert; offsets never skip unresolved; restart/retention gap; burst plus ongoing input; resource budget.
- **Failure/rollback:** any unaccounted delivery, payload mismatch, unsafe commit, Outbox overflow or simulation semantic drift rejects build; restore prior config/image and reconcile.
- **DoD:** approved normal rate + required recovery headroom, bounded queues at early/middle/late, profile shows margin, downstream Bronze/Silver/Gold are not starved.
- **Phase 3:** upstream behavior stays identical; Silver receives same canonical Bronze histories.

## PERF-03 — Profile and tune Bronze V1 semantic materialization

- **Objective:** confirm capacity beyond approved input and preserve semantic/invalidation authority.
- **Evidence:** current benchmark Bronze output rate 5.069 sequences/s while input 4.938 deliveries/s; no per-op timings; Phase 2 had finite transient backlog/catch-up evidence.
- **Files:** `de/bronze/processor.py`, `checkpoint_store.py`, `clickhouse_repository.py`, `models.py`, Raw adapter/ledger, `metrics.py`, tests in `de/tests/bronze` and integration acceptance; migration 007 only if query profile justifies additive index.
- **Steps:** profile per offset/batch source read, validation/hash, identity lookup, output fanout and each CH reconciliation, invalidation/journal writes and CAS. Compare batch size/poll/no-op discovery; consider safe batched identity/reconciliation. One SQLite owner retained unless redesigned explicitly.
- **Invariants:** duplicate/conflict classification, append-only sequence links, invalidation and full Raw lineage unchanged; exact reconciliation before cursor.
- **Tests:** independent semantic oracle; duplicates same/different bytes; conflict after acceptance; CH ambiguity; missing/corrupted source; interleaved partitions and gaps; full downstream Silver/Gold oracle.
- **Failure/rollback:** mismatch in state/disposition/sequence/output refuses rollout; migration additive forward-only, retain old image/state restore plan.
- **DoD:** sustained approved arrival plus derived recovery demand with bounded Raw→Bronze queue and matching source oracle.
- **Phase 3:** journal ordering and disposition unchanged; Silver receives equivalent entries and invalidation feed.

## PERF-04 — Silver V1 confirmed bottleneck optimization

- **Objective:** move Silver service rate above approved arrival plus recovery headroom while preserving materialization and invalidation semantics.
- **Evidence:** measured 0.960 sequences/s; pending +162→+2401 at +4.109/s. Active worker cap 10 and configured 2s wait are confirmed configuration limits, but inner bottleneck is unmeasured.
- **Prerequisite:** PERF-01 spans and isolated/profiled Silver work mix. Do not guess that poll delay alone explains the gap.
- **Files:** `de/silver/processor.py`, `config.py`, `repositories.py`, `checkpoint_store.py`, `archive_resolver.py`, fact/dimension builders, `incremental_reader.py`, `feed_read_budget.py`, Silver and real integration tests; migration 008 only if measured SQL needs an additive index.
- **Steps:** (1) baseline accepted/conflict/invalidation/recovery span; (2) backlog-aware poll and fair bounded lane scheduling; (3) tune batch cap with peak RSS/CH latency; (4) reduce repeated per-entry/per-obligation reconciliation via exact grouped content query or transaction only if supported; (5) bounded immutable archive/dimension reuse only by fingerprint/version; (6) rerun Bronze→Silver and all public feed proofs.
- **Alternatives:** single-owner bounded batching first; worker parallelism deferred due SQLite serialization, sequence ordering and invalidation races. Do not skip final Bronze source point read, obligation freezing, output visibility/reconciliation or VALID+cursor atomic finalization.
- **Correctness tests:** accepted→conflicted lifecycle; replay/current validity; fact set/value/availability/lineage; obligation immutability; CH before/after ambiguous write; failed validity/CAS transaction rollback; source replacement/gap; concurrent append; sequence continuity; independent invalidation progress; archive mutation; phase-3 real migration and regressions.
- **Perf tests:** accepted-only and mixed disposition passes; throughput vs batch size; CH query attribution; CPU/SQLite/CH/archive share; latency and memory; source append during scan; finite backlog drain while input continues.
- **Failure/rollback:** any semantic mismatch, invalidation starvation, unbounded pass/memory, or cursor advancement on incomplete obligation rejects. Revert code/config, retain Silver authority and reconcile from durable cursor.
- **DoD:** approved λ + B/T target with stable Bronze→Silver backlog, bounded age/resources, full exact canonical oracle; no degradation to raw/fact correctness.
- **Phase 3:** strongest compatibility gate; all accepted A01–A33 evidence/invariants remain true. No edits to prior evidence/results.

## PERF-05 — Silver feed and Gold ACK/window work

- **Objective:** establish independent sustainable Gold ACK and completed-work rates once source can supply them; optimize window path without conflating ACK with work completion.
- **Evidence:** ACK rate matched Silver's 0.960/s; 440 member reads/9.12MB; one completed proof over 219.96s; current source-limited benchmark cannot estimate Gold capacity.
- **Files:** `de/silver/incremental_reader.py`, feed contract/budget, public DTO serializer/tests; `de/gold_runtime/silver_readers.py`, `processor.py`, `processing_ledger.py`, `checkpoint_store.py`, `window_scheduler.py`, `de/gold/windowing.py`, `config.py`, `gold_core_capacity.py`, Gold runtime and Silver feed tests. Additive contract/version migration only with measured proof and separate approval.
- **Steps:** fixed-rate repeatable Silver source; separate feed ACK from member materialization. Profile JCS/receipt, page budgets, exact `after=s-1` reads, invalidation-fence restart, membership fanout, 60/300 completion, generation churn. Then test bounded batch-read/API, local member input cache or generation coalescing one at a time.
- **Correctness:** full sequence receipts, independent invalidation cursor and fixed Pi, exact suppression, membership identity, current-effective bundle and source binding, fixed high-water fence, generation CAS; lifecycle/completeness/finality semantics unchanged.
- **Tests:** all concurrent append, invalidation before/during/after work, unrelated invalidations, stale generation, replay isolation, crash between source and local ACK, exact memberships, terminal OPEN windows, long history, service outage and independent oracle.
- **Failure/rollback:** any skipped invalidation, stale work completion, missed member, duplicate authoritative work or contract disagreement rejects. Revert to prior public method and preserve old Gold checkpoint compatibility.
- **DoD:** controlled source ACK capacity and separate 60/300 proof service satisfy approved arrival+recovery demand; full path confirms same results.
- **Phase 3:** Silver public API remains backward-compatible unless explicit additive contract approval; no raw fact table bypass.

## PERF-06 — Integrated shared-resource and pipeline balance

- **Objective:** verify optimized complete pipeline under normal app resource contention and tune service budgets fairly.
- **Files:** `docker-compose.yml`, worker Dockerfiles/dependency versions, all service configs/metrics, isolated/integrated benchmark runner and runbook. Avoid changing Spring/Dashboard business semantics.
- **Steps:** run matched isolated and full app stacks; collect host and container profiles, query attribution, workload and realtime service metrics; sweep CPU/memory budgets and worker settings with one variable at a time. Exercise finite burst then normal arrivals and outages.
- **Tests:** SUMO pacing/capture fidelity; Projector/Orion successful work and latency; all analytical queues and Gold work; controlled CH/Kafka contention; disk exhaustion warning boundary in disposable state.
- **DoD:** all stage and realtime metrics remain inside owner-approved envelope; no moving backlog into unmeasured buffers; recover within approved target.
- **Rollback:** restore Compose limits/config and previous image; never truncate Kafka/SQLite/CH evidence.
- **Phase 3:** valid/lineage semantics unchanged; integration profile may alter resource allocation only.

## PERF-07 — Performance Acceptance certification

- **Objective:** execute PA-01..PA-12 using the accepted optimized build, isolated state, production-like config, independent source oracles and owner-approved thresholds.
- **Prerequisite:** PERF-00..06 DOD; no critical blocker; owner confirms acceptance campaign resource allocation.
- **Evidence:** raw benchmark time series, workload manifest, query attribution, container/host telemetry, JUnit/runtime logs, independent source and stage oracle, hashes, failures and full drain inventory.
- **DoD:** every required scenario passes all correctness, stable backlog, work completion, latency, recovery, resource, history and realtime coexistence objectives; before/after matrix supports no regression. Phase 4B only after owner reviews and approves evidence.
- **Failure:** any continuous queue growth, incomplete eligible Gold work, semantic mismatch, unsafe cursor/offset, unbounded resource, unacceptable realtime regression or missing independent oracle means fail; retain data and revert optimization.
- **Phase 3 compatibility:** complete applicable Phase 1–4A regression matrix unchanged; no tests removed/weakened.

## Package ordering summary

`PERF-00 → PERF-01 → (PERF-02 and PERF-03 measured as needed) → PERF-04 → PERF-05 → PERF-06 → PERF-07`. PERF-02/03 may precede PERF-04 if instrumentation shows upstream constraints; PERF-05 source-only profile can run after PERF-01 in parallel operationally but must not be treated as full-path certification. No work starts by assuming a later stage is fast enough.
