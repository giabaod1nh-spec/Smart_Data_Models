# Decisions and review-resolution traceability

## Decision status model

- **INHERITED INVARIANT:** accepted semantics/authority constraints; not a new owner decision.
- **G0 OWNER APPROVAL:** workload and compatibility freeze required before profiling implementation.
- **G1 OWNER APPROVAL:** numerical objectives selected from measured evidence.
- **RECOMMENDATION:** technical path proposed for later owner review.
- **MEASUREMENT GATE:** choose only after evidence.
- **SEPARATE ARCHITECTURE APPROVAL:** contract/state/ownership change not covered by this plan.
- **DEFERRED:** outside current endpoint or unsupported by evidence.

## G0 — Initial workload and compatibility freeze

Owner should approve: (1) W1 exact Core comparable profile; (2) W2 intended normal profile based on 0.01s step, simulation-time capture/cache defaults and participant/scenario rules, with actual source rate measured rather than fixed; (3) W3 controlled stress boundaries and explicit labels for accelerated SUMO vs fixtures; (4) normal co-resident app scope; (5) authority/durability/recovery invariants; (6) available hardware and safe isolated diagnostic limits; (7) no reduced source cadence/fidelity/payload. G0 authorizes PERF-00B, PERF-01 and profiling. It deliberately requires no final throughput/latency/recovery SLO.

## G1 — Evidence-based performance objectives

After profiles, engineering recommends and owner approves: normal required λ; recovery burst/outage B/T and global deadline; source→Gold ACK and eligible 60/300 completion latency; backlog/age limits; CPU/RSS/SQLite/WAL/CH/Kafka/disk budgets and retention runway; acceptable realtime regression; sustained test level/duration/confidence method. Each value must cite measurements, workload/product rationale, hardware impact, trade-off and validation method. No threshold here is approved; no arbitrary 10s target.

## Decision register

| ID | Topic / status | Recommendation and rationale | Risk / validation |
|---|---|---|---|
| D-01 | Source workload — G0 OWNER APPROVAL | Freeze W1/W2/W3 and normal 0.01s SUMO vs Core 1s diagnostic; count source offered load and pacing. | Cadence/payload reduction hides demand and changes fidelity; manifest + independent source oracle. |
| D-02 | Recovery/headroom — G1 OWNER APPROVAL | Set finite B/T and global recovery target from operation need; use `μ ≥ λ+B/T` as estimate only. | Upstream catch-up can amplify downstream; global PA-04/05 proof. |
| D-03 | Latency — G1 OWNER APPROVAL | Define source→durable Gold ACK and eligible→current-work completion percentiles from added timestamps. | Existing p95 459.8s queue-inclusive and acknowledged subset only; not a goal. |
| D-04 | Resource/retention — G0 initial envelope + G1 final limits | Identify available hardware and safe disposable bounds at G0; freeze service/disk/retention budgets at G1. | Local Docker/RF1 assumptions; constrained and history tests. |
| D-05 | Benchmark tier/duration — G0 method, G1 final duration | L0–L4; L4 2–4h only when L1–L3 pass and drift/confidence warrants. | Avoid costly endurance before short trial qualifies. |
| D-06 | Roadmap — RECOMMENDATION | Instrument and profile all, tune Silver first by current evidence; early Gold G-A/G-B; upstream PERF-02/03 changes conditional. | Prevent queued work transfer; matched full-path regressions after every change. |
| D-07 | Silver scheduler — INHERITED INVARIANT + MEASUREMENT GATE | Preserve recovery precedence, invalidations-before-changes fixed prefix, conflicted change requires local invalidation; then evaluate bounded fairness/backlog poll. | No naive round robin; continuous related/unrelated workload, gaps, restart and no-spin tests. |
| D-08 | Silver grouped reconciliation — MEASUREMENT GATE | Evaluate exact per-obligation grouped lookup/insert/reconcile after profile; preserve partial outcomes and contiguous completion. | CH ambiguity, partial insert, conflicting key, earlier sequence gaps; detailed protocol in `design.md`. |
| D-09 | Silver cache/static work — MEASUREMENT GATE | Bounded cache keyed by verified immutable binding/archive/topology/builder version; skip writes only after exact content proof. | Fingerprint change/restart/eviction must fail closed; no archive integrity bypass. |
| D-10 | Gold materialization — MEASUREMENT GATE | Evaluate A existing feed optimization, then C bounded working representation; consider B public API or D hybrid only if measured benefit demands complexity. | B/D need separate contract approval; all require current validity, invalidation fence, exact membership and restart oracle. |
| D-11 | Gold work coalescing — MEASUREMENT GATE | State-specific OPEN/PENDING coalescing; fence RUNNING; preserve COMPLETE; DIRTY always current durable obligation. | Atomic Gold transaction and stale generation rejection; never delete history. |
| D-12 | Parallelism/sharding — DEFERRED / SEPARATE ARCHITECTURE APPROVAL | Retain single-owner bounded processing until CPU profile demonstrates need. | SQLite ownership/order, invalidation and CH contention redesign. |
| D-13 | New public API or persistent cache — SEPARATE ARCHITECTURE APPROVAL | Keep current API/state unless measured query amplification blocks G1. | Versioning, compatibility matrix, migrations, replay and recovery approval. |
| D-14 | Backpressure — INHERITED INVARIANT + G1 operations approval | Durable finite buffer, bounded memory, visible degraded/fault state; do not drop. Set alerts/runbook limits at G1. | Retention/runway and producer pause semantics must be tested. |
| D-15 | Integrated stack — G0 scope + G1 regression tolerance | Include normal Projector/Orion/Mongo and relevant serving services for PA-09; set acceptable regression after matched measurement. | No analytical starvation of realtime services. |
| D-16 | Phase 4B — DEFERRED / OWNER GATE | Wait for full PA pass and owner approval. | Completed Gold foundation only; later KPI/mart/API/UI separately benchmarked. |

## Inherited invariants

- Durable SQLite Outbox; at-least-once delivery; original Raw payload bytes; safe committed Kafka prefix.
- Bronze semantic identity/duplicate/conflict authority, append-only journal and independent invalidation.
- Silver canonical authority, frozen obligations, exact reconciliation, contiguous verified prefixes and immediate suppression; current validity row alone is not historical proof.
- Gold public receipt verification, independent durable local ACK/invalidation lanes, suppression, current-effective input, fixed source/invalidation boundary and stale generation fencing.
- Replay state/source isolation; V1/V2 separation; no cross-database atomicity or unlimited retention.
- Pure simulation-time Tumbling windows: 60s/300s, epoch-zero, half-open intervals; no rolling/provisional behavior. During Core/optimization completeness UNKNOWN and publication finality DISABLED.
- No Phase 4B KPI, mart, Spring Analytics API or Dashboard implementation in this work.

## R01–R12 review-resolution matrix

| Finding | Verified issue/evidence | Resolution and documents | Remaining gate/package / status |
|---|---|---|---|
| R01 Phase 2 evidence | Tracked JSON exists; 12.3217934s, 37.3013543s, 15.8129024s; final samples max difference 415. | `audit.md`, `baseline.md`, B-12 corrected. | Finite catch-up only; no Bronze capacity inference. **RESOLVED IN PLAN** |
| R02 SUMO workload | Normal step 0.01s; publish/cache 1 sim-second; nodes A/B/C/D/M; Core overrides 1.0s; scenario can add captures. | W1/W2/W3 in audit, baseline, requirements, instrumentation, acceptance, decisions. | G0 owner freezes effective config. **REQUIRES OWNER APPROVAL** |
| R03 Roadmap order | Previous plan upstream-first conflicted with measured Silver and Gold unknown. | PERF-00A/B→01/profiling incl early Gold→G1→evidence-gated PERF-04/05 and conditional 02/03→06/07 in plan/design/blockers/handover. | Change order only if upstream blocks valid profile. **RESOLVED IN PLAN** |
| R04 Core hardening/scheduler | Replay final `discover()` outside `_lane`; static, un-reproduced; Silver has mandatory recovery/invalidation ordering. | PERF-00B, precise invariant/scheduler tests in plan, acceptance, audit. | Reproduce; minimal fix only if confirmed. **RESOLVED WITH IMPLEMENTATION GATE** |
| R05 Silver grouped reconciliation | Per-obligation reconcile/insert/reconcile exists; causal cost unknown. | Bounded exact grouped protocol, partial 99/100 rule, failure/retry/fallback/cache in design and PERF-04. | L0 profile and partial/ambiguous/crash tests. **REQUIRES FUTURE PROFILING** |
| R06 Gold options | Current exact single-member calls, 440 reads/9.12MB/one 219.96s proof; invalidation global fence can restart unrelated proof. | Options A–D, complete comparison/selection gates, restart amplification and tests in design, plan, acceptance. | Early G-A/G-B, G-C required; B/D need owner architecture approval. **REQUIRES FUTURE PROFILING** |
| R07 Work coalescing | `_advance_work` creates generations/work rows on membership changes. | Input generation vs current obligation vs execution/completed history; state-specific coalescing and crash tests in design/plan/acceptance. | Measure growth; no deletion of COMPLETE. **RESOLVED WITH IMPLEMENTATION GATE** |
| R08 Benchmark levels | Four-hour trial unsuitable for every iteration. | L0–L4 and qualification before endurance in requirements/acceptance/plan. | G1 selects final duration; **RESOLVED IN PLAN** |
| R09 G0/G1 | Single objective gate circular before profiling. | G0 workload/compatibility; G1 post-profile numeric objectives across docs. | G0 owner sign; then profile; G1 owner sign. **REQUIRES OWNER APPROVAL** |
| R10 Global recovery | Stage formula not global proof. | Recovery start/end and every upstream/downstream queue incl Gold work in requirements/acceptance. | PA-04/05 with ongoing W2 source. **RESOLVED IN PLAN** |
| R11 Instrumentation/Gold profiles | Source ACK can undercount offered load; hidden backlog and Gold states not distinguished; query attribution ambiguous. | Exact counters, G-A/B/C, current-generation work and approximate vs exact CH attribution in instrumentation/acceptance. | PERF-01 overhead/driver support validation. **REQUIRES FUTURE PROFILING** |
| R12 Failure/equivalence/persistent state | Batch/cache/coalesce could alter correctness or persist incompatible state. | Mandatory failure matrix, four-way old/new state compatibility, independent oracles and no rewind in plan/acceptance/handover. | Package-specific acceptance; **RESOLVED WITH IMPLEMENTATION GATE** |
