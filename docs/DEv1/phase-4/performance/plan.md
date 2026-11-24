# End-to-end performance optimization implementation plan

**Planning only. No optimization or Core fix is implemented in this revision.** The measured bottleneck is Silver apply; its inner cause is not measured. Gold completed-work capacity is unknown and must be profiled early to prevent bottleneck transfer. PERF-02/03 inspect every upstream stage but implement changes only when evidence shows need or a stage prevents valid downstream profiling.

## Dependency order

```text
PERF-00A workload/compatibility freeze (G0)
  → PERF-00B Core correctness hardening
  → PERF-01 end-to-end instrumentation + controlled baseline
  → profile all stages, including Silver apply and early Gold G-A/G-B
  → PERF-01 evidence packet and G1 performance-objective freeze
  → evidence-gated optimization: PERF-04 Silver, PERF-05 feed/Gold,
       PERF-02/03 upstream only where measured or required
  → PERF-06 integrated balancing
  → PERF-07 acceptance L0–L4, PA-01..PA-12
  → owner approves evidence → Phase 4B planning
```

If an upstream limit makes Silver/Gold profiling invalid, move the relevant PERF-02/03 optimization earlier with measured rationale. Gold component/canonical profiling starts after PERF-01, before selecting Silver optimization parameters. After every significant optimization: run applicable correctness regression, operation test, stage profile, matched full-path trial, inspect every queue and shared resource, identify migration, and choose next work based on evidence. Every package includes this end-to-end regression gate.

## PERF-00A — Initial workload and compatibility freeze (G0)

- **Objective/evidence:** freeze W1/W2/W3 and existing semantics before implementing instrumentation. Core uses 1.0s diagnostic step; normal YAML is 0.01s; normal publish/cache interval is one simulation-second, participants A/B/C/D/M by default; current ~5 observations/wall-second is an estimate, not an approved cadence.
- **Owner inputs:** intended scenario/topology and capture semantics; relevant application-stack scope; available hardware and initial safe diagnostic resource ceilings; isolation/fault-test boundaries; fidelity constraints. No final latency/recovery SLO required.
- **Files:** docs `requirements.md`, `baseline.md`, run manifests and benchmark fixtures only. Inspect `Visualize/configuration/model_parameters.yaml`, config env overrides, CLI/run loop, `gold_core_capacity.py`, contracts and handovers. Do not change runtime config.
- **Steps/tests:** validate manifest captures effective environment/config, scheduled vs completed capture counters, simulation/wall pacing, source event/byte shape, seeds and stack inventory; verify W1 reproduces historical benchmark and W2 uses normal config. Any reduced cadence/payload fails.
- **DoD/compatibility:** owner records G0 workload/compatibility decision; no source/contract behavior changed. If W2 cannot be produced at intended pace, resolve source issue before performance conclusions.

## PERF-00B — Core correctness hardening

- **Objective:** reproduce and minimally remediate the static replay final-boundary transient failure path only if confirmed. In `de/gold_runtime/processor.py:run_cycle`, final replay `discover()` before marking COMPLETE is outside `_lane()` retry/backoff.
- **Changes:** deterministic failure injection at final replay validation; transient authority failure must not mark COMPLETE or terminate through wrong path; temporary dependency restoration retries and completes; permanent binding/boundary change remains fail-closed/ABORTED. Implement minimal change only after reproduction.
- **Files/tests:** `de/gold_runtime/processor.py`, replay/runtime tests, Gold checkpoint/store tests, Core isolated test fixtures. Verify live SQLite bytes/cursors/work unaffected and replay/live namespaces isolated; run full Core correctness regressions.
- **Failures/DoD:** if not reproducible, record versioned evidence and retain static finding as not reproduced; do not label defect fixed. If reproduced, fix preserves replay contract; transient, permanent, restart and source replacement tests pass. No data state migration expected; if changed, compatibility matrix and approval are mandatory.
- **Silver compatibility:** inspect scheduler before any fairness work. Preserve startup recovery precedence, invalidations-before-changes within fixed boundary, `CONFLICTED` requires local invalidation, exact contiguous source prefixes. Do not implement naive round-robin. PERF-04 tests continuous changes/invalidation, related/unrelated invalidation, outstanding recovery, conflict before accepted finalization, append during pass, outage/restart and busy-loop protection.

## PERF-01 — Complete instrumentation, stage profiling and controlled baseline

- **Objective:** independent offered-load accounting, hidden-backlog visibility, nested operation costs, Gold work lifecycle and shared-resource/query attribution.
- **Files to inspect/change if authorized later:** producer/TraCI/backend/snapshot/config; Outbox/publisher; Raw V1 consumer/repository/ledger; Bronze V1 processor/repository/checkpoint; Silver processor/repository/checkpoint/archive/feed; Gold reader/processor/ledger/store; health/metrics; `de/tools/gold_core_capacity.py`; Compose for evidence-only sampling; tests. Do not modify contracts or authority schema for telemetry absent justification.
- **Steps:** implement bounded counters/spans and workload manifest; distinguish scheduled/completed captures, Outbox append/pending/ACK, Kafka/Raw/Bronze/Silver/Gold units; expose each memory/durable queue and age/bytes; per-stage host/container resources; exact or confidence-labeled CH query attribution; measured overhead. Run L0-L2 controlled profiles to identify producer/Outbox/Kafka/Raw/Bronze, Silver accepted/conflict/invalidation/recovery, feed and Gold cost.
- **Gold early work:** run G-A local fixture and G-B actual canonical read capacity after instrumentation; G-C full pipeline starts when source can reach target. Separate ACK from eligible current-generation 60/300 completion.
- **Tests/DoD:** counter accounting oracle, no high cardinality or blocking TraCI, output/authority invariance; spans correlate every critical operation or explicitly mark attribution unavailable; sample overhead measured; all-stage initial baseline includes W1/W2 and raw evidence. No optimization selected on aggregate query count.
- **Compatibility:** observation-only fields; no event payload, checkpoint, authority, cursor, disposition or health readiness decision changed. Applicable Phase 1–4A regression plus independent full-path oracle after implementation.

## G1 — Evidence-based performance-objective freeze

After PERF-01, engineering proposes normal rate, burst/outage B/T and global recovery, source→ACK and eligible-work latency, backlog/age envelope, resources/history/retention, realtime regression and L3/L4 duration. Each includes observed data, product rationale, resource/trade-off and test method. Owner approves target values; they are not invented here. Do not begin final tuning claims or PA pass/fail threshold before G1.

## PERF-02 — Producer / Outbox / Kafka / Raw optimization (conditional)

- **Trigger:** PERF-01 shows an upstream bottleneck, insufficient headroom, or upstream rate prevents valid downstream component measurement. Investigation always occurs; code/config changes do not happen absent evidence.
- **Files:** `Visualize/app/traci_runner.py`, `Visualize/simulation/backend.py`, observation stack, Outbox store/worker/publisher, Kafka producer and `de/kafka_raw/v1_consumer.py`, Raw repository/ledger/config; additive migration only if measured and separately approved.
- **Design:** controlled payload-equivalent capture, Outbox batch/status/SQLite profile, Kafka batch/flush/linger/partition, Raw poll/validation/insert/reconcile/ledger/commit. Preserve V1 bytes, identity, at-least-once Outbox and persistence-before-offset commit. No frequency/fidelity reduction.
- **Tests/DoD:** bytes/contract equality; duplicate, retry, ambiguous write, no unsafe offsets, retention gap, outage, restart, bounded buffers and measured B/T. After each change run Raw/Bronze/Silver/Gold full E2E accounting and realtime pacing check. Rollback image/config without deleting or rewinding data.

## PERF-03 — Bronze V1 semantic processing optimization (conditional)

- **Trigger:** per-operation profile shows Bronze is limiting or insufficiently headroomed, or its output rate prevents Silver/Gold measurement. Phase 2 catch-up does not alone trigger a change.
- **Files:** `de/bronze/processor.py`, checkpoint/repository/metrics/config, Raw source reader and Bronze tests; migration 007 only on measured index/query need.
- **Design/tests:** profile bounded Raw reads, hash/identity, disposition, fan-out, frozen intent, per-target CH write/reconcile, invalidation/journal/checkpoint. Candidate batching retains exact per-observation result, conflict classification, original Raw lineage and append-only sequence. Test same/different payload duplicate, conflict after accept, missing/corrupt source, ambiguous CH, interleaved partitions, gaps, outage/restart and phase3 invalidation oracle.
- **DoD/rollback:** equivalent semantic outputs and source lineage, stable Bronze and every downstream queue under approved load; same E2E gate. One owner remains unless separately approved state/ordering redesign. Additive state compatibility matrix required for migration.

## PERF-04 — Silver V1 apply optimization

- **Trigger/evidence:** first confirmed backlog bottleneck at tested profile (+4.109 change sequences/s backlog, Silver ~0.960/s); inner operation cost required from PERF-01.
- **Files:** `de/silver/processor.py`, config, repositories, checkpoint, archive resolver, fact/dimension builders, tests; migration 008 only if profiled query justifies it.
- **Candidate order:** (1) backlog-aware polling; (2) bounded entry/byte/time cap sweep; (3) grouped frozen-obligation reconciliation design in `design.md`; (4) reduce measured redundant source/evidence reads only with equivalent source binding/final revalidation; (5) bounded immutable archive/dimension reuse keyed by fingerprints/version; (6) transaction reduction preserving SQLite atomic finalization. Single-owner first; concurrency deferred.
- **Exact batch rule:** per-obligation classifications survive partial success; 99/100 match leaves exact outstanding row and blocks any later source sequence beyond incomplete prefix. Ambiguous writes reconcile by key/hash before retry; conflicts fault; local status commits after CH evidence; crash repeats safely. Single-entry fallback for ambiguity. Final Bronze current point-read and atomic VALID+cursor remain.
- **Scheduler tests:** preserve recovery-before-new-work, invalidation prefix before changes, conflict local suppression, exact fixed boundaries and safe prefix. Prove fairness across independently processable lanes without violating dependency; related/unrelated invalidation, ongoing inputs, unresolved obligation, blocked prefix, retry/backoff/no-spin, source append and outage/restart.
- **DoD:** exact Phase 3 facts/value/availability/lineage/obligation and validity equivalence, full regressions; throughput/backlog/resource headroom meets G1 on Silver and complete path, with Gold work and every upstream queue measured. Roll back without state rewind.

## PERF-05 — Silver canonical feed and Gold work optimization

- **Trigger/evidence:** early G-A/G-B establishes feed/member query and work costs. Current per-member `entries=1` calls and global invalidation fence are measured risks; no API or cache is approved.
- **Files:** Silver incremental reader/contract/budget; Gold `silver_readers.py`, `processor.py`, `processing_ledger.py`, store, scheduler, windowing and `gold_core_capacity.py`; acceptance tests. New public contract/migration only after separate owner approval.
- **Alternatives:** Option A internal optimization of existing feed; B additive bounded Silver sparse-member API; C bounded Gold-owned verified working representation; D hybrid. Selection criteria in `design.md`; A first, B/D require architectural approval, C requires persistent state compatibility.
- **Work generation:** state-specific OPEN/PENDING coalescing; RUNNING invalidation/fencing; COMPLETE immutable; DIRTY durable recompute; atomic Gold SQLite update; preserve receipts and suppression. Never blindly delete history.
- **Tests:** exact sparse/non-contiguous member set; current validity; bundle/transition hash; append/conflict/invalidation; cache stale/fingerprint/eviction/restart; replay/live isolation; invalidation within/outside window/other run/final check; crash before/after coalescing; multiple pending/late input/stale completion; old dirty window; 60/300 exact membership and persistent state matrix.
- **DoD:** G-B ACK/member read and both duration-specific completed current-generation work rates meet G1 with invalidation/recompute demand; G-C confirms no transfer; full Gold/Core and end-to-end regressions. Retain current global invalidation fence absent equivalent verified narrower proof.

## PERF-06 — Integrated resource and pipeline balance

- **Files:** Compose/resource profiles and all affected services, benchmark tools; no automatic config change before owner approval.
- **Steps/tests:** matched W2 normal app stack and isolated comparison; host/container CPU/RSS/IO/network/disk, CH/Kafka pressure, SUMO pacing, projector/Orion/Spring/Dashboard; tune one resource/control at a time. Test burst with normal production and isolated dependency outages.
- **DoD:** every queue and work set stays in G1 envelope; global recovery and realtime regression pass; full source oracle exact. Restore prior code/config on failure; never truncate authority/source.

## PERF-07 — Tiered Performance Acceptance

- **Prereq:** G0+G1, PERF-00B/01, required evidence-driven PERF-02..06; L1/L2 pass before L3 and L4; no unclassified correctness or resource issue.
- **Execution:** PA-01..PA-12 in `acceptance.md`; L0 microprofile, L1 2–5m smoke, L2 10–15m matched iteration, L3 30–60m integrated qualification, L4 2–4h+ only when final endurance/history confidence justifies.
- **DoD:** exact independent accounting; queue trend and global recovery; Gold ACK and eligible 60/300 work sustained; resources/history/realtime within approved objectives; full Phase 1–4A regressions. Owner approves before Phase 4B. Any gate failure/inconclusive keeps Phase 4B blocked.

## Cross-cutting compatibility and rollback gates

Every optimization-changing batch, cache, transaction, concurrency or persistence requires tests for partial success, ambiguous responses, conflict, missing evidence, crash before/after local commit and incomplete earlier sequence. Cache/API cases include source append, invalidation, binding/archive replacement, restart and eviction before dirty recompute. Persistent changes require matrix: old code/old state; new/old; new/new; old/new explicitly supported or forbidden. Define forward recovery, backup boundary, retained sources and owner/operator steps. Never assume image rollback reverses state; never rewind/truncate authority.

All packages preserve Outbox at-least-once, original Raw bytes, safe Kafka offsets, Bronze semantic authority, Silver canonical authority/frozen obligations/contiguous prefixes, independent invalidations, public receipt integrity, Gold-local atomicity/suppression/current-effective input/generation fencing/replay isolation and V1/V2 state isolation. Windows stay pure 60s/300s event-time, epoch-zero, half-open; completeness UNKNOWN and finality DISABLED. No KPI/marts/API/Dashboard implementation.
