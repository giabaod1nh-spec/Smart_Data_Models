# Performance planning blockers and dependencies

| ID | Class / blocker | Evidence or missing fact | Blocks | Resolution/owner |
|---|---|---|---|---|
| B-01 | Confirmed capacity blocker | Core normal trial: Silver 0.960 seq/s; backlog +4.109/s for 544.92s at 4.946 obs/s diagnostic. | Performance Acceptance, production activation, Phase 4B | Engineering profile/optimize PERF-01/04; owner confirms target workload. |
| B-02 | Gold work capacity unmeasured | Only one completed work proof in 219.959s; source-fed ACK rate limited by Silver. | Full performance certification, Phase 4B | Controlled canonical source capacity and integrated Gold work PA-11. |
| B-03 | Missing causal attribution | No inner Silver spans; 49,761 upstream CH queries shared principal; no service-specific query attribution. | Safe optimization choice | PERF-01 instrumentation; query IDs and span correlation. |
| B-04 | Workload ambiguity | Historical 1s diagnostic vs normal SUMO configuration; collision/detector fingerprint warnings; no owner-approved λ. | Implementation tuning and PA-01 | Owner resolves profile/fidelity and signs workload manifest. |
| B-05 | No objectives | No approved B/T recovery, latency, hardware/resource, disk/retention or realtime regression thresholds. | PERF-00 and final acceptance | Owner choices recorded before optimization campaign. |
| B-06 | Incomplete full-path evidence | Prefix oracle only 597/3,000 and `full_drain=false`; normal benchmark final Gold cursor 544; only one work completion. | Capacity and 4B | Re-run after optimized capacity, independent complete drain oracle. |
| B-07 | No complete stage backlog age | Core time series has several counters, but not aligned oldest-age distributions at every stage. | Queue stability acceptance | Instrument each durable queue and equivalent unit. |
| B-08 | Upstream capacity not certified | Raw near offered rate and Bronze outputs slightly higher in measured interval; no sweep/headroom. | Integrated tuning | Profile Raw/Bronze; PA-03. |
| B-09 | No sustained conflict workload | Correctness race tests exist; no approved conflict rate or continuous invalidation load. | Invalidation capacity / PA-06 | Owner defines representative vs stress rates; isolated tests. |
| B-10 | Shared app contention not measured | Core used isolated DE stack; normal Orion/Projector/Spring/Dashboard resources absent. | Deployment readiness | PERF-06/PA-09 with owner-defined acceptable regression. |
| B-11 | Retention/storage policy incomplete | Outbox ACK cleanup defaults to seven days; Gold metadata persists with history; SQLite/CH/Kafka disk runway not projected. | Long history, outages and activation | Owner approves source retention, backups, pruning safety and disk budgets; test PA-08/10. |
| B-12 | Historical evidence absent | Named Phase 2 raw evidence directories not present in tracked checkout. Some aggregate values are in phase documents only. | Fine-grained historical comparison | Retrieve/retain original report only if owner needs exact historical recalculation; not a blocker to current plan. |
| B-13 | Architecture candidates unapproved | Larger batches, grouped reconciliation, cache, new API batch read, concurrency all affect resource or correctness envelope. | Implementation package selection | Use measurement decision gates and owner review for contract/schema/concurrency changes. |
| B-14 | Existing regression baseline has inherited failures | Core report records one absent Gold4 runbook test failure in selected suite and ten architecture failures inherited from Pre. | Final regression verdict | Reproduce/classify unchanged baseline before declaring no regressions; do not weaken/remove tests. |
| B-15 | Disk/hardware envelope specific | Host/VM recorded but is not a product supported footprint; CH/Kafka local RF=1. | Production/operational readiness | Owner declares target host, storage and HA/outage assumptions. |

## Blocker classes

### Planning completion

No blocker: design and evidence-gap plan are complete. Missing runtime evidence is explicitly identified and routed to implementation packages.

### Optimization implementation

Blocked by workload and objective freeze (B-04/B-05), operation instrumentation (B-03), and isolated safe reproduction. Silver inner root cause remains unknown although Silver is the confirmed stage-level queue bottleneck. Do not implement guessed query or parallelism optimization before PERF-01.

### Performance Acceptance

Blocked by optimizations not implemented, missing stable workload/thresholds, Gold work capacity, per-stage age/resources, full-drain oracle, conflict workload, integrated profile and retention envelope.

### Production activation

Blocked by insufficient capacity at the observed workload, no approved operations envelope/retention policy, full application contention evidence and deployment/credential ownership. Existing Gold service is not certified as production-operational by the capacity run.

### Phase 4B

Blocked by all required Phase 4B gate items in `acceptance.md`, including sustainable completed Gold work and owner approval. Acknowledged Silver cursor is not enough.

## Dependency map

`Owner workload/resource decisions → PERF-00 manifest → PERF-01 instrumentation → upstream/semantic stage profiling (PERF-02/03) → Silver root-cause tuning (PERF-04) → controlled Gold work test (PERF-05) → integrated resource balance (PERF-06) → PA-01..12 certification (PERF-07) → owner approval → Phase 4B planning`.

If instrumentation shows an upstream limiter, reorder PERF-02/03 ahead of Silver changes; keep Silver bottleneck designation and remeasure. If Gold public reads dominate, propose contract change separately; it is not a current blocker to planning or a pre-approved API change.
