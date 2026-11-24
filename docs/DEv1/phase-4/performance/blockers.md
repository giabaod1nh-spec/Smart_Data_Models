# Performance planning blockers and dependencies

| ID | Classification | Evidence / blocker | Blocks | Resolution |
|---|---|---|---|---|
| B-01 | Confirmed capacity blocker | Core diagnostic: Silver 0.960 sequences/s; Bronze→Silver +4.109 sequences/s backlog over 544.92s at ~4.946 observations/s. | Optimization acceptance, activation, Phase 4B | Profile inner Silver and optimize by evidence; confirm W2 at G0. |
| B-02 | Gold capacity unmeasured | One work proof in 219.959s; source-limited Gold ACK. | Sustainable full acceptance / 4B | Early G-A/G-B then G-C (PERF-01/05, PA-11). |
| B-03 | Missing operation/query attribution | No nested Silver spans; 49,761 shared upstream CH queries not stage-attributable. | Root-cause changes | PERF-01 exact attribution where supported, otherwise explicitly approximate/confidence-labeled. |
| B-04 | G0 owner workload freeze | Normal step 0.01s; publish/cache intervals 1 simulation-second; default nodes A/B/C/D/M; Core tool step=1s; scenario can add captures. | Valid W2/profile implementation | Owner approves effective config, stack and source-fidelity bounds; measured rate is not fixed to five/s. |
| B-05 | G1 numeric objectives pending (not a profiling blocker) | No approved normal λ/headroom, latency, B/T, resource/retention or realtime thresholds. | Final optimization targets and PA PASS criteria | Profile first; engineering proposes values with evidence; owner approves G1. |
| B-06 | Full drain missing | Prefix oracle 597/3,000 and `full_drain=false`; normal Core endpoint Silver/Gold 544 and one completed proof. | Capacity certification/4B | Optimized full-path run with independent complete source/work oracle. |
| B-07 | Stage age/accounting incomplete | No aligned oldest-age/hidden queue distribution for every stage. | Stability/global recovery gates | PERF-01 source and durable obligation/cursor inventory. |
| B-08 | Upstream headroom unknown | Raw near offered and Bronze output measured; no capacity sweep. | Full pipeline capacity | Profile all stages; change PERF-02/03 only if evidence/invalid downstream test requires. |
| B-09 | Continuous invalidation capacity unknown | Race correctness tests, no representative continuous invalidation load/rate. | PA-06, Gold work stability | G0 declares representative vs stress workloads; test both. |
| B-10 | Integrated contention absent | Core benchmark isolated DE stack, not normal realtime/serving services. | Deployment/PA-09 | Matched stack profile and G1 realtime regression tolerance. |
| B-11 | Retention/storage envelope unapproved | Outbox ACK cleanup defaults seven days; Gold metadata and source history grow; disk runway not forecast. | Long outage/history/activation | G0 hardware bounds, G1 retention/disk budget and PA-08/10. |
| B-12 | **Resolved** — historical Phase 2 files are present | `git ls-files` confirms reports and samples for `ffbed382b2`, `70f64d12e4`, and independent hardening `bc3c691c4a`. JSON: catch-up/total 12.3217934/42.2592266s, 37.3013543/73.9950448s, 15.8129024/49.3353630s. Final sampled Raw-minus-Bronze COMPLETED max is 415. | No longer blocks planning | See paths/calculation/limitations in `baseline.md`. Finite catch-up, not sustainable Bronze capacity. |
| B-13 | Architecture options need evidence/approval | Grouped reconciliation, immutable cache, Gold cache/coalescing, sparse Silver API and concurrency have distinct costs/state semantics. | Any corresponding implementation | PERF-00B/01 then decision-specific approval; B/D API/hybrid and concurrency are separate approval. |
| B-14 | Historical regression failures | Core report records inherited Gold4 runbook missing-file failure and architecture failures. | Final regression verdict | Reproduce baseline/classify unchanged; no weakening or deleting tests. |
| B-15 | Hardware/HA scope | Prior VM is not supported product envelope; Kafka local RF=1. | Activation and outage certification | G0 host/HA assumption and G1 resource objective. |
| B-16 | Static Core replay concern, unconfirmed | Final `discover()` for replay completion is outside `_lane()` retry wrapper. No deterministic reproduction retained. | Core hardening sign-off | PERF-00B reproduces transient/permanent cases; do not call fixed absent evidence. |

## Blocker stages

- **G0 / profiling entry:** B-04 and safe isolated test environment; G1 values are not required yet.
- **Core hardening:** B-16 requires a deterministic test and minimal remediation only if reproduced; Silver scheduler constraints are inherited invariants.
- **Performance profiling:** B-03/B-07/B-08 and early Gold G-A/G-B; exact operation cost remains unknown.
- **G1 / optimization target:** B-05/B-11 and workload distributions from profiles.
- **Optimization:** Silver inner cause and Gold actual materialization capacity; upstream PERF-02/03 implementation is conditional.
- **Performance Acceptance:** optimizations not yet implemented; B-02/B-06/B-07/B-09/B-10/B-11 remain.
- **Production activation:** measured insufficient capacity and missing approved operating/retention/resource/contended environment evidence.
- **Phase 4B:** all acceptance/owner gates plus sustainable current-generation Gold work; cursor ACK alone is insufficient.

## Synchronized dependency map

`G0 workload/compatibility approval → PERF-00B Core hardening → PERF-01 instrumentation and controlled baseline → profile Producer/Raw/Bronze + Silver apply + early Gold G-A/G-B → G1 evidence-based target approval → PERF-04 Silver / PERF-05 feed and Gold; PERF-02/03 code changes only if measured or required → PERF-06 integrated stack → PERF-07 L0–L4 / PA-01..12 → owner accepts evidence → Phase 4B planning`.

If an upstream stage prevents valid downstream profiling, move its conditional optimization before the blocked package and repeat full-path measurement. Any API/persistent-state/ownership change has separate design approval. Planning is complete; capacity, implementation and activation are not.
