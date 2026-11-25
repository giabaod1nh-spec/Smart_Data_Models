# Performance Acceptance checkpoint

Technical acceptance remains INCONCLUSIVE. Earlier isolated normal-step diagnostics accumulated Silver backlog. Exact grouped reconciliation, backlog-aware polling and canonical-read improvements raised Silver progress. The latest healthy paced prefix showed falling Silver lag and flat Gold ACK lag. Compose stops cut off observability and left the first Gold proof incomplete. Representative integrated capacity, recovery and endurance are still open.

| Scenario | Technical evidence and verdict |
|---|---|
| PA-01 Intended operation | INCONCLUSIVE. The latest valid prefix offered 4.52 observations/s at 0.903× pace. Silver and Gold ACK each reached 4.60 sequences/s across 62.01 wall seconds. The first eligible Gold window was incomplete and the fixture omitted representative application coexistence. |
| PA-02 Sustained load | INCONCLUSIVE. The short diagnostics do not cover sustained normal operation. The 30-minute qualification was held after the host memory stop. |
| PA-03 Controlled capacity | INCONCLUSIVE. Retained canonical reads are operation measurements. No integrated capacity sweep was executed. |
| PA-04 Burst recovery | INCONCLUSIVE. No approved steady envelope and no genuine burst with ongoing production were measured in this round. |
| PA-05 Dependency recovery | INCONCLUSIVE for the full scenario. The stopped-source restart recovered all 985 pending Outbox rows, fully drained the durable path and completed all six crossed Gold proofs. The final physical audit passed. Recovery under continuing arrivals and the individual dependency outage cases remain open. |
| PA-06 Conflict, invalidation and late data | INCONCLUSIVE for performance. The enabled real feed and Gold Core tests passed for targeted correctness, including related and unrelated changes, late observations and suppression. Continuous churn capacity remains open. |
| PA-07 Restart and replay | INCONCLUSIVE for the full scenario. A retained replay completed 450 contiguous changes in 69.11s and matched the original 300-member proof hash. The stopped incident fully converged under the same binding and verified 1,535 contiguous changes, 1,001 receipt digests, 3,070 memberships and six current proof identities. The retained physical audit passed. Applicable live regression remains open. |
| PA-08 Retained history | INCONCLUSIVE. The small verified retained probe established read costs on its existing prefix. It did not cover retained-history growth. |
| PA-09 Application coexistence | INCONCLUSIVE. Owner services were present. Representative projector and serving traffic were outside these diagnostic fixtures. |
| PA-10 Constraints and overload | INCONCLUSIVE. The diagnostic stopped at the host memory floor and preserved durable evidence. Controlled overload and recovery were outside its measured scope. |
| PA-11 Gold capacity | INCONCLUSIVE. The owner-approved stopped-source trial completed all five remaining proofs in 338.13s from worker readiness. The final 1,500-member proof stream took 136.20s. The measured post-ACK tail ran at 9.64 member reads/s. Both durations still require sustained paced completion and promotion evidence. |
| PA-12 Semantic equivalence | INCONCLUSIVE for the full scope. The prior broad regression had 475 passes, 51 skips and one inherited missing Gold 4 runbook failure. The latest focused suite passed 65 tests. The stopped audit verified 1,535 exact physical Raw payloads, Bronze delivery lineage, all three Silver fact families, 1,001 Gold receipt digests and six current-generation work IDs. The source-derived audit checked selected business fields in 11,973 frozen facts. Audited copies rejected altered receipt evidence, an altered vehicle total and an altered work ID. Live cross-phase scenarios remain open. |

The stopped scheduler snapshot verified 590 ACKed Outbox observations and Bronze identities, with 517 Gold changes, 286 receipts and 1,034 memberships. Its first source-crossed 60-second work item remained RUNNING. The 120-second and 300-second window ends were beyond the stopped source clock, so those OPEN windows are consistent with the retained input. The oracle did not independently recompute Raw bytes, Silver fact values or the unfinished Gold proof from a complete source manifest.

See [profiling-result.md](profiling-result.md) for the runs, oracle limitations, and evidence paths. L4 requires separate approval and was not executed. The owner operating envelope and Phase 4B remain pending.

The initial 330-simulation-second W2 attempt stopped before source startup at 2.38 GB available host memory. The subsequent run lost its services after production began and became the preserved incident fixture. Its trajectory fails the ordinary measurement guard in offline replay. Its bounded restart has exact delivery coverage and incomplete Silver and Gold work. The acceptance oracle requires drained eligible work and both requested durations.

The downstream-only continuation stopped at worker readiness with 1.75 GB available against the 2.5 GB reserve. It added startup evidence and advanced the durable prefix. It produced no measured drain interval or additional completed proof.

The owner-approved stopped-source continuation completed the 2,700 remaining member verifications. Its verified checkpoint, sample summary and passing read-only physical audit are under `.pytest_cache/perf-w2-paced-01/recovery-approved/`. This fixture's durable and physical verification is complete. Representative acceptance remains open.

## Latest paced minimum-gate attempt — 2026-11-25

**Verdict: ENVIRONMENT BLOCKED.** The owner-approved `perf-w2-paced-02` run did not reach its acceptance boundary. The isolated five-node stack started with 4.79 GB available host memory after startup. During normal-paced production, available memory crossed the existing 1 GiB safety floor at 206.05 simulated seconds. The runner stopped production and cleanly stopped the project. Available host memory after cleanup was 2.36 GiB. No OOM kill or container restart was recorded. Do not treat the partial run as a pass or as a failed semantic-integrity result.

The run produced 1,035 source observations over 206.05 simulated seconds (4.999 observations/s between the first and last usable samples). Raw received 1,035 and persisted 1,031; Bronze processed 1,025; Silver processed 1,005; Gold acknowledged through change 1,011. Across the 128 valid samples with source progress, Silver lag ranged from 0 to 25 changes (median 10). Gold change backlog ranged from 0 to 13 and invalidation backlog stayed at zero; at the final sample Silver reported 11 changes of lag and Gold's change backlog was zero. These snapshots show progress but do not establish bounded backlog over a complete run. The Gold worker reported up to two open work items and two completed work items; observed materialization totals were 39.93 seconds median and 55.72 seconds maximum across two samples, with a 42.92-second oldest running item at the final sample. This is a capacity risk to measure again, not enough evidence to label an unbounded queue.

There were 136 measurements, 130 valid and six invalid. Three early samples were startup observations. Three later samples reported Raw unavailable while host memory was between 2.35 and 2.68 GiB. Raw had no restart and was not OOM-killed. The saved source Outbox and active-run SQLite files pass `PRAGMA quick_check`, but that checks only those local files. The source manifest remains `RUNNING` because the approved run stopped early. The bounded post-source drain and independent cross-layer durable oracle did not run. Source time did not reach 300 seconds, so no 300-second completed-work claim is possible. The two Gold work completions were not independently checked as source-crossed 60-second proofs.

The approved command and its exact source, time-series, and cleanup evidence are retained under `.pytest_cache/perf-w2-paced-02/`, including `samples.jsonl`, `early-stop.txt`, producer logs, `source-profile.json`, and the run manifest. Preserve this partial fixture; do not resume or reuse its volumes as a fresh accepted diagnostic.

### Phase 4B entry gate and remaining acceptance

Before Phase 4B, the minimum gate still requires a valid full normal-paced run with stable measurements; concurrent Raw, Bronze, Silver, and Gold progress; demonstrably bounded Bronze-to-Silver, Gold change/invalidation, and eligible-work backlogs; independently verified completed 60-second and 300-second work; a bounded post-source drain; and a passing independent durable correctness oracle. The run must remain above the existing 1 GiB runtime floor and satisfy the 2.5 GB post-start reserve. None of these end-to-end criteria is discharged by the partial run above.

PA-01's full paced result and PA-11's two-duration paced completion remain **INCONCLUSIVE**. A complete semantic/oracle check and a bounded drain are also mandatory parts of the gate. Phase 3/4A correctness evidence remains valid for its own tested cases, including the retained stopped-source recovery and physical audit. It does not fill the missing fresh paced run.

The remaining scenarios below stay in the original PA-01–PA-12 acceptance scope. Deferral means **OPEN**, never PASS:

| Scenario | Before Phase 4B minimum gate | Reasonable final integrated-acceptance deferral |
|---|---|---|
| PA-01 Intended operation | Complete the fresh paced source-to-Gold gate described above. | Representative owner application traffic can be added once integrated. |
| PA-02 Sustained load | No 30-minute endurance claim is required for this minimum milestone, but retain the existing safety and correctness checks. | Full 30-minute qualification with real application coexistence. |
| PA-03 Controlled capacity | Keep open; do not use the partial rate as a capacity claim. | Capacity sweep after workload and serving demand are available. |
| PA-04 Burst recovery | Keep open. | Integrated burst profile after a normal operating envelope is established. |
| PA-05 Dependency recovery | Preserve the already verified stopped-source recovery subcase; continuing-arrival and individual outage cases stay open. | Integrated outage matrix, provided no Phase 3/4A correctness blocker is found. |
| PA-06 Conflict, invalidation and late data | Preserve targeted correctness tests; continuous invalidation-churn capacity stays open. | Churn/performance profile against integrated marts and serving traffic. |
| PA-07 Restart and replay | Preserve the verified same-binding replay and stopped-source recovery subcases; remaining live cases stay open. | Broader integrated restart/replay qualification. |
| PA-08 Retained history | Keep growth-scale performance open. | Larger retained-history profile after retention and query patterns are defined. |
| PA-09 Application coexistence | Not applicable to this isolated minimum gate. | Must be completed after representative projectors and serving APIs exist. |
| PA-10 Constraints and overload | Preserve the hard memory gates; no overload acceptance is inferred. | Controlled overload and recovery with the integrated deployment envelope. |
| PA-11 Gold capacity | Complete both 60-second and 300-second work in the fresh paced gate. | Broader capacity, promotion, and sustained member-throughput qualification. |
| PA-12 Semantic equivalence | Pass the existing source-derived Silver/Gold Core oracle in the fresh gate. | Mart/API-level equivalence after business marts and serving APIs exist. |

The current evidence therefore supports **ENVIRONMENT BLOCKED**, not READY FOR OWNER REVIEW or TARGETED OPTIMIZATION REQUIRED. The resource floor ended the run before the performance and durable-correctness decision boundary. The Gold materialization timings and Raw observation gaps are follow-up measurements, not authorization for speculative tuning.
