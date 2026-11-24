# Performance Acceptance checkpoint

Technical acceptance remains INCONCLUSIVE. The isolated normal-step diagnostics established insufficient Silver service at the tested rates, increased Silver progress with exact grouped reconciliation and backlog-aware polling, and exposed unfinished Gold materialization with no Gold ACK backlog. The latest diagnostic stopped on host memory pressure. The observed source pace fell below 1× in all three sampled runs. No representative integrated or endurance capacity is certified.

| Scenario | Technical evidence and verdict |
|---|---|
| PA-01 Intended operation | INCONCLUSIVE. The normal-step diagnostics preserved the source settings and measured 4.19–4.45 observations/s with pacing 0.838–0.890. The latest prefix still grew at Bronze-to-Silver and its first eligible window was incomplete. The tested stack omitted representative application coexistence. |
| PA-02 Sustained load | INCONCLUSIVE. The short diagnostics do not cover sustained normal operation. The 30-minute qualification was held after the host memory stop. |
| PA-03 Controlled capacity | INCONCLUSIVE. Retained canonical reads are operation measurements. No integrated capacity sweep was executed. |
| PA-04 Burst recovery | INCONCLUSIVE. No approved steady envelope and no genuine burst with ongoing production were measured in this round. |
| PA-05 Dependency recovery | INCONCLUSIVE. Core dependency and restart tests provide targeted correctness evidence. No current full-path outage recovery was timed. |
| PA-06 Conflict, invalidation and late data | INCONCLUSIVE for performance. The enabled real feed and Gold Core tests passed for targeted correctness, including related and unrelated changes, late observations and suppression. Continuous churn capacity remains open. |
| PA-07 Restart and replay | INCONCLUSIVE for the full scenario. Targeted local and real Core cases passed. A fresh retained Gold replay completed 450 contiguous changes in 69.11s, verified 900 memberships and matched the original 300-member proof hash. The complete current pipeline restart run is pending. |
| PA-08 Retained history | INCONCLUSIVE. The small verified retained probe established read costs on its existing prefix. It did not cover retained-history growth. |
| PA-09 Application coexistence | INCONCLUSIVE. Owner services were present. Representative projector and serving traffic were outside these diagnostic fixtures. |
| PA-10 Constraints and overload | INCONCLUSIVE. The diagnostic stopped at the host memory floor and preserved durable evidence. Controlled overload and recovery were outside its measured scope. |
| PA-11 Gold capacity | INCONCLUSIVE. The latest paired retained read path measured 11.08 members/s. A separate retained replay completed one 300-member 60-second proof in 39.88s. The latest production diagnostic left its first 60-second proof RUNNING. The 300-second stream and sustainable paced completion remain open. |
| PA-12 Semantic equivalence | INCONCLUSIVE for the full scope. The broad local Raw, Bronze, Silver, Gold runtime and contract suites recorded 475 passed, 51 skipped and one inherited failure because the legacy Gold 4 runbook is absent from both the Core baseline and HEAD. The independent retained audit passed for 450 source rows through exact Raw bytes, Bronze lineage, physical Silver facts and the current Gold proof digest. The latest paced profile and remaining live cross-phase scenarios are still pending. |

The stopped scheduler snapshot verified 590 ACKed Outbox observations and Bronze identities, with 517 Gold changes, 286 receipts and 1,034 memberships. Its first source-crossed 60-second work item remained RUNNING. The 120-second and 300-second window ends were beyond the stopped source clock, so those OPEN windows are consistent with the retained input. The oracle did not independently recompute Raw bytes, Silver fact values or the unfinished Gold proof from a complete source manifest.

See [profiling-result.md](profiling-result.md) for the runs, oracle limitations, and evidence paths. L4 requires separate approval and was not executed. The owner operating envelope and Phase 4B remain pending.

The 330-simulation-second W2 diagnostic stopped before source startup at 2.38 GB available host memory against a 2.5 GB preflight reserve. The audit's 60-second-only success cannot satisfy the two-duration gate. The CLI rejected the retained fixture when both durations were required.
