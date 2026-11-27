# D-A14 — Proposed authoritative amendment text

**Owner selected Conditional Entry. Authorization is pending.** This Stage A document records the decision and exact text to submit. Existing minimum gate still governs until the authoritative amendment has been approved and recorded. Retained Performance Acceptance remains ENVIRONMENT BLOCKED.

## Destination and approval procedure

Insert the text below in `docs/DEv1/phase-4/performance/acceptance-result.md`, immediately following the Phase 4B entry-gate paragraph. Reference that approved amendment in the governing Phase 4A performance/Phase 4B entry decision record if repository owner requires. This task must not edit those files. Performance owner and project owner approve exact scope and effective revision in a separate scoped change. Source owners authorize UP-01 and UP-02 packages individually. Work-package owners record fixture/limits and design approvals before use. Validate the merged/accepted authoritative revision and authorization record before opening isolated work. A decision in a Stage A Markdown file alone does not activate entry.

## Exact insertion proposed

> **D-A14 — Conditional implementation exception for isolated Phase 4B Stage A.** Effective only when approved and recorded by the project and Performance Acceptance owners. Separately authorized UP-01 schedule/lifecycle and UP-02A/UP-02B source correctness work may proceed within approved isolated environments. After applicable designs and oracle are pinned, A-I01 may implement fixture-backed independent metrics, and A-I02 may implement additive state and rehearsal on disposable SQLite. A-I03 may implement isolated failure-path slices on disposable stores after A-I02 and G-MART/G-REV design approval. Scope excludes live Business publication, production migrations, production activation/cutover, and A-I04/A-I05/A-I06/A-I07 without separate applicable approvals. G-COV and G-REV continue to govern official publication. No requirement or production performance criterion is waived.
>
> Pin input versions and fixture IDs, verify isolation, use bounded runtime and disposable databases, enforce 2.5 GB post-start host reserve and 1 GiB runtime memory floor when applicable, plus approved test limits for bytes, memory, disk, transactions and deadlines. Stop on proof/authority invariant failure, guard violations, lost isolation or stalled drain outside budget. Stop isolated workers, disable feature and preserve durable checkpoints, artifacts and authoritative evidence. Rollback rehearsals use disposable stores and version-aware recovery. Do not mutate protected baselines.
>
> This exception does not change PA-01–PA-12, the normal-paced minimum gate for full entry, either duration, bounded drain, independent durable oracle, representative Realtime coexistence or operational qualification. Full integration, live activation and production changes require approved prerequisite results and the existing gate evidence. Do not record G-PERF PASS based on isolated fixtures.

First coding task after effective amendment and fixture-owner sign-off: **A-I01a** pure bounded reducer + independent metrics on frozen 60/300 fixture, no durable stores or upstream dependency. UP-01A can start in parallel only under its separate authorization. See [freeze roadmap](plan.md).
