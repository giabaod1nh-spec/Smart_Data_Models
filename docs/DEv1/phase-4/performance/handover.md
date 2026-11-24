# Performance optimization handover

## Current state

Planning only. No production code, test, migration, Compose, runtime config or benchmark was changed/executed for this plan. Repository baseline at audit start: `a92767728ee6eec906a26104a2cb7fb0d33b8bb2`, branch `codex/phase-1-v1-raw`, clean worktree. Current retained Core result is **CAPACITY INSUFFICIENT** for diagnostic 4.95 observations/s: Silver 0.960 sequence/s and Bronze→Silver backlog growth 4.109/s. Gold completed-work capacity is unknown. Production activation and Phase 4B are not ready.

## Execution prerequisites

1. Owner approves intended SUMO step/capture/participant/scenario workload and whether the one-second historical profile is comparison-only.
2. Owner approves normal arrival target, finite burst B and recovery time T for each stage-relevant unit, end-to-end latency objectives, local hardware/resource limits, disk/retention/runway and acceptable realtime regression.
3. Preserve current accepted semantics and source binding. No pruning/rewind of Raw/Bronze/Silver journals while current consumers depend on them.
4. Prepare isolated namespaces, databases, broker topic, SQLite paths/volumes and run artifacts; inventory before/after. Fault tests never use production state.
5. Capture baseline commit/config/image versions and raw profiles before each code change; do not overlap regression test load with performance trials unless concurrency is the scenario.

## Recommended execution order

`PERF-00` freeze requirements → `PERF-01` instrument and independently account → profile producer/Raw/Bronze, change order based on evidence (`PERF-02/03`) → optimize Silver (`PERF-04`) → profile/tune canonical feed and Gold work (`PERF-05`) → full integrated stack and resource balance (`PERF-06`) → controlled acceptance PA-01..12 (`PERF-07`). Keep separate verdicts for correctness, capacity, resource envelope, operations and phase readiness.

## Required evidence package per optimization

- Approved workload manifest plus source/config hashes and environment versions.
- Before/after matched workload inputs, raw time series, samples and uncertainty method.
- Per-stage λ/μ, queue slope/age/bytes and backlog throughout warm-up, early/mid/late steady, burst, recovery and drain.
- Nested spans, service-attributed CH queries, CPU/RSS/IO/network/disk/WAL and Kafka resources.
- Independent accounting/oracle for source, Raw, Bronze dispositions/changes, Silver canonical values/lineage/validity/obligations, Gold receipt/membership/suppression/work generation and full drain.
- Test and fault-injection inventory, failure logs, known baseline failures, images and reproducible commands.
- Before/after matrix including realtime stack, rollback target and state compatibility.

## Rollback and operations

Retain prior image/config and non-destructive recovery runbook. A rollback never rewinds authoritative state or deletes source evidence. Stop the changed worker safely; preserve SQLite files/WAL and CH/Kafka records; restore compatible code/config; verify source binding and durable cursors; independently reconcile pending work; resume only after no skipped/duplicate semantic changes are found. If schema changed, use a forward-compatible additive migration; do not promise destructive down-migration.

## Phase 4B entry gate

Start 4B only when: Core correctness hardening and applicable Phase 1–4A regressions pass; upstream guarantees are unchanged; approved representative rate is sustained with no increasing required-stage queues; finite backlog recovers under continuing normal arrival within owner-approved B/T; Gold ACK **and** eligible 60s/300s work remain sustainable; conflict/invalidation/restart/outage and long-history cases pass; per-service resource and realtime coexistence are accepted; full independent end-to-end oracle passes; no critical performance blocker remains; owner signs the evidence. Future KPI/mart/Spring API/Dashboard performance is a separate acceptance.

## Related documents

- [audit.md](audit.md): actual path, source inventory and findings.
- [baseline.md](baseline.md): retained evidence recalculation and limitations.
- [requirements.md](requirements.md): workload/queue/headroom model and proposed duration.
- [design.md](design.md): options and recommended architecture.
- [instrumentation.md](instrumentation.md): profile and metrics specification.
- [plan.md](plan.md): packages and gates.
- [acceptance.md](acceptance.md): PA-01..12, oracles and entry gate.
- [decisions.md](decisions.md): owner decision register.
- [blockers.md](blockers.md): staged blockers/dependencies.
