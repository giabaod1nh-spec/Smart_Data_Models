# Performance optimization handover

## Current evidence and readiness

Planning revision only. Core diagnostic showed insufficient capacity at its tested profile: ~4.946 source observations/s, ~0.960 Silver applied sequences/s and +4.109 Bronze→Silver backlog sequences/s. This diagnostic uses a 1.0s SUMO step, whereas normal YAML defaults to 0.01s; normal publish/cache intervals are each one simulation-second, participants default A/B/C/D/M, and scenario triggers can add captures. W2 rate must be measured with actual simulation pacing and full app stack. Gold completed-work capacity is not established: one proof in the trial; independent 597-sequence prefix oracle was not a full drain.

Phase 2 report/sample artifacts are tracked and recalculated in `baseline.md`. The 12.32s, 37.30s and 15.81s values are finite catch-up results; the sampled counter difference of 415 is not necessarily a simultaneous durable backlog. They do not demonstrate Bronze maximum capacity.

## Final execution order

1. **PERF-00A / G0:** owner freezes W1 historical comparable, W2 intended normal workload, W3 stress bounds, source fidelity, stack scope, invariant set, hardware and safe isolated profiling bounds. No final SLO required.
2. **PERF-00B:** reproduce Core replay-final-boundary transient failure; minimal fix only if confirmed. Test Silver scheduler fairness without weakening recovery/invalidation ordering.
3. **PERF-01:** bounded complete-path instrumentation and controlled baseline; profile producer/Outbox/Kafka/Raw/Bronze, Silver apply, Silver feed, and early Gold G-A/G-B. Attribute hidden queues and query cost.
4. **G1:** engineering presents measured target workload/headroom/latency/age/recovery/resource/retention/realtime goals; owner approves numeric acceptance objectives.
5. **Evidence-gated optimization:** PERF-04 Silver first given current stage-level bottleneck; PERF-05 canonical feed/Gold based on early capacity results. PERF-02/03 upstream changes only when measured or an upstream constraint prevents valid downstream profiling. Re-measure whole pipeline after each change.
6. **PERF-06:** integrated resource and pipeline balance with intended app stack.
7. **PERF-07:** qualify L0→L1→L2→L3; use L4 only when endurance/history drift/final confidence warrants. Execute PA-01..PA-12.
8. Owner reviews full evidence. Only then may Phase 4B planning begin.

## Required evidence per package

- Effective workload manifest/config/image/version and source pacing/accounting.
- Before/after nested spans and per-stage service/arrival rates, queue unit/age/bytes/slope, resources and uncertainty.
- Gold ACK and current-generation 60s/300s completion/rework separately.
- Independent producer/Raw/Bronze/Silver/Gold/full-drain oracle, including partial disposition, invalidation, binding and retained-payload limits.
- Fault/restart, regression and persistent-state compatibility matrix; evidence hashes and reproducible procedure.
- Full path matched trial after every significant optimization; all upstream/downstream queues and normal realtime services checked.
- Rollback image/config, state compatibility, forward recovery and operator action without source rewind/truncation.

## Rollback and operations

Preserve the previous compatible image/config and all source/checkpoint state. Stop changed workers safely, retain SQLite/WAL and CH/Kafka evidence, restore only a state-compatible version, verify binding/cursors, reconcile pending work and compare the independent oracle before resuming. If old code cannot read new state, do not image-rollback blindly; follow approved forward recovery/backup procedure. Never delete authoritative data to make a queue empty.

## Phase 4B gate

Core hardening and applicable Phase 1–4A regressions pass; accepted upstream guarantees remain unchanged; W2 capacity is sustainable with G1 headroom and no required queue growth; burst/outage global recovery passes while normal production continues; Gold ACK and eligible current-generation 60s/300s work both sustain; conflict/invalidation/replay/history cases and full independent drain pass; resources and realtime coexistence fit approved envelope; no critical blocker remains; owner accepts evidence. KPI, marts, Spring API and Dashboard performance remain a separate later acceptance.

## Related planning files

- [audit.md](audit.md) — source path, historical/runtime findings and review evidence.
- [baseline.md](baseline.md) — recalculated Core/Phase 2 measurements and W1–W3 details.
- [requirements.md](requirements.md) — G0/G1, workload and global recovery model.
- [design.md](design.md) — Silver batching, Gold A–D and cross-stage impacts.
- [instrumentation.md](instrumentation.md) — offered-load, hidden backlog, Gold and query measurement.
- [plan.md](plan.md) — PERF-00A/B through PERF-07 dependency order.
- [acceptance.md](acceptance.md) — L0–L4, PA-01–PA-12 and independent oracles.
- [decisions.md](decisions.md) — inherited invariants, G0/G1 decisions and R01–R12 matrix.
- [blockers.md](blockers.md) — current stage-specific blockers and dependencies.
