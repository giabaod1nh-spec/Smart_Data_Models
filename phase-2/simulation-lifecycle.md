# Phase 2 simulation lifecycle

The active loop is `Visualize/app/traci_runner.py:run` → `SumoBackend.step` → optional V2 publication in the runner. The independent V2 publish counter in the runner is **not** the V1 sampling sequence.

At each `SumoBackend.step`, commands and demand actuators run first. `traci.simulation_step()` advances SUMO and increments `_completed_simulation_steps` only when the call succeeds. Signal pending phases/preemption are then updated, and `simulation_time_sec` is read directly from `traci.simulation.getTime()` without rounding. In ADAPTIVE mode, `AgentManager.maybe_tick` runs next. Its `get_snapshot_fresh` reads TraCI independently; the observation cache never replaces this control input.

The backend then performs live-stream/trip bookkeeping. At `OBSERVATION_INTERVAL_SEC` it increments `_observation_seq` **before** collecting nodes. Each node's `SumoSnapshotProvider.build_snapshot` performs the existing physical TraCI reads. With V1 enabled, the same read produces a measurement-quality sidecar; the V2 view drops the sidecar and retains the legacy numeric values. A failed node capture remains absent from the V2 `fresh_snaps` dict, as before. `context_tick` and cache update still run on available nodes. The V1 cycle then requires every frozen participating node. A failure consumes the sequence and yields a `V1CycleResult` with a reason and no observations.

On a complete capture, `capturedAt` is assigned once after cache/context collection. The immutable canonical snapshot stores the unrounded SUMO clock, completed step, sampling sequence, run ID, frozen topology digest, and all participating intersections. V1 events are built and validated from that snapshot. The backend exposes the last result and catalog readiness through `get_stats().v1_observation` when V1 is enabled; failure transitions are also logged. The runner may write the first successful cycle to a JSON file when explicitly requested. No V1 Kafka, Outbox, Orion, or projector path runs in Phase 2.

Clock ownership:

- `simulationTimeSeconds`: unrounded `SumoBackend.simulation_time_sec` from SUMO.
- `simulationStep`: `_completed_simulation_steps`, incremented after successful TraCI advancement.
- `cycleSequence`: `_observation_seq`, incremented for every attempted observation cycle.
- `capturedAt`: one UTC wall-clock timestamp assigned at successful canonical capture completion.
- V2 publish `cycle_sequence`: separate runner counter, retained for existing V2 behavior.

Read/fallback inventory: `SumoSnapshotProvider._build_direction` skips failed lane/vehicle reads; E2 detector methods can return partial data; lane occupancy falls back to `0.0` when no read succeeds. V1 quality marks affected core metrics unavailable and the builder emits `null` plus `metric_availability`. The V2 view retains its previous numeric values. Required phase/color, identity, approach, run context, and archive failures invalidate the whole V1 cycle.
