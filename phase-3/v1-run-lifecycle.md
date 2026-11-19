# V1 run identity and fencing

The backend freezes its in-memory runtime catalog, archives it, verifies the archived V1 digest against the SUMO net, and then writes `run_manifest.json` atomically. The manifest records `simulationRunId`, legacy `topology_hash`, V1 `topologyDigest`, actual `participatingIntersectionIds`, `runGeneration`, and `runStatus`. It is read back before SUMO publishes.

`Visualize/artifacts/runs/active_run.sqlite3` is the authoritative live-run selector. The launcher allocates a generation, writes and verifies the manifest, starts SUMO, and atomically activates that run. A stale or concurrent generation is rejected. Observations never activate runs. A later run replaces the live selection; old messages cannot select their own run. An explicit `select_replay(run_id)` lookup does not change the live selection.

The registry status is authoritative; the manifest status is a readable mirror. `RUNNING` permits new publication. `COMPLETE` means required captures were durably accepted by SQLite, though Kafka delivery may still be draining. `INCOMPLETE` records a failed structural capture or persistence failure. `STOPPED` means the runtime ended before its configured completion condition. Phase 4 should compare the incoming run ID and generation with the selector, and handle late messages for the selected completed run without switching back to an older run.

A structural failure writes `v1_failed_cycle.json` in the run folder, including sequence, reason, timestamp, and count, before stopping the run. The V1 observation topic contains no lifecycle or failure records.
