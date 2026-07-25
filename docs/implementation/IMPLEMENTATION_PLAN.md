# Implementation Plan — RT-DE Contract v1.0.0

Maps Contract SHALL → current Realtime modules. **Not** part of the Contract SoT.

## Runtime obligations

| SHALL | Current module |
|-------|----------------|
| Publish `simulationTime`, `simulationRunId`, `scenarioId` on 4 entity types | `Visualize/integration/orion/entity_mapper.py` (`_sim_meta_props`) |
| Publish `currentPhase` on TrafficLight + Intersection | same (`_current_phase_prop` ← snapshot `phase`) |
| Forward run/scenario onto snapshot before publish | `Visualize/simulation/backend.py` (`_attach_publish_identity`) |
| Per-run topology package + hash match | `backend.py` start: copy catalog next to `run_manifest.json` |
| Upsert to Orion | `Visualize/integration/orion/client.py` |
| Publish cadence | `Visualize/app/traci_runner.py` (`PUBLISH_INTERVAL`, default 1.0s) |

## Duration semantics

`greenDurationCurrent` / `redDurationCurrent` / `yellowDuration` = **configured** lengths from TraCI program (seconds), not remaining time.

## Access mechanism (deployment choice)

DE resolves packages by `simulationRunId`. This project uses a **shared Docker named volume** (or bind mount of `Visualize/artifacts/runs`). See [deployment.md](deployment.md).

Alternatives (S3, HTTP, K8s PVC) may replace the volume later **without** changing Topology Contract SHALL.
