# Data preservation through V2

Evidence: `Visualize/observation/snapshot_provider.py:92`, `Visualize/simulation/backend.py:455`, `Visualize/integration/orion/entity_mapper.py`, `Visualize/integration/kafka/event_mapper.py`. “Orion” means the mapped entity attribute, subject to successful upsert and later updates; no same-cycle live trace was available.

| Source snapshot field | Entity/V2 representation | Orion representation | Finding |
|---|---|---|---|
| `node_id` | logical URN suffix and event `nodeId` | logical entity ID | Preserved by mapping; physical TLS ID is not Orion identity. |
| `simulation_run_id` | `simulationRunId` Property and event field | `simulationRunId` | Preserved. |
| `simulation_time_sec` | `simulationTime` Property and event field | `simulationTime` | Preserved as seconds. |
| `scenario` | `scenarioId` Property and event field | `scenarioId` | Per-node scenario retained; cycle summary can be `mixed`. |
| `phase` | `currentPhase` on Intersection/TrafficLight | `currentPhase` | Preserved on those types. |
| `colors[direction]` | TrafficLight `currentStatus` after color/status mapping | `currentStatus` | Transformed; original color token may not be recoverable exactly. |
| `green_duration`, `red_duration`, `yellow_duration` | TrafficLight duration Properties | durations | Fallback defaults can replace missing values. |
| `directions[*].vehicle_count` | VehicleSensor, Intersection total, Camera count | `vehicleCount`/`totalVehicleCount` | Per-direction and aggregate values retained with different scopes. |
| `pcu_equivalent`, turn counts, speed, waiting count, queue length, occupancy | VehicleSensor Properties; selected Camera aggregate | same attributes | Units/aggregation in entity mapper; inspect physical sensing definitions before v1 contract. |
| `vehicle_class_composition`, arrival rate and waiting reasons | VehicleSensor optional/defaulted properties | selected sensor attributes | Some absent values become defaults or omitted attributes. |
| `incidents`, derived phenomena, operational state, probable causes | Boolean/status/selected cause properties | selected properties | Full underlying context list is not preserved. |
| `next_phase` | no mapping found | absent | Source snapshot field lost in v2 entity construction. |
| `phase_remaining` | no mapping found | absent | Source snapshot field lost. |
| `blocked_direction` | no mapping found | absent | Source snapshot field lost. |
| `yellow_commitment_count` | no mapping found | absent | Source snapshot field lost. |
| `vehicles_blocked_at_entry`, raw `spillback_pressure`, `downstream_edge_full` | only selected derived flags | selected booleans | Raw values lost or reduced. |
| snapshot `config_hash`, `pcu_profile_id`, schema versions, link states | no general entity mapping found | absent | Run manifest and runtime files may retain some provenance; v2 event does not carry the full snapshot. |

The four specifically requested control/context fields are **not** automatically required for v1. Phase 1 must decide whether each is needed by a consumer. This matrix establishes only current preservation/loss.
