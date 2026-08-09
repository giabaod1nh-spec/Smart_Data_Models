# Kafka Event Delivery Contract 2.0.0

**Official name:** Kafka Event Delivery Contract  
**Version:** `2.0.0`  
**Grain:** exactly **one Kafka record = one Entity Event** (`TrafficEntityObserved`).

Related contracts (do not conflate names):

| Name | Version | Role |
|------|---------|------|
| NGSI-LD Entity Contract | 1.0.0 | Inner `entity` shape; Orion → Server → Dashboard |
| Kafka Event Delivery Contract | 2.0.0 | This envelope (Producer → Kafka → Projector / Raw) |
| Legacy Orion Notification Delivery Contract | 1.0.0 | Subscription Notification → de-webhook |

## Schema & examples

| Path | Role |
|------|------|
| [traffic-entity-event-v2.schema.json](./traffic-entity-event-v2.schema.json) | Machine schema |
| [examples/intersection-event.json](./examples/intersection-event.json) | Golden single-entity event |
| [examples/trafficlight-event.json](./examples/trafficlight-event.json) | Golden |
| [examples/vehiclesensor-event.json](./examples/vehiclesensor-event.json) | Golden |
| [examples/camera-event.json](./examples/camera-event.json) | Golden |
| [examples/run-started-event.json](./examples/run-started-event.json) | Control: `TrafficSimulationRunStarted` |
| [examples/full-cycle-manifest.example.json](./examples/full-cycle-manifest.example.json) | **Oracle only** — not a topic payload |
| [traffic-simulation-run-started-v2.schema.json](./traffic-simulation-run-started-v2.schema.json) | Run-start control schema |

## Deterministic identities

```text
eventId = SHA-256(contractVersion + "|" + simulationRunId + "|" + cycleSequence + "|" + entityId)
entityPayloadHash = canonical_hash(entity)   # shared contracts.canonical_json
```

Canonical algorithm: see [../canonical_json.py](../canonical_json.py) and [../kafka/COMPATIBILITY.md](../kafka/COMPATIBILITY.md).

## Cycle invariants (same simulationRunId + cycleSequence)

- Identical `cycleEntityCount` (= N) on every event
- `entitySequence` unique and in `[0, N-1]`
- `entity.id` unique within the cycle
- `entitySequence < cycleEntityCount`
- Envelope sim metadata matches entity Properties

## Node micro-batch (K-3)

- Optional additive field `nodeEntityCount` (required for Projector apply path)
- Within `(simulationRunId, cycleSequence, nodeId)`: identical `nodeEntityCount`, no duplicate `entityId`, distinct count ≤ `nodeEntityCount`

## Active-run control

Only `TrafficSimulationRunStarted` may activate a producer session / simulation run for Orion current-state. Entity Events never flip active run.

Violations → quarantine + contract_violation (runtime consumers).

## Topics

See [../kafka/TOPICS.md](../kafka/TOPICS.md).
