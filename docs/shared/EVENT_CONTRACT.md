# Kafka event contract

## Contract identity

| Item | Value |
|---|---|
| Contract | Kafka Event Delivery Contract |
| Version | `2.0.0` |
| Main topic | `traffic.entity-events.v2` |
| Producer | SUMO/TraCI durable outbox |
| Consumers | Projector and Raw Consumer |
| Record grain | One Kafka record per entity event |
| Record key | `simulationRunId:nodeId` |
| Delivery | At least once |
| Ordering | Per `(simulationRunId, nodeId)` partition key |

Machine-readable schemas are authoritative:

- [`traffic-entity-event-v2.schema.json`](../../contracts/events/traffic-entity-event-v2.schema.json)
- [`traffic-simulation-run-started-v2.schema.json`](../../contracts/events/traffic-simulation-run-started-v2.schema.json)
- Golden examples in [`contracts/events/examples/`](../../contracts/events/examples/)

## `TrafficEntityObserved`

| Field | Type | Requirement | Meaning |
|---|---|---|---|
| `eventId` | 64-character lowercase hex string | Required | Deterministic event identity |
| `eventVersion` | string | Required, `2.0.0` | Envelope version |
| `contractVersion` | string | Required, `2.0.0` | Contract version used for identity |
| `eventType` | string | Required, `TrafficEntityObserved` | Event discriminator |
| `source` | string | Required | Producing source, normally `sumo` |
| `simulationRunId` | string | Required | Simulation run identity |
| `simulationTime` | number, `>= 0` | Required | Simulation clock in seconds |
| `scenarioId` | string | Required | Active scenario |
| `nodeId` | string | Required | Partitioning node |
| `cycleSequence` | integer, `>= 0` | Required | Monotonic cycle number within producer session |
| `entitySequence` | integer, `>= 0` | Required | Entity position in the cycle |
| `cycleEntityCount` | integer, `>= 1` | Required | Expected entities in the cycle |
| `nodeEntityCount` | integer, `>= 1` | Required by runtime apply path | Expected entities for the node micro-batch |
| `capturedAt` | ISO-8601 UTC string | Required | Wall-clock capture timestamp |
| `producerId` | string | Required | Producer identity |
| `producerSessionId` | string | Required | Producer process/session identity |
| `traceId` | string | Required | Trace identity |
| `correlationId` | string | Required | Run/cycle correlation identity |
| `entityPayloadHash` | 64-character hex string | Required | Canonical hash of `entity` |
| `entity` | NGSI-LD object | Required | Entity Contract 1.0.0 payload |

Deterministic identity:

```text
eventId = SHA-256(contractVersion + "|" + simulationRunId + "|" + cycleSequence + "|" + entity.id)
entityPayloadHash = SHA-256(canonical_json(entity))
```

Canonical JSON and hashing are defined by [`contracts/canonical_json.py`](../../contracts/canonical_json.py).

## `TrafficSimulationRunStarted`

This control event establishes the active producer session/run for current-state projection.

| Field | Type | Requirement |
|---|---|---|
| `eventType` | string | Required, `TrafficSimulationRunStarted` |
| `eventVersion` | string | Required, `2.0.0` |
| `contractVersion` | string | Required, `2.0.0` |
| `source` | string | Required |
| `producerId` | string | Required |
| `producerSessionId` | string | Required |
| `simulationRunId` | string | Required |
| `startedAt` | ISO-8601 UTC string | Required |
| `scenarioId` | string | Required |

Only this event type may activate a run in the Projector.

## Consumer rules

- A duplicate is possible; consumers must be idempotent.
- Raw lineage is `(topic, partition, offset)`.
- Invalid envelopes go to quarantine; they must not be silently dropped.
- Consumers commit only a contiguous durably processed prefix for each partition.
- Envelope simulation metadata must equal the corresponding properties inside `entity`.
