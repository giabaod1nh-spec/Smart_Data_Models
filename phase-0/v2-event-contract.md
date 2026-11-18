# V2 event contract inventory

Source contract: `Visualize/integration/kafka/event_mapper.py`, `Visualize/integration/kafka/event_validator.py`, `contracts/canonical_json.py`, and `contracts/events/*-v2.schema.json`. Actual historical payloads: [v2 outbox cycle](fixtures/v2-outbox-historical-cycle.json). The historical fixture is from an older topology and must not set current entity cardinality.

## TrafficEntityObserved

One event encloses one **already built NGSI-LD entity** in `entity`. It is a full entity payload for upsert, not an entity change/patch and not a TraCI observation. `eventVersion=contractVersion=2.0.0`, `eventType=TrafficEntityObserved`, `source=sumo`.

Envelope fields: `eventId`, `eventVersion`, `contractVersion`, `eventType`, `source`, `simulationRunId`, `simulationTime`, `scenarioId`, `nodeId`, `cycleSequence`, `entitySequence`, `cycleEntityCount`, `capturedAt`, `producerId`, `producerSessionId`, `correlationId`, `entityPayloadHash`, `entity`; optional `nodeEntityCount`, `traceId`. Inspect the JSON Schema and `validate_entity_event` for exact machine validation; this list is the source-construction inventory, not a claim that every field is independently enforced by every consumer.

- Identity: SHA-256 of `2.0.0|simulationRunId|cycleSequence|entity.id`. One event ID corresponds to one entity in one cycle.
- Payload hash: SHA-256 of canonical JSON of the **inner entity** only. Canonical JSON sorts keys, uses compact separators and ASCII escaping.
- Kafka key: `simulationRunId:nodeId`; all entities for the same logical node in a run share the partition key.
- `capturedAt` is a publisher wall-clock timestamp. `simulationTime` is SUMO time. `cycleSequence` is per publishing cycle, `entitySequence` is index within the complete cycle, `cycleEntityCount` is total entities across all nodes, and `nodeEntityCount` is the count for that node.
- Current catalog: A=8, B=8, C=10, D=8, M=10, total **44** entity events per complete cycle (when all five nodes are published). This is a current source/catalog expectation, not proven by a retained live broker cycle.

## TrafficSimulationRunStarted

Control record produced by `build_run_started_event` and durably outboxed before the first entity cycle. Fields include `eventType`, v2 versions, source, `producerId`, `producerSessionId`, `simulationRunId`, `startedAt`; `scenarioId` and `traceId` are optional. The Outbox row ID is SHA-256 of `TrafficSimulationRunStarted|producerSessionId|simulationRunId`; the event JSON may omit `eventId`. The Projector then uses its own fallback ledger identity `run-started:{session}:{run}`. Kafka key `simulationRunId:__run__` may hash to a different partition than entity events. This record activates the run in Projector state.

## Meaning for migration

The v2 contract preserves the mapped entity, run, simulated time, scenario, sequencing and delivery metadata. It does not preserve every source snapshot field. Grouping all v2 events for a node does not invert the NGSI mapping. Preserve the original snapshot separately when defining v1.
