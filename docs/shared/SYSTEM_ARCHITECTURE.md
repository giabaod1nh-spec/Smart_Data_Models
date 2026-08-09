# System architecture

## Architecture rule

Kafka is the only production transport for both realtime projection and historical ingestion.

```text
SUMO / TraCI
    |
    v
Durable Outbox
    |
    v
Kafka: traffic.entity-events.v2
    |-- Projector --> Production Orion --> Server --> Dashboard
    `-- Raw Consumer --> Raw v2 / Quarantine --> Bronze
```

SUMO must not write directly to Orion. The Projector is the only owner allowed to project Kafka entity events into Production Orion. Raw Consumer is the only owner allowed to classify Kafka records into Raw v2 or quarantine. Bronze reads Raw v2 and quarantine; it does not consume Kafka directly.

## Components

| Component | Responsibility | Authoritative input | Output |
|---|---|---|---|
| SUMO/TraCI | Produce simulation entities | Simulation state | Durable outbox records |
| Durable Outbox | Persist before publish and deliver at least once | Entity event | Kafka record |
| Kafka | Durable transport and offset identity | Event Contract 2.0.0 | Independent consumer streams |
| Projector | Maintain current context | Kafka | Production Orion entities |
| Production Orion | Current NGSI-LD state | Projector | Server reads |
| Raw Consumer | Preserve and classify history | Kafka | Raw v2 or quarantine |
| Bronze Processor | Normalize historical records | Raw v2 and quarantine | Bronze tables and checkpoint |
| Server | Expose application and realtime APIs | Production Orion | Dashboard API |
| Dashboard | Present application state | Server API | User interface |

## Data authority

- Realtime current-state authority: Production Orion, populated only by Projector.
- Historical delivery identity: Kafka `(topic, partition, offset)`.
- Historical persisted authority: exactly one Raw v2 or quarantine classification per Kafka record.
- Normalized historical authority: Bronze tables plus their checkpoint.
- User-facing access: Dashboard reads Server APIs, not Orion, Kafka or ClickHouse directly.

## Failure boundaries

- A Projector or Orion failure must not stop Raw ingestion.
- A Raw or ClickHouse failure must not stop realtime projection.
- Consumers commit only work they have durably applied according to their own checkpoint rules.
- Kafka delivery is at least once; consumers must be idempotent.

## Rollback Assets

Webhook, Orion subscription, Raw v1 and non-default migration configuration are Rollback Assets. They are not part of the default runtime.
