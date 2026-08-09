# Final ownership matrix

| Surface | Owning component/team | May write | Must not do |
|---|---|---|---|
| Kafka topic and topic configuration | Platform / DE | Topic configuration and records through approved producers | Change a contract incompatibly without coordination |
| Kafka event production | Realtime SUMO/TraCI durable outbox | `traffic.entity-events.v2` | Write directly to Orion or ClickHouse |
| Projector | Realtime platform | Production Orion current state and Projector checkpoint | Write Raw or Bronze tables |
| Production Orion | Projector is the only entity writer | NGSI-LD entity current state | Act as historical storage |
| Raw v2 and quarantine | DE Raw Consumer | Raw v2, quarantine and Raw ledger | Read or write Orion |
| Bronze | DE Bronze Processor | Bronze tables and Bronze checkpoint | Consume Kafka directly or read Raw v1 |
| Server | Server team | Application database; read Production Orion | Read Kafka, Raw or Bronze directly |
| Dashboard | Dashboard team | Client-side/UI state; read Server APIs | Read Orion, Kafka or ClickHouse directly |
| Event and entity contracts | Realtime + DE + Server | Versioned schemas under `contracts/` | Change wire fields without compatibility review |

## Authoritative boundaries

| Concern | Authority |
|---|---|
| Event wire format | `contracts/events/` |
| NGSI-LD entity shape | `contracts/entity/` |
| Current realtime entity state | Production Orion |
| Historical record identity | Kafka topic, partition and offset |
| Historical persisted record | Raw v2 or quarantine |
| Normalized history | Bronze tables and checkpoint |
| UI-facing API | Server |

## Rollback Assets

Webhook, Orion subscription, Raw v1 and non-default migration configuration are Rollback Assets and have no steady-state owner.
