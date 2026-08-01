# Contracts — Smart Traffic RT / DE / Kafka

This tree holds **named** interface contracts. Do not refer to them generically as “Contract v2”.

## Named contracts

| Official name | Version | Path / notes |
|---------------|---------|--------------|
| **NGSI-LD Entity Contract** | **1.0.0** | `entity/`, `simulation/`, `trafficlight/`, `topology/` — Orion → Spring Server → Dashboard |
| **Kafka Event Delivery Contract** | **2.0.0** | `events/`, `kafka/` — Producer → Kafka → Projector / Raw Consumer |
| **Legacy Orion Notification Delivery Contract** | **1.0.0** | `delivery/` — Orion Subscription → de-webhook (until historical cutover) |

`contracts/VERSION` remains **1.0.0** for the Entity + Notification delivery lineage. Kafka Event Delivery uses `contractVersion: "2.0.0"` inside each event envelope (see `events/`).

## Compatibility (Entity / Notification 1.0.0)

- **Allowed after freeze:** additive Properties; new optional entities with a new contract version note.
- **Breaking (forbidden without VERSION bump + DE migration):** rename fields, change units/semantics, change Relationship targets.

Kafka Event Delivery compatibility: [`kafka/COMPATIBILITY.md`](kafka/COMPATIBILITY.md).

## Ownership legend

Each `*_contract_v1.md` starts with Producer / Consumer / Owner / Delivery (or Access).

| Role | Typical party |
|------|----------------|
| Producer | Realtime Simulation |
| Consumer | Data Engineering |
| Owner | Realtime Team |
| Delivery | Orion Subscription → DE Webhook (legacy) or Kafka topics (v2) |

## Layout

| Path | Content |
|------|---------|
| `entity/` | Field SHALL + golden NGSI-LD payloads |
| `simulation/` | simulationTime / simulationRunId / scenarioId semantics |
| `trafficlight/` | currentPhase SoT; duration = configured |
| `topology/` | Per-run package contents; resolve by simulationRunId |
| `delivery/` | Notification schema + golden `notification.example.json` |
| `events/` | Kafka Event Delivery Contract 2.0.0 schema + examples |
| `kafka/` | Topics, delivery semantics, compatibility |
| `canonical_json.py` | Shared canonical hash (Producer / DE / DVT / Projector) |
| `tests/` | Offline contract tests only |

## How DE starts (legacy Notification path)

1. Read `delivery/notification.example.json` (golden) — code webhook parser **now**.
2. Read `entity/payloads/*.jsonld` for entity shapes inside `data[]`.
3. Resolve topology package by `simulationRunId` (see `topology/`).

## How Kafka path starts (Event Delivery 2.0.0)

1. Read `events/README.md` + `events/traffic-entity-event-v2.schema.json`.
2. Use `events/examples/*-event.json` as golden Entity Events.
3. Read `kafka/DELIVERY_SEMANTICS.md` and `docs/architecture/KAFKA_FAILURE_SEMANTICS.md`.

## Related (not Contract)

| Location | Role |
|----------|------|
| [`docs/implementation/`](../docs/implementation/) | How RT implements SHALL |
| [`docs/architecture/`](../docs/architecture/) | Kafka failure semantics / pipeline ADRs |
| [`integration/`](../integration/) | Live E2E proof |
| [`Visualize/`](../Visualize/) | Runtime source — never under `contracts/` normative text |

## Tests

```bash
pytest contracts/tests
```
