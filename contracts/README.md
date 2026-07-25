# RT-DE Contract v1.0.0

**Source of Truth** for the interface between **Realtime (Producer)** and **Data Engineering (Consumer)**.

## Contract Version

```text
Contract Version: 1.0.0
Applies to entity types:
  - Intersection
  - TrafficLight
  - VehicleSensor
  - Camera
```

DE parsers SHALL bind to this version. Bump major on breaking field/semantics/relationship changes; additive fields → minor/patch.

**Do not** require a `contractVersion` Property on Orion entities in v1.0.0.

## Compatibility policy

- **Allowed after freeze:** additive Properties; new optional entities with a new contract version note.
- **Breaking (forbidden without VERSION bump + DE migration):** rename fields, change units/semantics, change Relationship targets.

## Ownership legend

Each `*_contract_v1.md` starts with Producer / Consumer / Owner / Delivery (or Access).

| Role | Typical party |
|------|----------------|
| Producer | Realtime Simulation |
| Consumer | Data Engineering |
| Owner | Realtime Team |
| Delivery | Orion Subscription → DE Webhook |

## Layout

| Path | Content |
|------|---------|
| `entity/` | Field SHALL + golden NGSI-LD payloads |
| `simulation/` | simulationTime / simulationRunId / scenarioId semantics |
| `trafficlight/` | currentPhase SoT; duration = configured |
| `topology/` | Per-run package contents; resolve by simulationRunId |
| `delivery/` | Notification schema + **golden** `notification.example.json` |
| `tests/` | Offline contract tests only |

## How DE starts

1. Read `delivery/notification.example.json` (golden) — code webhook parser **now**.
2. Read `entity/payloads/*.jsonld` for entity shapes inside `data[]`.
3. Resolve topology package by `simulationRunId` (see `topology/`); access mechanism in `docs/implementation/`.

## Related (not Contract)

| Location | Role |
|----------|------|
| [`docs/implementation/`](../docs/implementation/) | How RT implements SHALL (file map, volume, Orion pin) |
| [`integration/`](../integration/) | Live E2E proof; `captured/` is evidence only — **not** golden SoT |
| [`Visualize/`](../Visualize/) | Runtime source (mapper, backend) — never under `contracts/` |

## Boundary (what does NOT belong here)

- `IMPLEMENTATION_PLAN.md`, deployment/volume narratives as normative text
- Live E2E / `thin_webhook.py` / Orion harness
- `notification.captured.example.json` (lives in `integration/captured/`)
- Runtime Python modules (`entity_mapper.py`, `backend.py`, …)

## Tests

```bash
pytest contracts/tests
```