# Simulation Contract v1

| Role | Party |
|------|-------|
| Producer | Realtime Simulation |
| Consumer | Data Engineering |
| Owner | Realtime Team |
| Delivery | Orion Subscription → DE Webhook (fields on entities) |

## SHALL

| Field | Meaning |
|-------|---------|
| `simulationTime` | Current simulation clock (seconds). **Trusted** by DE for history. |
| `simulationRunId` | Opaque run UUID. Restart TraCI ⇒ **new** run id. |
| `scenarioId` | Active scenario for the entity’s node. |
| `dateObserved` | Wall-clock UTC when payload was built. Audit only — **not** sim clock. |

## Lifecycle (minimal)

1. Run starts → `simulationRunId` assigned; topology package written (Topology Contract).
2. Running → entities upserted to Orion with updating `simulationTime`.
3. Process end → no mandatory RunCompletion event in v1.0.0.

## Restart

A new process / TraCI start creates a **new** `simulationRunId`. DE MUST treat it as a separate run.
