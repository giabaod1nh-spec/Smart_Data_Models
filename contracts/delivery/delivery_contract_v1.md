# Delivery Contract v1

| Role | Party |
|------|-------|
| Producer | Realtime (entities via Orion-LD notifications) |
| Consumer | DE Webhook |
| Owner | Realtime Team |
| Delivery | Orion Subscription → DE Webhook |

## SHALL

DE SHALL ingest **NGSI-LD Subscription Notifications**. DE is **not** required to poll Orion entity REST for the baseline pipeline.

### Notification envelope

Typical fields: `id`, `type` (`Notification`), `subscriptionId`, `notifiedAt` (if present), `data` (array of entities).

Each element of `data[]` SHALL conform to the Entity Contract for its `type`.

### Artifacts

| File | Role |
|------|------|
| [`notification.schema.json`](notification.schema.json) | Machine-readable shape |
| [`notification.example.json`](notification.example.json) | **Golden payload — primary SoT for DE parser** |
| [`subscription_template.json`](subscription_template.json) | Example subscription watching four entity types |

Live captured notifications (E2E) live under [`integration/captured/`](../../integration/captured/) and are **evidence**, not the golden Contract.

### DE responsibilities

- Idempotent dedup key recommendation: `(simulationRunId, entity.id, simulationTime)`.
- Defensive parse: unknown/missing additive fields MUST NOT crash the consumer.
- Reconciliation: DE MAY query Orion if notifications are lost (at-least-once / best-effort notify).

### Orion-guaranteed vs DE-responsible

Only behaviours **measured** against the pinned Orion image in `docs/implementation/deployment.md` may be described as Orion-guaranteed. Retry counts and custom headers MUST NOT be invented in this Contract without verification.
