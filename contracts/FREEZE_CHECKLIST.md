# RT-DE Contract Freeze Checklist v1.0.0

## Spec
- [ ] `contracts/VERSION` = 1.0.0
- [ ] README applies-to: Intersection, TrafficLight, VehicleSensor, Camera
- [ ] Ownership headers on every `*_contract_v1.md`
- [ ] `delivery/notification.schema.json` + `notification.example.json` (golden)
- [ ] `delivery/subscription_template.json`
- [ ] Entity golden payloads under `entity/payloads/`

## Runtime (Realtime)
- [ ] All four entity types publish `simulationTime`, `simulationRunId`, `scenarioId`
- [ ] TrafficLight + Intersection publish `currentPhase`
- [ ] Per-run package: `run_manifest.json` + `network_topology_catalog.json` with matching `topology_hash`
- [ ] Orion image pinned (not `:latest`)
- [ ] SDM `schema.json` marked DEPRECATED where wrong

## Tests
- [ ] `pytest contracts/tests` offline green
- [ ] `integration/` live E2E green (when stack available)
- [ ] Captured notification at `integration/captured/notification.captured.example.json` (not under `contracts/`) validates against `contracts/delivery/notification.schema.json`

## Repo layout (reorg)
- [ ] No `IMPLEMENTATION_PLAN.md` inside `contracts/`
- [ ] Implementation docs under `docs/implementation/`
- [ ] Live E2E under `integration/` only

When all required items are complete:

**RT-DE Contract v1.0.0 frozen — DE has enough information to build the webhook ingestion pipeline.**
