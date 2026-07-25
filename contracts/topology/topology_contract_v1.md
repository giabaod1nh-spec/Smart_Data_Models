# Topology Contract v1

| Role | Party |
|------|-------|
| Producer | Realtime |
| Consumer | Data Engineering |
| Owner | Realtime Team |
| Access | Package keyed by `simulationRunId` (mechanism in `docs/implementation/`) |

## Package contents (SHALL)

For each `simulationRunId`, Realtime SHALL provide:

```text
<runPackage>/<simulationRunId>/
  run_manifest.json
  network_topology_catalog.json
```

## Invariants

- `run_manifest.topology_hash` SHALL equal `network_topology_catalog.topology_hash`.
- DE SHALL resolve the package using `simulationRunId` from entity Properties.
- If the package is missing or hashes disagree, DE SHALL NOT attach topology to that run (fail-soft).

## Non-goals (v1)

- Publishing adjacency as NGSI-LD Relationships on Intersection.
- Hard-coding Docker volume paths inside this Contract (see Implementation docs).

## Fixtures (golden / offline)

See [`fixtures/`](fixtures/) — examples for tests; **not** a substitute for per-run packages.
