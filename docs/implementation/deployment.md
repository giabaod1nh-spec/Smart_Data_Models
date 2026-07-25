# Deployment — Orion pin & run artifacts

## Orion-LD image

Pinned in [`docker-compose.yml`](../../docker-compose.yml):

```text
fiware/orion-ld:1.7.1
```

Do **not** use `:latest` for Contract freeze verification.

Mongo: `mongo:4.4` (existing compose).

## Run package paths

Realtime (host / default Visualize):

```text
Visualize/artifacts/runs/<simulationRunId>/
  run_manifest.json
  network_topology_catalog.json
```

Canonical container path when RT/DE are containerized:

```text
/app/artifacts/runs/<simulationRunId>/...
```

Compose declares volume `shared-run-artifacts` for that purpose. Mount the same volume on DE at `/app/artifacts/runs`.

## DE lookup

1. Read `simulationRunId` from notification `data[]` entity Properties.
2. Open `/app/artifacts/runs/<simulationRunId>/` (or host equivalent).
3. Verify `topology_hash` equality; otherwise skip topology for that run.
