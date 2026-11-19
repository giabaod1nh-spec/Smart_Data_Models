# Canonical cycle snapshot

`Visualize/observation/canonical_cycle.py` defines one immutable sampling-cycle model. The cycle owns `simulationRunId`, unrounded `simulationTimeSeconds`, `simulationStep`, `cycleSequence`, `capturedAt`, `topologyDigest`, and a tuple of intersection states. Each intersection holds a private JSON byte copy of the collected source state and a separate measurement-quality byte copy. This prevents later mutation of the provider's dictionaries or context cache from changing a completed capture. `to_dict()` is for local evidence, not a V1 wire message.

The participating set is the backend's `publish_nodes`, frozen at backend construction. The run catalog is the exact in-memory catalog from which `configuration.config` derived runtime TLS/approach/sensing maps; the runtime coordinator and capacity actuator receive that same frozen content. At V1-enabled backend construction, it is copied into immutable bytes. At run startup it is digested with the Phase 1 V1 digest. The existing run archive at `Visualize/artifacts/runs/<simulationRunId>/network_topology_catalog.json` is read back and compared without rewriting its V2 package bytes; if absent, the frozen content is written first. Runtime map and legacy manifest-hash mismatch marks V1 catalog readiness false. This does not alter the legacy `topology_hash` or V2 payload path. Current V1 accepts only a single TLS per logical node and the North/South/East/West directions supported by the V1 schema.

The quality sidecar records failed lane, vehicle, movement, occupancy and detector reads, including optional spillback/entry/yellow probes. For a failed or partial aggregate, `build_v1_cycle` sets affected core fields to `null` with `metric_availability: UNAVAILABLE`. Measured zero remains numeric zero. Optional metrics affected by an incomplete aggregate are omitted. The raw V2-compatible source retains its prior numeric fallback. The provider obtains both views from the same physical read pass; ADAPTIVE control retains its own fresh reads.

`generate_v1_cycle` rejects a missing participant, identity, TLS phase/color, topology relationship, metadata, quality sidecar, or verified archive. It returns a failure result with no events. The pure `build_v1_cycle` uses the Phase 1 `build_observation` and validator for event identity, payload hash, declared-field selection, and schema/semantic checks. Rebuilding from the same canonical snapshot yields identical events.

Run a local capture from the repository root with `SUMO_HOME` and Python dependencies configured:

```powershell
python Visualize/app/traci_runner.py --no-gui --no-orion --no-api --v1-observations --v1-capture-file phase-2/fixtures/current-sumo-cycle.json --v1-capture-after 60 --max-sim-time 61
```

`phase-2/fixtures/current-sumo-cycle.json` is a real headless SUMO capture from the selected A/B/C/D/M topology. It records its run ID, sequence, completed step, catalog digest/archive path, canonical source state, five V1 observations and local structural/semantic validation outcome. The run archive is a local artifact under `Visualize/artifacts/runs/`; the tracked generated catalog provides the same digest for fixture validation.
