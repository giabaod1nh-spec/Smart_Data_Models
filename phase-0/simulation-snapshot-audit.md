# Simulation snapshot and topology audit

The source observation boundary is `Visualize/observation/snapshot_provider.py:92` and `Visualize/simulation/backend.py:428`. TraCI is read on the simulation thread; `SumoBackend` caches one physical snapshot per node and attaches run identity and derived context. `traci_runner.capture_entity_list` reads those cached node snapshots for publication. A live full snapshot was not persisted or available at audit time, so cross-node cycle consistency is established from code and `PublishCycle` validation, not from a captured source-to-Orion trace.

## Snapshot shape and ownership

- Per node: `node_id`, `directions`, `colors`, current/next phase, phase remaining/durations, scenario, blocked direction, incidents, simulation time, spillback/box metrics, emergency preemption, run identity, model/config/profile provenance.
- Per direction: approach vehicle IDs are de-duplicated, then TraCI vehicle type/speed/lane/movement data and detector readings produce vehicle count, PCU, left/straight/right counts, speed, waiting count, queue by movement, occupancy, density, arrival rate and composition. Zero vehicles and missing detector values use explicit fallbacks in source; exact live values were not sampled.
- Backend context merge: aggregate/direction traffic state, derived phenomena, operational state, probable causes and link states are added from the runtime model. Mapper consumes selected fields; the full context is not serialized into v2 events.
- Camera is a *virtual NGSI-LD aggregate* created by the entity mapper; no separate TraCI camera observation was found in the snapshot provider.

Current catalog hash: `60413af0786d82c9ebe372fd2fb598085c09accb810d98a3f9338991465c837e`. A/B/D have three catalog directions; C/M have four. Current selected `node_to_tls_groups` has one TLS for each of A/B/C/D/M. Thus the plan's explicit “multi-TLS B/C/M” example is not representative of this current catalog. The code has topology group abstractions, but a live multi-TLS fixture is unavailable.

The current Orion run manifest uses this catalog hash and the same net-file SHA-256 as the checkout. It records seed 42, Fixed control mode and normal demand. It also records an old expected network hash and legacy network ID, so provenance fields must be classified by actual vs configured meaning. The historical Outbox cycle is from topology hash `a3ac1e8962383708bc06f96adb55c6f4c270454b8f17a09183b7eb44c7930c1b` and produced only 40 entities. Do not use it as current-topology cardinality evidence.

Open evidence: a frozen same-step snapshot for all five nodes, empty/zero-value cases under the current net, detector-missing behavior in a live run, scenario change, restart, and a same-cycle raw TraCI → snapshot → mapped entity comparison. Those require an isolated observation run or an already archived snapshot; none was available without changing the running system.
