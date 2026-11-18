# Phase 1 source review

- `traffic_observation.py` previously copied the entire internal snapshot into
  the wire payload, generated `capturedAt` at builder time, and checked only a
  subset of metrics. The envelope had no per-node `scenarioId` or full catalog
  identity. The revised builder selects declared fields and the revised
  validator delegates wire structure to JSON Schema.
- The previous JSON Schema allowed arbitrary payload/direction fields and made
  key sensor metrics optional, while the shared V2 mapper directly indexes
  `pcu_equivalent`, turn counts, and `occupancy_pct`. V1 now declares the
  complete metric keys and explicit null/availability semantics. The shared
  mapper remains unchanged.
- `append_observation_cycle()` already emits one event per node with a
  run/node Kafka key, but it currently supplies neither source capture time nor
  V1 digest. Phase 2/3 must supply frozen metadata before activating it.
- The SQLite Outbox rejects every duplicate `eventId`, including identical
  events. Phase 3 must add the specified idempotent insertion behavior.
- The legacy topology hash excludes projection-relevant TLS groups and node
  locations. A separate V1 digest now covers them without altering legacy
  manifests, DE contracts, or the active V2 flow.
- The current observation projector helper calls the shared entity mapper,
  which reads global configuration. Phase 4 must replace that path with an
  independent V1 mapper and a run-specific catalog resolver.
- Phase 0 did not capture a current same-cycle source snapshot and V2 output.
  The fixtures in this phase are code-derived or synthetic; they are not parity
  evidence for cutover.
