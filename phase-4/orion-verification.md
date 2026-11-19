# Orion-LD verification, 2026-11-19

- V1 service built and ran from `Visualize/Dockerfile.projector`; Kafka and
  Orion were reached from the actual container.
- The selected Phase 3 A/B run was `COMPLETE` at generation 3. The V1 consumer
  fenced 15 older observations and applied the two selected observations.
- Live Orion read-back compared all 44 expected entities and found zero
  mismatches. A/B carried selected-run observations; C/D/M retained static
  topology without V1 dynamic attributes.
- The five-node Phase 3 run was replayed into a separate Orion/Mongo instance.
  Its 15 observations produced 44 catalog-derived entities; read-back found
  zero mismatches. The two newer A/B messages were fenced in that replay.
- A copy of the isolated replay ledger was advanced from the five-node run to
  the A/B run against the same isolated Orion. Read-back again compared all 44
  entities with zero mismatches, including cleared dynamic fields on C/D/M.
- The named Docker ledger volume survived a container rebuild and restart.
  The initial Windows bind-mount ledger was retired after intermittent SQLite
  open failures; its old quarantines are not used by the active consumer group.
- The final production ledger contains 2 `APPLIED`, 15 `FENCED`, and 0
  quarantined offsets; generation 3 is `READY`. The Kafka group settled with
  one member and zero lag on partitions 0 and 2. Partition 1 had no messages.
- Focused V1 Projector and contract tests passed (36 cases). They cover terminal run
  statuses, duplicate and stale protection, attribute transitions, partial
  batch retry, permanent partial failure, timeout after Orion writes, crash
  recovery, topology change, ownership ambiguity, and replay isolation.
- The Phase 3 registry identity mismatch was repaired with a SQLite backup at
  `Visualize/artifacts/runs/active_run.pre-v1-projector.20261119T090820Z.sqlite3`
  (SHA-256 `9b7667900f94999f6729180996829999c28a8c4431576e2da070677c21b1d207`).

The local live Orion target currently reflects the selected A/B run. The
full-network and transition results are retained in the isolated replay
ledger. The separate replay Orion/Mongo containers were stopped after the
checks; their volume was retained. The stopped V2 Projector container was
removed after V1 read-back passed. Spring Server and Dashboard were not changed.
