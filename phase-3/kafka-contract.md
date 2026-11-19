# V1 Kafka contract

Topic: `traffic.observations.v1`.

- Partitions: 3; replication factor: 1 for local development.
- Cleanup: delete; retention: 604800000 ms (7 days) and 1073741824 bytes; segment: 134217728 bytes.
- Message key: UTF-8 `simulationRunId:intersectionId`.
- Message value: the complete Phase 1 `IntersectionObservation` envelope, canonical UTF-8 JSON, byte-for-byte from SQLite.
- Producer: existing durable worker, `acks=all`, idempotence enabled, retries configured, maximum five in-flight requests per connection. Delivery completion is persisted only after broker acknowledgement.

The key maps a run/intersection to one partition while partition configuration is stable. Application-level Outbox retries can still deliver an older `cycleSequence` after a newer one. Phase 4 must deduplicate by event ID and prevent stale sequences from overwriting current entity state. A SQLite cycle transaction does not make Kafka visibility atomic across partitions.
