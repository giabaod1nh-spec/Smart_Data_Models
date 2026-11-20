# Phase 1 and V1 Data Engineering implemented decisions and corrections

## Phase 2 final decisions — 2026-11-20

- Phase 2 is certified complete after all 31 gates passed. Final evidence and exact test commands are recorded in `phase-2/acceptance.md`.
- SQLite remains the single semantic identity, invalidation, journal, intent, and checkpoint authority. ClickHouse stores immutable observation/disposition/conflict/invalidation materialization and is reconciled against frozen SQLite obligations.
- Bronze refactors the existing modules in place; no parallel V1-prefixed Bronze production module tree was added. Historical V2 schema/data remains preserved and its migrated entrypoint refuses accidental V2 startup.
- Raw physical delivery identity and source completeness remain Raw-owned. Bronze reads the Raw ledger read-only, follows its safe boundary and topic identity, and uses semantic `eventId` for uniqueness/conflicts.
- `CONFLICTED` remains monotonic. Current canonical reads consult SQLite authority and fail closed on missing/corrupt ClickHouse mirrors; Silver consumes append-only semantic changes and invalidations using Silver-owned durable cursors.
- Raw replay publishes owner-declared source coverage after reconciliation. Bronze replay uses frozen manifest hash, all declared partitions, an isolated `replay:<run_id>` namespace, and a separate per-run SQLite checkpoint. Nonzero starts are supported only when declared by Raw replay coverage.
- Supported deployment is one local Bronze owner with a preserved matching Raw ledger, SQLite authority, topic identity, and ClickHouse output. Distributed ownership and recovery from arbitrary simultaneous loss/replacement of both evidence stores are not certified.
- Latest image evidence includes main Compose fresh flow (100/100/100), final measured run (600 ACKED, catch-up 12.322 seconds), 165 regression tests, and 14 runtime integration tests. See the Phase 2 final certification matrix.

Earlier Phase 2 continuation notes below are historical checkpoints; pending descriptions are superseded by the final certification.

2026-11-20 closure evidence: Raw replay requires replay-only declared coverage for non-zero starts; completed replay is quiescent DELETE-journal SQLite to support :ro mounts without writable SHM. Actual interrupted/resume test passed. Main deployment must preserve the matching SQLite authority for existing live output; a fresh authority cannot silently reinterpret immutable audit rows. Observation reconciliation looks up namespace/event/revision and compares the canonical hash as evidence instead of filtering by the expected hash. Unit regression and5 real persistence fault cases passed.

Historical checkpoint, 2026-11-20: replay binds its manifest by canonical hash and uses exclusive CAS checkpoints in a separate per-run SQLite file. At that checkpoint, Raw-replay mode, pending-write recovery and final gate certification were still outstanding; all were later executed and passed as listed in `phase-2/acceptance.md`.

- 2026-11-19: use PERSISTENCE_CONFIRMED as the canonical ledger persistence state. CONFIRMED_RAW/CONFIRMED_QUARANTINE are read statuses.
- Physical integrity hashes the entire immutable Kafka transport including null flags, key, ordered headers and timestamp. Value hash alone is insufficient.
- Kafka progression follows actual delivery order rather than integer increments; missing committed history/retention loss fails closed.
- Require actual topic UUID through confluent-kafka describe_topics; raise minimum client to 2.15.0. No weak fingerprint fallback is certified.
- Add RFC3339 validator dependency because shared JSON Schema FormatChecker otherwise accepted invalid capturedAt in the observed environment.
- Actual migration 006 resolves contradictory plan DDL: validated envelope fields are represented in envelope_json; Quarantine preserves original bytes rather than truncating malformed IDs into FixedString; ordered headers use JSON array; hashes are FixedString(64) and normalized from bytes when read.
- Base64 is authoritative for exact bytes. Binary integration passed on ClickHouse 24.3.
- Safe local ownership uses file lock plus static group.instance.id. Broader multi-host operation is not certified.
- No shared ClickHouse pause: user requested isolated ClickHouse. The isolated outage retry was authorized and passed; container restored to running/unpaused.
- The 120-second sustained run is observed evidence, not a substitute for the plan's 1200-second certification without an explicit documented acceptance decision.
- Final certification uses a separate 1200-second SUMO run with the supported SUMO_STEP_LENGTH=0.1 configuration, isolated artifacts and broker. It produced 6000 observations and passed complete Kafka/Outbox/Raw byte audit. The earlier 120-second result is supplemental only.
- Run-scale restart required chunking ClickHouse HTTP reconciliation IDs at 500 to avoid parameter field limits. A regression test and actual 6000-delivery restart verify the fix. Startup assignment failures are fail-closed; readiness excludes ASSIGNING.
- Topic UUID is periodically rechecked; startup retries transient metadata propagation after topic creation. A randomly named empty disposable test topic on the acceptance broker verified recreation rejection; project topics were not recreated for this test.
- Full-worker failure probes use only phase1acceptance-clickhouse-1. Separate real Kafka tests verify ledger failure both before and after broker acknowledgement. V2 sentinel schema/data remain unchanged after repeated migration 006.
- Master Plan and detailed plan are unchanged. These corrections remain within Phase 1; no Bronze semantic processing is introduced.

