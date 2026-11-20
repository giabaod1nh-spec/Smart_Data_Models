# Phase 1 implemented decisions and corrections

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

