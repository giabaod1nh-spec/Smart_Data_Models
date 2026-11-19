# Phase 3 verification summary

- Phase 2 baseline: commit `e10e5b2` was already present and the working tree was clean before Phase 3 edits.
- Focused Phase 3, Phase 2, Phase 1, legacy Kafka, and V1 contract tests: **56 passed** with an explicit temporary `pytest-asyncio` loop-scope setting.
- Python compile check and `git diff --check`: passed.
- Registry identity smoke check after final review: passed; changing mutable run status keeps selection valid, changing immutable scenario metadata is rejected.
- Docker Compose configuration: valid.
- Kafka initializer: V1 topic created, then a second run verified it already existed with 3 partitions and replication factor 1.
- Five-node SUMO run `c76c237e-b408-41d6-941d-5af244cb8dc4`: 3 complete cycles, 15 Outbox rows, 15 broker ACKs, 15 exact Kafka key/value matches.
- `--nodes A,B` SUMO run `b45098da-6bea-4a81-8c81-b90e7f592d47`: manifest participants `A,B`, one complete cycle, two broker ACKs, two exact Kafka key/value matches.
- V2 Outbox archive: SQLite integrity `ok`, 93,247 ACKED rows, zero pending; checksum is in `cutover-inventory.json`.
- V2 Projector and V2 Raw/Bronze/Silver/Gold consumers were stopped. Orion/Dashboard refresh remains deferred to Phase 4.

The repository's existing `Visualize/.venv` points to a Python 3.10 installation no longer present. Verification used a separate Python runtime with temporary workspace dependencies; this does not affect the application code.
