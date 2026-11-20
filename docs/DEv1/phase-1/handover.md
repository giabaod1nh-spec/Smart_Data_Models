# Implemented Phase 1 Raw interface — certified handover

Promotion status: PHASE 1 COMPLETE — READY FOR PHASE 2 PLANNING. See acceptance.md, Final implementation report, for certified gates and evidence.

## Source and runtime

Topic traffic.observations.v1; live group de-kafka-raw-v1. V1 profile: v1-raw. Start with docker compose --profile v1-raw up -d --build de-kafka-raw-v1. Migration command supports --v1-raw or explicit --migration de/migrations/006_create_kafka_raw_v1.sql.

Actual schema authority: de/migrations/006_create_kafka_raw_v1.sql. The four tables are smart_traffic.v1_raw_events, v1_quarantine_events and corresponding _replay tables. All use MergeTree, monthly consumed_at partitions and physical-lineage sorting; replay adds replay_run_id to the sorting key.

V1 Raw stores envelope_json containing every required immutable top-level field; payload_text preserves decoded text; value_bytes_base64 is the original Kafka value. Never reserialize payload_text to reconstruct bytes. Base64 decode, then compare payload_bytes_hash. Key bytes and null flags are independent. headers_json is an ordered array of [name, value_is_null, base64_value]; duplicate names and null/binary values remain distinct. broker_timestamp_ms is nullable; no consumer timestamp substitution occurs.

Transport hash: SHA-256 of canonical JSON over topic, partition, offset, topic_identity, key null flag/base64, value null flag/base64, ordered headers and broker timestamp/type. It excludes consumed_at. Null and empty have equal empty-byte hashes but different transport hashes. Immutable envelope hash covers the 12 required fields listed in v1_validator.ENVELOPE_FIELDS, canonicalized by shared canonical_json.

## Physical ledger and read interface

Live SQLite: de/artifacts/kafka_raw_v1/ledger.sqlite3, WAL and FULL synchronous. Objects: deliveries, partition_progress, batches, integrity_faults. Canonical persistence state PERSISTENCE_CONFIRMED is separate from logical result CONFIRMED_RAW/CONFIRMED_QUARANTINE. COMMIT_ELIGIBLE is broker-retryable; COMMITTED follows acknowledgement or verified startup recovery.

Consumer commits a prefix of actual delivered records, so valid skipped Kafka integer offsets do not stall progression. An unresolved delivered record blocks all later commits for its partition. Retention gaps and missing broker history are faults, never successful ingestion.

Use V1ClickHouseRepository.logical_read(ledger, topic, partition, start_offset, end_offset). The interval is [start,end); omitted end is unbounded. Rows include physical identity, resolved status, physical_rows, complete persisted record and safe_next_offset. Matching retry rows collapse. Dual destinations or differing transport hashes produce CONFLICT. Unconfirmed ledger rows produce UNRESOLVED and no record body.

A downstream agent can provide a read-only ledger adapter exposing db:
~~~python
import sqlite3
from pathlib import Path
from types import SimpleNamespace
connection = sqlite3.connect(Path("de/artifacts/kafka_raw_v1/ledger.sqlite3").resolve().as_uri() + "?mode=ro", uri=True)
connection.row_factory = sqlite3.Row
ledger = SimpleNamespace(db=connection)
rows = repository.logical_read(ledger, "traffic.observations.v1", 1, 0, 20)
~~~
Keep the SQLite directory available with WAL/SHM files; do not copy only an active main database file or mutate Raw tables/ledger. A portable snapshot needs SQLite backup or a coordinated read transaction. V1LedgerReader provides the concrete mode=ro/query_only connection. final-handover.xml certifies a separate-process read-only snapshot containing the full 6000-observation run plus quarantines, safe boundaries and replay invariance.

## Replay and operational limitations

python -m de.kafka_raw.v1_replay requires topic, partition, start-offset, end-offset and safe replay-run-id. It uses separate replay tables and de/artifacts/kafka_raw_v1/replay/<id>/ledger.sqlite3; it never commits Kafka offsets. Gap repair report-only opens live SQLite mode=ro; repair delegates to replay only. Kafka retention bounds apply.

Deployment supports one local owner with an OS lock and static Kafka group.instance.id. Actual topic UUID is obtained using describe_topics; unavailable/changed identity fails closed. confluent-kafka minimum was raised to 2.15.0 for the API. No fingerprint is represented as UUID.

Health/ready/metrics/cutover-ready are served at port 8097. The isolated acceptance stack uses host port 18097. Certified operational evidence includes idle readiness, actual HTTP503 during ClickHouse outage, safe broker commits and automatic recovery, actual broker-commit/ledger-failure restart, topic recreation protection, 1200-second SUMO ingestion and three zero-lag partitions. The supported scope remains one local owner, not distributed multi-host operation.

Bronze exclusively owns cross-offset eventId comparison, semantic deduplication, immutable-envelope conflict disposition/invalidation and observation materialization. Raw preserves both physical deliveries and has no semantic ledger or semantic-conflict classification.

