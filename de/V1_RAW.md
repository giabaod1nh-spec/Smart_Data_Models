# V1 Raw implemented runtime

Phase 1 is complete for the supported local single-owner deployment. Detailed report and Phase 2 interface: docs/DEv1/phase-1/{acceptance,handover}.md. Those documents and runtime artifacts are ignored by existing repository rules; explicitly preserve/archive them with the implementation (or explicitly stage the approved docs with git add -f). No Git settings were changed.

## Start and inspect (PowerShell, repository root)

~~~powershell
docker compose --profile v1-raw up -d --build de-kafka-raw-v1
Invoke-RestMethod http://localhost:8097/ready
Invoke-RestMethod http://localhost:8097/metrics
docker compose logs --tail 100 de-kafka-raw-v1
docker compose stop de-kafka-raw-v1
~~~

The service depends only on topic init, Kafka, ClickHouse and migration 006. Existing V2 workers stay in their legacy profile. Do not run more than one live V1 owner for this group/ledger. Data and SQLite state persist; stopping a service does not delete volumes.

## Isolated acceptance

~~~powershell
docker compose -p phase1acceptance -f docker-compose.v1-acceptance.yml up -d --build
$env:SUMO_HOME='D:\SUMO'
$env:PATH="$env:SUMO_HOME\bin;"+$env:PATH
python -m de.tools.v1_raw_sumo_acceptance --run-id YOUR-UNIQUE-ID --duration 1200 --step-length 0.1
python -m de.tools.v1_raw_runtime_probe
docker compose -p phase1acceptance -f docker-compose.v1-acceptance.yml stop
~~~

Use a fresh run ID. The helper uses separate producer artifacts and bootstrap localhost:39092. Isolated ClickHouse is at 18124 and health at 18097. The outage probe pauses only the isolated ClickHouse and always unpauses it. Keep acceptance volumes if evidence/replay is needed; no volume deletion is required.

## Actual Raw boundary

Authoritative schema: de/migrations/006_create_kafka_raw_v1.sql. Original value/key bytes use base64 plus null flags; ordered header tuples are represented by headers_json. envelope_json contains all required immutable V1 fields. physical_record_hash includes the full transport metadata and excludes consumed_at. Exact bytes are retained for invalid inputs too.

V1ClickHouseRepository.logical_read accepts [start_offset,end_offset), returns original bytes and metadata with CONFIRMED_RAW, CONFIRMED_QUARANTINE, UNRESOLVED or CONFLICT, physical duplicate count and persisted safe_next_offset. Use V1LedgerReader for read-only SQLite access or a consistent SQLite backup. Never query MergeTree row counts as immediate physical uniqueness.

Bronze owns semantic eventId comparison and observation materialization. Raw does not semantic-deduplicate events across offsets.

## Verified evidence

Run phase1-isolated-long01: 1200 complete cycles x 5 intersections, 6000 ACKED observations, 6000 Kafka/Outbox/Raw byte matches; zero final lag and buffer. Measured workload about 15.57 physical deliveries/sec. Test XML, configs, logs, image, hashes, SQLite snapshot and performance JSON live under de/artifacts/v1_raw/acceptance/. This rate is observed workload, not maximum capacity.

A permanent physical integrity fault, changed UUID, retention loss or unverified broker-committed history fails closed. Resolve the evidence with an operator; do not reset group offsets, erase ledger files or substitute the active run/topology.

