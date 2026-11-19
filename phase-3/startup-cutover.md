# Phase 3 V1 startup and cutover

V1 publishing is opt in. From the repository root, run with `--v1-observations --v1-publish --no-orion`. Set `ORION_PUBLISH_ENABLED=false`, `KAFKA_PUBLISH_ENABLED=false`, and `KAFKA_OUTBOX_ENABLED=false`. The runner validates these settings before opening Kafka, Orion, or SUMO. It never invokes V2 entity fanout in V1 mode.

```powershell
$env:ORION_PUBLISH_ENABLED = 'false'
$env:KAFKA_PUBLISH_ENABLED = 'false'
$env:KAFKA_OUTBOX_ENABLED = 'false'
python Visualize/app/traci_runner.py --no-gui --no-orion --no-api --v1-observations --v1-publish --nodes A,B,C,D,M --max-sim-time 60
```

`V1_OUTBOX_DB` defaults to `Visualize/artifacts/kafka_outbox/v1-outbox.sqlite3`; it must differ from the archived V2 path. `V1_KAFKA_TOPIC` must be `traffic.observations.v1`. `--v1-observations` alone remains a non-publishing capture mode.

`docker/kafka-init.sh` creates or verifies the V1 topic. The V2 Projector and Raw/Bronze/Silver/Gold services are assigned the `v2-legacy` Compose profile, so a normal `docker compose up` does not start them. Existing running containers must be stopped once with `docker compose stop orion-projector de-kafka-raw-consumer de-bronze-processor de-silver-processor de-gold-runtime`; this was done for the local cutover on 2026-11-19. Orion and Dashboard stay stale until the V1 Projector is implemented in Phase 4.

The previous V2 Outbox was backed up with `Visualize/tools/archive_v2_outbox.py`, which uses SQLite's backup API and verifies integrity. The backup and new V1 Outbox are local runtime artifacts under the ignored `Visualize/artifacts/` directory. See `cutover-inventory.json` for the backup checksum and record counts.
