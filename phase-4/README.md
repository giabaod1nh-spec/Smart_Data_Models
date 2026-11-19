# Phase 4: V1 observation to Orion-LD

The active Projector consumes `traffic.observations.v1` and writes the current
state of each selected logical intersection to Orion-LD. It does not use the
V2 entity-event mapper. One observation generates one Intersection and Camera
plus a TrafficLight and VehicleSensor for each archived approach. The archived
catalog determines IDs, locations, directions, and relationships.

## Run and inspect

```powershell
docker compose up -d --build orion-projector-v1
docker compose ps orion-projector-v1
Invoke-RestMethod http://localhost:8094/health
docker compose logs --tail=40 orion-projector-v1
```

`/ready` returns 200 only after the selected run has been reconciled and the
Projector has no reported fault. The health response distinguishes
`selectedGeneration` from `reconciledGeneration`; selection by SUMO does not
make Orion immediately current. The ledger and quarantine are in the named
`v1-projector-data` Docker volume. Run manifests, frozen catalogs, and the
active-run SQLite selector are mounted read-only from
`Visualize/artifacts/runs`.

The active V1 consumer group is `projector-v1-production-cutover`. It starts
at the earliest retained offset if no group offset exists. An older run is
durably fenced before its offset is committed. `RUNNING`, `COMPLETE`,
`INCOMPLETE`, and `STOPPED` selected runs may drain valid published messages.
One failed message pauses its Kafka partition; other partitions continue.

## Write and recovery contract

The Projector owns the attributes listed in `v1_projection.STATIC` and
`v1_projection.DYNAMIC`. The V1 mapper does not import global SUMO config.
The complete field and operation boundary is in [attribute-ownership.md](attribute-ownership.md).
Static entities are reconciled from the selected run's archived catalog.
Run activation clears V1 dynamic state from the preceding run for all nodes;
new samples then populate the participating nodes. An obsolete direction
entity is deleted only when its actual Orion attributes are all Projector
owned and its identity matches the preceding archived catalog. The full
reconciliation operation plan and each completed operation are journaled
before the new generation becomes ready. Ambiguous ownership faults
reconciliation and leaves the entity intact.

An available zero is written as numeric zero. A required null metric removes
the prior measurement and records `UNAVAILABLE` or `NOT_APPLICABLE` in the
entity's `metricAvailability` Property. An absent optional metric removes a
previously projected value. The ledger records each entity upsert and each
necessary attribute removal as separate operations. `APPLIED` and the cycle
watermark are committed together only after every operation succeeds. Orion
`207` results, timeouts, and crashes are retried with stable entity IDs. A
permanent failure after partial application holds the offset for repair.

Orion coordinates may be returned rounded to six decimals. The read-back
verifier accepts at most `1e-6` degree difference for a GeoProperty and
compares all other projected values directly.

## Isolated historical replay

The replay target has its own Orion and Mongo containers. It never changes
the live active-run selector or writes the live Orion instance.

```powershell
docker compose --profile v1-replay up -d mongo-db-v1-replay orion-v1-replay
docker compose run --rm --no-deps orion-projector-v1 python -m tools.v1_projector_consumer --replay-run <run-id> --replay-orion-url http://orion-v1-replay:1026 --group <separate-group> --db /app/Visualize/artifacts/projector/<separate-ledger>.sqlite3 --health-port 8095
docker compose run --rm --no-deps -e ORION_URL=http://orion-v1-replay:1026 orion-projector-v1 python -m tools.verify_v1_orion --db /app/Visualize/artifacts/projector/<separate-ledger>.sqlite3 --run-id <run-id>
```

Run `tools.verify_v1_orion` against the live container with the selected run
ID to compare Orion entities with the latest APPLIED observation for each
node. The verifier also checks that excluded nodes have no V1 dynamic values.

## Phase 3 registry repair

The retained local Phase 3 registry contained three manifest identity hashes
that differed from the current immutable-hash calculation. The guarded
`tools.repair_v1_registry_identity` command audits each run's ID, generation,
participants, status, and archived topology digest, backs up SQLite, and
updates only mismatched hashes. It is a one-time local cutover operation. New
runs should pass the current identity verifier without repair.

Spring Server, Dashboard, and Raw/Bronze integration are outside this phase.
The old V2 Projector service has been removed from active Compose after the
V1 read-back succeeded; V2 source and historical artifacts remain for later
analytics migration.
