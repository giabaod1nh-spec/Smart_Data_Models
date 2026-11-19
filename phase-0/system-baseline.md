# System baseline

Captured 2026-11-16 about 11:05–11:08 UTC. Repository HEAD `7495d5a9deb03fbbc9525bf3e63ce9f718206686`; one pre-existing untracked file, `Visualize/topology/selected_intersections.json`, was present. The audit did not edit it. This checkout is not necessarily the code used to build the running images: the current Orion run manifest names commit `78f98642ea5b28082aeba75847fdabe33e84455a`.

## Version and topology identity

- Host SUMO: `1.27.1`; current Orion run manifest also records SUMO `1.27.1` and TraCI version `0.0.0` (the latter should not be interpreted as a verified library release).
- `Visualize/Visualize/intersection.net.xml` SHA-256 `62638b276817a58de2a27601fe0f6f2aae4a9c129a455aa1e1726e5c88a3d9f1`; the current run manifest records the same actual network hash.
- Current catalog's semantic `topology_hash`: `60413af0786d82c9ebe372fd2fb598085c09accb810d98a3f9338991465c837e`; catalog file SHA-256 `9cb03c72679573e4e702e238a8d0f3b6b7438b049317075c480b2d2fa1375c37`. Current run manifest records the same semantic topology hash.
- `configuration/config.py` SHA-256 `1902da191450460e5dc92a0e9e4015b4d914eb4ee9cbb05bedf4471430900ddc`.
- The current run manifest reports `network_hash_mismatch` against an older expected hash and still labels `network_id` as `intersection_grid_j1_j4`. Those metadata fields are stale relative to the actual OSM net; use the actual network/catalog hashes for this audit.

## Runtime inventory (read-only Docker inspection)

Running: Kafka `apache/kafka:3.8.1`, Orion-LD `fiware/orion-ld:1.7.1`, Mongo `mongo:4.4`, ClickHouse `24.3`, Orion Projector, Raw, Bronze, Silver and Gold workers, context provider and Dozzle. No host `sumo`, `sumo-gui`, or Python process was observed at capture time. Container tags do not prove the source revision embedded in locally built images.

Inspected runtime image IDs: Orion Projector `sha256:f26a121b54df7334395e62841f85bc85100a050b7cbe887fbac0555672e775a9`; Raw consumer `sha256:9a71597bfda1934668a81b00fcd9390a6e3f9b7fc7e5d7f23ed08a93589d1e28`. These identify the running build more precisely than its local image tag.

Deployed Orion Projector: `KAFKA_TOPIC=traffic.entity-events.v2`, `PROJECTOR_GROUP_ID=projector-k5-production`, `PROJECTOR_WRITE_MODE=active`, `PROJECTOR_CONSUMER_MODE=normal`, `PROJECTOR_TARGET_NAMESPACE=production`, `PROJECTOR_SHADOW_MODE=false`, `ORION_URL=http://orion:1026`, SQLite at `/app/Visualize/artifacts/projector/k5-production.sqlite3`. Raw group observed as `de-kafka-raw-v2`. Values came from selected nonsecret container environment variables, not merely Compose defaults.

Kafka runtime: topic `traffic.entity-events.v2`, three partitions, replication factor 1, delete retention of 604800000 ms and 1073741824 bytes. At inspection, projector group committed offsets were p0=51602, p1=20723, p2=20902; Raw group p0=51602, p1=20733, p2=20912. Log-end and earliest retained offsets were p0=51602, p1=20733, p2=20912. Thus no v2 message was retained for a current live capture at that instant. The projector was 10 events behind on p1 and p2 according to group metadata, although those offsets were already below the retention floor; investigate the runtime's gap reconciliation before interpreting this as ordinary backlog.

Orion GET queries scoped by type returned Intersection=5, Camera=5, TrafficLight=17, VehicleSensor=17. All 44 returned entities had run ID `6c94ec62-7223-4ecb-96c0-4150a3efba3e` and `simulationTime=128.05`. Their raw responses and the run manifest are in [fixtures](fixtures/README.md). This verifies current Orion state, not a synchronized source-to-broker-to-Orion capture.

## Configuration and evidence references

- Source entry points: `Visualize/app/traci_runner.py`, `Visualize/tools/projector_live_consumer.py`, `de/kafka_raw/main.py`.
- Source defaults: `Visualize/configuration/config.py`, `Visualize/integration/kafka/durable_publisher.py`, `de/kafka_raw/config.py`.
- Deployment: `docker-compose.yml`, `deploy/profiles/`, `deploy/migration/`.
- Contracts: `contracts/events/traffic-entity-event-v2.schema.json`, `contracts/events/traffic-simulation-run-started-v2.schema.json`.
- Existing tests relevant to the boundary include `tests/architecture/test_topic_ownership.py`, `tests/architecture/test_phase1_observation_boundary.py`, `contracts/tests/test_mapper_contract_fields.py`, and `de/tests/kafka_raw/test_raw_consumer.py`. This audit inspected the locations but did not run tests or claim their results.

Source behavior is described in subsequent documents. Runtime failure windows, active TraCI snapshot contents, and a same-cycle v2 fixture remain unobserved.
