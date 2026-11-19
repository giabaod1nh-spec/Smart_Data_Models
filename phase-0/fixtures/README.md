# Fixture provenance

These files are read-only captures; they do not form a single synchronized experiment.

- `orion-current-{intersection,camera,trafficlight,vehiclesensor}.json`: exact `GET /ngsi-ld/v1/entities?type=<type>&limit=100` responses with `Accept: application/ld+json`, captured 2026-11-16 about 11:05 UTC. Counts 5/5/17/17, all run `6c94ec62-7223-4ecb-96c0-4150a3efba3e`, simulation time 128.05. Orion state was queried read-only.
- `orion-current-run-manifest.json`: archived manifest for that run. Actual net SHA-256 `62638b276817a58de2a27601fe0f6f2aae4a9c129a455aa1e1726e5c88a3d9f1`, topology hash `60413af0786d82c9ebe372fd2fb598085c09accb810d98a3f9338991465c837e`.
- `orion-current-run-catalog.json`: the archived topology catalog from the same run, copied to keep the fixture self-contained.
- `v2-outbox-historical-cycle.json`: actual frozen `payload_json` from local SQLite Outbox, run `e89cc3d9-bc70-4570-ab0e-e13392702dac`, cycle 32, 40 ACKED entity records. Exported via a SQLite **read-only** connection. Includes stored key, topic, status, broker partition/offset, timestamps and event JSON. This run is from 2026-08-10 and **not** the current Orion run.
- `v2-outbox-historical-run-started.json`: actual ACKED control record for the same historical run. Its Kafka key is `run:__run__`, so its partition can differ from entity records.
- `v2-outbox-historical-run-manifest.json`: archived provenance for that historical cycle, topology hash `a3ac1e8962383708bc06f96adb55c6f4c270454b8f17a09183b7eb44c7930c1b`.
- `v2-outbox-historical-run-catalog.json`: archived topology catalog for that cycle.
- `selected-intersections-current.json`: read-only copy of the current, pre-existing untracked selected-intersection configuration. The source file was left untouched.
- `runtime-metadata.json`: sanitized topic/group offsets, image IDs, hashes and Orion counts observed during this audit.

At audit time Kafka `traffic.entity-events.v2` had three partitions but no retained records: earliest and end offsets were equal on each. No SUMO process was active, and no persisted full physical snapshot matching the current Orion run was found. Therefore there is **no current-topology same-cycle snapshot → v2 event → Orion fixture** here. Do not treat the historical v2 event and current Orion GET as a parity pair. A controlled future capture must record run ID, cycle, source snapshot, catalog/network hashes, Kafka key/partition/offset and Orion output together.
