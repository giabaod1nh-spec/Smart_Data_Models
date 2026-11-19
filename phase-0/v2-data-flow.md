# V2 data flow

```text
SUMO step → SnapshotProvider.build_snapshot / SumoBackend cache
  → traci_runner.capture_entity_list → entity_mapper.build_all_entities
  → [Intersection, Camera, TrafficLight*, VehicleSensor*]
  → durable_publisher.append_cycle → SQLite kafka_outbox
  → OutboxDeliveryWorker → Kafka traffic.entity-events.v2
  ├→ Orion Projector → node buffer / ledger → Orion batch upsert
  └→ DE Raw consumer → ClickHouse Raw → Bronze → Silver → Gold
```

The active `final` path uses Outbox → Kafka → Projector for Orion writes; local/direct Orion and migration/shadow modes exist in code/config and must not be mistaken for the currently inspected production container. See `docs/shared/PHASE1_BOUNDARIES.md` and the live environment values in [system baseline](system-baseline.md).

## Transition audit (source)

1. `Visualize/observation/snapshot_provider.py:92` reads TraCI after a step and creates a per-logical-node dict with direction metrics, colors, phases, scenario, incidents and simulation time. `Visualize/simulation/backend.py:428` returns a cached snapshot and attaches run identity; `:455` adds context from runtime state. Cached snapshots reduce repeated TraCI reads. No full snapshot is persisted in the v2 event.
2. `Visualize/app/traci_runner.py:133` loops `backend.publish_nodes`, gets one cached snapshot per node and calls `build_all_entities`. The mapping occurs on the TraCI side before Kafka. `Visualize/integration/orion/entity_mapper.py:331` creates one Intersection and Camera plus one TrafficLight and VehicleSensor per catalog direction. It derives locations, directions and URNs from current configuration/catalog. Mapping is deterministic for a fixed snapshot and fixed config except fallback wall-clock `dateObserved` when no `captured_at` is supplied.
3. `Visualize/integration/orion/publish_cycle.py:40` validates duplicate IDs and consistent run/time, then serializes entities into immutable bytes. Its `EXPECTED_ENTITIES_PER_NODE=10` is warning-only; the current five nodes legitimately produce 8/8/10/8/10 entities. Node scenarios may differ; cycle summary becomes `mixed`.
4. `Visualize/app/traci_runner.py:165` deep-copies the entity list for direct Orion and Kafka paths. In the observed final profile, `DurableKafkaPublisher.append_cycle` deep-copies again, builds/validates v2 events and durably inserts the full entity cycle into SQLite. A RunStarted control row is persisted separately before the first cycle of a run.
5. `Visualize/integration/kafka/outbox_worker.py` reads eligible rows and produces their frozen `payload_json` bytes with the stored key; a delivery callback records Kafka partition/offset on ACK. Kafka gets entity-shaped events, never the original full TraCI snapshot.
6. `Visualize/tools/projector_live_consumer.py` parses/validates broker records, invokes `OrionProjector`, and manages manual offsets. `Visualize/integration/projector/core.py:519` takes `event.entity` directly. The Projector does not reconstruct NGSI-LD entities from physical observations; it may rewrite the namespace, buffer by node/cycle, skip stale records, coalesce in catch-up mode and batch upsert.
7. `Visualize/integration/orion/client.py:405` sends `POST /ngsi-ld/v1/entityOperations/upsert?options=update` and interprets 201/204/207/transport failures. Orion is current-state storage; a GET after later cycles is not evidence that each earlier Kafka event was applied.

## Copying, ordering and failure boundaries

The producer's entity snapshot is deep-copied before event construction. The Outbox stores JSON text and key, so retries reuse the stored payload. Events for the same `(run,node)` share a Kafka key and have partition order; RunStarted uses `run:__run__` and can be on another partition. The Projector explicitly buffers entity records pending RunStarted. No cross-partition order is guaranteed by the topic layout. Runtime evidence for one complete current cycle is missing; see [fixtures](fixtures/README.md).
