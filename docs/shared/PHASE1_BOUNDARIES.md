# Phase 1 boundary and delivery status

## Active production path

```text
SUMO → TraCIAdapter → SumoBackend snapshots
     → legacy NGSI-LD entity capture → SQLite Outbox → Kafka entity-events.v2
     → Orion Projector → Orion-LD
                           ↘ Raw → Bronze → Silver → Gold
```

The existing `final` deployment profile selects Outbox → Kafka → Projector as
the sole writer. The local no-profile default still permits direct Orion until
the observation consumers and live cutover are verified. `migration` remains
an explicit dual-path profile for shadow comparison; startup rejects direct
Orion and Kafka together in any other profile. Logical entity URNs (for example `urn:ngsi-ld:Intersection:A`)
remain stable. `simulationRunId` identifies the active run, while ClickHouse
stores history. The current entity count follows the OSM topology rather than
a fixed number.

The SQLite Outbox persists a `TrafficSimulationRunStarted` record before the
first cycle. The simulation does not wait for Kafka acknowledgement. Its relay
can publish the same record more than once after a crash, so Projector and Raw
must remain idempotent. Kafka outages can be buffered only while local storage
has capacity. If local persistence fails, the runner stops with an error rather
than silently advancing and claiming complete telemetry. A process crash after
a SUMO step but before the Outbox commit can still leave a gap; recovering that
step requires deterministic replay or SUMO checkpointing.

## New domain contract, migration pending

`contracts/traffic_observation.py` and its JSON Schema in `contracts/events/`
define `IntersectionObservation` v1:
one event per logical intersection and sampling cycle. It has a deterministic
`eventId`, key `simulationRunId:intersectionId`, simulation step/time, capture
time, payload hash and a physical/context snapshot independent of NGSI-LD.
Unknown metrics stay null. Numeric values must be finite; queue and count
metrics must be non-negative. The current snapshot uses:

- `vehicle_count`: distinct vehicles on configured approach lanes, count.
- `waiting_vehicle_count`: vehicles below `HALTING_SPEED_MS`, count.
- `queue_length_m`: stop-line E2 or fallback estimated queue length, metres.
- `average_speed_kmh`: moving-vehicle mean, km/h. This preserves the current
  source unit; renaming to m/s requires an explicit versioned conversion.

`DurableKafkaPublisher.append_observation_cycle` can persist the new event,
and `observation_to_entities` can recreate the existing Orion entity shapes.
The runtime, Projector Kafka consumer and ClickHouse Raw/Bronze consumers still
use `TrafficEntityObserved` v2. Do not change `KAFKA_TOPIC` to
`traffic.observations.v1` in a deployed stack yet: the current consumers would
reject or ignore it. Completing the contract migration requires a versioned
consumer cutover and historical table mapping, followed by retirement of the
legacy entity capture. No partial dual publish is enabled by default.

## TraCI and control

`TraCIAdapter` owns connection start, simulation stepping and close. Existing
signal, scenario, observation and AI components temporarily receive a
compatibility port for domain getters and setters. The simulation loop drains
manual commands at its defined step boundary. Safety transitions are enforced
by `SumoSignalController`. Emergency preemption remains mode-dependent and is
not enabled in ADAPTIVE by this refactor.
