# V2 entity events to V1 domain observations

The active Raw/Bronze path recognizes V2 `EntityChange`/`RunStarted` events,
then Silver unwraps NGSI-LD properties. It does not currently consume
`IntersectionObservation` events. Phase 1 defines the data required by a
future V1 Raw/Bronze adapter; it does not change that runtime.

| Current V2 field | V1 source | Future analytics owner |
|---|---|---|
| Envelope run, node, scenario, time, cycle | V1 envelope | Raw identity/lineage mapping |
| Sensor `vehicleCount`, `pcuEquivalent` | Direction `vehicle_count`, `pcu_equivalent` | Direction fact |
| Sensor `averageSpeed`, `queueLength`, `occupancyRate` | Direction `average_speed_kmh`, `queue_length_m`, `occupancy_pct` | Direction fact |
| Sensor `waitingVehicleCount`, `arrivalRatePcuPerSec` | Direction optional measurements | Direction fact, preserving availability |
| Sensor turn counts and vehicle class mix | Direction turn counts and optional composition | Direction fact / vehicle-mix processing |
| TrafficLight `currentStatus`, `currentPhase` | Direction color and payload phase | Signal fact |
| TrafficLight timing fields | Optional phase/green/red/yellow duration | Signal fact when present |
| Intersection `totalVehicleCount`, `numberOfApproaches` | Sum valid direction counts; count archived approaches | Analytics aggregation |
| Intersection/Camera traffic status | Direction PCU and documented threshold profile | Analytics derivation, not an NGSI payload copy |
| Camera mean speed/occupancy | Aggregate valid direction measurements | Analytics aggregation with explicit missing-data rule |
| Incident flags | Current `incidents` plus optional operational state | Analytics derivation |

The V1 wire guarantees the nine required per-direction metric keys, phase,
color, logical identity, per-node scenario, and an incident list. Required
metric keys can carry explicit unavailable/null status; downstream must track
coverage and must not turn null into zero. Threshold-based traffic status also
depends on the run's configuration profile, which must be resolved from run
provenance when analytics is migrated. V1 deliberately carries no NGSI-LD
entity serialization, static camera data, or Projector-generated URNs.

Remaining downstream implementation work: recognize the V1 topic, preserve
raw event bytes and digest, map one observation into direction/signal facts,
define coverage-aware aggregate rules, and migrate Silver/Gold readers. The
current V2 Raw/Bronze schema and materializations stay untouched in Phase 1.
