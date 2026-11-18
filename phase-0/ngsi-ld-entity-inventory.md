# NGSI-LD entity inventory

Evidence: `Visualize/integration/orion/entity_mapper.py` and read-only Orion GET fixtures. The mapper creates logical URNs; physical TLS IDs do not replace the logical node letters. The current live Orion set has **44 entities**: 5 Intersection, 5 Camera, 17 TrafficLight, 17 VehicleSensor.

Read-only analysis of the four saved GET responses found 44 unique IDs and **zero references to missing IDs within that captured entity set**. This establishes current relationship integrity at the queried run/time; it does not prove that all future updates remain atomic.

## Current topology and cardinality

| Node | Primary TLS | Catalog TLS group | Directions | Entity count |
|---|---|---|---|---:|
| A | 411926415 | 411926415 | East, South, West | 8 |
| B | 5778300421 | 5778300421 | North, South, East | 8 |
| C | 411926497 | 411926497 | North, East, South, West | 10 |
| D | 411925926 | 411925926 | North, East, West | 8 |
| M | 411918581 | 411918581 | North, East, South, West | 10 |

This catalog (`Visualize/generated/network_topology_catalog.json`) records **one physical TLS per selected logical node**. A multi-TLS group for B/C/M is not demonstrated by the current selected catalog, despite older architecture language suggesting one. Future/grouped behavior needs separate evidence. The mapper's direction set is `configuration.config.directions_for_node(node)` and rejects missing catalog directions in snapshot `directions` or `colors`.

## IDs and relationships

| Type | URN pattern | Source relationships | Current count |
|---|---|---|---:|
| Intersection | `urn:ngsi-ld:Intersection:{node}` | `refCameras`, `refTrafficLights`, `refVehicleSensors` | 5 |
| Camera | `urn:ngsi-ld:Camera:{node}` | `refIntersection` | 5 |
| TrafficLight | `urn:ngsi-ld:TrafficLight:{node}-{direction}` | `refIntersection`, `refCamera` | 17 |
| VehicleSensor | `urn:ngsi-ld:VehicleSensor:{node}:{trafficDirection}` | `refIntersection`, `refCamera`, `refTrafficLight` | 17 |

`refTrafficLights` and `refVehicleSensors` are generated only for catalog directions. `numberOfApproaches` equals the number of those directions (3 or 4). The VehicleSensor's traffic-direction suffix is translated through `DIRECTION_TO_TRAFFIC`, whereas TrafficLight uses the physical direction token; do not infer the sensor URN by string-copying the light URN.

## Attribute/source inventory

All entities include `simulationRunId`, `simulationTime`, `scenarioId`, `location`, and a name. `location`/name come from topology/configuration metadata, not per-step TraCI coordinates. The Orion GET fixtures show `name` expanded to the NGSI-LD IRI; source sends an `@context`.

- **Intersection:** `intersectionStatus`, `numberOfApproaches`, `frequentCongestion`, `refCameras`, `refTrafficLights`, `refVehicleSensors`, `dateObserved`, `totalVehicleCount`, `overallTrafficStatus`, `hasActiveIncident`, `currentPhase`, `derivedTrafficState`, `hasSpillback`, `isBoxBlocked`. Optional anomaly and probable-cause fields are emitted only when source values exist. Counts/status are derived from direction values and context; reference lists are topology-derived.
- **Camera:** `cameraNum`, `cameraType`, `cameraUsage`, `cameraStatus`, `refIntersection`, `dateObserved`, `trafficDirection`, `vehicleCount`, `averageSpeed`, `occupancyRate`, `trafficStatus`, `incidentDetected`, `incidentType`, `incidentSeverity`, `confidence`, `recommendedSignalAction`. This is a virtual aggregate over directions; `confidence=1.0` and recommendation `KEEP` are placeholders in the mapper, not observed camera ML output.
- **TrafficLight:** `currentStatus`, `currentPhase`, `timingMode`, `workingState`, `trafficDirection`, `greenDurationCurrent`, `redDurationCurrent`, `yellowDuration`, `refIntersection`, `refCamera`. Status/durations come from controller snapshot with fallback defaults. It has no `dateObserved` in the current mapper.
- **VehicleSensor:** `sensorType`, `sensorStatus`, `trafficDirection`, `refIntersection`, `refCamera`, `refTrafficLight`, `dateObserved`, `vehicleCount`, `pcuEquivalent`, `leftTurnCount`, `straightCount`, `rightTurnCount`, `averageSpeed`, `waitingVehicleCount`, `queueLength`, `queueStraight`, `queueLeft`, `queueRight`, `occupancyRate`, `trafficStatus`, `arrivalRatePcuPerSec`, `waitingReasonCounts`, `theoreticalSpeed`, `spillbackRisk`, `operationalState`. Optional `dominantWaitingReason`, `vehicleClassComposition`, `derivedTrafficState` appear when source data exists.

Numeric null/missing behavior is not uniform: several required sensor fields use direct dictionary indexing and fail if missing; optional fields use defaults or are omitted. Empty relationship lists can be produced for absent directions only if the catalog itself has none, but `build_all_entities` expects catalog-present directions. Orion uses upsert with `options=update`; missing attributes should be evaluated against actual broker behavior before treating omission as deletion or retention. The captured Orion state is one run at simulation time 128.05, not a history of updates.
