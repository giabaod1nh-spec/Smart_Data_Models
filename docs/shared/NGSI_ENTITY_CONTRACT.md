# NGSI-LD entity contract

The authoritative schema and golden payloads live in [`contracts/entity/`](../../contracts/entity/). Contract version: `1.0.0`.

## Entity types

| Type | Identity pattern | Purpose |
|---|---|---|
| `Intersection` | `urn:ngsi-ld:Intersection:{nodeId}` | Intersection state and entity relationships |
| `TrafficLight` | `urn:ngsi-ld:TrafficLight:{nodeId}-{Direction}` | Signal state for one approach |
| `VehicleSensor` | `urn:ngsi-ld:VehicleSensor:{nodeId}:{DIRECTION}` | Traffic observations for one approach |
| `Camera` | `urn:ngsi-ld:Camera:{nodeId}` | Camera aggregates and incident state |

## Common required properties

| Field | NGSI-LD type | Value type | Semantics |
|---|---|---|---|
| `simulationTime` | `Property` | number | Simulation seconds; analytical event time |
| `simulationRunId` | `Property` | string | Opaque run identity |
| `scenarioId` | `Property` | string | Active scenario |

`dateObserved`, when present, is audit wall-clock time and must not replace `simulationTime`.

## Required domain fields

| Entity | Required fields |
|---|---|
| Intersection | `name`, `location`, `currentPhase`, common simulation fields |
| TrafficLight | `currentStatus`, `currentPhase`, `trafficDirection`, configured durations, `refIntersection`, common simulation fields |
| VehicleSensor | `trafficDirection`, `vehicleCount`, `pcuEquivalent`, `averageSpeed`, `queueLength`, `refIntersection`, common simulation fields |
| Camera | Common simulation fields; observation and incident properties are present when applicable |

## Relationship model

| Relationship | Direction | Cardinality |
|---|---|---|
| `refIntersection` | TrafficLight/VehicleSensor/Camera to Intersection | many-to-one |
| `refTrafficLights` | Intersection to TrafficLight | one-to-many |
| `refCameras` | Intersection to Camera | one-to-many |
| `refVehicleSensors` | Intersection to VehicleSensor | one-to-many |
| `refTrafficLight` | VehicleSensor to TrafficLight | many-to-one |
| `refCamera` | TrafficLight/VehicleSensor to Camera | many-to-one |

## NGSI-LD value rules

- `Property`: domain value is in `value`.
- `GeoProperty`: GeoJSON coordinates use `[longitude, latitude]` order.
- `Relationship`: target URN is in `object`; it may be one URN or an array where the contract allows it.
- `@context` resolves to `http://localhost:3004/datamodels.context-ngsi.jsonld` for host clients.
- Additive optional fields are allowed; removing or changing required fields requires a compatible contract version change.
