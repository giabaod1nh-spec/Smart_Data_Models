# Entity Contract v1

| Role | Party |
|------|-------|
| Producer | Realtime Simulation |
| Consumer | Data Engineering |
| Owner | Realtime Team |
| Delivery | Orion Subscription → DE Webhook |

## Applies to

Intersection, TrafficLight, VehicleSensor, Camera (Contract Version **1.0.0**).

## Common SHALL (every published entity)

| Property | NGSI type | Data type | Unit / notes |
|----------|-----------|-----------|--------------|
| `simulationTime` | Property | number | Simulation seconds (NOT wall-clock) |
| `simulationRunId` | Property | string | UUID of the simulation run |
| `scenarioId` | Property | string | Scenario id (e.g. `normal`, `morning_peak`) |

DE SHALL use `simulationTime` for analytical time. DE SHALL NOT use `dateObserved` as simulation clock.

## VehicleSensor (primary observation grain)

SHALL include (among others): `trafficDirection`, `vehicleCount`, `pcuEquivalent`, `averageSpeed` (km/h), `queueLength` (m), `refIntersection`, plus common sim fields.

Golden: [`payloads/VehicleSensor.example.jsonld`](payloads/VehicleSensor.example.jsonld)

## TrafficLight

SHALL include: `currentStatus` (lamp color), **`currentPhase`** (network phase name), configured durations, `trafficDirection`, `refIntersection`, plus common sim fields.

Golden: [`payloads/TrafficLight.example.jsonld`](payloads/TrafficLight.example.jsonld)

## Intersection

SHALL include: `name`, `location`, status/load fields, **`currentPhase`**, relationships to TL/Camera/VS, plus common sim fields.

Golden: [`payloads/Intersection.example.jsonld`](payloads/Intersection.example.jsonld)

## Camera

SHALL include: observation aggregates and incident flags when relevant, plus common sim fields.

Golden: [`payloads/Camera.example.jsonld`](payloads/Camera.example.jsonld)

## Relationships

| Relationship | Cardinality | Notes |
|--------------|-------------|-------|
| `refIntersection` | N→1 | From TL, Camera, VS |
| `refTrafficLights` / `refCameras` / `refVehicleSensors` | 1→N | From Intersection |
| `refTrafficLight` | VS→TL | Same approach |
| `refCamera` | TL/VS→Camera | Same node |

Topology adjacency is **not** an Orion Relationship in v1 — see Topology Contract.
