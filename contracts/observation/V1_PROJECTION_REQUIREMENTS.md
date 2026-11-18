# Phase 4 V1 projection requirements

The V1 Projector will own NGSI-LD construction. Phase 1 changes no production
mapper. The present V1 helper calls shared `build_all_entities()` and reads
global `cfg`; it is an inspection aid, not the Phase 4 implementation.

For each validated observation, resolve its archived catalog using run ID and
V1 digest before mapping. Use catalog approach directions, TLS group, and node
locations. Keep the stable logical IDs: `Intersection:<node>`, one
`Camera:<node>`, and one TrafficLight and VehicleSensor per catalog approach.
`refTrafficLights` and `refVehicleSensors` must reference exactly the entities
produced for that logical node. Do not infer four approaches or use a physical
TLS as the Orion identity.

| Entity | V1 domain inputs | Projection responsibility |
|---|---|---|
| Intersection | node, directions, PCU/counts, phase, incidents, context | total count, number of approaches, overall traffic status, relationships |
| Camera | direction counts/speeds/occupancy, incidents | aggregate measurements and incident indicator; static camera metadata is Projector-owned |
| TrafficLight | color, phase, timing, control/timing mode | status, durations and intersection relationship |
| VehicleSensor | per-direction counts, PCU, turns, speed, queue, occupancy, optional mix/context | measured attributes and intersection/camera/light relationships |

Static names, URNs, locations, sensor/camera kinds, and relationships belong to
the Projector plus archived catalog. The observation supplies measured and
controller state, not NGSI-LD `Property` or `Relationship` objects.

`null` with `metric_availability` means an existing Orion measurement cannot
remain displayed as fresh. The V1 mapper must remove it or replace it with a
clearly unavailable state. Absent optional fields likewise must not silently
retain a prior dynamic value in a complete-state update. Choose the Orion
replace/delete operation in Phase 4 and test transitions available →
unavailable → available. Never pass null required metrics to the existing V2
mapper, which directly indexes numeric values. A catalog or digest mismatch
must stop projection for that observation.
