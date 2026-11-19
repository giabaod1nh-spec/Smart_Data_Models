# V1 Orion attribute ownership and write operations

The Projector owns exactly the attributes in `v1_projection.STATIC` and
`v1_projection.DYNAMIC`. The lists below are the explicit write boundary.
`id`, `type`, and `@context` are NGSI-LD identity/context fields. Any other
attribute found in Orion is externally owned and is left intact. An obsolete
entity with any unknown attribute is not deleted.

## Intersection

- Static, upserted at reconciliation and with each observation: `name`,
  `location`, `intersectionStatus`, `numberOfApproaches`, `refTrafficLights`,
  `refVehicleSensors`, `refCameras`.
- Observation state, upserted when present and explicitly removed when a
  previous value becomes absent: `simulationRunId`, `simulationTime`,
  `scenarioId`, `dateObserved`, `totalVehicleCount`, `overallTrafficStatus`,
  `frequentCongestion`, `hasActiveIncident`, `currentPhase`,
  `derivedTrafficState`, `hasSpillback`, `isBoxBlocked`, `anomalyScore`,
  `anomalyLabel`, `probableCauseType`, `affectedBy`, `causeDetectedAt`,
  `metricAvailability`.

## Camera

- Static: `name`, `location`, `cameraNum`, `cameraType`, `cameraUsage`,
  `cameraStatus`, `trafficDirection`, `refIntersection`.
- Observation state: `simulationRunId`, `simulationTime`, `scenarioId`,
  `dateObserved`, `vehicleCount`, `averageSpeed`, `occupancyRate`,
  `trafficStatus`, `incidentDetected`, `incidentType`, `incidentSeverity`,
  `metricAvailability`.
- Legacy dynamic cleanup only: `confidence`, `recommendedSignalAction`. V1
  does not invent camera ML output or a signal recommendation.

## TrafficLight

- Static: `name`, `location`, `workingState`, `trafficDirection`,
  `refIntersection`, `refCamera`.
- Observation state: `simulationRunId`, `simulationTime`, `scenarioId`,
  `currentStatus`, `currentPhase`, `timingMode`, `greenDurationCurrent`,
  `redDurationCurrent`, `yellowDuration`.
- Legacy dynamic cleanup only: `metricAvailability`.

## VehicleSensor

- Static: `name`, `location`, `sensorType`, `sensorStatus`,
  `trafficDirection`, `refIntersection`, `refCamera`, `refTrafficLight`.
- Observation state: `simulationRunId`, `simulationTime`, `scenarioId`,
  `dateObserved`, `vehicleCount`, `pcuEquivalent`, `leftTurnCount`,
  `straightCount`, `rightTurnCount`, `averageSpeed`, `waitingVehicleCount`,
  `queueLength`, `occupancyRate`, `queueStraight`, `queueLeft`, `queueRight`,
  `trafficStatus`, `arrivalRatePcuPerSec`, `waitingReasonCounts`,
  `dominantWaitingReason`, `theoreticalSpeed`, `vehicleClassComposition`,
  `derivedTrafficState`, `spillbackRisk`, `operationalState`,
  `metricAvailability`.

For each observation, the ledger records an upsert operation for every
generated entity and a separate delete operation for every previously present
dynamic attribute that is no longer supplied. Numeric zero is present and is
never treated as null. Required null values are removed and represented in
`metricAvailability` using the V1 `UNAVAILABLE` or `NOT_APPLICABLE` state.
Optional absence removes an old value. No entity is `APPLIED` until all these
operations complete.

At run reconciliation, the ledger freezes a plan from a fresh Orion inventory:
delete verified obsolete entities, upsert catalog static entities, then remove
the previous run's dynamic attributes from all current catalog nodes. The
plan and completed operation IDs persist in SQLite. New-generation observations
wait until the full plan is complete.
