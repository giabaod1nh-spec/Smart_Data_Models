# VehicleSensor

Data Model for a per-approach vehicle sensor in a Smart Traffic Simulation System (NGSI-LD / FIWARE Orion).

One entity = traffic metrics for a **single approach direction** at an Intersection.

## Attributes

### `id`
- Unique identifier. Pattern: `urn:ngsi-ld:VehicleSensor:{intersectionId}:{direction}`
- Attribute type: **Property**.
- **Required**

### `type`
- Entity type.
- One of: `VehicleSensor`.
- Attribute type: **Property**.
- **Required**

### `name`
- Sensor display name.
- Attribute type: **Property**.
- Optional

### `description`
- Sensor description.
- Attribute type: **Property**.
- Optional

### `dateCreated`
- Entity creation timestamp.
- Attribute type: **Property**.
- Optional

### `dateModified`
- Last modification timestamp.
- Attribute type: **Property**.
- Optional

### `location`
- Sensor geographical position (GeoJSON Point).
- Attribute type: **GeoProperty**.
- Optional

### `sensorType`
- Sensor hardware/virtual type.
- One of:
  - `VIRTUAL`
  - `INDUCTIVE_LOOP`
  - `CAMERA_DERIVED`
  - `RADAR`
  - `MAGNETOMETER`
- Attribute type: **Property**.
- Optional

### `sensorStatus`
- Sensor operational status.
- One of:
  - `ok`
  - `offline`
  - `maintenance`
  - `error`
- Attribute type: **Property**.
- **Required**

### `trafficDirection`
- Approach direction monitored by this sensor.
- One of:
  - `NORTHBOUND`
  - `SOUTHBOUND`
  - `EASTBOUND`
  - `WESTBOUND`
- Attribute type: **Property**.
- **Required**

### `refIntersection`
- Related Intersection.
- Attribute type: **Relationship**.
- **Required**

### `refCamera`
- Related Camera (same approach, for cross-validation).
- Attribute type: **Relationship**.
- Optional

### `refTrafficLight`
- Related TrafficLight governing this approach.
- Attribute type: **Relationship**.
- Optional

### `dateObserved`
- Observation timestamp.
- Attribute type: **Property**.
- **Required**

### `vehicleCount`
- Headcount of vehicles on this approach.
- Attribute type: **Property**.
- **Required**

### `pcuEquivalent`
- Passenger Car Unit equivalent (primary CBR+GA input).
- Attribute type: **Property**.
- **Required**

### `vehicleClassComposition`
- Share of motorcycle / car / bus / truck (0.0–1.0 each).
- Attribute type: **Property**.
- Optional

### `leftTurnCount`
- Vehicles for left turn.
- Attribute type: **Property**.
- Optional

### `straightCount`
- Vehicles going straight.
- Attribute type: **Property**.
- Optional

### `rightTurnCount`
- Vehicles for right turn.
- Attribute type: **Property**.
- Optional

### `averageSpeed`
- Average speed (km/h).
- Attribute type: **Property**.
- Optional

### `waitingVehicleCount`
- Vehicles currently waiting.
- Attribute type: **Property**.
- Optional

### `queueLength`
- Queue length (metres) for the approach.
- Attribute type: **Property**.
- Optional

### `queueStraight`
- Queue length (m) on straight lane.
- Attribute type: **Property**.
- Optional

### `queueLeft`
- Queue length (m) on left-turn lane.
- Attribute type: **Property**.
- Optional

### `queueRight`
- Queue length (m) on right-turn lane.
- Attribute type: **Property**.
- Optional

### `occupancyRate`
- Occupancy percentage (0–100).
- Attribute type: **Property**.
- Optional

### `trafficStatus`
- Congestion level for this approach.
- One of:
  - `FREE_FLOW`
  - `LIGHT`
  - `MODERATE`
  - `HEAVY`
  - `CONGESTED`
- Attribute type: **Property**.
- Optional

### `arrivalRatePcuPerSec`
- Arrival rate in PCU/s.
- Attribute type: **Property**.
- Optional

### `waitingReasonCounts`
- Counts by waiting reason (e.g. `RED_PHASE`, `CONGESTION`).
- Attribute type: **Property**.
- Optional

### `dominantWaitingReason`
- Dominant waiting reason.
- One of:
  - `RED_PHASE`
  - `CONGESTION`
  - `INCIDENT`
- Attribute type: **Property**.
- Optional

### `theoreticalSpeed`
- Theoretical / Greenshields-derived speed (km/h).
- Attribute type: **Property**.
- Optional

## Examples

### OK
