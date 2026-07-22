# Camera

Data Model for a traffic surveillance camera.

## Attributes

### `id`
- Unique identifier.
- Attribute type: **Property**.
- **Required**

### `type`
- Entity type.
- One of: `Camera`.
- Attribute type: **Property**.
- **Required**

### `name`
- Camera display name.
- Attribute type: **Property**.
- Optional

### `description`
- Camera description.
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
- Camera geographical position.
- Attribute type: **Property**.
- **Required**

### `cameraNum`
- Camera number within intersection.
- Attribute type: **Property**.
- Optional

### `cameraType`
- Camera hardware type.
- One of:
  - `FIXED`
  - `PTZ`
  - `DOME`
  - `DAY/NIGHT`
  - `C-MOUNT`
  - `BULLET`
- Attribute type: **Property**.
- Optional

### `cameraUsage`
- Camera usage.
- One of:
  - `TRAFFIC`
  - `SURVEILLANCE`
  - `RLVD`
  - `ANPR/LPR`
- Attribute type: **Property**.
- Optional

### `orientationAngle`
- Camera orientation (degrees).
- Attribute type: **Property**.
- Optional

### `streamURL`
- Live stream URL.
- Attribute type: **Property**.
- Optional

### `cameraStatus`
- Camera operational status.
- One of:
  - `ok`
  - `offline`
  - `maintenance`
  - `error`
- Attribute type: **Property**.
- **Required**

### `refIntersection`
- Related Intersection.
- Attribute type: **Property**.
- **Required**

### `refTrafficLight`
- Related Traffic Light.
- Attribute type: **Property**.
- Optional

### `refVehicleSensor`
- Related Vehicle Sensors.
- Attribute type: **Property**.
- Optional

### `dateObserved`
- Observation timestamp.
- Attribute type: **Property**.
- **Required**

### `trafficDirection`
- Traffic direction.
- One of:
  - `NORTHBOUND`
  - `SOUTHBOUND`
  - `EASTBOUND`
  - `WESTBOUND`
  - `ALL_DIRECTIONS`
- Attribute type: **Property**.
- Optional

### `monitoredLane`
- Monitored lanes.
- Attribute type: **Property**.
- Optional

### `detectionZone`
- Detection area.
- Attribute type: **Property**.
- Optional

### `vehicleCount`
- Current detected vehicle count.
- Attribute type: **Property**.
- **Required**

### `averageSpeed`
- Average speed (km/h).
- Attribute type: **Property**.
- Optional

### `occupancyRate`
- Occupancy percentage.
- Attribute type: **Property**.
- Optional

### `trafficStatus`
- Traffic flow status.
- One of:
  - `FREE_FLOW`
  - `LIGHT`
  - `MODERATE`
  - `HEAVY`
  - `CONGESTED`
- Attribute type: **Property**.
- **Required**

### `incidentDetected`
- Indicates whether an incident exists.
- Attribute type: **Property**.
- Optional

### `incidentType`
- Type of incident.
- One of:
  - `NONE`
  - `ACCIDENT`
  - `MINOR_ACCIDENT`
  - `BROKEN_VEHICLE`
  - `WRONG_WAY`
  - `LANE_BLOCKED`
  - `ROAD_CLOSED`
  - `PEDESTRIAN`
  - `CONSTRUCTION`
- Attribute type: **Property**.
- Optional

### `incidentSeverity`
- Incident severity.
- One of:
  - `NONE`
  - `LOW`
  - `MEDIUM`
  - `HIGH`
  - `CRITICAL`
- Attribute type: **Property**.
- Optional

### `confidence`
- AI confidence score.
- Attribute type: **Property**.
- Optional

### `recommendedSignalAction`
- Suggested traffic light action.
- One of:
  - `KEEP`
  - `EXTEND_GREEN`
  - `SHORTEN_GREEN`
  - `SWITCH_GREEN`
  - `EMERGENCY`
- Attribute type: **Property**.
- Optional

## Examples

### OK