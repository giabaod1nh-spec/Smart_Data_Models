# Intersection

Data Model for a road intersection in a Smart Traffic Simulation System (NGSI-LD / FIWARE Orion).

- `id`: Unique identifier.
  - Attribute type: **Property**.
  - Required
- `type`: . One of : `Intersection`.
  - Attribute type: **Property**.
  - Required
- `name`: Display name.
  - Attribute type: **Property**.
  - Optional
- `description`: Description of the intersection.
  - Attribute type: **Property**.
  - Optional
- `location`: GeoJSON Point.
  - Attribute type: **Property**.
  - Required
- `dateCreated`:
  - Attribute type: **Property**.
  - Optional
- `dateModified`:
  - Attribute type: **Property**.
  - Optional
- `intersectionStatus`: Operational status.. One of : `ACTIVE`, `MAINTENANCE`, `MALFUNCTION`, `POWER_OUTAGE`.
  - Attribute type: **Property**.
  - Required
- `numberOfApproaches`: Number of roads connected.
  - Attribute type: **Property**.
  - Optional
- `frequentCongestion`: Frequently congested.
  - Attribute type: **Property**.
  - Optional
- `refTrafficLights`: Related traffic lights.
  - Attribute type: **Property**.
  - Optional
- `refCameras`: Related cameras.
  - Attribute type: **Property**.
  - Optional
- `refVehicleSensors`: Related vehicle sensors.
  - Attribute type: **Property**.
  - Optional
- `dateObserved`: Last aggregation timestamp.
  - Attribute type: **Property**.
  - Optional
- `overallTrafficStatus`: Overall traffic condition.. One of : `FREE_FLOW`, `LIGHT`, `MODERATE`, `HEAVY`, `CONGESTED`.
  - Attribute type: **Property**.
  - Optional
- `totalVehicleCount`: Total vehicles detected.
  - Attribute type: **Property**.
  - Optional
- `hasActiveIncident`: Indicates whether any incident is active.
  - Attribute type: **Property**.
  - Optional

## Examples

### OK
