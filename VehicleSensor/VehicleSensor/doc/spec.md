# VehicleSensor

Data Model for a per-approach vehicle sensing device (physical or virtual/simulated: induction loop, radar, magnetometer, or camera-derived counting) in a >
-  `id`: Unique identifier. Pattern: urn:ngsi-ld:VehicleSensor:{intersectionId}:{direction} Example: urn:ngsi-ld:VehicleSensor:Intersection001:NORTHBOUND
   -  Attribute type: **Property**.
   -  Required
-  `type`: NGSI entity type. Must be VehicleSensor.. One of : `VehicleSensor`.
   -  Attribute type: **Property**.
   -  Required
-  `name`: Human-readable display name of the sensor.
   -  Attribute type: **Property**.
   -  Optional
-  `description`: Description of the sensor installation (which approach/purpose).
   -  Attribute type: **Property**.
   -  Optional
-  `dateCreated`: Entity creation timestamp.
   -  Attribute type: **Property**.
   -  Optional
-  `dateModified`: Last modification timestamp.
   -  Attribute type: **Property**.
   -  Optional
-  `location`: GeoJSON Point — physical/logical location of the sensor for this approach (e.g. stop-line position of the approach it covers).
   -  Attribute type: **GeoProperty**.
   -  Required
-  `sensorType`: Sensing technology, including virtual/simulated types since this is a simulation project (not always real hardware).. One of : `VIRTUAL`, >
   -  Attribute type: **Property**.
   -  Optional
-  `sensorStatus`: Operational health of the sensor.. One of : `OK`, `ERROR`, `MAINTENANCE`, `OFFLINE`.
   -  Attribute type: **Property**.
   -  Required
-  `trafficDirection`: Approach direction this sensor covers (all lanes of that approach combined). No ALL_DIRECTIONS option — a sensor always covers exact>
   -  Attribute type: **Property**.
   -  Required
-  `refIntersection`: The Intersection entity this sensor is deployed at.
   -  Attribute type: **Relationship**.
   -  Required
-  `refCamera`: The Camera whose detectionZone overlaps this approach, used to cross-validate vehicleCount/trafficStatus between the two independent data s>
   -  Optional
-  `queueLength`: Overall queue length on this approach, in metres.
   -  Attribute type: **Property**.
   -  Optional
-  `queueLeft`: Queue length (metres) specifically on the left-turn lane(s).
   -  Attribute type: **Property**.
   -  Optional
-  `queueStraight`: Queue length (metres) specifically on the through lane(s).
   -  Attribute type: **Property**.
   -  Optional
-  `queueRight`: Queue length (metres) specifically on the right-turn lane(s).
   -  Attribute type: **Property**.
   -  Optional
-  `waitingReasonCounts`: Breakdown of waiting vehicles by cause, keyed by reason code (e.g. RED_PHASE, CONGESTION, INCIDENT) with integer counts as values.
   -  Attribute type: **Property**.
   -  Optional
-  `dominantWaitingReason`: The single most common reason vehicles are currently waiting, if any.. One of : `RED_PHASE`, `CONGESTION`, `INCIDENT`.
   -  Attribute type: **Property**.
   -  Optional



## Examples

### OK



### OK


