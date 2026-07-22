# TrafficLight

Data Model for a traffic light signal pole in a Smart Traffic Simulation System (NGSI-LD / FIWARE Orion). Controlled adaptively based on Camera observation>

-  `id`: Unique identifier. Pattern: urn:ngsi-ld:TrafficLight:{intersectionId}:{signalId}
   -  Attribute type: **Property**.
   -  Required
-  `type`: NGSI entity type.. One of : `TrafficLight`.
   -  Attribute type: **Property**.
   -  Required
-  `name`: Human-readable name.
   -  Attribute type: **Property**.
   -  Optional
-  `description`: Description of the signal pole.
   -  Attribute type: **Property**.
   -  Optional
-  `dateCreated`: Entity creation timestamp.
   -  Attribute type: **Property**.
   -  Optional
-  `dateModified`: Last modification timestamp.
   -  Attribute type: **Property**.
   -  Optional
-  `location`: GeoJSON Point.
   -  Attribute type: **GeoProperty**.
   -  Optional
-  `currentStatus`: Current traffic light color.. One of : `RED`, `YELLOW`, `GREEN`, `FLASHING_YELLOW`, `OFF`.
   -  Attribute type: **Property**.
   -  Required
-  `phaseStartedAt`: Time when current phase started.
   -  Attribute type: **Property**.
   -  Optional
-  `timingMode`: Signal control mode.. One of : `FIXED_TIME`, `ADAPTIVE`, `MANUAL`, `EMERGENCY_PRIORITY`.
   -  Attribute type: **Property**.
   -  Required
-  `workingState`: Hardware status.. One of : `OK`, `BULB_BURNT`, `CONTROLLER_ERROR`, `OFFLINE`.
   -  Attribute type: **Property**.
   -  Optional
-  `trafficDirection`: Direction controlled by this signal.. One of : `NORTHBOUND`, `SOUTHBOUND`, `EASTBOUND`, `WESTBOUND`, `ALL_DIRECTIONS`.
   -  Attribute type: **Property**.
   -  Optional
-  `greenDurationCurrent`: Current green duration (seconds).
   -  Attribute type: **Property**.
   -  Optional
-  `redDurationCurrent`: Current red duration (seconds).
   -  Attribute type: **Property**.
   -  Optional
-  `yellowDuration`: Yellow duration (seconds).
   -  Attribute type: **Property**.
   -  Optional
-  `refIntersection`: Related Intersection entity.
   -  Attribute type: **Relationship**.
   -  Required
-  `refCamera`: Related Camera entity.
   -  Attribute type: **Relationship**.
   -  Optional


