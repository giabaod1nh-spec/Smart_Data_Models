# V1 domain metric contract

The unit and source columns describe the current `SumoSnapshotProvider` and
post-step context. Phase 2 must capture these values from one immutable
sampling snapshot. `R` means required key in every direction; `O` means the
key may be absent. Required keys can hold `null` only with a matching
`metric_availability` entry (`UNAVAILABLE` or `NOT_APPLICABLE`). A number,
including zero, must not have such an entry.

| Direction field | Type, unit | Source / calculation | Status | Consumers |
|---|---|---|---|---|
| `vehicle_count` | int, vehicles | Unique IDs on approach lanes | R | Sensor, Camera, Intersection, analytics |
| `pcu_equivalent` | number, PCU | Sum of vehicle type PCU weights | R | Sensor, density, analytics |
| `left_count`, `straight_count`, `right_count` | int, vehicles | Inferred route movement per vehicle | R | Sensor, turning analytics |
| `average_speed_kmh` | number, km/h | Mean moving vehicle speed | R | Sensor, Camera, analytics |
| `waiting_vehicle_count` | int, vehicles | Vehicles below halt threshold | R | Sensor, analytics |
| `queue_length_m` | number, metres | Stop-line E2 queue or vehicle-length estimate | R | Sensor, analytics |
| `occupancy_pct` | number, 0–100% | E2 occupancy, else mean lane occupancy | R | Sensor, Camera, analytics |
| `queue_length_vehicles` | number, vehicles | Queue metres / passenger-car length | O | Future analytics |
| `queue_by_movement` | three numbers, metres | E2 movement queues or length estimate | O | Sensor |
| `density` | LOW/MEDIUM/HIGH | PCU threshold classification | O | Sensor status |
| `estimated_density_veh_per_km` | number, vehicles/km | PCU / approach length estimate | O | Future analytics |
| `arrival_rate_pcu_per_sec` | number, PCU/s | One-second arrival window | O | Sensor, analytics |
| `waiting_reason_counts` | two ints, vehicles | Red-phase vs congestion split | O | Sensor |
| `dominant_waiting_reason` | enum or null | Larger waiting reason count | O | Sensor |
| `theoretical_speed_kmh` | number, km/h | Greenshields estimate | O | Sensor |
| `moto_front_pct` | number, 0–100% | Motorcycle fraction near stop line | O | Demo / future analytics |
| `vehicle_class_composition` | class → fraction | Per-type counts / approach count | O | Sensor, vehicle mix |
| `approach_length_m` | number, metres | Full approach lane length | O | Spillback analytics |
| `full_link_halting_count` | int, vehicles | Full-link halted vehicles | O | Spillback analytics |
| `full_link_jam_length_m` | number, metres | Full-link sum of jam lengths | O | Spillback analytics |
| `storage_utilization_lane`, `storage_utilization_pcu` | numbers, ratios | Full-link storage estimates | O | Spillback analytics |
| `nominal_storage_pcu_geometric` | number, PCU | Geometric storage estimate | O | Spillback analytics |

Top-level guaranteed state: logical node, run, simulation time, per-node
scenario, control mode, current phase, direction/color maps, and `incidents`
(possibly empty). Optional top-level domain fields are signal timing,
blocked direction, spillback/box indicators, preemption status, derived traffic
state, operational state, direction contexts, inferred causes, and anomaly
annotations. Their exact types are in the JSON Schema.

| Top-level field | Type, unit | Source / calculation | Status | Consumers |
|---|---|---|---|---|
| `phase`, `colors` | phase name; direction → red/yellow/green | Signal controller after SUMO step | R | Signal facts, TrafficLight |
| `incidents` | array | Scenario manager current incident list | R | Incident flags |
| `next_phase` | phase name | Signal controller next phase | O | Signal display |
| `phase_remaining`, `phase_duration` | number, seconds | Signal controller active program | O | TrafficLight/display |
| `green_duration`, `yellow_duration`, `red_duration` | number, seconds | Signal controller configured timing | O | TrafficLight/analytics |
| `timing_mode` | mode name | Control runtime | O | TrafficLight |
| `blocked_direction` | direction or null | Scenario manager | O | Incident display |
| `downstream_edge_full` | bool | Downstream spillback probe | O | Spillback |
| `spillback_pressure` | number, ratio | Downstream occupancy pressure | O | Spillback |
| `spillback_detected`, `intersection_box_blocked` | bool | Spillback/box state | O | Intersection |
| `vehicles_blocked_at_entry`, `yellow_commitment_count` | int, vehicles | Post-step vehicle probes | O | Demo/analytics |
| `preemption_active` | bool | Signal controller | O | Control display |
| `derived_traffic_state` | state name | Context engine node aggregate | O | Intersection |
| `derived_phenomena`, `operational_state` | named bool maps | Context engine | O | Intersection/Sensor |
| `direction_contexts` | direction → state name | Context engine per approach | O | Sensor |
| `probable_causes` | array | Rule-based cause inference | O | Intersection |
| `anomaly_score`, `anomaly_label` | number; string | Optional anomaly classifier | O | Intersection |

`0` is a measured zero. An absent optional key means the producer did not
promise that measurement for this observation; it must not be interpreted as
zero or as the prior value. `null` is only valid for a required direction
metric and must be explained by `metric_availability`. `UNAVAILABLE` means
measurement failed; `NOT_APPLICABLE` means the metric is not meaningful for
that approach. An empty `incidents`, `probable_causes`, or vehicle class map
means none at this capture. A direction not present in the archived topology
must not be emitted; a direction in the topology must be represented even when
its measurement is unavailable.

If a previously available value becomes unavailable, the next observation
contains `null` plus status. Phase 4 must clear or explicitly replace the old
Orion attribute rather than letting a PATCH retain stale data. The current
provider sometimes falls back to `0.0` after failed occupancy getters and can
skip failed per-vehicle reads while still emitting aggregate zeroes. Phase 2
must distinguish partial or failed measurement from measured zero. The current V2 mapper assumes
some direction metrics are always numeric; Phase 4's V1 mapper must handle
null/status without changing that shared mapper.
