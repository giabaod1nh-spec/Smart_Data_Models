# TrafficLight Contract v1

| Role | Party |
|------|-------|
| Producer | Realtime Simulation |
| Consumer | Data Engineering |
| Owner | Realtime Team |
| Delivery | Orion Subscription → DE Webhook |

## Phase SoT

| Property | Values | Role |
|----------|--------|------|
| **`currentPhase`** | `NS_GREEN`, `NS_YELLOW`, `EW_GREEN`, `EW_YELLOW` | **Normative** network phase name |
| `currentStatus` | `GREEN`, `YELLOW`, `RED`, … | Lamp color for one approach |
| `greenDurationCurrent` / `redDurationCurrent` / `yellowDuration` | integer seconds | **Configured** cycle lengths — **NOT** remaining time |

DE SHALL read `currentPhase` from a single TrafficLight or Intersection notification. DE SHALL NOT infer phase solely from aggregating four lamp colors (partial Orion notifications).

Color↔phase tables may appear as **non-normative** appendix only.

## Intersection

Intersection entities SHALL also publish the same `currentPhase` string for the node.
