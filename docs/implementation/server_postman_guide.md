# Server Postman Guide (Phase 1)

Base URL: **http://localhost:8080**

Environment variables:

| Variable | Value |
|----------|-------|
| `baseUrl` | `http://localhost:8080` |
| `username` | `admin` |
| `password` | `admin123` |

Enable Postman cookie jar for session (`JSESSIONID`).

## 1. Auth

**POST** `{{baseUrl}}/api/auth/login`

```json
{ "username": "{{username}}", "password": "{{password}}" }
```

## 2. System health (no login)

**GET** `{{baseUrl}}/api/system/health`

## 3. Entity reads (after login)

| Method | URL |
|--------|-----|
| GET | `{{baseUrl}}/api/intersections` |
| GET | `{{baseUrl}}/api/intersections/A` |
| GET | `{{baseUrl}}/api/traffic-lights/A-North` |
| GET | `{{baseUrl}}/api/vehicle-sensors/A:NORTHBOUND` |
| GET | `{{baseUrl}}/api/cameras/A` |

Response includes Contract v1 fields: `simulationTime`, `simulationRunId`, `scenarioId`, `currentPhase`, etc.

## 4. Realtime aggregate

**GET** `{{baseUrl}}/api/realtime/intersections/A`

Check `metadata.consistent` and `metadata.consistencyIssues`.

## 5. Control proxy (allowlisted)

| Method | URL | Body |
|--------|-----|------|
| GET | `{{baseUrl}}/api/control/scenario` | — |
| POST | `{{baseUrl}}/api/control/scenario` | `{"scenario":"morning_peak","target_intersection":"A"}` |
| POST | `{{baseUrl}}/api/control/phase` | `{"intersection_id":"A","phase":"NS_GREEN"}` |
| GET | `{{baseUrl}}/api/control/network-state` | — |
| GET | `{{baseUrl}}/api/control/snapshot/A` | — |

Remember: `queued: true` = requested, not yet applied.

## 6. Admin health details

**GET** `{{baseUrl}}/api/system/health/details`

## 7. Debug raw NGSI-LD

**GET** `{{baseUrl}}/api/test/raw/urn:ngsi-ld:Intersection:A`

## Allowlist sync checklist

When `Visualize/api/control_api.py` changes:

1. Update `ControlProxyAllowlist` patterns
2. Compare with FastAPI `GET http://localhost:9090/openapi.json`
3. Update WireMock integration tests
4. Update this guide
