# Smart Traffic Server (Phase 1)

Spring Boot BFF at **http://localhost:8080** — reads Orion entity state and proxies Visualize Control API commands.

## Prerequisites

- Java 21
- PostgreSQL (`traffic` DB, user `erp_user` / `123456`)
- Docker: Orion + context-server (`docker compose up -d mongo-db orion context-server`)
- Visualize running with Control API `:9090` and Orion publish (for live control/read)

## Profiles

Set **`SPRING_PROFILES_ACTIVE`** (not hard-coded in repo):

```powershell
$env:SPRING_PROFILES_ACTIVE = "local"
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
cd server
.\mvnw.cmd spring-boot:run
```

| Profile | Use case |
|---------|----------|
| `local` | Dev on Windows host (localhost Orion/Control) |
| `docker` | Server inside Docker network |
| `test` | Unit/integration tests (H2 + WireMock) |

## API overview

| Group | Base path | Auth |
|-------|-----------|------|
| Auth | `/api/auth/*` | login public |
| Entities | `/api/intersections`, `/api/traffic-lights`, ... | ADMIN |
| Realtime aggregate | `/api/realtime/intersections/{id}` | ADMIN |
| Control proxy | `/api/control/**` | ADMIN (allowlist) |
| System health | `/api/system/health` | public |
| Health details | `/api/system/health/details` | ADMIN |

## Control commands — requested vs applied

Control API returns `{"queued": true}` for POST scenario/phase/overlay commands. That means the command was **enqueued** to SUMO, not necessarily **applied** yet.

Verify applied state via:

- `GET /api/realtime/intersections/{id}` (`metadata.scenarioId`, `currentPhase`, `consistent`)
- Entity GET endpoints (`scenarioId`, `simulationTime` on Orion-backed DTOs)

## Consistency (`/api/realtime/intersections/{id}`)

Orion stores **current** entity state; sequential publish may briefly skew `simulationTime`. The aggregate endpoint:

1. Loads intersection + related entities
2. Checks `simulationRunId`, `scenarioId`, `simulationTime` (tolerance `0.0001`)
3. Retries once (~150ms) if inconsistent
4. Returns `metadata.consistent` and `consistencyIssues[]` if still mismatched

## Control proxy allowlist

Only these upstream paths are proxied (see `ControlProxyAllowlist`). New Visualize routes require allowlist + test update.

## Tests

Project requires **Java 21**. If Maven reports `release version 21 not supported`, your shell is using an older JDK (often JDK 11 from `PATH`). Set `JAVA_HOME` first:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
.\mvnw.cmd clean test
```

- Tier A (CI): WireMock, contract matrix, allowlist — always run
- Tier B (live): `.\mvnw.cmd test -Dgroups=live` when Orion stack is up

## Postman

See [docs/implementation/server_postman_guide.md](../docs/implementation/server_postman_guide.md).
