# Development guide

## Shared development contract

All teams develop against the same boundaries:

- Realtime produces Event Contract 2.0.0 through the durable outbox to Kafka.
- Projector is the only writer of Production Orion traffic entities.
- DE Raw Consumer reads Kafka and writes Raw v2 or quarantine.
- DE Bronze Processor reads Raw v2 and quarantine.
- Server reads Production Orion and exposes APIs.
- Dashboard reads Server APIs.

## Local prerequisites

| Area | Requirement |
|---|---|
| Containerized runtime | Docker Desktop and Compose v2 |
| SUMO producer | Python 3.10+, Eclipse SUMO, `Visualize/requirements.txt` |
| Server | Java 21, Maven wrapper, PostgreSQL |
| Dashboard | Maintained in its owning repository |

## Start order

1. From repository root, run `docker compose up -d`.
2. Wait for Raw, Bronze and Projector readiness endpoints.
3. Start SUMO with the command in [README.md](README.md).
4. Start Server with the command in [README.md](README.md).
5. Start Dashboard from its owning repository when available.

## Team workflows

### Realtime

- Update machine schemas and golden examples before changing an event or entity wire field.
- Keep direct Orion publishing disabled in the canonical runtime.
- Preserve deterministic `eventId` and `entityPayloadHash` behavior.

### Data Engineering

- Treat `(topic, partition, offset)` as historical lineage.
- Classify every consumed record exactly once as Raw v2 or quarantine.
- Keep Bronze checkpoint advancement atomic with durable Bronze output.

### Server

- Read current state from Production Orion only.
- Keep Orion and Control API URLs profile-driven.
- Expose UI-facing data through Server endpoints rather than leaking infrastructure endpoints.

### Dashboard

- Read only Server APIs.
- Do not query Orion, Kafka or ClickHouse directly.
- Publish the Dashboard startup command, port and health URL in this directory when the Dashboard repository is connected.

## Contract change workflow

1. Change the machine-readable schema under `contracts/`.
2. Update golden examples and compatibility rules.
3. Update producer and affected consumers.
4. Run contract, unit and architecture tests.
5. Update the corresponding file in `docs/shared/` in the same change.

## Quick checks

```powershell
docker compose config --services
docker compose ps
Invoke-WebRequest http://localhost:1026/version
Invoke-WebRequest http://localhost:8091/ready
Invoke-WebRequest http://localhost:8092/ready
Invoke-WebRequest http://localhost:8093/ready
Invoke-WebRequest http://localhost:8081/api/system/health
```

The default service render must not contain `de-webhook`.

## Rollback Assets

Webhook, Orion subscription, Raw v1 and non-default migration configuration are Rollback Assets. Team feature development must not depend on them.
