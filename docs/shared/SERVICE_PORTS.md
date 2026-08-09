# Service ports and health endpoints

## Canonical containerized runtime

| Service | Host port | Container port | Health/readiness |
|---|---:|---:|---|
| MongoDB | `27017` | `27017` | Dependency of Orion; no HTTP health endpoint |
| Production Orion | `1026` | `1026` | `GET http://localhost:1026/version` |
| Context server | `3004` | `80` | `GET http://localhost:3004/datamodels.context-ngsi.jsonld` |
| ClickHouse HTTP | `8123` | `8123` | `GET http://localhost:8123/ping` |
| ClickHouse native | `9000` | `9000` | Native protocol |
| Kafka external listener | `29092` | `29092` | Kafka protocol; topic-list Compose health check |
| Kafka internal listener | none | `9092` | Kafka protocol for containers |
| Raw Consumer | `8091` | `8091` | `GET /health`, `GET /ready` |
| Bronze Processor | `8092` | `8092` | `GET /health`, `GET /ready` |
| Projector | `8093` | `8092` | `GET /health`, `/prepared`, `/ready`, `/metrics` |

`de-migrate` and `kafka-init` are one-shot services and do not expose application ports.

## External processes

| Process | Default host port | Health |
|---|---:|---|
| SUMO Control API | `9090` | `GET http://localhost:9090/health` |
| Spring Server | `8081` in shared startup command | `GET http://localhost:8081/api/system/health` |
| PostgreSQL for Server | `5432` unless overridden | Database protocol |
| Dashboard | Owner-supplied | Owner-supplied |

## Context URLs

- Host clients: `http://localhost:3004/datamodels.context-ngsi.jsonld`.
- Orion inside Docker may use `http://host.docker.internal:3004/datamodels.context-ngsi.jsonld` when configured by the host-side Server profile.

## Rollback Assets

The webhook port `8080` is reserved for Rollback Assets and is absent from the default runtime.
