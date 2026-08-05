# Final runtime manifest

## Canonical entrypoint

From the repository root:

```powershell
docker compose up -d
```

This command starts the canonical containerized runtime from the single top-level `docker-compose.yml`.

## Default service list

`docker compose config --services` must contain:

```text
mongo-db
orion
context-server
clickhouse
de-migrate
de-kafka-raw-consumer
de-bronze-processor
de-silver-processor
de-gold-runtime
kafka
kafka-init
orion-projector
```

It must not contain `de-webhook`.

## Migration mode

Default Compose migration command is:

```text
python -m de.scripts.migrate_clickhouse --gold-m1
```

This applies the explicit Gold M1 chain (002–005). Gold runtime never migrates; it only verifies migration 005.

## External processes

### SUMO/TraCI producer

```powershell
Set-Location Visualize
$env:ORION_PUBLISH_ENABLED="false"
$env:ORION_SYNC_PUBLISH="false"
$env:KAFKA_OUTBOX_ENABLED="true"
$env:KAFKA_PUBLISH_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
python -m app.traci_runner --gui --nodes A,B,C,D
```

### Spring Server

```powershell
Set-Location server
$env:SPRING_PROFILES_ACTIVE="local"
$env:SERVER_PORT="8081"
.\mvnw.cmd spring-boot:run
```

### Dashboard

Dashboard source is not present in this repository. Its owner must provide the repository revision, startup command, port and health URL. Dashboard must read the Server/Business Service API and must not query ClickHouse/Gold directly.

## Runtime identities

| Item | Value |
|---|---|
| Main Kafka topic | `traffic.entity-events.v2` |
| Kafka host bootstrap | `localhost:29092` |
| Kafka container bootstrap | `kafka:9092` |
| Projector consumer group | Value configured by the canonical Compose file |
| Projector target namespace | `production` |
| Raw consumer group | `de-kafka-raw-v2` |
| Bronze checkpoint namespace | `live` |
| Silver namespace | `live` |
| Gold namespace | `live` |
| Gold entrypoint | `python -m de.gold_runtime.main` |
| Gold checkpoint | `/app/de/artifacts/gold/checkpoint.sqlite3` |
| Gold instance lock | `/app/de/artifacts/gold/instance.lock` |

## Health endpoints

| Service | Health/readiness |
|---|---|
| Orion | `GET http://localhost:1026/version` |
| Context server | `GET http://localhost:3004/datamodels.context-ngsi.jsonld` |
| ClickHouse | `GET http://localhost:8123/ping` |
| Raw consumer | `GET http://localhost:8091/health`, `/ready` |
| Bronze processor | `GET http://localhost:8092/health`, `/ready` |
| Projector | `GET http://localhost:8093/health`, `/prepared`, `/ready`, `/metrics` |
| Silver processor | `GET http://localhost:8095/health`, `/ready` |
| Gold runtime | `GET http://localhost:8096/health`, `/ready` |
| SUMO Control API | `GET http://localhost:9090/health` |
| Server | `GET http://localhost:8081/api/system/health` |

## Rollback Assets

Webhook, Orion subscription, Raw v1 and non-default migration configuration are Rollback Assets. They require explicit operator action and are excluded from the canonical command.
