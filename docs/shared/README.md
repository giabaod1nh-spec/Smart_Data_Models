# Shared project documentation

`docs/shared/` is the team-facing Source of Truth for Realtime, Data Engineering, Server and Dashboard development.

## Start the containerized runtime

Prerequisites: Docker Desktop with Compose v2.

```powershell
git clone <repository-url>
Set-Location Smart_Data_Models
docker compose up -d
docker compose ps
```

The default command starts Kafka, Orion, ClickHouse, the Projector, Raw consumer, Bronze processor and their supporting services. It does not start a webhook.

## Run SUMO

Install Python 3.10+, Eclipse SUMO and the dependencies in `Visualize/requirements.txt`, then run from the repository root:

```powershell
Set-Location Visualize
$env:ORION_PUBLISH_ENABLED="false"
$env:ORION_SYNC_PUBLISH="false"
$env:KAFKA_OUTBOX_ENABLED="true"
$env:KAFKA_PUBLISH_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
python -m app.traci_runner --gui --nodes A,B,C,D
```

## Run Server

Prerequisites: Java 21 and a PostgreSQL instance matching the Server configuration.

```powershell
Set-Location server
$env:SPRING_PROFILES_ACTIVE="local"
$env:SERVER_PORT="8081"
.\mvnw.cmd spring-boot:run
```

## Run Dashboard

This repository does not contain the Dashboard source or a Dashboard startup command. The Dashboard repository must consume the Server API and document its own command and health URL before it is added here.

## Primary health endpoints

| Component | Endpoint |
|---|---|
| Orion | `http://localhost:1026/version` |
| Context document | `http://localhost:3004/datamodels.context-ngsi.jsonld` |
| ClickHouse | `http://localhost:8123/ping` |
| Raw consumer | `http://localhost:8091/ready` |
| Bronze processor | `http://localhost:8092/ready` |
| Projector | `http://localhost:8093/ready` |
| SUMO Control API | `http://localhost:9090/health` |
| Server | `http://localhost:8081/api/system/health` |

See [SERVICE_PORTS.md](SERVICE_PORTS.md) for the complete port and health table.
