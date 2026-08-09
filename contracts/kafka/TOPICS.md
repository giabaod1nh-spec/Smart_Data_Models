# Kafka topics (Event Delivery Contract 2.0.0)

## Local compose (K-1)

| Item | Value |
|------|--------|
| Image | `apache/kafka:3.8.1` (pinned — never `latest`) |
| Mode | KRaft, 1 broker |
| Internal bootstrap | `kafka:9092` (containers) |
| External bootstrap | `localhost:29092` (Windows/host producers) |
| Data volume | `./data/kafka` → `/var/lib/kafka/data` |
| Init | `kafka-init` waits `service_healthy`, then create-or-verify |

**Local thesis:** RF=1 — **not** production HA. Do not claim multi-broker durability from this stack.

**Production target:** ≥3 brokers, `replication.factor=3`, `min.insync.replicas=2`, `acks=all`.

Topic create defaults (idempotent init):

- `cleanup.policy=delete`
- `retention.ms=604800000` (7d)
- `retention.bytes=1073741824` (1 GiB/topic)
- `segment.bytes=134217728`
- RF=1

## Main

| Topic | Partitions | Retention | Notes |
|-------|------------|-----------|-------|
| `traffic.entity-events.v2` | 3 | 7 days + 1 GiB | Entity Events; key = `simulationRunId:nodeId` |

**Ordering domain:** per `(simulationRunId, nodeId)` only. Global cycle order is **not** guaranteed across partitions. Projector assembles by `simulationRunId + cycleSequence`.

**Skew gate (local):** keys `run:A|B|C|D` must hit **≥2** partitions; fail if one partition receives **100%**.

## Quarantine vs DLQ (distinct roles)

| Topic | Partitions | When |
|-------|------------|------|
| `traffic.entity-events.quarantine.v2` | 1 | **Invalid contract**: schema fail, unsupported version, missing required, cycle invariant break |
| `traffic.entity-events.dlq.v2` | 1 | **Valid event**, processing exhausted after retries (Orion/CH/downstream) |

DLQ envelope MUST identify consumer (`failedConsumer`, `consumerGroup`, `failureStage`, original topic/partition/offset, error detail). Optionally split `projector-dlq` / `raw-dlq` later.

See [DELIVERY_SEMANTICS.md](./DELIVERY_SEMANTICS.md) and `docs/architecture/KAFKA_FAILURE_SEMANTICS.md`.
