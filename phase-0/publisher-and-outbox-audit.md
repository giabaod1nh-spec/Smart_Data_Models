# Publisher, Outbox and Kafka delivery audit

Source: `Visualize/app/traci_runner.py:165`, `Visualize/integration/orion/publish_cycle.py`, `Visualize/integration/kafka/durable_publisher.py`, `outbox_schema.py`, `outbox_store.py`, `outbox_worker.py`, `contracts/canonical_json.py`.

## Capture and persistence

`fanout_publish_cycle` captures an entity list once from cached node snapshots, deep-copies it and constructs a `PublishCycle`. The v2 durable publisher deep-copies again, creates one `TrafficEntityObserved` per entity, validates the full cycle and inserts all entity rows in a single SQLite `BEGIN IMMEDIATE` / `COMMIT`. It separately persists `TrafficSimulationRunStarted` before a run's first cycle. Therefore the **entity cycle** is atomic in SQLite; the run-start row and the first entity cycle are not one joint transaction. A failed validation prevents the cycle append. `PublishCycle`'s fixed 10-per-node expectation is warning-only.

Outbox row fields include frozen `payload_json`, `payload_hash`, `event_key`, topic, run/cycle/entity sequence, status, attempts, retry time, error, broker partition/offset, timestamps, event kind and outbox sequence. The worker checks row identity/hash before delivery. Its send path uses the stored JSON bytes and key, so ordinary retries preserve event ID and payload.

## State transitions (source)

```text
SQLite COMMIT: OUTBOXED
   → worker selects eligible row → QUEUED → Kafka produce callback
   ├→ ACKED + partition/offset
   └→ FAILED_RETRYABLE + next_retry_at → QUEUED again
Malformed frozen row → FAILED_PERMANENT
Startup orphaned QUEUED → FAILED_RETRYABLE
Old ACKED rows → cleanup after configured retention
```

Kafka producer config in `DurableKafkaPublisher.start`: `acks=all`, idempotence enabled, LZ4, `linger.ms=10` default, delivery timeout 30 s, request timeout 10 s, max five in-flight requests per connection, and high retry count. Worker default is 32 in-flight rows with callback results written to SQLite in batches. ACK means broker delivery was confirmed to the producer; it does **not** mean Orion applied the entity or that the whole pipeline is exactly once.

The Outbox checks free disk and faults on append/commit failure. `OutboxAppendError` propagates to the TraCI runner; the source comment says the runner faults/pauses rather than silently continuing. A crash after SUMO advances but before the Outbox transaction remains a possible telemetry gap. A crash after broker delivery but before SQLite ACK can cause duplicate publication on restart. The Projector's event ledger is intended to absorb that duplicate.

## Runtime limits and unknowns

The local read-only Outbox contains historical ACKED data through 2026-08-10. Its latest complete run/cycle has 40 entities from an older topology; [fixture](fixtures/v2-outbox-historical-cycle.json). The active broker retained no messages at inspection, and no simulation producer was running. The audit did not trigger Kafka outage, disk exhaustion, timeout or restart; those outcomes are source-informed, **not runtime-proven**. Verify any hard delivery guarantee in an isolated run before cutover.
