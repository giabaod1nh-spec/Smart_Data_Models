# Delivery semantics — Kafka Event Delivery Contract 2.0.0

Full failure matrix and cutover rules: [`docs/architecture/KAFKA_FAILURE_SEMANTICS.md`](../../docs/architecture/KAFKA_FAILURE_SEMANTICS.md).

## Summary (normative)

1. **`produce()` ≠ broker ACK.** Local enqueue success is not durability.
2. **At-least-once** produce and consume. Duplicates possible; Raw uses `(topic, partition, offset)` lineage.
3. **K-2a migration:** RAM buffer allowed; crash-before-ACK **may lose** events (explicit limitation). Buffer full → FAILED evidence, not silent success; K-4.5 must fail if rejects occurred.
4. **K-5 cutover:** **durable local outbox is mandatory**. No outbox → no cutover. **No accepted-loss policy.**
5. **Historical consumer never coalesces or drops** valid events. Projector may coalesce for current-state freshness only.
6. **Offset commit:** contiguous processed prefix **per partition** only.
7. **Quarantine** = invalid contract; **DLQ** = valid but processing failed.
