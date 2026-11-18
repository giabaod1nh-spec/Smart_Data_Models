# V2 Projector and Event Ledger audit

Source: `Visualize/tools/projector_live_consumer.py`, `Visualize/integration/projector/{core,poison,node_buffer,offset_tracker,store,schema,retry,bootstrap}.py`, `Visualize/integration/orion/client.py`. Runtime: active `orion-projector` container with `write_mode=active`, `consumer_mode=normal`, group `projector-k5-production`.

## Input, run and entity state

The consumer disables Kafka auto-commit and validates raw JSON. Poison records go through DLQ and a terminal ledger disposition; a failed DLQ write remains unresolved. `TrafficSimulationRunStarted` activates `(source, producerId, producerSessionId, simulationRunId)`. Entity events arriving before RunStarted can wait in a buffer because the control key may be on another partition. Events from another run are marked stale/fenced under the active-run policy.

For an entity event, the Projector reads the already built `event.entity`; it does not call the TraCI snapshot mapper. Node buffers use `nodeEntityCount` and cycle/node keys. Complete buffers are batched; timed-out/incomplete nodes can be partially applied. Catch-up mode can coalesce to the latest event per entity. Orion therefore holds current state, not a lossless record of all v2 samples.

The ledger's `event_id` is a global primary key with exactly one `entity_id` field. `projector_entity_state` is keyed by `(simulation_run_id, entity_id)` and stores last cycle, event ID, payload hash and simulation time. `projector_active_runs`, `projector_partition_commits`, and `projector_offset_gap_ledger` persist run/offset state. `eventId` reused with a different `entityPayloadHash` faults the Projector; identical already-applied IDs are skipped. An older cycle or simulation time is classified as stale/regressed rather than overwriting a newer entity.

## Write and recovery path

```text
valid active-run entity events → node buffer → ledger/entity-state check
  → batch Orion upsert (options=update)
  → classify success, retryable, permanent or ambiguous entity IDs
  → SQLite ledger + entity-state transaction
  → contiguous per-partition offset persistence/broker commit
```

Orion client treats 201/204 as batch success, parses 207 by entity ID, and treats timeout/transport failure as ambiguous. Retryable/ambiguous outcomes pause application and retry; permanent errors can receive terminal status. A partial node flush may receive `NODE_PARTIAL_APPLIED`, which is a completed status for offset handling, not proof that every expected node entity was present. The SQLite partition-commit table is the durable offset authority; the consumer reconciles it against broker commits and retention floors on assignment. Broker group commits represent the next record to read, while local tables store processed offsets.

## Failure-window matrix

| Window | Source-level behavior | Runtime verification |
|---|---|---|
| Orion succeeds, SQLite ledger transaction fails | HTTP side effect may exist without durable success; replay should re-upsert. In-memory completion marks occur before the SQLite call, so the exact same-process fault path warrants isolated verification. | Unknown |
| Orion succeeds, crash before broker commit | SQLite ledger/partition authority and duplicate checks can prevent unintended reapplication after restart. | Unknown |
| Orion returns 207 | Client splits success/retryable/permanent/ambiguous IDs; Projector pauses for retryable outcomes. | Unknown |
| Orion times out after server applies | Client reports ambiguous IDs and retries; upsert is intended to be repeatable. | Unknown |
| Old cycle arrives later | Entity-state cycle/time checks classify regression. | Source verified; runtime not exercised |
| RunStarted arrives on another partition | Pre-run buffer exists; no cross-partition Kafka ordering is assumed. | Source verified; runtime timing not exercised |

`store.cleanup` can remove old terminal ledger rows only after their offsets are durably committed; inactive run entity state also has retention. Phase 4 cannot reuse this schema unchanged for one v1 observation producing 8–10 entity upserts, because the existing event ledger row has one `entity_id`.
