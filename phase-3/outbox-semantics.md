# V1 Outbox semantics

The V1 publisher accepts only a successful Phase 2 `V1CycleResult`. It validates the selected run, manifest, archived catalog, participant set, and full Phase 1 event contract. It copies each event and serializes the entire envelope to deterministic UTF-8 JSON once. The worker sends the persisted text as UTF-8 bytes; retry never reads live TraCI state.

`append_v1_cycle` checks all event IDs and inserts missing records in one SQLite transaction. An identical existing event is reused without changing its delivery status. A matching ID with different bytes or identity metadata rejects the entire cycle. Lock/busy errors can be retried with the same frozen rows. Other database errors fail the run. SQLite uses WAL and `synchronous=FULL`.

The Outbox provides atomic **local** cycle persistence and at-least-once Kafka delivery. Kafka can expose intersection messages separately, including duplicates or older cycles after retry. Acknowledged rows are cleaned after seven days; Outbox duplicate recognition therefore lasts only while a row is retained. Phase 4 needs its own event ledger and stale-sequence guard.
