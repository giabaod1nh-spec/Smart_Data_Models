# Phase 0 — V2 pipeline audit

Audit captured 2026-11-16 UTC. This directory is documentation and read-only evidence; it changes no production code or configuration.

## Result

The active stack has a v2 Kafka topic, Orion Projector, and Raw consumer. Orion currently holds 44 project entities for run `6c94ec62-7223-4ecb-96c0-4150a3efba3e`. The broker retains no v2 records at the audit instant (`earliest == end` on all three partitions). A real 40-entity historical v2 cycle was recovered from the local SQLite outbox, but it belongs to another run and topology. These two fixtures **must not** be used as a same-cycle parity pair.

**Exit gate: NO-GO for a production cutover.** The code and deployed configuration are mapped, but a current-topology source snapshot and its actual v2 Kafka/Orion result have not been captured together. Crash and partial-upsert recovery were inspected in code, not exercised in an isolated runtime. Phase 1 can use the contract findings, but any parity or cutover claim must wait for the missing evidence.

## Deliverables

- [System baseline](system-baseline.md)
- [V2 data flow](v2-data-flow.md)
- [V2 event contract](v2-event-contract.md)
- [Data preservation matrix](data-preservation-matrix.md)
- [NGSI-LD entity inventory](ngsi-ld-entity-inventory.md)
- [Publisher and outbox audit](publisher-and-outbox-audit.md)
- [V2 dependency inventory](v2-dependency-inventory.md)
- [Projector and ledger audit](projector-and-ledger-audit.md)
- [Simulation snapshot audit](simulation-snapshot-audit.md)
- [Migration risk register](migration-risk-register.md)
- [Fixture provenance and gaps](fixtures/README.md)

Evidence labels used throughout: **source** = inspected repository implementation; **runtime** = read-only live observation; **historical** = archived run/outbox; **unknown** = not verified. A source-level behavior is not presented as a tested failure guarantee.
