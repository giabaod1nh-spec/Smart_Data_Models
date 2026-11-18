# Migration risks and open decisions

Classification is limited to evidence obtained in this audit. Critical items block a production cutover; they do not prevent drafting a v1 contract in Phase 1.

| ID | Classification / severity | Finding and evidence | Impact / required decision | Next phase |
|---|---|---|---|---|
| P0-01 | Confirmed limitation / Critical | Raw consumer uses v2 schemas and Silver allowlist names v2 (`de/kafka_raw/config.py`, `de/silver/config.py`). | Full v2 producer removal would stop analytics ingestion. Assign a v1 Raw/Bronze/Silver migration or a controlled compatibility path. | Phase 1–5 |
| P0-02 | Unknown / High | No current v2 broker records retained; no same-cycle source snapshot and Orion outcome. Broker earliest=end on all three partitions. | Cannot prove current-topology value parity. Capture a new isolated source snapshot/event/entity trio before cutover. | Phase 1/4 |
| P0-03 | Confirmed behavior / High | V2 mapping happens before Kafka (`traci_runner.py`, `event_mapper.py`); Projector consumes `event.entity`. | Define projector ownership explicitly in v1; do not treat current Projector as already doing semantic mapping. | Phase 2/4 |
| P0-04 | Confirmed behavior / High | One v2 event maps one entity; current ledger row has one `entity_id` (`projector/schema.py`). | One v1 observation creating 8–10 entities needs observation-level identity and per-entity recovery/partial-result state. | Phase 4 |
| P0-05 | Potential risk / High | RunStarted key differs from node keys; cross-partition reorder is possible and buffered by current Projector. | Define v1 run lifecycle and partition/order policy before migrating. | Phase 1/4 |
| P0-06 | Confirmed behavior / Medium | Catch-up and incomplete-node flush can skip/coalesce samples (`projector/core.py`). | Distinguish current-state parity from lossless analytics requirements. | Phase 1/4 |
| P0-07 | Confirmed limitation / High | Source snapshots contain control/context fields absent from mapped entities (`data-preservation-matrix.md`). | Decide which source fields v1 must guarantee; v2 cannot reconstruct them. | Phase 1 |
| P0-08 | Unknown / High | Orion success followed by SQLite failure/crash and 207/timeout windows were inspected only in source. | Exercise on an isolated stack before claiming recovery guarantees. | Phase 4/validation |
| P0-09 | Confirmed inconsistency / Medium | Current run manifest records actual net hash matching checkout but also a stale expected hash and legacy `intersection_grid_j1_j4` ID. | Pin actual net/catalog/config hashes for fixture provenance; fix metadata separately if required. | Phase 1 |
| P0-10 | Confirmed observation / Medium | Current selected catalog has one physical TLS per logical node, not a multi-TLS B/C/M fixture. | Keep group support but do not claim current multi-TLS validation without a genuine example. | Phase 1/4 |
| P0-11 | Decision / High | V1 granularity, cycle identity, topic/key, null behavior, topology provenance and static/dynamic ownership are not finalized. | Record contract decisions before producer/projector implementation. | Phase 1 |
| P0-12 | Decision / High | Outbox retains historical v2 rows and cutover may occur while v2 group offsets lag/retention gaps exist. | Define drain/expiry, rollback and offset/start policy before removing v2. | Phase 5 |

## Phase exit review

1. Existing v2 publishes a RunStarted control event and one complete NGSI-LD entity per entity event, with current source cardinality 44 per five-node cycle.
2. Source snapshot fields are selectively mapped; several controller/context fields are lost before Kafka.
3. Projector provides run fencing, node buffering, ledger dedup, stale/time checks, optional catch-up coalescing and batch upsert; it is not a lossless event archive.
4. Outbox gives durable post-commit retry and at-least-once broker delivery. Orion/SQLite/offset recovery remains unverified in the listed fault windows.
5. Orion Projector and DE Raw directly consume v2; Bronze/Silver/Gold depend on its historical shape downstream.
6. Preserve logical URNs, entity types/relations, correct per-node direction counts, current-state semantics and durable delivery unless explicitly redesigned.
7. Intentionally move NGSI-LD construction from TraCI-side producer to the v1 Projector and preserve full source observations in the new Kafka contract.
8. Missing current-run parity fixtures, v1 semantics, analytics consumer disposition and recovery evidence remain open.

**Gate:** no-go for production replacement. The audit is adequate input for Phase 1 design, but its requested live parity/failure evidence is not complete.
