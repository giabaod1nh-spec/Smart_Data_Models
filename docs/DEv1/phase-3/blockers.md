# Phase 3 blockers and dependencies

Status as of 2026-11-21. This file records concrete interface dependencies, not ordinary implementation tasks.

## Interface-contract blockers before dependent implementation

1. **Atomic Bronze bootstrap and point lookup are absent.** Existing `V1SemanticReader` exposes current identity, change/invalidation pages, lineage and invalidation provenance, but no verified `read_observation(namespace,event_id,revision)` and no accepted-state snapshot paired atomically with a change high-water. `BronzeClickHouseRepository.read_valid_observations` is current-state keyset pagination and explicitly not time travel. Owner: Phase 2/Bronze interface. Required before P3-01 can claim reliable bootstrap or reconstruct an accepted event solely from a journal change. Additive API, read-only; test exact revision/content, conflict, missing mirror, concurrent state transition and query-only behavior.
2. **Final Phase 2 certification is pending for latest local sources.** Latest `implementation-status.md` records the regression 186/186 but still lists final certification work: authoritative acceptance reconciliation, replay integrity/invariance, rebuild, runtime suite, current local fingerprint and final evidence reconciliation. Phase 3 planning does not wait on this. P3-01 must verify the implemented interfaces against current runtime/source before relying on them. Owner: Phase 2; not a redesign request.
3. **Gold handover must adopt invalidation-aware canonical reader.** The selected safe design requires Gold to consume Silver's canonical reader/validity overlay, not raw ClickHouse facts alone. This should be recorded in Phase 4 planning before Silver acceptance is declared complete. Owner: Phase 4 planning.

## Planning-independent

- Phase 2 certification tasks unrelated to Silver's verified input, journal, lineage, invalidation, schema or runtime boundary do not prevent this audit/plan.
- No current source evidence requires Orion-LD, Projector, or reconstructed NGSI entities for the analytics path.
- Archive mount and Docker runtime integration are Phase 3 implementation tasks, not current blockers.
- Final V1 table names/columns are a Phase 3 additive schema decision; migration 008 is available by sequence after 007.

## Work that can start safely

- P3-02 resolver unit design/fixtures based on producer manifest and digest helpers (no reliance on current active topology).
- P3-03 schema/model/fact builder scaffolding after reviewing D3-06, using fixtures and no production persistence.
- P3-04 availability mapping and independent metric oracle work.
- P3-07 migration-selector/Compose design after verifying migration command behavior.
- P3-08 acceptance harness planning.

P3-01 adapter integration, P3-05 source-driven invalidation, P3-06 bootstrap/replay cutoffs, and full P3-08 runtime acceptance require verified Bronze contracts above. No Phase 2 source modification is authorized by this plan.

