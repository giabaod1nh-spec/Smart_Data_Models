# Phase 3 implementation kickoff

> Runtime acceptance closure (2026-11-22): A01–A33 evidence was reviewed; actual600-event run/canonical4680 facts PASS. See [acceptance-result.md](acceptance-result.md) for assertion-by-assertion evidence, test counts and operating limitations. Historical design/pending statements below are preserved chronology and are superseded by that closure record.


**State:** implementation in progress; Phase 3 is not certified. Approved planning requirements remain the acceptance baseline.

Implementation and real CH/runtime verification are recorded in `implementation-status.md`. That checkpoint identifies completed subcases and remaining A01–A33 requirements; focused tests alone do not certify the phase.

## Public invalidation feed

Replay resume is current-boundary-only. If source history/binding or archive identity/fingerprint changed before resume, an existing RUNNING ID with the matching frozen manifest is durably marked ABORTED under its instance lock and cannot be resumed. Transient authority unavailability leaves RUNNING retryable. A previously COMPLETE ID retains its historical report/rows/hash but refuses resume after source advances; COMPLETE never claims those old rows are a reconstruction of the newer authority boundary.

`CanonicalSilverReader.read_invalidation_page(after_sequence=0, limit=100, source_binding=None)` returns `InvalidationFeedPage(rows,next_sequence,source_high_water,source_binding,has_more,mirror_pending)`. The session owns independent read-only Bronze/Silver SQLite connections. This method reads the global live invalidation sequence; consumers must process all rows and then filter/map their run/window coordinates, rather than skipping unknown runs in the sequence.

Persist both `next_sequence` and `source_binding` when acknowledging a page. A resumed call with a positive sequence must supply the saved binding; replacement/mismatch fails `SOURCE_BINDING_CHANGED`. Sequence is exclusive, limit is 1–100 entries, and a changed source boundary during a page fails `SNAPSHOT_STALE` without returning any rows. A missing durable local tombstone fails `INVALIDATION_LAG`; unknown/hash-mismatched suppression fails `VALIDITY_UNKNOWN`. Errors expose Python `code`/`retryable`; HTTP mapping belongs to Phase 4.

Each entry includes event/invalidation ID, source sequence/revision, run/generation/digest, intersection/cycle/simulation time, immutable conflict hashes and primary/conflicting delivery lineage. `mirror_status` is PENDING/RECONCILED/FAULT, and `mirror_pending` counts non-reconciled mirrors in that page. Durable local INVALIDATED state plus frozen content and Bronze source identity are checked even while CH is unavailable. The feed does not query CH or claim a fact scan is complete. Consumers must still use the canonical fact reader's completeness barrier before finalizing derived results. A feed page can be consumed while its CH mirror remains pending; it must never restore a suppressed event.

## Reviewed source baseline

- Branch: `codex/phase-1-v1-raw`
- Reviewed HEAD: `fe33bda0ab23aba50127cac499101321f38d3e4b`
- At the start of this correction pass, the working changes were limited to the six Phase 3 planning documents carried from R1–R12. This task continues from the actual worktree without resetting them.
- Preserve Phase 2 source and its current certification state. Do not reset, clean, stash or overwrite local work.

## Required contracts before Silver reads

- Canonical V1 event: `contracts/observation/V1_CONTRACT.md`, `V1_METRIC_CONTRACT.md`, `contracts/traffic_observation.py` and the schema artifact actually used by code.
- Topology and archive: `contracts/topology/v1_digest.py`, archived manifest/catalog under `<archive-root>/runs/<simulationRunId>/`, and lifecycle registry at `<archive-root>/runs/active_run.sqlite3`.
- Bronze authority: read-only `V1SemanticReader`; fixed-high-water `read_page`; current identity, invalidation provenance and bounded `read_delivery_lineage`; canonical Bronze ClickHouse content reconciliation.
- Before P3-01 materialization, verify source identity and that retained `v1_changes` and `v1_invalidations` are complete from sequence 1. Begin at cursor 0 and fail closed on a gap or unproven retention. No atomic bootstrap snapshot is required under this binding no-prune contract.
- Add one Bronze-owned read-only `read_valid_observation(namespace,event_id,expected_revision)` for exact current ACCEPTED event reconstruction with content/hash reconciliation. Do not use paginated current ClickHouse reads as point or as-of lookups. Keep evolving delivery lineage in a separate bounded query. Do not embed `initial_lineage_json` in canonical facts or logical hashes.

## Work packages

P3-01 Bronze adapter/source verifier; P3-02 archive resolver and independent lifecycle refresher; P3-03 additive V1 schema/models/fact builders; P3-04 V1 dimensions/availability/quality; P3-05 durable validity and complete-only canonical reader; P3-06 checkpoint/recovery and stable-boundary replay; P3-07 migrate the existing Silver service/Compose profile and add the 008 migration gate; P3-08 CORE and targeted RESILIENCE acceptance plus Phase 4 handover. EXTENDED sustained benchmark is optional. The authoritative in-place module file list and gates are in `plan.md` and `audit.md`; there is one runtime, not a sibling implementation lane.

## First implementation task

P3-02 archive fixtures and lifecycle refresher scaffolding can start immediately, alongside P3-01 source-verification fixtures. P3-01 may implement the read-only schema/allocator/journal verification and config/source-binding plumbing; it must not materialize source events until the Bronze-owned exact-current point read and operational no-prune contract are verified. No atomic bootstrap dependency. Exercise empty/complete/gapped history, allocator consistency, exact `live` namespace, wrong/replaced source paths, concurrent bounded-page appends, point-read revision/state mismatch and missing mirror.

### P3-05 entry conditions

Begin only after P3-01 proves/binds complete journals and the exact-current accepted-event point read is available. Implement in-process Python `de.silver.canonical_reader.CanonicalSilverReader`; no HTTP reader service. `read_valid_page(query,after_key,limit,token)` uses complete event bundles (intersection plus every expected direction traffic/signal), exclusive stable key `(run_id,digest,event_id)`, limit 1–100 bundles, duplicate collapse and same-key content conflict failure. Tokens bind filter/time range, namespace, both Bronze high-waters, both Silver applied cursors, binding, schema/reader/order versions, page size and last key. Start/end barriers fail closed. Typed exceptions carry stable error codes/suggested HTTP mappings; Python API does not return HTTP status codes. Source churn discards the full scan; restart from page one at most three times (four total attempts) with 100/250/500 ms backoff between retries, then report incomplete `SOURCE_CHURN` and publish no finalized result. Transient I/O retries the same page/token at most three times (four total attempts) with 100/250/500 ms backoff. No starvation-free or multi-page snapshot promise.

### P3-06 entry conditions

Replay may start/resume only when manifest/source/archive/code versions and both allocator high-waters match and history verifies. Bronze must remain stable. Logical hashes include event/fact content and immutable primary delivery coordinates/IDs/hashes; exclude evolving duplicate lineage, processing timestamps and replay-only identifiers. Do not write `initial_lineage_json`; retrieve all current delivery lineage separately. A benign duplicate with unchanged semantic high-waters cannot change fact hashes/count. Any semantic advance before resume, during a batch or completion returns `SOURCE_BOUNDARY_CHANGED`; abort/retire the ID and create a new manifest. An aborted replay is not a historical reconstruction.

## Commands and evidence

Capture a fresh baseline before implementation:

```powershell
git branch --show-current
git rev-parse HEAD
git status --short
git diff --stat
rg --files de/silver de/tests/silver contracts/observation contracts/topology
```

## One Silver runtime and Compose contract

- Service: existing `de-silver-processor`, profile `v1-silver`, entrypoint `python -m de.silver.main`, health port 8095.
- Dependencies: completed `de-migrate-v1-silver` (008), healthy `de-bronze-v1`, healthy ClickHouse. `de-migrate-v1-silver` uses `docker compose --profile v1-bronze --profile v1-silver run --rm de-migrate-v1-silver`, selects 008 only, and depends on completed `de-migrate-v1-bronze` (006/007) plus healthy ClickHouse.
- Enable profiles `v1-raw`, `v1-bronze`, `v1-silver` together for an end-to-end run. Do not enable `v2-legacy`; Gold V2 must not start. The old `de-bronze-processor` is not a dependency.
- Mount `./de/artifacts/bronze_v1:/app/de/artifacts/bronze_v1:ro` with WAL/SHM companions, `./Visualize/artifacts/runs:/app/Visualize/artifacts/runs:ro`, and `./de/artifacts/silver_v1:/app/de/artifacts/silver_v1` read/write. Use V1-only allowlists and distinct live/replay checkpoint/lock namespaces.
- Readiness requires 008 schema verification, read-only Bronze source binding/full-history verification, archive/registry access, lock and worker readiness. Failure stays not-ready or exits; never fall back to V2.

Planned implementation-time validation commands (not run in this documentation task):

```powershell
docker compose --profile v1-raw --profile v1-bronze --profile v1-silver config
docker compose --profile v1-raw --profile v1-bronze --profile v1-silver config --services
docker compose --profile v1-raw --profile v1-bronze --profile v1-silver up -d de-kafka-raw-v1 de-bronze-v1 de-silver-processor
docker compose --profile v1-bronze --profile v1-silver run --rm de-migrate-v1-silver
Invoke-WebRequest http://localhost:8095/ready
```

Actual implementation must verify rendered dependencies/mount modes, migration logs, that no Gold V2 service starts, health readiness, and a real SUMO→Raw→Bronze→Silver run. Store acceptance evidence in `de/artifacts/silver_v1/acceptance/<run-id>/`: tests, schema/migration inventories, source-to-output oracle, archive hashes, lineage/invalidation reports, health/cursor/resource series and replay isolation snapshots. Historical V2 ClickHouse tables/data stay untouched; this does not require the old Silver V2 process to remain executable.

## Phase 4 handover boundary

Phase 3 exit requires its own implementation and evidence: Silver V1 fact/dimension tables; durable event validity and invalidation feed; tested in-process complete-only `CanonicalSilverReader`; bundle/order/token/error/retry contract; source/provenance/availability mappings; immutable run/topology binding and lifecycle projection; A01–A34 evidence according to CORE/RESILIENCE/EXTENDED classification. It does not require Gold V1 to be implemented or running. Phase 4 adoption is downstream, not a Phase 3 blocker or exit condition.

The next phase refactors existing `de/gold/` and `de/gold_runtime/` in place as one Gold V1 implementation/runtime. It must consume the canonical reader and invalidation feed and never bypass validity with raw fact-table scans. Preserve historical Gold V2 database data during cutover; executable V2 runtime preservation is not required. Phase 4 owns nullable analytics, archived-topology direction mapping, freshness/window revisions, invalidation-to-window recompute, lifecycle quality and correct run/scenario selection. Spring Gold repository SQL, DTO/mapper nullable compatibility, and Dashboard compatibility remain unproven until their later acceptance; do not implement them in Phase 3 or claim compatibility.

The Master Plan remains unchanged in this task. Later reconcile its §6 Phase 3/Phase 4 execution text, §8 Gate N, and §9 V2 retirement strategy with the approved in-place Silver and later in-place Gold cutovers. Preserve valid V1 semantic requirements and historical database-retention requirements.

## Current stop conditions

- Stop P3-01 integration until the exact-current point read and complete-history source binding are verified.
- Stop on any unreconciled identity/hash/archive generation/digest, source gap, journal sequence, lineage assertion, or output content. Never coerce unavailable/null to zero, use active topology for historical events, advance over an unresolved intent, or report an unexecuted gate as PASS.
- Stop before destructive V2 changes, Phase 4/5 implementation, or changes outside the approved Phase 3 scope.

## Final kickoff status

The selected Phase 3 contracts are implementation instructions, not evidence of completed implementation. P3-01 remains gated by the exact-current Bronze point read and verified operational no-prune source binding. Keep source authority, both cursors, token semantics, bounded retry, stable replay boundary, lifecycle degraded state, V1-only profiles, migration gate and single-runtime rules. Stop on identity/schema/content conflicts, source churn after retry budget, failed migration/source verification, or any plan to start Gold V2 on V1. P3-08 delivers tested Silver interfaces/evidence and Phase 4 handover; it does not wait for Gold implementation.

## Documentation updated

- `audit.md`
- `plan.md`
- `decisions.md`
- `blockers.md`
- `acceptance.md`
- `handover.md`

### Retired V2 processor execution

`SilverProcessor` now owns only V1 journal execution. The V2 `_run`, `process_stream_once`, stream recovery/dependency polling and V2 ledger/fact helper functions were removed after auditing references. Its constructor no longer instantiates or accepts the V2 BronzeReader/TransformationEngine. Official `python -m de.silver.main` and the existing Compose service remain the V1 runtime.

Historical V2 pure builders/readers/models/repository helpers remain for historical consumers and tests; they are not wired to the Silver executable. `test_processor_batch.py` retains its old V2 assertions with an explicit module skip because its stream/ledger processor API was retired. V1 replacements are journal_worker/runtime_startup/recovery tests and health_api tests. Historical V2 database tables and data remain governed by the additive migration preservation tests.

### Operational Bronze journal retention binding

Keep the bound Bronze V1 SQLite authority and its WAL companions at the configured path while Silver or replay state refers to it. Retain all v1_changes and v1_invalidations from sequence1; do not prune, replace, rewind, or restore an older authority beneath an existing binding. Bronze owns writes; Silver's Compose mount is read-only. V1 source code currently contains no explicit journal DELETE/DROP/TRUNCATE path. Legacy Bronze replay checkpoint DELETE statements target historical bronze_checkpoint/bronze_processing_ledger and do not target V1 journals.

Run `python -m de.scripts.inspect_silver_retention_v1` to capture full retained-history/schema/allocator/link verification, journal min/max/count and source code hashes at a stable boundary. Current evidence `de/artifacts/silver_v1/acceptance/source-retention-binding.json` records changes1..2200 and empty invalidations at H0; `source-retention-mounts.json` records actual RO Bronze/RW Silver mount flags. These records establish current inventory and the runtime retention rule, not an attestation about external schedulers or protection against privileged file rewriting. Full source verification runs at startup/restart; page polling checks the binding and monotonic allocators. A changed binding/schema/history must fail closed and requires operational investigation before resumption.

### Whole-run runtime gate

After the Silver cursors cover the source boundary, run `python -m de.scripts.certify_silver_run_v1 --run-id phase3-silver-20261122-a01 --cycles 120`. It derives exact expected event IDs from the archived participants/cycles, verifies all source identities ACCEPTED and all expected Silver VALID, invokes independent field/value/availability/primary-lineage reconciliation, completes canonical scan at page_size100, verifies exact family counts and stable before/after source boundary, and validates/hash-links the recorded producer→Raw→Bronze source oracle. Output `acceptance/<run>/silver-certification.json` remains INCOMPLETE on missing population, any exception, mismatch or churn; CLI exits1. PASS refers only to this run gate, not all Phase3 requirements. The recorded producer/source oracle is linked explicitly and is not silently presented as a fresh producer rerun.
