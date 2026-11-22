# Phase 3 Silver V1 implementation checkpoint
## Subset topology identity and reader token versions — 2026-11-22

Topology dimensions now derive the complete sorted node/direction inventory from the verified archived catalog, independently of the run participant subset. Run dimensions/bindings retain the manifest subset, and event validation stays participant-scoped. Archive verification checks every catalog node, including nonparticipants, for a nonempty approach map. Tests prove a subset shares the exact topology dimension with a full run; the journal worker produces only A's three directions while retaining B in the topology dimension; a malformed nonparticipant catalog fails closed. Archive/journal group **20 passed**, `subset-topology-junit.xml`.

Canonical page tokens now explicitly bind reader version 1, schema version 008 and ordering-key version 1. Missing/changed version claims are rejected as SNAPSHOT_STALE. Reader/feed group **6 passed**, `reader-token-versions-junit.xml`; git diff --check passed. These edits and the prior replay preflight fixes still require image rebuild/deployment.

The last successfully observed live snapshot was 18:00:28 UTC: change cursor 1256/2200, invalidation cursor 0, READY, lifecycle current, no fault. A subsequent build failed because Docker's named pipe was absent, and Docker Desktop/backend processes were absent. No inference of runtime convergence or A01 completion is made from stale evidence. Goal remains active; A01–A33 certification remains incomplete.

## Replay resume preflight — 2026-11-22

Changed source/archive preflight now acquires the existing replay ID lock and marks only a matching RUNNING manifest ABORTED before CH repositories are created. COMPLETE historical state/hash/rows are preserved but current-boundary resume is refused. Schema/gap/binding failures during replay become SOURCE_BOUNDARY_CHANGED/ABORTED; transient AUTHORITY_UNAVAILABLE retains RUNNING for recovery. Archive immutable/fingerprint changes are handled consistently at start/resume and during journal processing. Replay/source tests **15 passed**, XML `replay-preflight-junit.xml`.

Actual source advanced from frozen 1600 to current 2200 after fresh SUMO workload. Real boundary tests **2 passed**, XML `real-replay-boundary-junit.xml`: old COMPLETE replay refuses resume with unchanged complete inventory, and an isolated interruption fixture using the old manifest becomes ABORTED with zero physical CH outputs. Reports `completed-replay-source-advance-refusal.json` and `p3-preflight-abort-*.report.json`; no live/source state changed. Latest replay module copied into running container for CLI verification; next image rebuild must include it.

Latest real snapshot 17:56:13 UTC: cursor 1220/2200, READY, lifecycle current, no fault. Earlier sustained run has 540 VALID observations; fresh A01 still awaits its journal turn. Benign duplicate lineage growth under a stable replay boundary and full producer-conflict feed acceptance remain unverified.

## Atomic event finalization — 2026-11-22

Accepted-event finalization now commits local VALID publication and change-cursor generation CAS in the same SQLite transaction, after exact CH reconciliation and the final Bronze point-read check. It supports recovery of the earlier VALID-but-cursor-not-advanced state at the same revision/sequence and never restores INVALIDATED. Tests inject a cursor-update SQL failure after the validity UPDATE, verify both rollback, close/reopen SQLite, then recover successfully; unresolved obligations, stale generation and invalidated state are rejected. **23 passed**, XML `atomic-finalization-junit.xml`.

Worker image rebuilt; actual container worker/schema/source-lag/CH invalidation-fault/checkpoint/journal group **21 passed**, XML `atomic-finalization-live-junit.xml`. Latest real snapshot 17:50:52 UTC: source boundary 2200/0 (includes the new SUMO workload), cursor 1172/0, 1172 VALID plus one PENDING, lifecycle current and no fault; RECOVERING immediately after deployment. Fresh A01 run remains behind earlier source entries and is not yet reconciled in Silver. No throughput improvement/capacity claim is inferred from fewer transaction commits.

Next audit items include persisting ABORTED for an interrupted replay whose source advanced before resume preflight, completing the broader A01–A33 evidence map, final reader scan after convergence, and classifying/removing obsolete V2 executable paths/tests.

## Fresh actual SUMO workload — 2026-11-22

Started and observed successful exit (code 0) of the existing producer entrypoint:
`python -m de.tools.v1_raw_sumo_acceptance --run-id phase3-silver-20261122-a01 --duration 120 --step-length 0.1 --bootstrap localhost:29092 --artifact-root Visualize/artifacts`.
New generation 4 archived run is COMPLETE with failedCycleCount=0, participants A/B/C/D/M and cycles 1–120. Existing historical run registry rows were retained; producer activation selected the new generation through its normal owner API. No prior SUMO process was running.

Independent source oracle verified all **600/600** events: 600 ACKED Outbox, 600 Raw, 600 canonical Bronze; exact expected event IDs, full wire/envelope/payload equality, Raw byte hashes, Bronze canonical hashes and primary Raw ID/topic/partition/offset. Archive-derived expected Silver counts are **600 intersection + 2040 traffic + 2040 signal = 4680 facts**. Evidence: `acceptance/phase3-silver-20261122-a01/{producer.log,producer-completion.json,source-oracle.json}`. `reconcile_silver_source_v1.py` reads only Outbox/CH/archive and does not use the Silver builder.

Raw/Bronze accepted the fresh workload while live Silver remained READY and processing its earlier backlog. Bronze source boundary will now reach 2200 rather than the previous 1600; old replay manifests consequently must not resume as current-boundary replays. A01 remains IN_PROGRESS pending full Silver materialization, independent value/cardinality/availability/lineage reconciliation and canonical complete scan. No A01/Phase 3 PASS is claimed. Resource samples now include timestamps (the first historical point sample did not).

## Bounded acknowledgement and portable feed verification — 2026-11-22

CH-verified obligation acknowledgements now commit in one bounded SQLite transaction instead of one commit per output row. Cursor gates and frozen content remain intact. Tests verify rollback on an unknown hash, acknowledgement of only the verified prefix when a later output fails, lost-response reconciliation, persistent permanent faults and pending invalidation feed. Acknowledgement/recovery/journal/feed group **19 passed**, XML `acknowledgement-recovery-junit.xml`. This reduces acknowledgement transaction count; no throughput improvement is claimed without measurement.

Runtime image rebuilt with the public invalidation feed, full bootstrap linkage/page continuity checks and batch acknowledgement. Initial container suite collection failed because a fixture imported contract tests that read `Visualize/generated`; five Silver V1 tests now share a portable synthetic snapshot helper, with no generated topology dependency. Synthetic fixture/reader/replay group **24 passed**. Real container/feed/lost-response/recovery group **13 passed**, XML `live-feed-recovery-junit.xml`, including SQLite-only empty invalidation feed and the synthetic two-conflict pending-mirror feed test. All current runtime changes are now in the image.

Inspection now appends snapshots to `runtime-samples.jsonl`; resource samples are recorded separately in `resource-samples.jsonl`. Latest snapshot at 17:41:05 UTC: cursor 1089/1600, READY and lifecycle current; sustained run 409/600, smoke02 200/200. One Docker sample: 58.74 MiB RAM, 8.83% CPU; this is a point observation, not a capacity/throughput benchmark. Goal active; final A01–A33 gates remain incomplete.

## Public invalidation feed — 2026-11-22

Added `CanonicalSilverReader.read_invalidation_page` and `InvalidationFeedPage`: global exclusive sequence pagination, saved source binding required on resume, fixed-page source boundary checks, exact source/frozen/local INVALIDATED identity and coordinates, frozen hash verification, and per-entry mirror status/backlog. Feed reads SQLite only and remains usable while the CH tombstone mirror is pending; it does not certify fact completeness. Handover now documents consumer cursor/binding acknowledgement and Phase 4 usage.

Real SQLite/CH-offline fixture covers two conflicts, pending mirror before cursor advancement, missing-local invalidation lag with no partial page, exclusive pagination, mirror reconciliation, binding mismatch and frozen-content corruption. Reader/feed/recovery group **8 passed**, XML `invalidation-feed-junit.xml`. Also removed HTTP-status attributes from Python errors to match approved plan §integration method; the historical checkpoint entry describing those attributes is superseded. Full feed integration on a producer conflict remains pending.

Live snapshot at 17:35:13 UTC: cursor 1030/1600, READY and lifecycle current; smoke02 all 200 events materialized, sustained 350/600. Feed and latest journal page gate need the next runtime image rebuild before final certification.

## Registry readiness, migration preservation and source linkage — 2026-11-22

Real lifecycle worker fixture now verifies readiness true→false on registry outage→true on recovery, cached RUNNING/failed count retention and durable retry reset. Uses actual Bronze RO authority, separate replay checkpoint/CH targets and a copied archived registry, with all out-of-fixture source runs skipped. Lifecycle real-CH group **4 passed**, XML `real-lifecycle-junit.xml`; before/during/after worker snapshots retained in unique `p3-lifecycle-worker-*/worker-report.json`.

Real migration clean-install and populated V2 upgrade fixtures **2 passed**, XML `real-migration-junit.xml`. Unique CH databases contain actual migrations 001–005 plus per-table V2 sentinel rows; 006/007 then selected 008 twice verify all 20 V1 Silver tables. All prior object definitions and order-independent full-row content inventories match before/after, including pre-existing Gold seed rows. Reports `acceptance/p3_migration_*.json`; isolated test databases retained, no historical database deletion. Initial upgrade verifier was corrected for nondeterministic CH row order and seeded rows; no migration change was needed for that verifier failure.

Full-history bootstrap verification now checks each non-PENDING identity's matching event/sequence/revision/state change and exactly one bidirectional invalidation↔CONFLICTED change linkage. Contiguous counts alone cannot pass a broken link. Every bounded journal page is fully checked for expected sequences before materialization, including missing first/middle/tail entries. Source/journal/replay focused group **19 passed**, updated `source-reader-linkage-junit.xml`. Full-history linkage verified against actual 1600-change Bronze authority. Source file copied into container for that verification; latest page gate still requires the next worker image rebuild before final runtime acceptance.

Latest snapshot 17:30:30 UTC: live cursor 981/1600, READY and lifecycle current, no fault. Public Silver invalidation-feed interface remains a required implementation/handover gap identified by the checklist audit.

## Independent lifecycle polling — 2026-11-22

Live lifecycle refresh now runs in its own thread with a separate CH repository/client, independent of event journal batches and their dependency retries. Readiness requires a live lifecycle poller, current authority/cache and freshness within `SILVER_V1_LIFECYCLE_STALE_INTERVALS` (default 3). Health exposes `lifecycle_ok`/`lifecycle_error`; missed-poll reads fail readiness immediately without refreshing the snapshot heartbeat. Missed-poll STALE transitions are durably projected once while retaining confirmed status/count/hash. Poll success/retry count, last attempt/success, next poll and error persist in Silver SQLite; transient polling uses bounded exponential backoff capped at 30 seconds and resumes configured polling on success.

Tests prove lifecycle polls while the event worker remains blocked, stale readiness gating, one stale snapshot/recovery, restartable operational metadata, startup and outage recovery. Final worker/archive/metrics group **16 passed**, XML `lifecycle-independent-junit.xml`. Earlier config-inclusive group **20 passed**. Host HTTP TestClient run was interrupted after stalling; the same HTTP tests in Linux container **3 passed**, XML `health-api-junit.xml`. Real container/schema/source-lag/lifecycle group **8 passed**, XML `lifecycle-live-junit.xml` (before the final health metadata addition). Runtime rebuilt with all changes.

Latest saved snapshot at 17:24:37 UTC: live cursor 918/1600, READY and lifecycle_ok=true, retry_count=0; last_successful_poll 17:24:36, next_poll 17:24:41. All three known runs were polled in that cycle. Main lifecycle table contains nine rows, including missed-poll/recovery transitions observed during deployment. Full live authority-outage readiness fixture and final A04/A09 checklist audit still remain.

## Lifecycle and cross-ID replay — 2026-11-22

Second actual replay `p3-real-smoke02-second-20261122` COMPLETE at 1600/0. `inspect_silver_replay_v1 --replay-id ... --compare-with p3-real-smoke02-20261121` asserts identical frozen source/archive/version fields (excluding replay ID), logical hash and physical counts/run populations. Both have hash `5190facab9c92aa4b9096c631e31e3e6d385b27875fa08f3370d0fe45c533f82` and 200/680/680 fact cardinalities. Comparison saved in second replay `.report.json`. This adds real cross-ID determinism evidence; benign duplicate/source conflict abort cases still remain.

Lifecycle now validates registry status even when manifest is unavailable. Offline terminal/outage/restart tests pass for COMPLETE/INCOMPLETE/STOPPED. Real CH archived-fixture lifecycle tests **3 passed**, XML `real-lifecycle-junit.xml`: each emits exactly six snapshots for SYNCED→LAGGING→UNAVAILABLE→SYNCED→registry-outage/STALE→recovered/SYNCED, preserves last-confirmed count/hash, resets freshness after restart, and identical polls create no CH rows. Unique replay-only fixture reports retained under `acceptance/p3-lifecycle-*/`. No producer archive/registry files were mutated. Lifecycle/replay/startup focused group **17 passed**, XML `lifecycle-replay-focused-junit.xml`. Independent lifecycle timing/missed-interval freshness/backoff and health gating remain to finish.

Latest real live snapshot 17:15:30 UTC: cursor 802/1600, worker READY, no fault. Goal active; full Phase 3 certification not achieved.

## Durable invalidation recovery — 2026-11-22

Added real CH fault tests using synthetic invalidation provenance and unique replay-only targets/state. Both before-insert failure and lost-response-after-commit cases pass: local INVALIDATED is durable before CH write, invalidation cursor remains zero while pending, exact frozen JSON survives SQLite close/reopen, retry reconciles then advances cursor, and physical CH tombstone count stays one. **2 passed**, XML `real-invalidation-recovery-junit.xml`; per-fixture state/report retained under `acceptance/p3-invalidation-*/`. These are targeted resilience boundaries, not end-to-end producer conflict acceptance.

Obligation failures now persist attempts/last_error without changing frozen content or cursor. Permanent same-key/target conflicts set FAULT and require explicit recovery rather than silently retrying after restart. Transient errors retain PENDING and reconcile on resume. Focused runtime/recovery/journal/replay suite **22 passed**, updated `runtime-fault-focused-junit.xml`. Container rebuilt with these changes. Real snapshot at 17:11:21 UTC: cursor 740/1600, 740 VALID plus one PENDING, zero main invalidations/quarantines; recovering after deployment with live worker and no fault. A22/A23/A25 have additional boundary evidence but are not fully accepted.

## Canonical reader continuation — 2026-11-22

Reader now maps CH/network/archive/SQLite outages to AUTHORITY_UNAVAILABLE, repository same-key conflicts to CANONICAL_CONTENT_CONFLICT, and unreconciled Bronze/archive integrity to VALIDITY_UNKNOWN. Constructor authority failures close partial connections and return typed errors. Error classes expose suggested HTTP status (503 transient/lag, 409 stale/binding, 500 content conflict). Limits require integers 1..100; malformed/non-object tokens and cross-query keys fail before bundle reads. Allocator regression behind the applied Silver cursor fails SOURCE_BINDING_CHANGED; schema/binding changes are nonretryable.

Reader/source tests: **8 passed**, XML `canonical-reader-focused-junit.xml`; checks include four-attempt transient exhaustion, discarding staged stale pages before restart, malformed inputs, content/outage mapping and regression rejection. Real container/schema/source-lag suite remained **5 passed** after the initial mapping change. Final reader source copied into container; no live worker restart was needed. Latest diagnostic snapshot at 17:07:08 UTC: cursor 665/1600, READY, no fault. Full current-boundary CH bundle scan remains pending until catch-up.

## Real replay evidence — 2026-11-22 (Asia/Bangkok)

Replay manifest validation now rejects boolean/negative journal boundaries, invalid generation/run IDs, malformed digest/hash/source binding, and unsupported reader/processor/schema versions before state creation. Added read-only manifest freeze and replay inspection commands. Replay-focused tests: **12 passed**, XML `replay-focused-junit.xml`.

Actual CH replay `p3-real-smoke02-20261121` completed at frozen source boundary 1600/0 while live worker continued processing. Archived run `phase1-raw-20261119-smoke02` has 200 accepted observations. Physical replay targets contain exactly 200 intersection, 680 traffic, 680 signal, 200 validity, one run/topology/binding/lifecycle row each, zero quarantine/invalidation. All replay target run IDs match smoke02; replay SQLite namespace is `replay:p3-real-smoke02-20261121`, live SQLite retains only `live`. Independent Bronze-source oracle verified all **200/200** event bundles and primary lineage with no errors. First oracle attempt exposed FixedString bytes normalization in the oracle; its failed report is retained, and the corrected report passes.

Completed resume returned `idempotent=true` with logical hash `5190facab9c92aa4b9096c631e31e3e6d385b27875fa08f3370d0fe45c533f82`. Evidence in `de/artifacts/silver_v1/acceptance/`: `p3-real-smoke02-20261121.manifest.json`, `.report.json`, `.resume.json`, and `oracle-p3-real-smoke02-20261121.json`. Live snapshot at 17:03:31 UTC: cursor 594/1600, READY, no fault. This establishes one real replay/isolation/primary-content case; cross-ID deterministic replay, benign-lineage growth, source-boundary abort/crash matrix and full A28–A30 acceptance remain incomplete.

## Runtime continuation — 2026-11-21

Bootstrap now executes in the worker thread so the existing health entrypoint can serve during temporary startup failures. Full source-history/schema verification still precedes cursor resume; temporary CH/source/archive failures retry, while schema/immutable integrity faults fail closed. Startup outage fixture verifies no source binding/cursor progress while CH is offline and automatic readiness recovery. Startup/recovery/journal/replay group: **17 passed**, XML `startup-recovery-junit.xml`.

V2 Compose/checkpoint assumptions were migrated in `test_compose_render.py` and `test_container_runtime_contract.py`: V1 service dependencies/profile, RO Bronze/archive mounts, V1 source topic and source-bound checkpoint. Compose/config/startup: **16 passed**. Real container/schema/reader checks: **5 passed**, no skips, updated XML `live-runtime-junit.xml` (supersedes earlier two-check report). The broad old Silver suite was attempted but interrupted after failures and a non-progressing test; it has not passed and V2-specific regression classification remains required.

Real snapshot 16:53:17 UTC: cursor 399/1600, 399 VALID, zero pending obligations at capture, 399 intersection facts; worker recovering after deployment. This is not whole-run acceptance.

Worker fault classification now fails closed on journal gap/schema/binding errors, canonical same-key content conflicts and permanent checkpoint failures. Temporary authority unavailability and SQLite busy remain retryable. Focused runtime/source/journal/replay suite: **18 passed**, evidence `de/artifacts/silver_v1/acceptance/runtime-fault-focused-junit.xml`; tests verify cursor holds on faults and authority outage automatically recovers to READY. Startup-time temporary dependency handling still requires completion (current initial bootstrap may raise before health server starts).

Updated container deployed with fault classification. Real snapshot at 16:50:31 UTC: change cursor 339/1600, 339 VALID plus one PENDING event, two lifecycle rows; post-restart state RECOVERING with live worker and no fault. These counts are concurrent diagnostics, not final acceptance.

Independent read-only source oracle `python -m de.scripts.reconcile_silver_v1 --run-id phase1-raw-20261119-sustained01` verified 120/120 currently VALID events against Bronze-owned verified point reads on real ClickHouse. Checks include archived direction population, metric values/nulls, availability/payload JSON, signals/optional state, complete-only totals and primary lineage. Source run has 600 ACCEPTED events, so whole-run completeness is explicitly false. Report: `de/artifacts/silver_v1/acceptance/oracle-phase1-raw-20261119-sustained01.json`. No Silver builder is used to derive this oracle.

Real runtime integration checks: `SILVER_LIVE_ACCEPTANCE=1 python -m pytest de/tests/silver/test_live_runtime_v1.py -q --junitxml=de/artifacts/silver_v1/acceptance/live-runtime-junit.xml` inside Silver container — **2 passed**. Exact CH schema verified and canonical reader raised SOURCE_LAG before returning any partial page while source backlog existed. This proves only that A27 subcase, not full A27 acceptance. Latest diagnostic snapshot at 16:47:06 UTC: cursor 237/1600, worker READY/healthy, no fault; catch-up continues.

Docker Desktop was started successfully. Migration 008 executed on ClickHouse 24.3.18.7; exact schema verification passed for all 20 tables. Runtime found and fixed unsupported `Nullable(LowCardinality(String))` (now `LowCardinality(Nullable(String))`), DateTime64 parameter parsing, captured-at millisecond precision, naive UTC normalization, repeated immutable-dimension processing timestamps, and shared topology archive locators. Immutable hashes/content still reconcile; topology retains its first catalog locator. Existing V2 Silver tables were observed with zero rows before and after migration.

Lifecycle now has a durable CH projection acknowledgement and lost-response recovery test, wired into live/replay. Live journal passes are limited to ten events so lifecycle and health refresh between batches. Startup remains RECOVERING until its first successful worker pass. Added read-only runtime evidence command `python -m de.scripts.inspect_silver_v1`.

Latest saved real snapshot: `de/artifacts/silver_v1/acceptance/runtime-latest.json`, observed 2026-11-21 16:44:19 UTC. Bronze source high-water 1600/0; Silver cursor 126/0; 126 VALID events, one PENDING; 127 physical intersection facts, two run lifecycle rows, no quarantines. This is a concurrent diagnostic snapshot, not an atomic reconciliation or acceptance verdict. Silver is actively catching up. Full source oracle, actual five-node 120-cycle A01 workload, canonical reader real CH, replay/fault suite and A01–A33 certification remain outstanding.

Focused suite after resume: 29 passed. Worker/replay/repository checks after bounded-pass change: 10 passed. Goal remains active; Phase 3 is not certified.

Baseline: branch `codex/phase-1-v1-raw`, HEAD `fe33bda0ab23aba50127cac499101321f38d3e4b`. The six Phase 3 planning documents were already modified at entry and are preserved. This checkpoint records implementation evidence, not a Phase 3 acceptance claim.

| Package | Status | Work and evidence | Exact next executable task |
|---|---|---|---|
| P3-01 | IN_PROGRESS | Added Bronze-owned `BronzeClickHouseRepository.read_valid_observation` with current ACCEPTED/revision and canonical CH obligation checks. Added read-only `de/silver/source_binding.py` for exact schema, allocator, journal continuity and path binding verification. Added a concurrent writer-during-CH-query test; point-read tests now 2 passed. Source inspection found no V1 journal prune/delete path; out-of-band deletion remains an operational limitation. Real CH integration and runtime binding remain. | Run real CH point-read integration and operational no-prune attestation, then wire verified binding into Silver startup. |
| P3-02 | IN_PROGRESS | Added `archive_resolver.py` with historical manifest/catalog digest, participant/approach, generation and read-only registry checks; added `lifecycle_refresher.py` with a Silver-owned known-run SQLite index, independent bounded polling, idempotent snapshot history and degraded outage state. Fixed SQLite connection closure and added latest projection/restart recovery coverage. Runtime wiring, full lifecycle semantics and real-run evidence remain. | Wire resolver and poller into worker startup/processing; complete manifest count/hash and freshness semantics, recovery and operational evidence. |
| P3-03 | IN_PROGRESS | Added additive migration 008 with 10 main and 10 isolated replay tables, the 008-only `--v1-silver` selector, and a pure V1 event bundle builder in existing `fact_builders.py`. Existing repository has V1 static table allowlist, schema inventory, isolated inserts and same-key full-row reconciliation. The existing processor now uses builder/repository on ACCEPTED journal entries. Offline tests passed; real CH migration/materialization remains. | Apply migration against real ClickHouse and verify full schema, source-to-output values and V2 sentinels. |
| P3-04 | IN_PROGRESS | V1 builder preserves measured zero, nullable unavailable counts, optional payload absence and complete-only intersection total. Existing `dimension_builders.py` now builds archive-derived immutable run/topology/binding rows without mutable status. Focused tests passed; worker/repository integration and broader quality contract remain. | Connect immutable dimensions and quality rows to durable worker obligations. |
| P3-05 | IN_PROGRESS | Added Silver V1 SQLite source binding, separate cursors, frozen obligations and event validity schema with run/digest/time coordinates. Existing processor handles ACCEPTED/CONFLICTED changes, local invalidation, recovery and cursor gates. Added `canonical_reader.py`: read-only Bronze/Silver sessions, whole-event keyset pages, source high-water/binding barriers, validity/CH reconciliation, typed errors, token and bounded full-scan restart. Offline reader test passes; real CH and complete error/fault acceptance remain. | Verify reader against real CH, complete typed error and concurrent invalidation tests, and integrate Gold-shaped fixture. |
| P3-06 | IN_PROGRESS | Existing `replay.py` now runs synchronous V1 journal replay with frozen Bronze/archive boundary, isolated SQLite/lock and replay-only CH targets. Out-of-run sequences get durable local skips; changed boundary marks ID ABORTED/non-resumable. Accepted-event fixture verifies run filtering, facts/dimensions, identical logical hash across replay IDs (processing timestamps excluded), lost-insert-response resume and live SQLite isolation. Real CH replay, broader crash matrix and source-boundary race evidence remain. | Run real CH replay and fault injection for source advance, partial writes and live/replay isolation. |
| P3-07 | IN_PROGRESS | Added 008-only migration selector and `de-migrate-v1-silver` Compose service. Existing `de-silver-processor` uses profile `v1-silver`, V1 dependencies and RO Bronze/archive plus RW Silver mounts; `de.silver.main` starts the V1 journal worker. Startup verifies all 20 V1 main/replay tables plus exact column/type inventory from migration 008. Offline startup/schema fixture and Compose render pass. Docker daemon remains unavailable; real migration/readiness/runtime acceptance pending. | Verify full Compose graph, migration and readiness on real Docker/CH; retire remaining V2-only active imports/tests. |
| P3-08 | IN_PROGRESS | Real CH lifecycle/recovery/replay/migration subcases have recorded evidence; fresh five-node 120-cycle SUMO run reached Outbox/Raw/Bronze with 600 verified events. A01–A33 are not yet fully accepted. | Complete fresh-run Silver reconciliation/reader scan and audit all acceptance assertions against evidence. |

## Changed implementation files

- `de/bronze/clickhouse_repository.py`
- `de/silver/source_binding.py`
- `de/tests/bronze/test_v1_point_read.py`
- `de/tests/silver/test_source_binding_v1.py`
- `de/silver/archive_resolver.py`
- `de/silver/lifecycle_refresher.py`
- `de/tests/silver/test_archive_lifecycle_v1.py`
- `de/migrations/008_create_silver_v1.sql`
- `de/scripts/migrate_clickhouse.py`
- `de/tests/silver/test_migration_008_v1.py`
- `de/silver/fact_builders.py`
- `de/tests/silver/test_v1_fact_builder.py`
- `de/silver/dimension_builders.py`
- `de/silver/checkpoint_store.py`
- `de/tests/silver/test_v1_obligation_checkpoint.py`
- `docker-compose.yml`
- `de/silver/repositories.py`
- `de/silver/processor.py`
- `de/tests/silver/test_v1_repository_targets.py`
- `de/tests/silver/test_v1_recovery.py`
- `de/silver/config.py`
- `de/bronze/checkpoint_store.py`
- `de/tests/silver/test_v1_journal_worker.py`
- `de/tests/silver/test_v1_runtime_startup.py`
- `de/silver/canonical_reader.py`
- `de/tests/silver/test_canonical_reader_v1.py`
- `de/silver/replay.py`
- `de/tests/silver/test_replay_manifest.py`

## Executed check

`python -m pytest de/tests/bronze/test_v1_point_read.py de/tests/bronze/test_v1_downstream_reader.py de/tests/bronze/test_v1_output_reconciliation.py de/tests/silver/test_source_binding_v1.py -q --junitxml=de/artifacts/silver_v1/acceptance/p3-01/focused-junit.xml`

Result: **7 passed**. These focused tests do not certify real ClickHouse, source retention operations, Silver runtime, migration, Compose, or end-to-end acceptance.

P3-02 commands executed:

- `python -m pytest de/tests/silver/test_archive_lifecycle_v1.py -q` — **2 passed** after fixing a Windows fixture file-handle issue (first run: 1 failed, 1 passed).
- `python -c "...ArchiveResolver(...).resolve(...); LifecycleRefresher(...).poll()..."` against `Visualize/artifacts/runs/phase1-raw-20261119-smoke01` — resolved 5 archived participants, initial projection 1, repeated projection 0. The run's manifest is READY and this is component evidence, not a published-run runtime acceptance.

P3-01 focused regression rerun: `python -m pytest de/tests/bronze/test_v1_point_read.py de/tests/bronze/test_v1_downstream_reader.py de/tests/bronze/test_v1_output_reconciliation.py de/tests/silver/test_source_binding_v1.py -q` — **7 passed**.

Combined focused command: `python -m pytest de/tests/silver/test_archive_lifecycle_v1.py de/tests/bronze/test_v1_point_read.py de/tests/bronze/test_v1_downstream_reader.py de/tests/bronze/test_v1_output_reconciliation.py de/tests/silver/test_source_binding_v1.py -q` — **9 passed**. `git diff --check` passed.

Continuation: `python -m pytest de/tests/silver/test_migration_008_v1.py de/tests/silver/test_archive_lifecycle_v1.py -q` — **3 passed**. The test checks additive SQL inventory and selector routing, not ClickHouse DDL execution. The archived context validator was corrected to use the actual V1 event schema, which has no `runGeneration` field; generation comes from verified manifest/registry binding.

P3-03/P3-04 focused command: `python -m pytest de/tests/silver/test_v1_fact_builder.py de/tests/silver/test_migration_008_v1.py -q` — **2 passed**. This is offline builder/schema inventory evidence only.

`docker compose ps --format json` failed: Docker Desktop Linux engine named pipe was absent. Real ClickHouse migration/integration and SUMO acceptance remain unexecuted. Independent implementation continues.

`python -m pytest de/tests/silver/test_archive_lifecycle_v1.py de/tests/silver/test_v1_fact_builder.py de/tests/silver/test_migration_008_v1.py -q` — **4 passed** after adding archive-derived dimensions.

`python -m pytest de/tests/silver/test_v1_obligation_checkpoint.py -q` — **1 passed**. This verifies that the V1 cursor cannot advance with absent or unresolved frozen obligations; it does not certify crash recovery or ClickHouse reconciliation.

Goal continuation: `python -m pytest de/tests/silver/test_archive_lifecycle_v1.py de/tests/silver/test_v1_fact_builder.py de/tests/silver/test_migration_008_v1.py de/tests/silver/test_v1_obligation_checkpoint.py de/tests/bronze/test_v1_point_read.py de/tests/bronze/test_v1_downstream_reader.py de/tests/bronze/test_v1_output_reconciliation.py de/tests/silver/test_source_binding_v1.py -q --junitxml=de/artifacts/silver_v1/acceptance/continuation-focused-junit.xml` — **12 passed**. Lifecycle restart and invalidation outbox/CAS assertions are included. `docker compose --profile v1-silver config --services` succeeded and listed `de-migrate-v1-silver`; this is rendered configuration only.

`python -m pytest de/tests/bronze/test_v1_point_read.py -q` — **2 passed** after adding the concurrent transition test. This uses a fake CH client to force a committed SQLite transition during the external query; real CH integration remains pending.

Combined focused rerun with the eight test files above, omitting `--junitxml`: **13 passed**; `git diff --check` passed. The earlier JUnit XML predates the added race test and records 12 tests.

Next goal continuation: `python -m pytest de/tests/silver/test_v1_recovery.py de/tests/silver/test_v1_repository_targets.py -q` — **2 passed**. The fake CH client covers a lost insert response and static main/replay target isolation; actual ClickHouse reconciliation and worker activation remain unverified.

Combined focused command across the ten Silver/Bronze V1 test files listed above with `--junitxml=de/artifacts/silver_v1/acceptance/continuation-focused-junit.xml` — **15 passed**, 0 failed, 0 skipped. This XML supersedes the earlier 12-test file at the same path.

Journal/runtime continuation: `python -m pytest de/tests/silver/test_v1_journal_worker.py de/tests/silver/test_v1_obligation_checkpoint.py -q` — **5 passed**; `python -m pytest de/tests/silver/test_v1_runtime_startup.py -q` — **1 passed**. `docker compose --profile v1-silver config --services` succeeds and lists `de-silver-processor`. These tests use fake CH and do not establish real runtime acceptance.

Latest combined command across the twelve focused V1 files (including `test_v1_journal_worker.py` and `test_v1_runtime_startup.py`) with `--junitxml=de/artifacts/silver_v1/acceptance/continuation-focused-junit.xml` — **20 passed**, 0 failed, 0 skipped. `git diff --check` passed. `docker compose ps --format json` still fails because the Docker Desktop Linux engine named pipe is absent; actual CH migration/runtime acceptance remains pending. Repository V1 inserts now convert ISO timestamps to Python datetimes before ClickHouse `DateTime64` writes.

Canonical reader continuation: `python -m pytest de/tests/silver/test_archive_lifecycle_v1.py de/tests/silver/test_v1_fact_builder.py de/tests/silver/test_migration_008_v1.py de/tests/silver/test_v1_obligation_checkpoint.py de/tests/silver/test_v1_repository_targets.py de/tests/silver/test_v1_recovery.py de/tests/silver/test_v1_journal_worker.py de/tests/silver/test_v1_runtime_startup.py de/tests/silver/test_canonical_reader_v1.py de/tests/bronze/test_v1_point_read.py de/tests/bronze/test_v1_downstream_reader.py de/tests/bronze/test_v1_output_reconciliation.py de/tests/silver/test_source_binding_v1.py -q --junitxml=de/artifacts/silver_v1/acceptance/continuation-focused-junit.xml` — **21 passed**, 0 failed, 0 skipped. Reader test uses a real read-only SQLite authority and fake CH rows; no real CH acceptance is claimed.

Subsequent schema gate and validity mirror update: the same 13-file command again — **21 passed**, 0 failed, 0 skipped. Schema fixture now checks rejection of a changed ClickHouse column type. Accepted bundles include a frozen `silver_event_validity_v1` mirror row; Silver SQLite remains the immediate visibility authority.

Replay boundary start: `python -m pytest de/tests/silver/test_replay_manifest.py -q` — **1 passed**. V1 manifest validation binds source path/schema/namespace, both allocator high-waters, run/archive fingerprints and versions. The replay CLI currently refuses execution rather than invoking the obsolete V2 offset path; P3-06 is not implemented or accepted.

Combined focused rerun with the preceding 13 V1 files plus `test_replay_manifest.py`, JUnit XML at `de/artifacts/silver_v1/acceptance/continuation-focused-junit.xml`: **22 passed**, 0 failed, 0 skipped. `git diff --check` passed. Real CH/Docker and A01–A33 remain unexecuted.

V1 replay continuation: the same 14-file focused command with `--junitxml=de/artifacts/silver_v1/acceptance/continuation-focused-junit.xml` — **24 passed**, 0 failed, 0 skipped; `git diff --check` passed. Tests cover exact source-boundary rejection, isolated empty replay/idempotent resume, non-resumable abort and out-of-run skip, using fake CH. Real ClickHouse replay and live/replay isolation remain unverified.

Accepted-event replay fixture: `python -m pytest de/tests/silver/test_replay_manifest.py -q` — **3 passed**. It uses real Bronze SQLite and archived run files with fake CH to verify target-run output cardinality, unrelated-run skip, isolated state, deterministic logical hash across replay IDs and recovery from a lost insert response. It does not certify physical CH isolation.

Git tracking: `.gitignore:50` ignores `docs/**`, including this checkpoint. The smallest safe tracking action is an explicit `git add -f docs/DEv1/phase-3/implementation-status.md` when staging is authorized; leave broad artifact ignore rules intact. `de/artifacts/` also remains ignored for runtime evidence. No files were staged, committed or pushed.

## Remaining dependencies

Operational no-prune attestation and a real ClickHouse point-read integration remain required before P3-01 verification or materialization. Phase 3 completion also requires the untouched P3-02–P3-08 work and actual A01–A33 evidence.


### Runtime integration of subset/token fixes — 2026-11-22 07:36 Bangkok

Docker Desktop restarted successfully after daemon disappearance. Built Silver image manifest `06b6c2bb8e5e891e2ea07822ff37b1468401f16bffe7a857d85e8006d737fd76` and recreated only the existing `de-silver-processor` service via `docker compose up -d --no-deps`. Worker/reader/feed container group **16 passed**, XML `subset-reader-container-junit.xml`. This image includes previous replay preflight fixes.

Post-restart actual snapshot at 00:36:19 UTC: source 2200/0, retained cursor 1261/0, RECOVERING, worker alive, schema/SQLite/CH/lifecycle checks true, no fault. The last pre-shutdown durable cursor advanced to 1261; the service has resumed existing state. Fresh A01 oracle explicitly reports 0 current candidates against 600 accepted source events and run_fully_materialized=false. This negative completeness evidence is preserved in `acceptance/oracle-phase3-silver-20261122-a01.json`; it is not a pass. Continue independent acceptance work while the worker processes earlier backlog, then rerun full A01 oracle and canonical complete scan.

## Actual ClickHouse quality round trip — 2026-11-22

Added opt-in `test_real_quality_v1.py`, writing two synthetic contract-valid events only to unique replay namespaces/tables, then SELECTing all three fact families back. Actual container execution **1 passed**, XML `real-quality-junit.xml`. Reports `acceptance/p3-quality-*.json` preserve queried rows. Initial verifier run used an incorrect replay table suffix and failed; corrected to the migration's `_replay_v1` names before the passing runs. Retained fixture data is isolated from main tables.

Assertions cover all nine core metric values including measured zero; direction-specific vehicle_count NULL+UNAVAILABLE and NULL+NOT_APPLICABLE; exact optional direction JSON/presence; quality status/flags; partial total NULL versus complete zero sum; absent top-level state versus explicit false; absent signal timing/remaining versus explicit ACTUATED/zero with JSON presence preserved; scenario per event and exact 1/3/3 cardinality for both events. Events are submitted cycle9 then cycle3, but this builder/repository fixture does not certify the separate A19 journal/cursor requirement. It strengthens A10–A17 evidence without claiming full worker/source acceptance. Synthetic VerifiedRunContext is used here; real archive resolution is covered separately. The new test was copied into the running container; include it on the next image rebuild.

Latest observed actual live cursor at 00:36:58 UTC was 1269/2200 with worker alive and no fault, proving post-restart advancement from1261. Source highwater remains2200 and A01 awaits its journal position. Goal active and full Phase3 certification incomplete.

Independent SQLite connection CAS test added: both stores share a checkpoint and the second observes the old generation; after the first advances, the second is refused without changing either cursor or generation. Obligation/checkpoint group **7 passed**, XML `two-connection-cas-junit.xml`. This proves stale-generation protection across separate open connections; it does not claim simultaneous thread contention coverage. git diff --check passed.

## Late cycles and topology rejection through worker — 2026-11-22

Accepted-change worker test now materializes cycles4,9,2 at journal sequences1,2,3, asserting durable cursor progression, all three VALID event identities, and all three intersection facts. It exercises worker freeze/reconciliation/finalization with real Silver SQLite and a deterministic repository/point-read double. Separate parameter cases for missing direction, unexpected direction, digest mismatch and nonparticipant assert zero repository rows, zero validity, zero obligations, unchanged cursor0 and no lifecycle registration. ArchiveIntegrityError now includes exact sorted missing/unexpected direction sets, asserted by tests. Archive+worker group **24 passed**, XML `topology-late-worker-junit.xml`; git diff --check passed. This adds focused A05/A06/A07/A19 worker evidence; it does not replace actual source-owner or real-CH race acceptance. Diagnostic change and test updates require the next image build.

Latest actual snapshot 00:41:21 UTC: cursor1311/2200, READY, no fault, lifecycle current; sustained01 now600/600 VALID, long01 511VALID+1PENDING. Docker stats point observation:99.79MiB,8.04%CPU. A01 still awaits its later journal entries; no whole-run or capacity claim inferred.

## Retired V2 executable processor path — 2026-11-22

Audited references and removed V2 processor `_run`, process_stream_once, stream recovery/dependency polling, ledger/fact identity helpers, their imports, and constructor BronzeReader/TransformationEngine defaults/injection arguments. The official existing Silver class/entrypoint now owns V1 journal execution only. Retained historical pure helpers/model/repository contracts are not wired to executable Silver. `test_processor_batch.py` retains historical assertions but explicitly skips the retired V2 processor API; replacement V1 startup/journal/recovery/health coverage is named in the skip and handover. No V2 database object/data changed.

Local startup/journal/recovery/replay group **34 passed**, XML `retire-v2-worker-junit.xml`. Rebuilt image manifest `9ea7a6aad7a2257993ef4eb3e5e321632ae3c02958cd9a89541cc0a13b209345`, recreated existing service only, then container startup/journal/recovery/health/real-quality group **29 passed,1 historical module skipped**, XML `retire-v2-container-junit.xml`. Image includes topology diagnostic and latest tests. Snapshot00:43:50UTC: cursor1334/2200, RECOVERING after restart, worker alive and all dependencies/lifecycle true, no fault. Source2200/0 unchanged; backlog persists and A01 remains incomplete.

## Real owner API lineage and conflict feed — 2026-11-22

New `test_real_lineage_v1.py` creates a unique retained CH database with006/007/008, dedicated Bronze/Silver SQLite and an archive/registry fixture. Calls actual BronzeProcessor.materialize_v1_delivery with contract-format hashed raw IDs and actual CH outputs, then actual Silver run_v1_journal_pass using the RO Bronze reader/exact point-read and real CH repositories. R1 ACCEPTED_NEW materializes facts. R2 SEMANTIC_DUPLICATE leaves the full verified source boundary and exact sorted physical fact content/counts (including processing timestamps and primary R1 provenance) unchanged; no journal entries processed; read_delivery_lineage returns both R1/R2. R3 same identity/different payload becomes SEMANTIC_CONFLICT; Silver consumes invalidation+conflicted change, records local INVALIDATED, preserves original physical facts, and public SQLite-only canonical invalidation feed returns exactly sequence1 for that event.

Actual container test **1 passed**, XML `real-lineage-junit.xml`; report `acceptance/p3_lineage_*/report.json`. Earlier fixture/verifier errors (raw ID width, disposition name, CH unordered rows, list return shape) were fixed before passing; isolated failed-attempt DBs retained. No main/live source mutation. This supplies actual owner/API evidence for benign lineage and conflict-feed contracts; replay-manifest stability during benign growth and broader canonical scan/race acceptance remain separately required. New test copied into running container; include next rebuild. git diff --check checked.

## Real replay stability and canonical physical-content contract — 2026-11-22

Extended actual owner/API fixture to two events and real synchronous replay. Freeze manifest before R2; replay COMPLETE before benign delivery; same replay ID resumes idempotently after R2 with unchanged logical hash; a second replay ID rebuilt from the same frozen boundary after R2 yields the identical logical hash. Full source boundary remains unchanged by duplicate lineage. Registry timestamps in fixture now use real ISO instants for CH lifecycle projection (initial replay verifier failed on earlier fake t1 timestamp, corrected before passing).

Canonical reader against actual Bronze/Silver CH and RO SQLite completes two pages at page_size1 in stable event-ID order, then after one source conflict excludes only that event while retaining the other. Added identical physical traffic rows: reader collapses them to exact archived direction count. Added differing vehicle_count under the same physical logical key: reader raises CanonicalContentConflict and returns no page. Isolated corrupt fixture data is retained for evidence; main/runtime data never changed. Actual extended test **1 passed**, XML `real-lineage-replay-reader-junit.xml`; per-attempt retained database/state/report in `acceptance/p3_lineage_*/`. Current new test copied into container; next rebuild should include it. Supplies actual A18 replay-lineage and A21 content/pagination evidence; full concurrent page-boundary race and A01 runtime completeness are still required. git diff --check passed.

## Real canonical boundary race, readonly account and preconsumption conflict — 2026-11-22

Extended owner/CH fixture now commits R3 SEMANTIC_CONFLICT during canonical physical row fetching; reader returns SnapshotStaleError and no page. Then worker consumes the invalidation and canonical scan excludes only conflicted event. SELECT-only unique CH user is granted SELECT solely on fixture database with readonly=1; actual CanonicalReadSession completes two-page scan with that user and INSERT...SELECT WHERE0 is denied. Random fixture credentials are not written into evidence. This proves reader compatibility with enforced read credentials; production consumers must still configure SELECT-only accounts.

Second fixture branch creates source conflict before Silver consumes any ACCEPTED entries. Real worker processes one invalidation plus three change entries; conflicted event has zero physical facts across all3 families, local INVALIDATED, while other event materializes. Applied cursors changes3/invalidations1. Actual suite **2 passed**, XML `real-canonical-race-junit.xml`, reports retained under acceptance/p3_lineage_*/report.json. No main/source runtime data changed. New test copied into container; pending next image rebuild.

Latest live snapshot00:50:15UTC: READY, cursor1403/2200, no fault/lifecycle current; long01 603VALID+1PENDING, sustained01 600VALID, smoke02 200VALID. A01 still behind earlier entries. Goal active and full completion remains unproven. git diff --check passed.

## Replay conflict during materialization and completion — 2026-11-22

Real fixture injects a second source event conflict either immediately after a replay CH insert (mid-batch) or after journal processing before completion verification. Replay now rechecks frozen source boundary for otherwise-generic processing exceptions: a proven boundary change persists ABORTED and raises SOURCE_BOUNDARY_CHANGED; authority unavailable does not establish a permanent transition. Existing typed archive/source handling remains. Actual owner/CH integration **4 passed**, XML `real-replay-conflict-abort-junit.xml` (two preconsumption cases plus full fixture at each injection stage); local replay/recovery regression **13 passed**, XML `replay-midbatch-regression-junit.xml`.

The abort states are inspected directly from replay SQLite and reports retain source IDs and stage under acceptance/p3_lineage_*/report.json. Data and users remain fixture-isolated; production source/live state unchanged. Updated replay module and test copied into running container for execution; next image rebuild must include them. Full goal still active; A01 convergence and remaining acceptance mapping/exit checks are pending. git diff --check passed.

## Concurrent CAS, bounded append and integrated current image — 2026-11-22

Added actual simultaneous two-thread/two-SQLite-connection cursor CAS test using a barrier after both observed generation0: exactly one winner and one stale rejection, final generation1/cursor1. Added writer append after frozen highwater1 with actual Bronze semantic owner store and RO reader: frozen page contains only event1, after-cursor next pass contains event2. Source/obligation group **15 passed**, XML `concurrent-cas-fixed-boundary-junit.xml`.

Rebuilt image manifest `37d46d3279cc6336005bc6fac3d5620b1290274a6861ccb099e9fc105723ed69` and recreated only existing Silver service. This image includes replay generic-error boundary recheck, all latest owner/CH tests and concurrent-CAS/fixed-append tests. Integrated actual container group **69 passed**, XML `integrated-v1-acceptance-junit.xml`: source_binding, obligations, recovery, startup, journal, replay_manifest, canonical_reader, invalidation_feed, real_lineage (4 cases), real_quality, real_invalidation_recovery, real_lifecycle and health_api. This is a scoped integrated regression, not A01–A33 certification.

Latest pre-rebuild actual snapshot00:54:37UTC: cursor1452/2200, READY/no fault/lifecycle current, long01 652VALID+1PENDING. Goal remains active. Next: current acceptance assertion inventory, operational no-prune verification record, remaining canonical token/lag-only boundaries, actual A01 full reconciliation and canonical scan after convergence, final package/handover/exit evidence. Historical checkpoint paragraphs above are chronological and must not be treated as current completion claims.

## Reader independent cursor gates and acceptance index — 2026-11-22

Reader test explicitly covers change-only lag, invalidation-only lag, both lag and both current. Error cases call the page API with no fact repository/SQLite attached, proving typed retryable SOURCE_LAG/INVALIDATION_LAG before fact access; current case verifies boundary success. Reader group **9 passed**, XML reader-independent-cursor-gates-junit.xml. This focused boundary stub test complements actual CH/source race tests and does not claim a physical invalidation-only source transition (Bronze conflict also emits a change).

Added current execution evidence index to acceptance.md, mapping A01–A33 to scoped evidence and remaining assertion-level exit checks. It explicitly preserves incomplete A01 and the operational no-prune/remaining crash/isolation audit requirements. No final certification claimed. Test-only change needs next image rebuild; runtime code unchanged this turn. git diff --check passed.

## Archive file failures and retained-source binding evidence — 2026-11-22

Manifest/catalog missing files, malformed JSON and non-object roots now have explicit fixture cases. Missing file consistently raises retryable ArchiveUnavailable, malformed/nonobject consistently ArchiveIntegrityError; same historical resolver path is retried. Archive/lifecycle group **16 passed**, XML `archive-file-failure-junit.xml`.

New read-only `inspect_silver_retention_v1.py` captures full source verification and stable before/after boundary, WAL mode, journal counts/min/max, owner/verifier code hashes and explicit journal-pruning statement inventory. Actual report at00:59:12UTC: changes count2200/min1/max2200, invalidations empty/H0, exact schema binding7e1f77c6..., no explicit V1 journal DELETE/DROP/TRUNCATE in inspected owner files. Actual Docker mount inventory retained separately. Handover now states the operational preserve-authority/no-prune/no-rewind rule and limitations: cannot attest to external schedulers or prevent privileged out-of-band mutations. These are scoped evidence, not a claim of impossible external protection. Script copied into running container; tests/script pending next image build. Full goal still active. git diff --check passed.

## Actual abrupt subprocess recovery at durable boundaries — 2026-11-22

Added test-only crash_worker_v1 harness using os._exit(73), bypassing finally/close/flush. Parent fixture uses actual Bronze owner/API, historical archives, real CH and dedicated SQLite in a retained isolated database. Crash stages: immediately after full bundle freeze, after first CH insert, after one traffic fact insert (partial direction bundle), after SQLite acknowledgement, and after atomic finalization. Parent reopens durable state: cursor0/PENDING before finalization, cursor1/VALID after it, with obligation-status inventory recorded. Parent resumes actual journal pass and validates complete canonical bundles, exact primary provenance, stable duplicate/replay hashes, invalidation and content-conflict handling.

Actual all-stage suite **5 passed**, XML `real-abrupt-crash-junit.xml`; focused partial-fact prior check **1 passed**, XML `real-partial-fact-crash-junit.xml`. Each retained acceptance/p3_lineage_*/report.json includes exit73, stage, post-crash local snapshot and recovered cursor2. The test harness is not a production Silver entrypoint. No live worker termination/source mutation. New tests copied into running container; next image rebuild should include them.

Latest actual live snapshot01:02:39UTC: cursor1535/2200, READY/no fault/lifecycle current; long01 735VALID+1PENDING, sustained01 600, smoke02 200. Source stays2200/0. Fresh A01 begins after earlier1600 entries and remains pending; full Phase3 goal active. git diff --check passed.

## Whole-run A01 fail-closed verifier — 2026-11-22

Added read-only certify_silver_run_v1.py: full archived expected event-ID set and source ACCEPTED/population check, full Silver VALID-set check, independent existing source/fact oracle, canonical complete scan at100 bundles/page, exact logical counts and stable source boundary. Hash-links and validates recorded source-oracle archive identity/generation/digest/counts; recorded source evidence timestamp is explicit, not rerun. Exceptions/mismatches/incomplete state exit1 and save INCOMPLETE. PASS scope is only this run, not all Phase3.

Actual execution at01:04:13UTC returned INCOMPLETE: source600/expected600, Silver0, source2200/0; reason SILVER_POPULATION_INCOMPLETE. Saved acceptance/phase3-silver-20261122-a01/silver-certification.json. This verifies current backlog rejection, not the positive certification branch (awaits actual convergence). Script copied into running container before final additional recorded-oracle checks; next build/copy required for latest version. Goal active; no completion claimed. git diff --check passed.

## Measured backlog scan bottleneck fixed — 2026-11-22

Measured live pending obligation query inside container:2.637050s for11 rows, plan used namespace-only primary index and scanned reconciled history. Added additive partial SQLite index silver_v1_pending_obligation_idx(namespace,source_kind,source_sequence,target_table) WHERE status!='RECONCILED'. It includes FAULT and PENDING and leaves reconciliation/finalization/durability checks intact. Post-deploy same query0.002115s for13 rows with new index. Artifact acceptance/backlog-index-measurement.json records both measurements and limited scope (single-query samples, not capacity claim).

Host pytest attempt failed in fixture setup with WinError5 on Temp; not passed. Container obligation/recovery/journal group **27 passed**, XML backlog-index-junit.xml. Rebuilt/deployed image manifest111716038b05de6db8884d1e9244d9f1379904527faa7bbc6429985e8a2c0158; includes all latest scripts/crash tests. Snapshot01:07:05UTC: cursor1612/2200, READY/no fault/current lifecycle; all earlier1600 events VALID (long800,sustained600,smoke200); freshA01 now12VALID+1PENDING and advancing. Independent oracle sustained01 was terminated by recreate (exit1/no report) and restarted explicitly under exec session11593; inspect that handle before rerunning. Goal active; A01 whole-run certification pending.

## Actual replay/live inventory and official Compose command — 2026-11-22

Real owner/CH fixture now snapshots every main Silver V1 table's sorted full-row hash/count plus all main SQLite silver_* table hashes/counts before replay. After completed replay, idempotent resume and second replay ID, entire main inventory is equal. Actual scoped case **1 passed**, XML real-replay-live-inventory-junit.xml; before/after inventories persisted in fixture report. This closes the explicit real main-state isolation inventory gap (separate from physical path/lock tests).

Existing sustained01 independent source/fact oracle finished exit0:600 candidates/600 verified/600 accepted, run_fully_materialized=true, errors[]. Saved acceptance/oracle-phase1-raw-20261119-sustained01.json. This is an additional real run; fresh A01 remains the mandatory600/4680 gate.

Executed exact planned command `docker compose --profile v1-raw --profile v1-bronze --profile v1-silver up -d de-kafka-raw-v1 de-bronze-v1 de-silver-processor`, completed exit0. Artifacts compose-runtime-inventory.jsonl and migration-runtime.log:006/007 and008 services exit0, Silver/BronzeV1 healthy, RawV1 started. Historical V2 Bronze/Raw containers remain stopped(exit137); no legacy service was started by this command. Existing orphan warning retained without cleanup. Compose contract **4 passed**, XML compose-current-junit.xml. Actual endpoint/checkpoint/instance-lock/namespace tests **8 passed**, XML runtime-isolation-current-junit.xml. Opted-in endpoint failures now fail instead of skip. Test changes pending next image rebuild. Goal active; final A01 convergence/canonical run gate and assertion-level exit audit remain. git diff --check passed.

## Multiple archive generations and current package checkpoint — 2026-11-22

Added fixture retaining two distinct run IDs/generations/digests in one registry/archive root. Both old/new historical contexts resolve exact catalog and manifest hashes, bindings differ, cross-digest lookup fails, and lifecycle polls both known runs once then no-op. Archive/lifecycle **17 passed**, XML archive-multigeneration-junit.xml. Initial insertion misplaced an existing retry assertion; corrected before passing. Test-only update pending next image rebuild.

A01 preflight01:13:14UTC now expected600/source600/VALID370, still INCOMPLETE(SILVER_POPULATION_INCOMPLETE). Source boundary remains2200/0; no complete scan/oracle PASS yet. Resource point sample66.82MiB/28.51%CPU, not sustained capacity evidence.

Current package implementation status (supersedes older chronological package table; verification is scoped to cited evidence, final assertion audit pending):

| Package | Status | Remaining exit check |
|---|---|---|
| P3-01 | IMPLEMENTED | Consolidate source binding/retention/interface evidence into final assertion audit. |
| P3-02 | IMPLEMENTED | Map multi-generation/archive/lifecycle assertions to final evidence. |
| P3-03 | IMPLEMENTED | A01 independent full-run counts/values and final migration inventory references. |
| P3-04 | IMPLEMENTED | A01 full-run availability/identity/lineage oracle. |
| P3-05 | IMPLEMENTED | A01 canonical full scan plus final token/error/feed assertion inventory. |
| P3-06 | IMPLEMENTED | Consolidate crash/replay/isolation acceptance evidence and full assertions. |
| P3-07 | IMPLEMENTED | Final current-image/source fingerprint and runtime health/mount evidence. |
| P3-08 | IN_PROGRESS | A01 finish, all A01–A33 assertion audit, final verdict/handover. |

No external blocker identified; live worker is advancing. Goal remains active. git diff --check passed.

## Wide regression, runbook migration and A01 convergence — 2026-11-22

Ran entire Silver test directory in Linux container. Initial263pass/32skip/8fail: missing Compose/docs read fixtures. After providing repo fixtures269pass/32skip/2fail exposed obsolete docs/siliver plan references absent in actual host repo. Updated those doc assertions to current Phase3 plan/decisions/acceptance/handover (no weakened checks). Shared SILVER_RUNBOOK.md now begins with actual V1 profile command,008 prerequisites,source paths,outage/recovery,replay and forbidden-operation guidance; previous V2 text retained explicitly as historical archive.

Final entire directory: **272 passed,32 skipped,0 failed**, XML silver-wide-final-regression-junit.xml. Required repo Compose/docs were copied into test container; production Dockerfile intentionally contains runtime sources rather than all repository docs. Saved skip classification acceptance/wide-regression-skips.json:26 opt-in actual cases each matched to separate passing JUnit records;5 historical V2 evidence-format checks superseded by V1 acceptance;1 retired V2 executable batch module. No opt-in case missing separate passing XML. New docs/test changes pending next image rebuild, currently copied into test container.

A01 preflight01:15:16UTC had472VALID. Bounded live-worker observer session43640 then confirmed600/600VALID and launched the actual whole-run oracle+canonical verifier; it is still running under that handle with no result yet. Do not restart it merely because there is no intermediate output. Next action: poll session43640; inspect silver-certification.json only after terminal success/failure, then continue assertion-level audit and final runtime/source evidence. Goal active; no whole-run/Phase3 PASS claimed before result. git diff --check passed.

## Reader final token/filter/key assertions — 2026-11-22

Expanded reader contract test to reject changed page size, changed start/end time range and changed exclusive event key while reusing a token, and explicitly accept limit100 returning whole bundles. Reader group **9 passed**, XML reader-filter-key-size-junit.xml; no runtime code change. Positive actual A01 whole-run verifier still running under session43640; last poll confirms session ongoing with no terminal output. Existing certification JSON is the prior INCOMPLETE preflight and must not be interpreted as current process result. Do not restart the running verifier on silence. Remaining immediate sequence: obtain A01 terminal outcome, fix any mismatch, inspect canonical counts/source boundary/hash link, then finalize assertion/package/handover evidence and goal status. git diff --check passed.

## Final source-audit status and replay identity hash check — 2026-11-22

Added current-status notes to audit/blockers/decisions while retaining the preimplementation baseline and approved decisions. Missing point-read is now explicitly resolved; source-retention rule/evidence and external-mutation limitation remain documented. No external blocker.

Explicit logical replay hash test verifies timestamp/replayID changes do not affect hash, while runID,generation,digest,manifest identity and domain value each change it. Targeted test **1 passed,9 deselected**, XML replay-hash-identity-junit.xml. Existing real benign-lineage replay verifies repeated hash stability independently. Latest test-only changes pending next image rebuild.

A01 verifier session43640 remains running at last poll with no terminal output; its prior INCOMPLETE JSON is stale preflight output. Continue polling same handle. No final Phase3 verdict or package VERIFIED transition is justified until whole-run result and remaining assertion-level closure are inspected. git diff --check passed.


## Final runtime acceptance closure — 2026-11-22

Observer/verifier session43640 terminated exit0. Actual A01 report PASS:600expected/source/VALID,600independent fact-oracle verifications/errors[], canonical600intersection+2040traffic+2040signal=4680, stable source2200/0. Hash-linked recorded producer/Outbox/Raw/Bronze report is explicit. Final snapshot01:24:54UTC READY, cursor2200/0, both lags0, all25960obligations RECONCILED, lifecycle current, no fault/quarantine. Runtime code parity30/30files, no differences.

Assertion-level closure A01–A33 is recorded in acceptance-result.md; A34 optional. P3-01,P3-02,P3-03,P3-04,P3-05,P3-06,P3-07,P3-08 are VERIFIED against that scoped evidence and documented operating contract. Prior chronological IN_PROGRESS/INCOMPLETE notes are superseded, not erased. Wide regression272pass/32skip/0fail; all26opt-in skip cases have separate passing XML, historical5evidence checks and1retiredV2module are classified. Actual crash/replay/migration/lifecycle/quality/canonical/source/isolation cases and current600-event whole-run acceptance all passed.

No external blocker or required implementation remains. Runtime and source evidence stay in acceptance/, historicalV2data preserved, no commit/push/reset/clean. Operating limitations: preserve bound authority/journals (external privileged mutation is outside runtime guarantee), consumers use SELECT-only credentials/canonicalAPI, no arbitrary historical-as-of or capacity SLA, Phase4 code out of scope. Final source/worktree provenance is refreshed after documentation closure.

Verdict: PHASE 3 COMPLETE — RUNTIME ACCEPTANCE PASS.
