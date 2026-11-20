# Phase 2 acceptance — final certification and historical checkpoints

## Final certification — 2026-11-20

**Verdict: PHASE 2 COMPLETE — READY FOR PHASE 3 PLANNING.** All 31 mandatory Definition of Done gates below have passing implementation and execution evidence. The checkpoint sections following this final matrix are retained as historical records and are superseded by this certification.

Final verification commands and results:

- `python -m pytest contracts/tests de/tests/kafka_raw de/tests/bronze -q --basetemp de/artifacts/bronze_v1/acceptance/final-regression-temp --junitxml=de/artifacts/bronze_v1/acceptance/final-regression.xml` — **165 passed, 0 failed, 0 skipped, 18.42 seconds**.
- `$env:BRONZE_V1_ACCEPTANCE='1'; python -m pytest de/tests/integration/test_bronze_v1_acceptance.py -q --basetemp de/artifacts/bronze_v1/acceptance/final-runtime-temp-2 --junitxml=de/artifacts/bronze_v1/acceptance/final-runtime-certified.xml` — **14 passed, 0 failed, 0 skipped, 332.94 seconds** against rebuilt Raw/Bronze images and the isolated Kafka/ClickHouse stack.
- `python -m de.tools.bronze_sustained_acceptance` — **PASS** on the final acceptance image: `phase2-measured-ffbed382b2`, 120 simulation seconds, 600 observations, 120 cycles, five intersections, 14 periodic metrics/resource samples, 12.322-second catch-up, 42.259-second total. Outbox inventory verifies all 600 rows ACKED.
- Main Compose fresh streaming through `de-bronze-v1`: **100 Outbox ACKED = 100 Raw = 100 unique canonical Bronze observations**, 20 cycles, five intersections; all three checkpoints equal safe boundaries; zero pending intents/deliveries/reconciliation and zero integrity faults; idle READY. Evidence: `main-compose-fresh-stream.json`.
- Final read-only invalidation/provenance reconciliation: **8/8** SQLite invalidations match ClickHouse IDs, event IDs, original/conflicting Raw IDs, envelope hashes, and invalidation sequences. All 2,014 live observations have topology digest, nonnegative simulation time, and primary Raw lineage. Actual final schema was inspected with SQLite `PRAGMA table_info` and ClickHouse `DESCRIBE TABLE`; repeated additive migration/V2 sentinel preservation is covered by the final runtime suite.
- `git diff --check` — PASS. No application layer beyond Bronze was added; no commit, push, merge, reset, or destructive cleanup was performed.

Final acceptance matrix (each row is a mandatory gate; test references point to the final regression/runtime JUnit artifacts unless a more specific runtime artifact is named):

| Gate ID | Requirement | Executed verification | Actual result | Evidence artifact | Status |
|---:|---|---|---|---|---|
| 1 | Phase 1 certified baseline and source binding | Git branch/HEAD and implementation-status baseline review | `codex/phase-1-v1-raw`, `757060f8175fdb22d9ee27e7f8d175c82c0e2c45` | `implementation-status.md`; `final-runtime-certified.xml` | PASS |
| 2 | Preserve certified Raw implementation and behavior | Full Raw regression plus exact Raw schema/count comparison around migration | 165 regression tests pass; Raw DDL/data unchanged | `final-regression.xml`; `upgrade-preservation` case in `final-runtime-certified.xml` | PASS |
| 3 | Reuse-first Bronze refactor in place; no parallel production tree | Reviewed changed production paths and Bronze module inventory | Existing Bronze modules extended; no parallel `v1_*.py` Bronze module | Working-tree diff; `final-runtime-certified.xml` | PASS |
| 4 | Consume actual Phase 1 `logical_read` contract | Live and replay real Raw processing and source-boundary tests | Confirmed Raw delivery statuses feed Bronze; no Kafka duplicate consumer | `final-regression.xml`; `final-runtime-certified.xml` | PASS |
| 5 | Raw ledger/source strictly read-only | Separate-process `mode=ro`/`query_only` rejection and Compose `:ro` source mount | Writes rejected; main/acceptance mount read-only | `final-runtime-certified.xml`; `main-compose-fresh-stream.json` | PASS |
| 6 | Enforce safe boundaries and source identity | Bounded source-reader tests and real checkpoint/safe-offset comparison | All live partitions converge exactly to safe offsets | `final-regression.xml`; `main-compose-fresh-stream.json` | PASS |
| 7 | Valid Kafka offset gaps do not stall | Sparse/empty Raw range unit cases and delivered-order advancement | Sparse and empty ranges handled without integer-contiguous wait | `final-regression.xml` | PASS |
| 8 | Missing, unresolved, conflicting, or corrupt source evidence blocks advancement | Source fault tests plus fail-closed runtime persistence/replay checks | Faults fail closed; unsafe cursor advancement not accepted | `final-regression.xml`; `final-runtime-certified.xml` | PASS |
| 9 | Verify original bytes and transport/content hashes | Decode and reconcile every historical Raw payload against canonical output | Original bytes verified; all 700 prior and 100 main-run events reconciled | `full-reconciliation` case; `main-compose-fresh-stream.json` | PASS |
| 10 | Reuse shared V1 contract validator | Contract/codec regression and valid/invalid wire-event acceptance | Shared validator accepts supported events and rejects malformed contract | `final-regression.xml`; `final-runtime-certified.xml` | PASS |
| 11 | One logical valid observation per `eventId` | Full source/output reconciliation and semantic identity runtime assertions | Unique canonical grain verified; duplicate does not create another observation | `final-runtime-certified.xml`; `main-compose-fresh-stream.json` | PASS |
| 12 | Semantic duplicate disposition and lineage | Actual Kafka duplicate delivery test | Duplicate disposition retained with no second canonical row | `test_wire_duplicate_conflicts_and_monotonic_retry` in `final-runtime-certified.xml` | PASS |
| 13 | Persist complete physical delivery lineage/dispositions | Historical lineage counts plus invalidation-to-intent Raw ID reconciliation | Every checked delivery has completed intent; invalidation source deliveries resolve to intents | `full-reconciliation` case; final provenance verification | PASS |
| 14 | Changed immutable content creates conflict evidence | Actual payload and envelope metadata conflict delivery test | Conflicts stored with both identities/hashes and source IDs | `test_wire_duplicate_conflicts_and_monotonic_retry` | PASS |
| 15 | Exclude conflicted identities from canonical reader | Separate-process canonical scan after conflict | Conflicted IDs absent; missing/corrupt mirrors rejected | `test_canonical_and_readonly_interfaces_in_separate_process` | PASS |
| 16 | Conflict is monotonic across retries, restart, and replay | Late identical retry, worker restart, replay parity | Conflict remains monotonic; no valid resurrection | `final-runtime-certified.xml` | PASS |
| 17 | Durable invalidation/change stream with provenance | SQLite/ClickHouse row-by-row reconciliation of IDs, event, source Raw IDs, hashes and sequence | 8/8 invalidation mirrors match; journal and invalidation cursors readable | `final-runtime-certified.xml`; final provenance verification | PASS |
| 18 | Preserve zero, null, availability, absent optional fields and nested directions | Shared-contract actual Kafka→Raw→Bronze availability roundtrip | Measured zero, null + `UNAVAILABLE`, null + `NOT_APPLICABLE`, absent optional field and nested payload preserved exactly | `historical-availability.xml`; same case in `final-runtime-certified.xml` | PASS |
| 19 | Historical and out-of-order observations remain processable | Actual historical run with cycles published 9, 2, 5 and scenario changes | All three distinct event IDs accepted without freshness/current-run filtering | `historical-availability.xml`; `final-runtime-certified.xml` | PASS |
| 20 | Destination/content/namespace/identity/lineage-aware reconciliation | Lost response, partial outputs, finalization failure, conflict mirror outage, hash-lookup mismatch and canonical mirror faults | Frozen outputs reconciled by content and identity; retries recover | `final-runtime-certified.xml`; `final-regression.xml` | PASS |
| 21 | Monotonic checkpoint CAS and stale-generation protection | SQLite semantic-store CAS tests, source-bound runtime checkpoints, interrupted replay | CAS rejects invalid advancement and recovers monotonically | `final-regression.xml`; `final-runtime-certified.xml` | PASS |
| 22 | Crash/failure recovery preserves output, conflicts and invalidation | Five real persistence fault boundaries, pending-write ClickHouse outage, worker restart | All five recover; zero pending state after recovery; worker returns READY | `final-runtime-certified.xml` | PASS |
| 23 | Clean, upgrade and repeated migration | Fresh isolated Compose startup; migration 007 applied repeatedly | Clean start and repeated migration pass | `upgrade-preservation` case in `final-runtime-certified.xml` | PASS |
| 24 | Preserve V2 and Raw assets | V2 sentinel and exact V2/Raw schema and count before/after upgrade | Sentinel, Raw tables, schema and counts unchanged | `upgrade-preservation` case in `final-runtime-certified.xml` | PASS |
| 25 | Replay is namespace/state isolated and parity-safe | Actual Raw replay from nonzero offset; interrupted resume; full live-state/broker/V2 inventory comparison | Frozen manifest enforced; separate SQLite/namespace; live/Raw/broker/V2 invariance verified | `final-runtime-certified.xml` | PASS |
| 26 | Truthful health/readiness and operational metrics | Actual healthy/idle, restart, idle outage, pending-write outage and sampled workload checks | Readiness degrades on dependency fault and recovers; pending counts/faults clear; final workload metrics captured | `final-runtime-certified.xml`; `phase2-measured-ffbed382b2/samples.json` | PASS |
| 27 | Historical Raw-to-Bronze runtime | Reconcile original smoke and sustained SUMO runs from Raw bytes through canonical Bronze and lineage | 700 historical events exact-match; checkpoints at Raw safe boundaries | `full-reconciliation` case in `final-runtime-certified.xml` | PASS |
| 28 | Fresh SUMO→Outbox→Kafka→Raw→main Bronze runtime | Fresh unique SUMO run through actual main Compose Bronze service | 100 ACKED Outbox = 100 Raw = 100 canonical events; 20 cycles; healthy idle | `main-compose-fresh-stream.json` | PASS |
| 29 | Sustained processing and backlog recovery | Final 120-second measured SUMO run with periodic worker/Raw/resource samples | 600 ACKED; 14 samples; catch-up 12.322 sec; under declared 60-sec limit | `phase2-measured-ffbed382b2/report.json`, `samples.json`, `outbox-inventory.json` | PASS |
| 30 | Operate independently of Orion, Projector, Silver and Gold | Inspected and ran isolated Kafka/ClickHouse/Raw/Bronze Compose acceptance stack | Runtime acceptance requires no later analytics services | `docker-compose.v1-acceptance.yml`; `final-runtime-certified.xml` | PASS |
| 31 | Stable Silver handover contract | Interface review plus read-only valid observation, change, invalidation and lineage runtime cases | Silver can read canonical observations/change stream/invalidation/lineage; Silver owns its consumer cursor | `phase-2/handover.md`; `final-runtime-certified.xml` | PASS |

Runtime configuration and final image IDs are recorded in `main-compose-fresh-stream.json`; sustained-run sample files retain worker/Raw metrics and Docker resource observations. Main Compose uses the approved same-authority one-owner handoff for this verification, then the isolated acceptance Bronze worker was restored as the only owner. No live or historical data was reset. Limitations and operational invariants are in `phase-2/handover.md`.

## Superseded acceptance checkpoint — 2026-11-20

The following entries preserve the sequence of earlier checkpoints. Their pending descriptions and provisional gate table are superseded by the final certification above.

## Latest replay, deployment and workload closure

- `raw-replay-interrupted.xml`:1 passed2.41s. Actual Kafka Raw replay range[2,22), declared non-zero initial offset, matching isolated replay ledger/tables. Bronze interrupted with one durable pending intent and cursor7, then resumed to22 with20 deliveries and no pending intent. Changed frozen manifest rejected.
- `replay-full-invariance.xml`:1 passed149.23s. Latest source-scope validation, semantic parity and repeat cursor invocation passed; before/after inventories compare live identities/checkpoints/intents/changes/invalidations, live observation/disposition/invalidation CH rows, Raw ledger/tables, broker group description, and V2 sentinel table. Source was quiescent.
- Main Compose service executed with the resource override `docker-compose.bronze-main-acceptance.yml`. Migration dependency completed; preserved authority handoff yielded READY with zero distances at146/280/286; Raw mount RW=false and authority RW=true were observed. Main service stopped and acceptance worker restored. `main-compose-observed.json` records observations. Fresh streaming specifically through main service remains untested.
- Measured workload `phase2-measured-70f64d12e4`:120 simulation seconds,600 ACKED observations,120 cycles, participants A/B/C/D/M.23 periodic metrics/resource samples over73.995s; catch-up37.301s met the declared60-second post-source limit. Evidence: run subdirectory report.json/samples.json, producer.log, outbox-inventory.json. No production-capacity claim.
- Replay metadata amendment: completed Raw replay now publishes replay_source_ranges/partition_progress only after positive persistence reconciliation, checkpoints WAL and uses DELETE journal mode for quiescent :ro mount access. Initial replay test exposed inability to open a WAL database without SHM on a read-only directory; fix and rerun passed. Live Raw schema/WAL behavior unchanged.
- Observation persistence lookup was tightened to namespace/event/revision (hash compared as content, not used to filter out mismatches). `hash-lookup-regression.xml`:1 passed. Targeted real persistence rerun is tracked in implementation-status.md.

Still required before full certification: final image rebuild containing all latest changes, complete final regression/integration run, main-service fresh-stream check, dedicated historical/out-of-order and availability roundtrip evidence, full schema/provenance/health contract review and final31-gate PASS matrix. The table below retains checkpoint history where subcriteria remain. This document does not assert final completion.

## Additional independently executed acceptance

Latest regression:164 passed12.37s (`regression-latest.xml`). The following new tests executed against the existing isolated SQLite/ClickHouse state; no result is inferred from implementation existence:

| Actual test | Result | Artifact |
|---|---|---|
| Separate-process canonical/read-only interfaces |1 passed21.46s|reader-runtime.xml|
| Lost insert response, partial outputs, SQLite finalization failure, conflict mirror failure/recovery |4 passed5.87s|persistence-faults.xml|
| Actual ClickHouse pause after durable pending intent, unpause and reopened-SQLite recovery |1 passed3.39s|pending-write-outage.xml|
| Additive V2 sentinel/Raw schema preservation with repeated migration007 |1 passed2.71s|upgrade-preservation.xml|
| Complete original SUMO source/output reconciliation |1 passed19.78s|full-reconciliation.xml|

The canonical test scans all accepted identities through actual mirrors, verifies the count equals SQLite authority, excludes existing conflicted IDs, reads append-only changes/invalidations and lineage, tests PENDING exclusion on a consistent SQLite clone, proves mode=ro/query_only rejects writes, refuses unavailable authority, and uses precise query wrappers to simulate missing/corrupt mirrors. The latter failures are controlled fault injection around real source state, not destructive edits to live rows.

Full reconciliation independently verifies smoke100 and sustained600 distinct events, decodes and verifies all700 original transport/value hashes, compares all700 complete nested events with canonical Bronze output, asserts700 completed ACCEPTED_NEW lineage intents and no live pending intents, and checks every Bronze cursor equals Raw safe boundary. Capture cycle/Outbox manifest inventory and sustained time-series metrics still require their separate final report.

Persistence fault tests use separate replay namespaces and local temporary SQLite authorities with actual isolated ClickHouse outputs. They prove PENDING remains hidden after failed finalization, frozen obligations survive reopening, conflicts remain CONFLICTED during mirror failure, invalidations reconcile, and repeated persistence collapses via positive content checks. They do not substitute for the outstanding full worker interrupted-replay/main-deployment tests.

Migration preservation test creates actual V2 schema and a unique sentinel on the isolated database, then compares exact SHOW CREATE TABLE for V2 and Raw before/after two007 runs and confirms Raw counts and sentinel preservation. Historical migrations001–006 remain unchanged.

Replay source validation now additionally requires all source partitions, start_offset0, topic identity agreement and end no greater than current safe boundary. `context_complete=true` alone no longer passes. A replay scope is an explicit source cutoff, not a claim about conflicts arriving after that cutoff. The newest validation change still needs an image rebuild and replay regression.

Legacy main Compose V2 Bronze now explicitly exits with a historical Git reference; it cannot accidentally start the migrated V1 main with V2 settings. Main Compose deployment is still unverified. Remaining gates are therefore not certified COMPLETE.

Baseline: `codex/phase-1-v1-raw`, HEAD `757060f8175fdb22d9ee27e7f8d175c82c0e2c45`; implementation remains in the working tree. Existing user/previous edits preserved. No commit/push.

Evidence directory: `de/artifacts/bronze_v1/acceptance/`.

Executed evidence:

| Verification | Actual result | Evidence |
|---|---|---|
| Contract/Raw/Bronze regression | 164 passed, 17.08 seconds | regression.xml |
| Actual Kafka→Raw→Bronze duplicate/conflict and replay parity/resume | 2 passed, 49.80 seconds | semantic-replay-runtime.xml |
| Actual worker restart and idle readiness | 1 passed, 3.96 seconds | restart-runtime.xml |
| Actual isolated ClickHouse outage | 1 passed, 32.40 seconds | outage-runtime.xml |

Tests are in `de/tests/integration/test_bronze_v1_acceptance.py`; set BRONZE_V1_ACCEPTANCE=1 with the isolated phase2acceptance stack running. Regression command: `python -m pytest contracts/tests de/tests/kafka_raw de/tests/bronze -q --basetemp de/artifacts/bronze_v1/acceptance/regression-temp-05 --junitxml=de/artifacts/bronze_v1/acceptance/regression.xml`.

Fresh SUMO runs: phase2-smoke-20261120-01 (20 simulation seconds), phase2-sustained-20261120-01 (120). Combined worker processed700 deliveries and reached Raw safe offsets140/280/280. This counter alone is not accepted as complete reconciliation evidence. Actual canonical reader fetched10 verified observations. Full selected-run reconciliation/resource/backlog time series remains outstanding.

Actual isolated semantic test sends six wire-valid observations and asserts SQLite states: two ACCEPTED_NEW, one SEMANTIC_DUPLICATE, two SEMANTIC_CONFLICT, one ALREADY_CONFLICTED. Two identities end CONFLICTED, with four change-journal entries and two invalidations. Tests compare persisted state, not logs. Earlier fixture failure (payload run differed from envelope) was fixed; rerun passed. New random run IDs prevent source contamination across tests. No synthetic records were sent to the main runtime.

Replay captures all isolated Raw partitions, replays sufficient context, invokes the same run again, asserts cursor=end for every partition, zero pending intents, semantic-set parity, and unchanged live identity state. Not all live-state/broker/V2 invariance assertions or interrupted-mid-batch cases have yet executed.

Outage test pauses only phase2acceptance-clickhouse-1, checks HTTP503 and unchanged Bronze checkpoints, always unpauses in finally, then checks automatic ready recovery and unchanged semantic state. This is an idle dependency test; pending-write fault cases still need actual acceptance. Worker restart similarly checks semantic state and zero offset distance after recovery.

All31 DoD gates remain mandatory. Current certification disposition:

| Gate | Requirement | Evidence/status |
|---:|---|---|
|1|certified baseline|verified Git baseline|
|2|Raw preservation|regression passed; real full regression acceptance pending|
|3|in-place source migration|existing modules modified; legacy cleanup/entrypoint reconciliation pending|
|4|actual Raw interface|isolated fresh runtime uses logical_read|
|5|read-only Raw|acceptance mount :ro; source reader tests passed|
|6|safe bounds|source tests and runtime catch-up; complete gate matrix pending|
|7|valid gaps|sparse/empty source unit test passed|
|8|missing/unresolved/conflict blocking|unit coverage partial; expanded matrix pending|
|9|original bytes/hashes|700-source full reconciliation passed; full-reconciliation.xml|
|10|shared validator|regression and wire-valid runtime test passed|
|11|unique event grain|unit/runtime assertions passed; complete inventory pending|
|12|duplicates|actual runtime semantic test passed|
|13|all lineage|700 completed ACCEPTED_NEW lineage intents plus semantic runtime disposition assertions passed; full-reconciliation.xml|
|14|immutable conflict|payload and metadata runtime cases passed|
|15|conflict exclusion|actual canonical scan excludes conflicted IDs; reader-runtime.xml|
|16|monotonic conflict|runtime retry and restart state assertions passed|
|17|durable invalidation|runtime count asserted; full provenance/mirror acceptance pending|
|18|null/zero/absence|shared contract/codec tests; full payload roundtrip inventory pending|
|19|historical/out-of-order|full dedicated runtime case pending|
|20|content reconciliation|actual persistence fault cases and missing/corrupt reader mirror tests passed; persistence-faults.xml, reader-runtime.xml|
|21|CAS|unit test and runtime progress; full fault matrix pending|
|22|crash recovery|unit conflict recovery plus real restart; pending-write outage/finalization cases pending|
|23|clean/upgrade/repeat migrations|DDL clean/repeated executed, V2 sentinel/Raw schema upgrade passed; upgrade-preservation.xml|
|24|V2/Raw preservation|actual sentinel and exact schema preservation passed; upgrade-preservation.xml|
|25|replay isolation|separate SQLite/namespace and identity invariance passed; full invariance pending|
|26|health|idle/restart/outage passed; expanded pending metrics remaining|
|27|historical Raw processing|persisted replay executed; complete selected-history acceptance pending|
|28|fresh SUMO flow|actual smoke/sustained ran; complete reconciliation pending|
|29|sustained/backlog|measured120-second run,23 periodic samples, catch-up37.30 seconds within60-second limit; phase2-measured-70f64d12e4/report.json and samples.json|
|30|independence|isolated stack contains only Kafka/ClickHouse/Raw/Bronze/migrations|
|31|Silver handover|implemented-interface draft exists; full contract tests pending|

No gate marked final PASS solely from code existence. Final31-gate PASS/FAIL/BLOCKED matrix must replace these checkpoint dispositions only after complete evidence exists. Historical Phase1 reports are preserved.
