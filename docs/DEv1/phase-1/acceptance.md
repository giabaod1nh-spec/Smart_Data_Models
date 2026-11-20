# Phase 1 implementation acceptance — 2026-11-19

Current verdict: PHASE 1 COMPLETE — READY FOR PHASE 2 PLANNING. See the Final implementation report below for the certification results. The earlier checkpoint is retained as historical evidence.

Baseline: 803c6e421ec8cd00f14ea394fd338fef785b9157. Branch: codex/phase-1-v1-raw. Changes are uncommitted; no implementation commit or push is claimed. Master Plan and approved Phase 1 plan were preserved.

## Implementation and task status

Historical checkpoint (superseded by the final report below):

| Task | Status | Actual result |
|---|---|---|
| P1-01 | VERIFIED | Fetched final, fast-forwarded clean checkout, created implementation branch; shared V1 wire contract reused |
| P1-02 | IMPLEMENTED | Transport model, ordered headers, null flags, byte base64, shared validator and immutable envelope hash |
| P1-03 | IMPLEMENTED | Four additive ClickHouse tables, repository, SQLite ledger, batch metadata and logical read |
| P1-04 | IN_PROGRESS | Running consumer, topic UUID, safe delivered-order commits, restart, ownership callbacks; exhaustive recovery certification pending |
| P1-05 | IN_PROGRESS | Replay, report-only gap tool, health/ready/metrics and cutover endpoint source; full isolation/health matrix pending |
| P1-06 | IMPLEMENTED | V1 Compose profile, image and migration run successfully |
| P1-07 | IN_PROGRESS | Real smoke, sustained, binary, restart and isolated persistence outage evidence; remaining gates below |

## Executed tests

| Suite | Command | Passed | Failed | Skipped |
|---|---|---:|---:|---:|
| Contract and Raw regression | python -m pytest contracts/tests de/tests/kafka_raw -q --junitxml=de/artifacts/v1_raw/acceptance/regression.xml | 125 | 0 | 0 |
| Focused contract/Raw | python -m pytest de/tests/kafka_raw contracts/tests/test_observation_v1.py -q --junitxml=de/artifacts/v1_raw/acceptance/tests.xml | 53 | 0 | 0 |
| Real ClickHouse binary/logical-read/conflict | V1_RAW_INTEGRATION=1, V1_RAW_TEST_PORT=18123; pytest de/tests/integration/test_v1_raw_runtime.py | 1 | 0 | 0 |
| Actual isolated outage | V1_RAW_ISOLATED_OUTAGE=1; pytest de/tests/integration/test_v1_raw_outage.py --basetemp de/artifacts/v1_raw/test-tmp-007 --junitxml=de/artifacts/v1_raw/acceptance/outage.xml | 1 | 0 | 0 |
| Migration profile/Raw regression | python -m pytest de/tests/gold/test_migration_profile.py de/tests/kafka_raw -q | 35 | 0 | 0 |

The suites overlap; do not sum these numbers as unique tests.

Earlier observed failures were corrected: missing RFC3339 optional dependency allowed invalid capturedAt; ClickHouse FixedString results were bytes rather than str; replay AdminClient was destroyed before its future completed; first-start restart incorrectly rejected an existing ledger before its first broker commit. Sandbox socket and pytest temp-directory failures were rerun with approved access and a workspace temp directory.

The isolated outage command was initially denied accidentally. The user subsequently authorized retry. The retry passed on 2026-11-19; the denial is no longer a blocker. The test pauses only de-v1-raw-acceptance-clickhouse, observes no Kafka commit while persistence is unavailable, always unpauses in finally, and verifies commit after recovery. Kafka commit in this specific test is controlled by a test double; ClickHouse persistence is real. It does not certify a complete real-broker outage/rebalance matrix.

## Real runtime observations

- Smoke run: phase1-raw-20261119-smoke02, 40 simulation seconds, 40 complete cycles x 5 participating intersections = 200 semantic observations.
- All 200 producer rows ACKED; Raw contains 200 distinct event IDs; decoded stored bytes match Outbox bytes.
- Raw physical rows for the smoke were 278 because the early reconciliation defect caused repeated insert attempts. These retries remain preserved; logical read collapses matching physical rows.
- Sustained run: phase1-raw-20261119-sustained01, 120 simulation seconds, 120 complete cycles x 5 = 600 observations.
- Sustained Outbox rows = 600, Raw rows = 600, distinct IDs = 600, byte comparison true, producer states ACKED.
- Ledger total after both runs = 800 COMMITTED deliveries: partition counts 120, 240, 440.
- Broker committed next offsets = 120, 240, 440; corresponding log ends equal those values; lag 0 on every partition.
- Restart was exercised and idle runtime was ready with no buffered records.
- Replay acceptance01 copied 20 deliveries in partition 1, range [0,20), into isolated replay storage and performed zero live commits.
- No data, topic or volume was deliberately deleted. Existing V2 services automatically restarted when Docker Desktop started and were left running. Compose recreated Kafka/ClickHouse containers according to current configuration; preservation of all older Kafka historical state was not independently established by a before/after inventory.

Evidence: de/artifacts/v1_raw/acceptance/{phase1-raw-20261119-smoke02,phase1-raw-20261119-sustained01}/ contains reconciliation.json, schema.json and raw-lineage.json. XML evidence is under de/artifacts/v1_raw/acceptance/. Producer manifests are under Visualize/artifacts/runs/<run-id>/.

## Mandatory gate status

| Gate | Result | Evidence or missing certification |
|---|---|---|
| 1 correct topic/group | PASS | live broker group and runtime |
| 2 shared validation | PASS | shared contract tests |
| 3 valid persistence | PASS | real SUMO runs |
| 4 quarantine | PASS | real ClickHouse binary test; synthetic cases use replay tables |
| 5 bytes and metadata | PASS for tested cases | base64 binary/null/duplicate headers tests |
| 6 null/zero/availability | PARTIAL | shared validator coverage; complete runtime metric matrix pending |
| 7 physical integrity/idempotency | PARTIAL | transport hash, retry collapse and dual disposition tests; exhaustive fault matrix pending |
| 8 same event separate offsets | PASS | foundation test |
| 9 commit cannot skip unresolved | PARTIAL | delivered-order and partial persistence tests; real rebalance/loss certification pending |
| 10 ambiguous persistence | PASS for controlled case | lost-response unit test and reconciliation |
| 11 restart/failure recovery | PARTIAL | restart and actual isolated ClickHouse outage pass; broker-commit/ledger-failure crash matrix pending |
| 12 replay/repair isolation | PARTIAL | isolated replay executed; full before/after live-state assertion pending |
| 13 Phase 2 logical interface | PARTIAL | payload-bearing logical read exists; complete safe-boundary and read-only integration certification pending |
| 14 health/readiness | PARTIAL | idle readiness observed; dependency/rebalance health matrix pending |
| 15 clean/upgrade migrations | PARTIAL | clean isolated and repeated migration pass; V2 inventory comparison pending |
| 16 V2 preservation | BLOCKED | no destructive change; before/after historical inventory unavailable |
| 17 real producer flow | PASS | smoke/sustained byte comparison |
| 18 sustained verification | PARTIAL | 120 simulation seconds observed; approved 1200-second horizon and time-series resource/throughput evidence pending |
| 19 independence | BLOCKED | unrelated Orion/V2 workers remain running; no stopped-dependency or network-isolation proof yet |
| 20 documents complete | PARTIAL | actual handover/status/decisions recorded; final gate evidence incomplete |

## Remaining acceptance work

The remaining blockers are unverified acceptance requirements, not a continuing permission denial. Execute the full broker ownership/retention/commit-crash matrix, independently verify V2 preservation, run network-isolated independence acceptance and the longer sustained run, and certify health/lag/backpressure under failure. The current local runtime is not certified for Phase 2 promotion.

The deployed container can lag the latest uncommitted source corrections. Rebuild with docker compose --profile v1-raw up -d --no-deps --build de-kafka-raw-v1 before final certification; record image digest and rerun the acceptance matrix.

## Final implementation report

This report supersedes the historical checkpoint above. Evidence root: de/artifacts/v1_raw/acceptance/. Tests and runtime checks were executed on 2026-11-19.

### 1. Implementation Summary

V1 Raw now consumes real SUMO observations, validates the shared contract, preserves original transport bytes, persists Raw/Quarantine, records physical confirmation, commits Kafka safely, recovers across restart/failure, and exposes logical deliveries for Bronze. No downstream semantic processing was implemented.

### 2. Repository Baseline and Changes

Baseline remains 803c6e421ec8cd00f14ea394fd338fef785b9157 on codex/phase-1-v1-raw. There is no implementation commit or push; changes are in the working tree. Added v1_models, validator, config, ledger, repository, consumer, main, replay, migration 006, acceptance Compose, verification tools and tests. Modified only DE requirements, migration selection and Compose among pre-existing source files. Historical migrations 001–005, contracts, producer, Projector, Spring, Dashboard and approved plans are unchanged.

### 3. Task Completion

| Task | Status | Result / evidence |
|---|---|---|
| P1-01 | VERIFIED | approved baseline fetched, synchronized and branched; shared contract tests |
| P1-02 | VERIFIED | exact bytes/null/headers/envelope hash and availability tests |
| P1-03 | VERIFIED | actual four-table migration; clean/upgrade/repeat/binary/dual-disposition tests |
| P1-04 | VERIFIED | actual broker/ledger crash recovery; delivered-gap/partial/ownership tests; restart over 6000 records |
| P1-05 | VERIFIED | full-worker outage/503/safe offsets/recovery; replay live-state invariance; read-only handover |
| P1-06 | VERIFIED | main and independent acceptance runtime ready; isolated stack contains no Orion/analytics services |
| P1-07 | VERIFIED | 1200-second SUMO, byte audit, logs/config/schema/ledger/offset/performance evidence and final documents |

### 4. Implemented Architecture

SUMO canonical snapshot -> frozen V1 SQLite Outbox -> Kafka traffic.observations.v1 -> shared validation -> V1 Raw/Quarantine -> physical SQLite ledger -> synchronous broker commit.

Physical retries collapse through the logical read interface; distinct offsets remain separate even for the same eventId. Topic UUID changes, retention loss and inconsistent physical evidence fail closed. The supported deployment is one local owner per live group/ledger.

### 5. Data Schema and Ledger

The authoritative implemented DDL is de/migrations/006_create_kafka_raw_v1.sql. Actual SHOW CREATE TABLE evidence is schema.json. Four MergeTree tables preserve base64 value/key, null flags, ordered headers, nullable timestamps, envelope_json, hashes, disposition and versions; replay adds replay_run_id. The original Kafka bytes remain authoritative.

SQLite objects: deliveries, batches, partition_progress, integrity_faults. PERSISTENCE_CONFIRMED is the canonical persistence state; logical read labels are CONFIRMED_RAW/CONFIRMED_QUARANTINE. A confirmed delivered prefix becomes COMMIT_ELIGIBLE, Kafka acknowledges next offset, then SQLite records COMMITTED. Startup reconciles broker commits against positive ClickHouse evidence. No cross-system exactly-once claim is made.

### 6. Test Results

| Suite | Evidence | Passed | Failed | Skipped |
|---|---|---:|---:|---:|
| Contracts, Raw, migration profile | final-unit.xml | 143 | 0 | 0 |
| Actual Kafka crash/recreation, ClickHouse binary/availability, upgrade and outage | final-integration.xml | 7 | 0 | 0 |
| Read-only Phase 2 handover / replay invariance | final-handover.xml | 1 | 0 | 0 |

Total for these non-overlapping final suites: 151 passed. Earlier failing runs remain described above; none is counted as PASS without rerun. Full-worker outage probe is additional executable runtime evidence in isolated-probe/probe.json.

### 7. Real Runtime Acceptance

Final run phase1-isolated-long01 completed 1200 simulation seconds with SUMO_STEP_LENGTH=0.1 and one complete capture cycle per simulation second. Actual participant set A/B/C/D/M yields 1200 x 5 = 6000 observations.

Outbox = 6000 ACKED; Kafka deliveries verified = 6000; Raw physical rows = 6000; distinct event IDs = 6000. Every audited Kafka value matches Outbox and Raw bytes. Two separate full-worker outage probes injected 12 malformed deliveries, all durably quarantined.

Final isolated committed next offsets/log ends are partition 0: 12, partition 1: 2400, partition 2: 3600, with zero lag and no buffer. Worker restart revalidated all 6000 observations and regained all three partitions. Logical read and replay tests prove payload availability, physical retry collapse, conflict detection, exclusive end boundary and unchanged live multiset/offsets during replay.

Observed ingestion: 6000 deliveries over 385.26564 seconds between first and last Raw consumption timestamps, approximately 15.57 deliveries/second. This measures the actual producer workload and includes the controlled outage; it is not a maximum-capacity benchmark. Resource snapshot, bounded lag samples, recovery sample and final idle/caught-up status are stored. A sampled worker used about 72 MiB, Kafka 385 MiB and ClickHouse 724 MiB; use resource-snapshot.txt for the final measurement.

The separate phase1acceptance network contains only Kafka, ClickHouse, topic init, migration and Raw. Orion does not resolve from that worker. Other user workloads were not stopped.

### 8. Definition of Done

| Gate | Actual requirement/result | Evidence | Result |
|---|---|---|---|
| 1 | correct V1 topic/group | config, broker-offsets.txt | PASS |
| 2 | shared V1 validation | final-unit.xml | PASS |
| 3 | durable valid observations | 6000-row reconciliation | PASS |
| 4 | durable invalid quarantine | runtime-final.xml / probe.json | PASS |
| 5 | exact original bytes/metadata | kafka-byte-audit.json and binary/header tests | PASS |
| 6 | zero/unavailable/not-applicable/absent unchanged | availability round-trip test | PASS |
| 7 | physical idempotency/integrity | retry/dual-disposition/transport tests | PASS |
| 8 | same semantic ID at different offsets preserved | foundation and real Kafka recovery tests | PASS |
| 9 | no commit over unresolved delivered record | partial/gap/ownership tests and full-worker probe | PASS |
| 10 | ambiguous writes reconcile | lost-response test and actual persistence failure tests | PASS |
| 11 | restart/crash recovery | actual Kafka commit success then ledger failure test; 6000-row restart | PASS |
| 12 | replay/repair isolation | final-handover.xml, report-only behavior and replay-only repair path | PASS |
| 13 | usable logical Raw/read-only safe boundary | final-handover.xml | PASS |
| 14 | truthful readiness/idle/fault/recovery | health tests and actual HTTP503 probe | PASS |
| 15 | clean/additive/repeated migration | actual isolated schema and V2 sentinel upgrade | PASS |
| 16 | V2 historical assets preserved | unchanged historical source/migrations and sentinel data/schema comparison | PASS |
| 17 | actual SUMO/Kafka/ClickHouse flow | fresh isolated run manifests and byte audit | PASS |
| 18 | sustained workload verified | 1200 simulation seconds, performance.json, lag samples | PASS |
| 19 | independent of Projector/Orion/analytics | acceptance Compose inventory and isolated network | PASS |
| 20 | complete evidence/handover/status | this report and handover.md | PASS |

V2 preservation certification proves the additive migration preserves an existing V2 schema and sentinel history and that no destructive V2 change exists in this implementation. It does not claim a historical Kafka inventory was captured before the earlier Docker Desktop startup.

### 9. Deviations and Decisions

Approved-plan DDL inconsistencies were resolved in migration 006: envelope_json contains required envelope metadata, malformed attempted IDs remain in original evidence, headers use an ordered array representation, and transport integrity includes null flags. Raw never performs cross-offset semantic classification.

SUMO's supported step-length configuration was set to 0.1 for the certified 1200-second run. The earlier default-step run was stopped at approximately 160 simulation seconds and is not certification evidence.

Run-scale restart exposed ClickHouse HTTP form limits; reconciliation now chunks IDs into groups of at most 500. Assignment errors now fail closed and cannot result in partially ready startup. RFC3339 validation dependency, topic metadata propagation retries and a true UUID API minimum are explicit.

Known operational scope: local single owner, persistent SQLite directory/WAL, retained Kafka history and actual topic UUID support. Permanent integrity/retention faults require operator reconciliation; no reset/delete procedure is automatic. Producer detector hash warnings were retained as upstream provenance; Raw does not alter or interpret detector/topology semantics.

### 10. Phase 2 Handover

phase-1/handover.md documents the implemented schemas, read-only V1LedgerReader, payload-bearing logical_read([start,end)), safe_next_offset, conflicts/unresolved statuses and replay isolation. Bronze owns semantic deduplication, cross-offset eventId comparison, immutable-envelope conflict invalidation and materialization.

### 11. Final Verdict

PHASE 1 COMPLETE — READY FOR PHASE 2 PLANNING

No Phase 2 implementation was started.


