# Phase 4A-Core implementation and acceptance report

## Baseline and environment

The implementation started on `codex/phase-1-v1-raw` at `ad7f264e9cdb44a7a93ee5f3f4e5cf6534ffd369` with a clean working tree and index. The implemented Pre contract is at `9adc6fc7d4dd6ecada8b01b6d53b6ffe2d3b6a1b`. All work remains local. The host is Windows with Python 3.14.4 and 12 logical processors. Docker Engine 29.3.1 reported 12 processors and 8,186,413,056 bytes of VM RAM. Host D: had about 40 GB free at preflight. Docker Desktop initially lacked its Linux engine. Relaunching Docker Desktop restored it. Tests used disposable ClickHouse, fixture archive/SQLite stores, and a separately named Compose project for the actual SUMO pipeline. The existing historical database was outside those projects.

## Implemented boundaries

- The active entrypoint is `python -m de.gold_runtime.main`. Gold reads the Silver public live feed with SELECT credentials, commits verified receipt headers and sequence metadata to its own SQLite, and maintains independent change and invalidation cursors.
- A change receipt can commit only after the independent invalidation cursor covers its fixed safe Pi. A separate SQLite transaction persists each invalidation receipt, suppression, indexed impact and new work generations before advancing its cursor.
- The state inventory is in `de/gold_runtime/checkpoint_store.py` (`CORE_SCHEMA`). Important indexes are `core_transition_event`, `core_window_clock`, `core_promotion_queue`, `core_membership_impact` and `core_work_queue`. The application ID and user version reject legacy or unsupported SQLite state. Lease token and cursor generation protect local writes.
- Generic 60s and 300s membership uses exact epoch-zero half-open intervals in simulation time. Gold retains scenario per event. Eligible work streams each indexed member using its exact Silver sequence, checks bundle and immutable transition hashes, and completes through a local generation CAS. The current lifecycle is UNKNOWN. Window completeness is UNKNOWN and publication finality is DISABLED.
- Replay has separate state, owner, processing namespace and frozen run and boundary. Silver receives source namespace `live` in every mode. Source-boundary change aborts an active frozen replay. Replay has no arbitrary historical reconstruction promise.
- See [implementation-status.md](implementation-status.md) for the six architectural findings, receipt retention policy, alternatives and remaining limitations.

## Evidence record

The retained evidence directory is [core-evidence/](core-evidence/). The Stage B fixture uses real Bronze and Silver SQLite and ClickHouse 24.3.18.7 and the actual Gold coordinator and store. Independent oracle inputs include the Bronze journal and identity records, frozen Silver obligations, producer coordinates and archived identities. Selected complete canonical response bytes are represented by the JCS envelope and independent SHA-256 in `canonical_receipt_sample.json`. Production Gold SQLite holds the header and per-sequence metadata rather than the complete envelope.

| Assertions | Evidence and outcome |
|---|---|
| Receipt validation, duplicate behavior, both cursors, exact coverage and source immutability | Gold local transaction/receipt tests, actual source journal oracle and retained canonical receipt sample. |
| Multi-run isolation, 60/300 window identity, fractional boundaries, scenario provenance, late work and materialization after restart | Local fractional endpoint tests and `real_oracle_restart.json`. Actual materialized bundle checked against independently persisted Silver obligation rows. |
| Actual subprocess crashes before and after both feed commits | `real_crash_barriers.json`, exit 73 before commit and 74 after commit. Reopen from durable cursor with correct suppression and membership. |
| Fixed Pi, suppression first, pending mirrors and independent feed progress during a blocked change prefix | Local fixed-Pi test and `real_independent_invalidation.json`. Gold invalidation cursor reached 1 while actual Silver returned PREFIX_BLOCKED for change cursor 0. |
| Invalidation before, during and after work, source retention gap | Actual race/fencing fixtures and local duplicate, generation, rollback and recovery tests. |
| V1/V2 isolation, SELECT-only credentials, no business writes, HTTP runtime | `entrypoint_isolation.json`, dedicated SELECT principal and migration 005 business table sentinels. Local populated V2 SQLite remains unchanged. |
| Replay isolation and source advance | Live DB bytes unchanged during isolated replay. Reopening completed replay at newer source boundary is refused. |
| Architecture regressions | The ten failures listed in [Pre baseline comparison](pre/evidence/architecture-comparison.json) were reproduced in the architecture run. There are no additional architecture failures in that run. |

## Reproduction

From the repository root, use a local Python environment with `python -m pip install -r de/requirements.txt`. On Windows the optional `psutil` package makes process RSS and I/O counters available. The acceptance run used a local test dependency directory `.pytest_cache/phase4a-deps` for RFC 8785 and psutil. Production Compose uses the existing `v1-gold` service profile and requires an operator-provisioned SELECT role with access to the Bronze/Silver source evidence tables and required system metadata. Use isolated state directories for acceptance.

```powershell
docker run --pull never --detach --rm --name phase4a-core-acceptance-ch --publish 127.0.0.1:18123:8123 --env CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 clickhouse/clickhouse-server:24.3
docker exec phase4a-core-acceptance-ch clickhouse-client --query 'CREATE DATABASE IF NOT EXISTS smart_traffic'
$env:PYTHONPATH='.pytest_cache/phase4a-deps;.'
$env:SILVER_CLICKHOUSE_PORT='18123'
$env:GOLD_CORE_STAGE_B='1'
$env:GOLD_CORE_EVIDENCE='docs/DEv1/phase-4/core-evidence'
python -m pytest de/tests/gold_runtime/test_core_foundation.py de/tests/gold_runtime/test_core_stage_b.py -q --basetemp=.pytest_cache/phase4a-new-stage-b --junitxml=.pytest_cache/phase4a-new-stage-b.xml
$env:SILVER_LIVE_ACCEPTANCE='1'
python -m pytest de/tests/silver/test_incremental_contract.py de/tests/silver/test_incremental_reader.py -k 'not focused_feed_capacity' -q --basetemp=.pytest_cache/phase4a-new-pre-regression
docker stop phase4a-core-acceptance-ch
```

For a fresh disposable end-to-end project, use the same Linux worker image documented in `de/tools/gold_core_capacity.py`, or build the current worker image first. Only the isolated fixture grants and volumes are created. `prepare` requires a fresh path. Run `sample` and `produce` in separate shells so sampling spans the producer and recovery. The one-second step in this profile is an explicit measurement setting.

```powershell
python -m pip install --target .pytest_cache/phase4a-deps rfc8785==0.1.4
python -m de.tools.gold_core_capacity prepare .pytest_cache/phase4a-e2e-new
docker compose -p phase4a-core-e2e-new -f .pytest_cache/phase4a-e2e-new/compose.json up -d --no-build
Invoke-RestMethod http://127.0.0.1:18096/ready
Invoke-RestMethod http://127.0.0.1:18096/health
Invoke-RestMethod http://127.0.0.1:18096/metrics
python -m de.tools.gold_core_capacity sample .pytest_cache/phase4a-e2e-new --seconds 720
python -m de.tools.gold_core_capacity produce .pytest_cache/phase4a-e2e-new --seconds 600 --realtime
docker compose -p phase4a-core-e2e-new -f .pytest_cache/phase4a-e2e-new/compose.json exec -T gold python -m de.tools.gold_core_capacity oracle /app/Visualize/artifacts
docker compose -p phase4a-core-e2e-new -f .pytest_cache/phase4a-e2e-new/compose.json stop
```

The oracle exits nonzero if the full pipeline has outstanding source entries. Its JSON separately records whether the durably consumed prefix and memberships are exact. Service stop preserves the test volumes. A restart reuses the same bound stores and resumes from their cursors.

## Test execution

| Executed suite | Result |
|---|---|
| `python -m pytest de/tests/gold_runtime/test_core_stage_b.py de/tests/gold_runtime/test_core_foundation.py -q --basetemp=.pytest_cache/phase4a-stage-b-t7 --junitxml=.pytest_cache/phase4a-stage-b-closure.xml` | 19 passed, including 7 actual ClickHouse/Gold scenarios and 12 local contract/state scenarios. The local suite includes an active replay abort and lease loss after durable progress. |
| `python -m pytest de/tests/silver/test_incremental_contract.py de/tests/silver/test_incremental_reader.py -k 'not focused_feed_capacity' -q --basetemp=.pytest_cache/phase4a-pre-regression-temp --junitxml=.pytest_cache/phase4a-pre-regression.xml` | 15 passed and 1 deselected. The focused Pre capacity benchmark remains historical evidence. |
| `python -m pytest de/tests/bronze de/tests/silver de/tests/gold de/tests/gold_runtime tests/architecture/test_gold_runtime_compose.py -o faulthandler_timeout=30 -vv --basetemp=.pytest_cache/phase4a-upstream-gold-final-temp --junitxml=.pytest_cache/phase4a-upstream-gold.xml` | 472 passed, 50 opt-in or environment/legacy skips, 1 inherited failure. `test_gold4_handoff_files_exist` requires the absent `docs/gold/GOLD_4_RUNBOOK.md`. Git confirms that file was absent at the approved baseline and the test has no diff. |
| `python -m pytest tests/architecture -q --basetemp=.pytest_cache/phase4a-architecture-final --junitxml=.pytest_cache/phase4a-architecture.xml` | 117 passed, 10 inherited failures, no skips. See the named case inventory in [test-results.json](core-evidence/test-results.json) and the [Pre comparison](pre/evidence/architecture-comparison.json). |

The first real fixture attempt exposed missing initialization of the disposable ClickHouse bootstrap database. After initializing it, the fixture exposed an incorrect test reference to the worker's Silver settings. Both were corrected before the final runs. The isolated pipeline initially mounted code over a read-only SQLite mount parent, which prevented the Gold container from starting. Separating the code mount from state mounts resolved it. Sandboxed broad-test attempts stalled and the same broad suite completed under the diagnostic invocation above. Separate opt-in Phase 3 destructive/failure environments remain outside this run. Their existing Pre outcomes remain in the evidence register.

## Capacity and resource results

The actual isolated pipeline ran 600 simulation seconds over five intersections using a one-second SUMO step and real-time pacing. This explicit diagnostic profile generated 3,000 observations and 11,970,279 Outbox payload bytes. It is input for a capacity measurement rather than a new default simulation profile. SUMO emitted vehicle collision warnings at this step size and reported a detector network fingerprint warning. Each accepted archived topology still passed reconciliation. The measurement includes local host contention during the final regression tests.

The comparable steady measurement spans 544.92 wall seconds from simulation second 61 to 600. Producer rate was **4.95 observations/s**, **19,776 payload bytes/s**, and **0.989 simulation seconds per wall second**. Raw persisted 4.94 deliveries/s, Bronze produced 5.07 change sequences/s while clearing earlier backlog, and Silver and Gold each reached **0.96 change sequences/s**. These are each stage's own progress counters. Raw physical and semantic counts are separate. Kafka sampled offset lag peaked at 55. Bronze-to-Silver backlog grew from **162 to 2,401** at **4.11 sequences/s**, while Silver-to-Gold backlog peaked at 10 and ended at zero. All 3,000 source IDs later reconciled to Bronze. The oracle independently verified all 597 Gold transition entries at its later sample, 345 exact acknowledged receipts and 1,194 memberships. The measured run window did not fully drain. See [time series](core-evidence/pipeline-normal-sustained.json) and [prefix oracle](core-evidence/pipeline-prefix-oracle.json).

| Gold measure at the end of the normal trial | Observed |
|---|---|
| Source capture to durable Gold work intent p50/p95/p99/max | 224.27 / 459.80 / 484.37 / 494.60 s across 544 acknowledgments |
| Gold change-page read through durable acknowledgment p50/p95/p99/max | 0.437 / 1.906 / 3.880 / 5.774 s across 317 acknowledged pages |
| Gold acknowledgment transaction p50/p95/p99/max | 0.012 / 0.982 / 2.849 / 5.532 s |
| Materialization reads | 440 completed member reads, 9.12 MB of read envelopes, read-step p95 0.893 s. One completed work proof took 219.96 s over interleaved scheduler passes. This does not establish a window service rate. |
| Candidate scheduling delay | 0.015 s for the one observed completed candidate. A single sample does not establish a distribution. |
| Gold process resources | Peak RSS 76,333,056 bytes, about 72.8 MiB. SQLite reached 2,633,728 bytes and sampled WAL peak was 4,293,072 bytes. CPU time was 87.23 s. Process disk reads/writes were 25,571,328 / 65,536,000 bytes. |
| ClickHouse | 13,332 completed queries from the Gold reader, p50/p95/p99/max 6/18/40/1,885 ms. The shared upstream principal logged 49,761 completed queries. Those counts include reads and polling; physical rows read do not represent observations. |

The captured page-to-ack time measures Gold's feed path. The public DTO does not expose an applied-at wall-clock timestamp for each Silver sequence, so exact Silver-applied-to-Gold latency outside the feed call is unmeasured. Cross-stage backlog ages and per-operation upstream resource attribution were not measured for every stage. The time series retains the Outbox age, queue metrics and per-stage progress used in this report. Heap use stayed bounded in this tested envelope. Durable receipt/transition/work metadata grows with retained observations and generations. The deployment still needs retention and disk headroom decisions.

| Scenario | Outcome |
|---|---|
| A / C — normal and continuous | **CAPACITY INSUFFICIENT** for the observed 4.95 observations/s. Silver apply was the largest measured accumulating backlog. The measurements establish the stage and its observed rate. More profiling is needed to attribute time to reconciliation, SQLite/ClickHouse I/O and scheduling inside that stage. |
| B — accelerated | An unpaced 60-simulation-second run emitted 300 observations at 31.14 observations/s during the source capture interval. At the recorded snapshot, 295 were Kafka-acknowledged and five remained in durable Outbox. This ran with the earlier backlog still present, so it establishes the offered burst and buffering behavior. |
| D — true burst and catch-up | Accelerated source emission was observed. Catch-up at the representative normal rate remains uncertified, as the normal profile itself accumulated backlog. |
| E — dependency outage | Gold observed retryable `CLICKHOUSE_UNAVAILABLE` during the isolated ClickHouse interruption. The durable cursor stayed 837 during the fault and advanced to 862 on the same binding after 37.33 s including stop and restore. See [outage evidence](core-evidence/pipeline-clickhouse-outage.json). It is a targeted recovery check. |
| F — continuous conflicts | Real durable conflict, invalidation and work fencing were verified in Stage B. A sustained end-to-end conflict workload was not run. There is no approved representative conflict rate. |
| G — bootstrap and restart | Stage B verified cold bootstrap. The running pipeline's Gold service exited cleanly, resumed with unchanged binding and progressed from cursor 693 to 695 after 23.97 s including stop and restart. |
| H — constrained hardware | The existing 12-CPU, 8.19 GB Docker VM and host disk were observed. Resource boundedness passed for this tested envelope. A smaller hardware certification profile is pending resource objectives. |

No latency SLO or production operating envelope is approved. Preserve the upstream semantics and profile Silver apply first in a separate remediation effort. Also profile the Gold cost of canonical single-entry materialization under an upstream source that can reach the target rate. Continuous unrelated invalidation churn can restart a work proof, and the current-effective contract depends on retained Silver evidence.

## Acceptance mapping

- C01/C11/C13/C15/C19/C21/C25 use the actual SELECT-only entrypoint, the historical table and SQLite sentinels, and the architecture assertions. C02/C16 use the retained Silver compatibility fixtures. C03/C04/C17/C20/C22/C23 use the real Silver contract regressions and the Gold receipt, discovery, append, replacement and duplicate assertions.
- C05–C10/C24 use the canonical obligation oracle, numeric window assertions, scenario and archived run identity checks, and the independent completeness/finality dimensions. The active feed has no run lifecycle response. The UNKNOWN case is the supported behavior.
- C12/C14/C18 use migration reopen, SQL rollback, disk/page limits and measured resource/operational evidence. R01–R08/R10–R12 use actual crash, receipt-order, conflict, restart and replay fixtures and independent transaction oracles. R09 is inapplicable because the implementation does not stage canonical pages.
- I01–I04/I06/I08–I10 use the actual service, fixture data, architecture regression, oracle, retained restart/outage timelines and instrumentation. I05/I07 separate boundedness and the capacity outcome above.

## Independent verdicts

- **CORE IMPLEMENTATION — COMPLETE** for the accepted foundation. The actual Gold runtime and state integrated with the accepted Silver contract.
- **STAGE B CORRECTNESS — PASS**. Exact receipt verification, complete obligations, invalidation ordering and convergence, real crash recovery, replay isolation and deterministic input materialization passed in the tested environment.
- **RESOURCE BOUNDEDNESS — PASS** for the tested page, scheduler and local-volume envelope. Deployment retention and headroom remain to be approved.
- **CAPACITY — CAPACITY INSUFFICIENT** at the tested representative rate.
- **OPERATIONAL READINESS — NOT CERTIFIED**. Deployment and production activation remain owner gates. Phase 4B uses the foundation with its unknown coverage/lifecycle, disabled finality and current-effective-state limits.



## Local file inventory

| File | Change |
|---|---|
| `de/gold/windowing.py` | P4A-04 finite fractional time validation and exact epoch-zero Tumbling endpoints. |
| `de/gold_runtime/checkpoint_store.py` | P4A-03/05 isolated schema, migrations, atomic acknowledgments and lease fences. |
| `de/gold_runtime/instance_lock.py` | P4A-03/07 exact V1 lock path and owner acquisition before opening the state store. |
| `de/gold_runtime/silver_readers.py` | P4A-02B public discovery and receipt validation. The legacy reader remains outside the active path. |
| `de/gold_runtime/processing_ledger.py` | P4A-03/04/05 provenance, suppression, membership and work generations. |
| `de/gold_runtime/processor.py` | P4A-02B/04–07 bounded coordinator, invalidation ordering, input streaming, retries and recovery. |
| `de/gold_runtime/replay.py` | P4A-06 frozen current-effective replay manifest and isolated processing identity. |
| `de/gold_runtime/config.py` | P4A-03/07 isolated paths and bounded runtime settings. |
| `de/gold_runtime/main.py` | P4A-07 activates the existing entrypoint on Gold Core and handles controlled shutdown. |
| `de/gold_runtime/health_api.py` | P4A-07 cached foundation readiness and metrics endpoints. |
| `de/gold_runtime/metrics.py` | P4A-07 native process resource counters. |
| `docker-compose.yml` | P4A-07 activates the existing Gold service only under `v1-gold`, with isolated state and read-only source mounts. Automatic restart is disabled so permanent faults stay visible until remediation. |
| `de/tests/gold_runtime/test_core_foundation.py` | Added local window, receipt, transaction, isolation, replay and recovery checks. |
| `de/tests/gold_runtime/test_core_stage_b.py` | Added actual Gold/ClickHouse acceptance and independent oracles. |
| `tests/architecture/test_gold_runtime_architecture.py` | Added the active path check for public Silver access and absence of a legacy engine call. |
| `tests/architecture/test_gold_runtime_compose.py` | Updated old configuration assertions for the authorized V1 profile, mounts and stop policy. |
| `de/tools/gold_core_capacity.py` | Added the isolated Compose generator, producer and samplers, and read-only pipeline prefix oracle. |
| `docs/DEv1/phase-4/plan.md` | Reconciled receipt retention, exact input retrieval and replay. |
| `docs/DEv1/phase-4/acceptance.md` | Corrected public error mapping and split correctness from capacity and activation. |
| `docs/DEv1/phase-4/blockers.md` | Recorded resolved foundation dependencies and observed capacity blocker. |
| `docs/DEv1/phase-4/handover.md` | Documented the verified input stream and remaining Phase 4B gates. |
| `docs/DEv1/phase-4/implementation-status.md` | Added the package checklist and the six frozen decisions. |
| `docs/DEv1/phase-4/acceptance-result.md` | Added this result and reproduction report. |

Generated summaries in `docs/DEv1/phase-4/core-evidence/` are `canonical_receipt_sample.json`, `clickhouse-query-window.json`, `entrypoint_isolation.json`, `pipeline-accelerated.json`, `pipeline-clickhouse-outage.json`, `pipeline-normal-sustained.json`, `pipeline-prefix-oracle.json`, `pipeline-restart.json`, `real_crash_barriers.json`, `real_independent_invalidation.json`, `real_oracle_restart.json`, `real_receipt_race_0.json`, `real_receipt_race_1.json` and `test-results.json`. Raw JUnit/logs and benchmark samples, the generated Compose specification and isolated producer artifacts remain in `.pytest_cache/phase4a-*` as local reproducibility data. Disposable pipeline databases remain in the separate Compose project's stopped named volumes. No repository files were deleted or staged. HEAD stayed at the approved plan commit.

