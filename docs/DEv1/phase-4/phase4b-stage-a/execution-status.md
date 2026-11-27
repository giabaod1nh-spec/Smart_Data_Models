# Stage A execution status

Reviewed baseline and initial local/remote: `05c592d30faa9ace6cdb589d0a771f7031f01a0a`, branch `codex/phase-1-v1-raw`, clean working tree/index.

## Approval record

On 2026-11-27 (Asia/Bangkok), the Project Owner and Performance Acceptance Owner extended D-A14 to UP-01, UP-02A/B and A-I01–A-I07 functional implementation and correctness in isolated, disposable development environments. A finite source workload may drain sequentially. Physical source-to-Gold verification and approved metric/coverage semantics remain mandatory. Phase 4A performance qualification, representative performance tests, coexistence, resource capacity and operational acceptance remain open for the later milestone. See the [authoritative amendment](../performance/acceptance-result.md).

On 2026-11-27 (Asia/Bangkok), the requester confirmed both the project owner and Performance Acceptance owner roles and approved the frozen D-A14 exception. The effective amendment is in [Phase 4A Performance Acceptance](../performance/acceptance-result.md). The requester separately confirmed authority and approval for UP-01 (source, contract and Silver) and UP-02A/B in the frozen isolated scope. The requester also approved G-MART/G-REV design for A-I03 on disposable stores only. Official source adoption, G-COV, live publication, full-entry performance and operational gates retain their requirements.

## Implementation and evidence

| Package | Current status | Evidence / next dependency |
|---|---|---|
| A-I01a | Implemented, verified on isolated fixtures | Pure schedule/slot reducer and independent metrics in `de/gold/business.py`; `python -m pytest de/tests/gold/test_stage_a_fixture_oracle.py -q`: 12 passed. Fixture oracle covers both durations, three grains, quality, weighted rollups, direction P95 and network max, missing/conflict/waiting/invalid, deterministic bytes and member cap. No official canonical input or publication. |
| UP-01 | Isolated producer, Silver contract and Gold cursor fixtures verified | Integer-microsecond schedule, participant-complete atomic settlement with V1 Outbox, sparse failures, bounded pause/resume and terminal journal. Windows-safe archive export retries without a SQLite write transaction. Silver receipts stop at delayed canonical evidence. Gold separately commits metadata only after applicable change and invalidation watermarks. Integrated source/replay and source adoption remain open. |
| UP-02A/B | Isolated source and metadata fixtures verified | Opt-in raw detector and stopped-length components produce corrected values in the archive sidecar. Shared V1 facts retain legacy values. Silver recomputes values, matches the original or linked suppression, and validates revision lineage. Gold fences correction and withdrawal. Sampled independent SUMO oracle covered all 17 approaches. Source adoption and broader scenarios remain open. |
| A-I02 | Isolated SQLite state and work handoff verified | Disposable v1→v2 migration, lease-bound reservation, atomic Core proof and durable Business projection, metadata-only wake-up and stale-generation rejection. Fixture work reuses the Core stream interface and can republish a new revision against unchanged completed Core proof after a correction. Live coordinator integration and capacity qualification remain open. |
| A-I03 | Isolated publication state machine and adapter fixtures verified | Immutable result bytes, exact row-set verification, retry after partial write and SQLite fence. The three-grain ClickHouse adapter is isolated by database name and has an opt-in physical test. Physical execution remains open. |
| A-I04–07, functional integration | Authorized, correctness open | Proceed on the isolated development path under the revised D-A14 scope. Source and metric acceptances and source-to-Gold correctness remain required. Representative performance and operational qualification are deferred and open. |

Host Python lacked the repository-pinned `rfc8785==0.1.4`. Tests use the unchanged dependency installed in ignored `.pytest_cache/phase4b-deps` (add to PYTHONPATH). Phase 4A Performance Acceptance remains ENVIRONMENT BLOCKED. These fixture results carry no PA-01–12 credit.

## Realtime compatibility evidence

The owner instructed the implementation to preserve the existing Realtime capture, delivery and projection behavior. Corrected source measurements and schedule capture are explicit isolated paths. They have no activation in the default provider, active run, or V1 publisher. Historical V1 shape, event identity and retry rules retain their behavior. Projector, Orion-LD, Realtime API and Dashboard source are unchanged.

| Source behavior | Observed impact and isolation |
|---|---|
| Occupancy and Spillback | The legacy unit heuristic treats a TraCI reading of `0.8` as `80%` inbound and `0.8` outbound pressure. In the isolated version it is `0.8%` inbound and `0.008` outbound pressure. This can change threshold outcomes. The default provider retains the legacy conversions and emits no new provenance. |
| Queue Length | The live provider historically uses the nonzero E2 queue when present and otherwise uses the route estimate. The isolated method fixes the population per configured approach, preserves a complete E2 zero, and marks an incomplete or unavailable measurement accordingly. The same stopped-vehicle fixture gives 13 m by the existing fallback and 0 m by the isolated complete E2 method. Qualification requires separate evidence for each method and scope. |
| Observation cadence | Existing real-run evidence includes clock drift, such as nominal 60 s with actual 60.04 s. The isolated producer rejects mismatched clock/step and can seal an explicit failure without creating a V1 event. This can change which cycles appear if adopted. The current sampler, run selection, freshness and event-ID rule continue unchanged. |
| Wire compatibility | The owner selected archive sidecars. Shared V1 schema, serializer, canonical cycle and run manifest match baseline `05c592d` exactly. Corrected measurement records carry original event ID and payload hash, run and generation, archived manifest and topology, measurement method, scope and revision. The shared V1 event and Silver V1 facts retain their original bytes and values. |

Python regressions for the existing projector, canonical cycle, observation contract and Silver plus the source fixtures passed 78 tests. The existing Dashboard freshness policy and transforms passed 28 tests in a disposable copy. The architecture keyword scan produced its existing non-blocking warning.

The previous projector microbenchmark evaluated an in-envelope option that was subsequently removed. The latest differential check ran the exact source and Outbox modules from `05c592d` and compared 192 capture combinations covering E2 availability, mixed/dedicated lanes, jam zero/positive, occupancy edge and error values and quality flags. The default Outbox insert, pending retry and already-delivered ACK traces also matched the baseline. Projector, Orion-LD, Realtime API, Dashboard and AI controller were untouched. Representative Realtime coexistence belongs to integrated acceptance.

## Metadata authority and completion boundaries

Producer checkpoints and raw measurement revision rows share the dedicated isolated Outbox transaction with frozen V1 bytes on original capture. The bounded export writes and synchronizes immutable objects first, then advances its head. Windows uses a write-through rename and a non-replacing move for immutable files. The archive I/O runs outside SQLite writer transactions. Only the new disposable factory stamps the application identity, and reopening compares that identity and the saved schedule binding before applying schemas. Export remains off the Realtime runner.

The Silver public sidecar API verifies archived run identity, contiguous schedule and measurement revisions, linked canonical Bronze evidence, Silver's validity and effective or conflicted dispositions, and a final source boundary. Corrected measurement values are recalculated from archived raw components. A metadata cursor is separate from the existing public feed. Gold uses the public Silver interface and its own SQLite cursor, verifies required Core watermarks, and suppresses stale Business pointers in the same transaction as a correction. The tested path uses disposable SQLite, an explicit canonical-authority fixture at the Silver boundary and a ClickHouse adapter test double. Official proof requires a physical run with independent source oracle and retention/failure cases.

Legacy observations without direct raw evidence cannot be corrected from their published values. A schedule with strict clock/step checks can omit a shared V1 observation at a mismatched tick. The isolated source therefore remains separate from the default Realtime capture, and Stage A publication remains gated. A terminal window with members needs the approved Core eligibility path when the Core watermark cannot reach its end. D-A14 permits only the isolated subpackages, and full A-I04–A-I07 remain subject to full entry and respective gates.

On 2026-11-27 the first isolated SUMO run stopped at the 2.5 GB host guard (2,499,563,520 available bytes). After host memory recovered, SUMO 1.27.1 ran with seed 42, 0.01 s steps and no Realtime services on a scratch copy of the same pinned E-01 inputs. The oracle found two stopped-vehicle samples whose current route index did not prove approach membership. The isolated route code now marks these samples unavailable. The rerun took 13.557 s with a minimum 5,020,585,984 available host bytes. All 301 tested 1 Hz ticks matched the integer schedule, including 59.01/60.01 and 299.01/300.01. Direct raw getter arithmetic matched 119 inbound E2, 119 inbound lane, 35 outbound E2 and 35 outbound lane results. The independent vehicle/route oracle across 17 approaches found 108 measured queue zeros, 9 matching positive queues and 2 correctly unavailable route samples. This probe sampled seven capture times and supplies source evidence for the frozen scenario. It does not establish physical Silver persistence, live source adoption, coverage authority or pressure calibration.

The isolated Gold reducer profile used five intersections, 17 approaches, 23 result rows and 12 repeated computations at each duration. Every run matched independent output values and produced byte-identical results. At 60 s, 300 members took a median 15.23 ms and a maximum 15.92 ms, with a 14,341-byte result and 105,156 peak newly traced bytes. At 300 s, 1,500 members took a median 70.99 ms and a maximum 80.35 ms, with a 14,675-byte result and 584,080 peak newly traced bytes. This profile measures computation and encoding. DE I/O, backlog stability and drain need representative integrated evidence.

The Docker Linux API was unavailable and WSL listed only the Docker Desktop system distro. No disposable ClickHouse endpoint was available. The physical adapter fixture remains skipped. Official source-adoption and Phase 4A paced performance gates remain open.

## Isolated A-I02/A-I03 recovery verification

- Disposable Gold checkpoints require a dedicated directory and file-identity marker before SQLite opens them for writing. The v1 rehearsal and earlier v2 fixture shape both retain Core rows and Business result bytes. A current-generation change clears the Business pointer and advances its epoch inside the Core SQLite transaction for the isolated schema. An over-limit member count holds its work before reservation, and the next admissible window can proceed. Business materialization checks the cumulative encoded receipt and wall-time budgets.
- Exact read-back and effective publication now use two SQLite transactions. A durable verification record binds the row-manifest digest and claim to the scope epoch. The pointer can advance only while that epoch, the Gold lease and the current Core proof still match. After interruption, reconcilable staged results keep the same payload and revision. The fourth claim is reserved for exact read-only reconciliation. Both identical recovery and stale-generation suppression were exercised across reopening the disposable checkpoint.
- The relevant Gold suites passed 31 cases with one physical test skipped. The physical test now uses the real three-grain adapter and the SQLite publication protocol together. It stages one physical grain, interrupts, reconnects and reconciles the remaining grains, then interrupts again after durable verification and checks the effective pointer on restart. The test requires an explicitly configured disposable `STAGE_A_CH_PROBE` endpoint. Its database name must match `stage_a_probe_<16 lowercase hex digits>`. The fixture uses `STAGE_A_CH_PROBE_HOST`, `STAGE_A_CH_PROBE_PORT`, `STAGE_A_CH_PROBE_DATABASE`, `STAGE_A_CH_PROBE_USER` and `STAGE_A_CH_PROBE_PASSWORD` from the process environment and checks the frozen host, disk and wall-time guards.

## UP-01 horizon and process-recovery fixtures

- Gold classifies an aligned 60 s or 300 s interval from the locally applied schedule, slot evidence and lifecycle clock, including intervals without a Core window. A complete slot list alone remains `WAITING` while the clock is short of the boundary. An applied metadata-only terminal receipt makes a full interval of explicit missing outcomes `MISSING_WINDOW`, and marks an interval that ends beyond the cutoff `TERMINAL_PARTIAL`. Reopen checks retained the classifications without creating Core windows, advancing the canonical cursors, or writing a Business result. For member-bearing intervals, Core still owns admission to the verified proof path.
- Silver-reported conflict and later Core suppression retain distinct conflict status and consume their expected slots without metric credit. A window containing only those claims reports `CONFLICTED_WINDOW`. A mixed fixture keeps the measured-zero slot as valid coverage and reports the network result unavailable when its expected coverage is incomplete.
- A child process abruptly exited before commit, after commit, and after writing immutable archive files before replacing the head. Disposable reopening verified atomic Outbox rows, settled horizon and measurement evidence, byte-identical V1 retry, and no premature head advance. All 43 tests in the affected Gold oracle, Gold state and UP-01 Outbox suites passed. These runs exercise FZ-02 and FZ-03 in isolation. Full source and integrated acceptance remains open.

## Runnable functional checkpoint

| Package | Implemented and verified at this checkpoint | Remaining evidence |
|---|---|---|
| UP-01 | The isolated SUMO launcher wrote 1,500 original V1 events in 300 slots across five intersections and exported 22 schedule segments. Reopening the same Outbox installed its local registry without recapture. The existing Silver archive resolver accepted the terminal registry, run/manifest identity and both published archive heads. | Delivery on the dedicated Kafka topic, physical Raw/Bronze/Silver drain and negative-path reconciliation with this retained run. |
| UP-02A/B | The 1,500 raw measurement objects reconcile with original V1 anchors. An independent oracle checked 5,100 Occupancy, 5,047 Queue and 1,500 Spillback observations. Queue coverage explicitly preserves the 53 missing direction samples. | Physical Silver and Gold reconciliation. Pressure method, weights, severity thresholds and empty-traffic policy still require G-CAL. |
| A-I01–03 | Gold uses the exact canonical Core run key for both metadata streams. Terminal source evidence makes full member-bearing windows eligible without inserting a fake observation or advancing the canonical clock. One bounded public Silver traversal per window covers all Business metrics and grains. ClickHouse writes immutable rows with typed metric and quality columns. | Physical DDL, read-back, failure recovery and validation of published 60s/300s values against the oracle. |
| A-I04 | Gold Reader pins SQLite revisions, checks the full physical row manifest and rechecks the fence. It supports run catalog, latest/window/range selection, network and intersection metrics, direction trends, coverage gaps, traffic context, queue diagnosis and adjacent-window deltas. | Physical query evidence. Official pressure ranking, heatmap values and pressure persistence require G-CAL. |
| A-I05 | Pair comparison checks immutable manifests and comparable conditions before calculating matched-window summaries, per-intersection deltas and trends from Gold. | An actual Adaptive run plus verified treatment and comparability evidence. The retained run is Fixed-Time only. |
| A-I06 | Dedicated source registry and Outbox reopening are retryable. The coordinator fixture reopens the same published revision, then replays a terminal window into a separate checkpoint and namespace with the same Core proof and exact Gold rows. | Execute source-to-Gold replay and verify physical results, correction, interruption and invalidation. |
| A-I07 | The actual source oracle expects five 60s windows (115 rows) and one 300s window (23 rows). The runnable Gold drain reports verified members, requests, candidate durations, pending work age and publication counts. | Actual Kafka-to-Gold finite drain, value and lineage reconciliation, with the physical environment described below. Representative performance and operational acceptance are deferred. |

Core retains original event-ID proof order. Stage A groups up to eight contiguous change sequences and lets the public Silver reader enforce its lower configured caps. With a three-entry fixture cap, 12 members required four page requests and produced the same proof digest as 12 single-member requests. The isolated 60-second per-work deadline still applies to the original fixture entry point. The runnable drain uses its explicit wall budget and releases an unsealed claim on expiry. Projection encoding on the retained dataset produced 18,218–20,254 bytes for the 60-second windows and 99,034 bytes for the 300-second window before optional window context. The raw 300-second projection was 252,798 bytes. The original durable 256 KiB cap still applies to compressed bytes. The 600-slot, five-intersection source cap admits at most 3,000 initial measurement receipts plus schedule pages below the existing 4,096-receipt and 32 MiB bounds.

The expanded suites recorded 48 passes and one skipped physical ClickHouse test. A separate affected Core/Foundation run recorded 47 passes, including tests that overlap the expanded suite. The Docker Linux pipe was absent, and the session had no `STAGE_A_CH_PROBE*` configuration. No physical acceptance was recorded. The pinned catalog matched the actual SUMO network. Its detector profile declares a different expected file hash, which remains visible in the source manifest and blocks any assumption of broader detector/source qualification.

## Reproduce the disposable physical validation

Run from the repository root with the existing DE and Visualize dependencies, SUMO and `psutil`. Use a new ignored scratch directory and keep the 2.5 GB available host memory and 512 MiB disk reserves. Each worker runs in the foreground. Record its counters in a second terminal and stop that worker after it drains before starting the next one. Use the dedicated stack and confirm its ports are free before starting it.

1. Use the retained Outbox where available, or make a new fixed-time capture. Keep the JSON output with run ID, generation, topology and manifest digest.

   ```powershell
   $scratch = (New-Item -ItemType Directory -Path (Join-Path .pytest_cache ("stage-a-verify-" + [guid]::NewGuid().ToString("N")))).FullName
   python -m Visualize.tools.stage_a_source capture $scratch --duration 300 --seconds 300
   python -m Visualize.tools.stage_a_source export $outbox
   python -m de.gold_runtime.stage_a_oracle $outbox
   ```

   Set `$outbox` and `$archive` from the launcher output. For the retained local dataset, the archive is `.pytest_cache/stage-a-schedule-xj1wv3tl` and the Outbox is its `scheduled-outbox.sqlite3`. Use a resolved absolute path for `$archive`.

2. Start Kafka and ClickHouse using a new Compose project. This stack uses Kafka port 39092 and ClickHouse HTTP port 18124. Check the existing V1 topic and copy the cluster ID printed by the container into `$clusterId`.

   ```powershell
   $project = "stage-a-" + [guid]::NewGuid().ToString("N").Substring(0,8)
   docker compose -p $project -f docker-compose.v1-acceptance.yml up -d --wait kafka clickhouse
   docker compose -p $project -f docker-compose.v1-acceptance.yml run --rm init
   docker compose -p $project -f docker-compose.v1-acceptance.yml exec kafka /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server kafka:9092
   python -m Visualize.tools.stage_a_source deliver $outbox --bootstrap localhost:39092 --cluster-id $clusterId
   ```

3. Apply the existing V1 migrations to this ClickHouse only. Run Raw until `http://localhost:18097/cutover-ready` reports caught up and `/health` accounts for all 1,500 durable events.

   ```powershell
   $env:K4_CLICKHOUSE_HOST = 'localhost'
   $env:K4_CLICKHOUSE_PORT = '18124'
   python -m de.scripts.migrate_clickhouse --v1-bronze
   python -m de.scripts.migrate_clickhouse --v1-silver
   $env:DE_V1_RAW_BOOTSTRAP_SERVERS = 'localhost:39092'
   $env:DE_V1_RAW_CLICKHOUSE_HOST = 'localhost'
   $env:DE_V1_RAW_CLICKHOUSE_PORT = '18124'
   $env:DE_V1_RAW_LEDGER_PATH = Join-Path $scratch 'kafka_raw_v1/ledger.sqlite3'
   $env:DE_V1_RAW_HEALTH_PORT = '18097'
   python -m de.kafka_raw.v1_main
   ```

4. Run Bronze then Silver with their own disposable checkpoints. Save Bronze `/health` once partitions reach the retained Raw boundary. Save Silver `/health` once it reaches the resulting Bronze prefix. Retain both SQLite checkpoints and ClickHouse tables for Gold.

   ```powershell
   $env:DE_BRONZE_RAW_LEDGER_PATH = $env:DE_V1_RAW_LEDGER_PATH
   $env:DE_BRONZE_CHECKPOINT_PATH = Join-Path $scratch 'bronze_v1/checkpoint.sqlite3'
   $env:DE_BRONZE_CLICKHOUSE_HOST = 'localhost'
   $env:DE_BRONZE_CLICKHOUSE_PORT = '18124'
   $env:DE_BRONZE_HEALTH_PORT = '18098'
   python -m de.bronze.main

   $env:SILVER_V1_BRONZE_PATH = $env:DE_BRONZE_CHECKPOINT_PATH
   $env:SILVER_CHECKPOINT_PATH = Join-Path $scratch 'silver_v1/checkpoint.sqlite3'
   $env:SILVER_CLICKHOUSE_HOST = 'localhost'
   $env:SILVER_CLICKHOUSE_PORT = '18124'
   $env:SILVER_V1_ARCHIVE_ROOT = $archive
   $env:SILVER_HEALTH_HOST = '127.0.0.1'
   $env:SILVER_HEALTH_PORT = '18096'
   python -m de.silver.main
   ```

5. On that disposable ClickHouse server, allocate a database named `stage_a_probe_<16 lowercase hex digits>` and set the six `STAGE_A_CH_PROBE*` variables named in the physical probe above, including the opt-in. Set `$runId`, `$generation`, `$topology` and `$manifest` from the capture. Run the drain, record its JSON, and take `$runKey` from the catalog. The oracle fails if any of the six physical windows, 138 rows or physical metric values differ. Save the `replay_capture` object from the drain JSON and pass it using `--replay-capture` on a separately allocated checkpoint to run the frozen rebuild.

   ```powershell
   $gold = python -m de.gold_runtime.stage_a allocate $scratch
   python -m de.gold_runtime.stage_a drain --checkpoint $gold --run-id $runId --generation $generation --topology $topology --manifest $manifest --seconds 1800
   python -m de.gold_runtime.stage_a catalog --checkpoint $gold
   python -m de.gold_runtime.stage_a read --checkpoint $gold --run-key $runKey --duration 60 --start 0 --end 300
   python -m de.gold_runtime.stage_a read --checkpoint $gold --run-key $runKey --duration 300
   python -m de.gold_runtime.stage_a_oracle $outbox --checkpoint $gold
   python -m pytest de/tests/gold_runtime/test_stage_a_business_clickhouse.py -q
   ```

   Capture the output revisions, result and manifest hashes, duplicate/conflict outcomes and remaining work. The full pipeline run can be staged sequentially while Kafka and ClickHouse retain their durable data.
