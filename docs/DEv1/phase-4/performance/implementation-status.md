# Local performance execution

Execution starts from `df816be536c87b6ee36bdfddcf729e547d1c1ca9` on `codex/phase-1-v1-raw`. The only edit at resume was the Goal's Gold replay regression. The reviewed plan remains the reference. The implementation authorization governs G0, provisional G1 targets and benchmark duration.

## G0

The host has an Intel i5-11400H with 12 logical processors, 16,889,204,736 bytes of RAM and 36.51 GiB free on D: at preflight. Docker reported 12 CPUs and 8,186,417,152 bytes of memory. SUMO reports 1.27.1. Docker Desktop needed to start outside the sandbox. The owner ClickHouse, Orion, MongoDB and context provider containers were already running after startup.

W1 keeps the retained 1.0-second diagnostic configuration. W2 uses the normal 0.01-second SUMO step at realtime pacing, the accepted simulation-time capture configuration and nodes A/B/C/D/M, with required application coexistence at qualification. W3 labels accelerated SUMO, retained verified source and component fixtures separately. The V1 publisher uses `--v1-observations --v1-publish` and its own durable Outbox. The legacy Kafka publisher and legacy Outbox flags are false because V1 startup rejects them. The first diagnostic records effective settings and actual source progress.

The initial disposable project is `smart-perf-w2-base-01`. The generated Compose specification binds ports 18124, 39092 and 18095–18098 on loopback. Kafka topics and groups operate in a separate broker. ClickHouse `smart_traffic` and the Gold read-only principal operate in a separate server. Raw, Bronze, Silver, Gold, Kafka and ClickHouse use distinct project volumes. The producer Outbox and archives are under `.pytest_cache/perf-w2-base-01/producer`. No replay or owner checkpoint paths are mounted.

## Execution gates

- F01 tracks source window crossing, promotion and current work alongside ACK. F02 requires a measured package benefit and records the next bottleneck before final acceptance. F03 separates backlogged and paced Gold capacity.
- F04 treats capacity shortfall in diagnostics as profiling evidence and halts unsafe or invalid runs. F05 considers intra-event reconciliation first and requires dedicated ordering and recovery proof for cross-event grouping. F06 requires upstream current validity on any cache path.
- F07 bounds each coherent diagnostic round to approximately 30 minutes and most measurements to at most ten minutes. Final sustained qualification has a separate 30-minute authorization. Extended endurance needs explicit approval. F08 uses the current model until the technical difficulty or cost justifies owner-directed handoff.

## PERF-00B

The focused reproduction failed with `AUTHORITY_UNAVAILABLE` escaping final replay discovery. The completion lane now retries with normal backoff and retains fail-closed handling of binding replacement. The local Core foundation suite passed 14 tests. The combined replay run passed 21 tests with one legacy HTTP TestClient case deselected after it stalled in the first attempt. The HTTP case still needs independent classification. The fixture verifies unchanged live SQLite bytes. All seven enabled Stage B tests and ten Silver feed tests passed on the isolated ClickHouse instance with the direction grouping candidate. Thirty-five focused Silver tests passed. A six-test recovery rerun covered the two new partial and ambiguous grouped write cases. Test counts overlap where suites were repeated. JUnit is retained under `.pytest_cache/perf-*.xml`.

## First PERF-01 trial

Question: Does the normal-step source sustain its intended pace and which measured Silver costs explain its service rate? Retained W1 evidence used a different step and lacked nested Silver timings. Use 90 simulation seconds at realtime pacing, about 45 active samples and at least one source crossing of a 60-second window. A 300-second completion requires a later trial. Allow up to 90 seconds for Compose setup, roughly 5 seconds of initial sampling and up to 45 seconds of bounded drain. Stop early on integrity faults, unusable source pacing or unsafe resource pressure. Results will guide Stage L0 experiments and help select the first optimization. Artifacts live in the project data directory.

## Current checkpoint

PERF-00B replay work has passed focused and real Core tests. PERF-01 measured the first accumulating queue and attributed the largest Silver and retained Gold read costs. Further upstream operation attribution, paced Gold profiling and integrated resource measurement are open. PERF-04 same-event direction grouping reduced Silver's output reconciliation cost per accepted event and raised its progress in the comparison. Silver remained the first accumulating queue. PERF-05 grouped fact verification raised retained Gold read rate from 5.49 to 7.41 members/s. The 17 enabled feed and Gold Core regressions passed on isolated ClickHouse. The scheduler candidate removes waits after a full pass with verified backlog. Its 29 targeted Silver tests passed. The fresh scheduler diagnostic tests Silver backlog and Gold work after two 60-second window crossings. See [profiling-result.md](profiling-result.md).

The first full comparison attempt and the first retained read attempt hit the 1 GiB available host memory floor before measured workload. A later full comparison stopped during production after 50 ACKed rows. The read probe and fresh comparison completed after host memory recovered. All runs keep separate fixture volumes and stop their services after collection. Use the earliest bottleneck still supported by measured queue trends to select the next change. The cumulative time budget includes the baseline and fresh comparison sampling, the early-stopped 50-row run, retained read probes and the scheduler diagnostic. Later acceptance uses its separate duration authorization.

Technical acceptance remains open. PA-01, PA-02, PA-09 and PA-11 require a representative paced trial and completed-work evidence. PA-03 through PA-05, PA-08 and PA-10 require measured stress, recovery, history or resource coverage. PA-06, PA-07 and PA-12 have targeted correctness evidence and still require their remaining applicable checks. Phase 4B remains gated on technical acceptance and owner review.

## Retained source follow-up

The canonical inventory and Bronze point read remove repeated reads for exact keys. The paired probes verified whole-page and receipt equivalence. The latest Bronze/Silver focused suite passed 32 cases. The final real ClickHouse feed/Core run passed 18 cases and skipped one optional capacity case. A fresh retained Gold replay drained its single eligible 60-second work item. See [profiling-result.md](profiling-result.md) for timings and [acceptance-result.md](acceptance-result.md) for remaining gates.

## Local inventory

- Runtime: `de/bronze/clickhouse_repository.py`, `de/gold_runtime/processor.py`, `de/silver/canonical_reader.py`, `de/silver/incremental_reader.py`, `de/silver/metrics.py`, `de/silver/processor.py`, `de/silver/repositories.py`.
- Tests: `de/tests/bronze/test_v1_point_read.py`, `de/tests/gold_runtime/test_core_foundation.py`, `de/tests/silver/test_incremental_reader.py`, `de/tests/silver/test_v1_journal_worker.py`, `de/tests/silver/test_v1_recovery.py`, `de/tests/silver/test_v1_runtime_startup.py`.
- Tool: `de/tools/gold_core_capacity.py`. It records effective source settings, host samples, eligible work and retained read evidence.
- Independent evidence tool: `de/tools/gold_capacity_evidence.py`. It independently compares the stopped fixture through Raw bytes, Bronze lineage, physical Silver facts and Gold completed work.
- The broad local regression passed 475 tests and skipped 51 live or optional tests. One inherited test failed due to the absent `docs/gold/GOLD_4_RUNBOOK.md`. The initial regression exposed a missing metrics object on a test double. The fixture now supplies it and the suppression assertions pass. The health HTTP test passes with loopback enabled.
- Local records: this file, `profiling-result.md`, `optimization-decisions.md`, `acceptance-result.md` and `execution-handoff.md`.
- Evidence: the `.pytest_cache/perf-w2-*` project directories, the `.pytest_cache/perf-*.xml` result files and related test temporary directories. A local pinned dependency copy from test setup was retained at `.pytest_cache/perf-resume-deps/`. The benchmark containers used the pre-existing `.pytest_cache/phase4a-deps/`.
- The initial checkout was clean apart from the Goal's own replay regression. HEAD and the index match the starting revision. The isolated project configurations are generated artifacts. No tracked runtime configuration or migration was edited.
