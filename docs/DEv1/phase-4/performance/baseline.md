# Verified performance baseline

**Core evidence snapshot:** `a92767728ee6eec906a26104a2cb7fb0d33b8bb2`; **planning revision baseline:** `ca5cc3d7abc2b363922f885fa0999812dae48b83`, branch `codex/phase-1-v1-raw`.
**Source files:** Core `core-evidence/pipeline-normal-sustained.json` (229 time-series samples), `pipeline-accelerated.json`, `pipeline-prefix-oracle.json`, `pipeline-restart.json`, `pipeline-clickhouse-outage.json`, `clickhouse-query-window.json`, `test-results.json`, and `acceptance-result.md`; Phase 2 tracked reports/samples in `de/artifacts/bronze_v1/acceptance/`.

## Recalculated normal sustained trial

The JSON reports a steady interval of 544.921664 seconds. For monotonic counters, rate is `(last-first)/elapsed`; the recorded rate fields are consistent with this formula within rounding. Queue growth is calculated in its own native unit.

| Quantity | Start | End | Recalculation / result | Meaning |
|---|---:|---:|---:|---|
| SUMO simulation time | 61 s | 600 s | 539 / 544.922 = 0.989 simulation s/wall s | Diagnostic step configuration only. |
| Source/Outbox ACK | 305 | 3,000 | 2,695 / 544.922 = 4.946 observations/s | V1 semantic observations offered and ACKed. |
| Raw persisted deliveries | 300 | 2,991 | 2,691 / 544.922 = 4.938 deliveries/s | Physical delivery grain; at endpoint nine Raw deliveries pending relative to received. |
| Bronze change sequences | 183 | 2,945 | 2,762 / 544.922 = 5.069 sequences/s | Semantic journal grain; differs from delivery grain. |
| Silver applied sequences | 21 | 544 | 523 / 544.922 = 0.960 sequences/s | Applied contiguous source sequence prefix. |
| Gold change ACK sequences | 21 | 544 | 523 / 544.922 = 0.960 sequences/s | Gold durable ACK; it followed Silver prefix. |
| Bronze → Silver pending | 162 | 2,401 | +2,239 / 544.922 = +4.109 sequences/s | Persistent growth during this interval; confirmed insufficient capacity. |
| Silver → Gold pending | sampled max 10 | 0 end | Endpoints and sampled maximum only | Does not certify Gold work. |
| Gold completed foundation work | 0 | 1 | One proof completed | No sustainable work rate can be inferred. |

Last snapshot: Outbox 3,000 ACKed; Raw 2,991 persisted; Bronze 2,945 change sequences; Silver 544 applied; Gold 544 ACKed. The values count different types of work. Do not subtract semantic change sequence counts from delivery counts as a loss measure.

The steady source byte rate in the artifact is 19,776 bytes/s. Accelerated trial emitted 300 observations for 60 simulation seconds at 31.138 observations/s during active capture; 295 Kafka ACKed and five remained pending at sampled endpoint. This finite burst ran while earlier backlog was draining. It is not a requirement to support 31 observations/s and does not certify capacity.

These source counters are not interchangeable: expected/scheduled captures, successfully captured entities, durable Outbox rows, Outbox ACK, Kafka deliveries, Raw persisted deliveries, Bronze COMPLETED physical-input intents, semantic journal sequences, Silver applied sequences and Gold work each have distinct boundaries. During outage/overload, Kafka ACK can lag actual capture, and a capture slowdown can falsely make downstream appear sustainable. Instrument and report every counter independently.

## Latency and Gold work

| Metric | Recalculated retained sample | Scope and limit |
|---|---|---|
| Source capture → durable Gold work intent | n=544; p50 224.273 s; p95 459.799 s; p99 484.371 s; max 494.599 s | Acknowledged observations only; includes queue waiting and processing. No statement about outstanding records. |
| Gold page/read → durable ACK | 317 pages; p50 0.437 s; p95 1.906 s; p99 3.880 s; max 5.774 s | Feed iteration/commit duration; Silver applied-at to ACK timestamp is not public/measured. |
| Gold ACK transaction | p50 0.0118 s; p95 0.982 s; p99 2.849 s; max 5.532 s | Transaction timing distribution, n=317. |
| Materialization member reads | 440 reads; 9,117,236 encoded bytes; p50 0.271 s; p95 0.893 s; p99 3.387 s; max 5.959 s | Read-step distribution; 441 timing samples in the raw artifact. |
| Completed work proof | 1 sample, 219.959 s | Scheduler interleaving included; cannot be converted to capacity or p95. |
| Candidate scheduling delay | 1 sample, 0.015 s | No distribution claim. |

Gold acknowledged the Silver prefix but only completed one proof. The 60s/300s windows each create memberships, and dirty generations may create recomputation. Work throughput must be measured separately from ACK throughput.

## Resource/query observations

| Measurement | Retained value | Qualification |
|---|---:|---|
| Gold process peak RSS | 76,333,056 bytes (~72.8 MiB) | Sampled tested profile only. |
| Gold SQLite file | 2,633,728 bytes | Point/endpoint; metadata is retained across sequence/work history. |
| Gold WAL sampled peak | 4,293,072 bytes | Sampled max, not a disk growth projection. |
| Gold CPU time | 87.233 s | Process counter over the trial. |
| Gold reader ClickHouse | 13,332 completed queries | Separate Gold reader principal. |
| Shared upstream ClickHouse principal | 49,761 completed queries; query p50/p95/p99 4/13/28ms, max 1,547ms; physical rows read 27,273,768 | Shared principal, not assignable to individual stages. |
| Gold reader ClickHouse | query p50/p95/p99 6/18/40ms, max 1,885ms; physical rows 13,328,821 | Query counters do not mean observation count. |

The pipeline ran on Windows/Python 3.14.4 with a Docker VM reporting 12 CPUs and 8,186,413,056 bytes RAM; ClickHouse 24.3.18.7 and Docker Engine 29.3.1 are in the Core report. This is the benchmark environment, not an approved production resource envelope.

## Independent oracle and recovery evidence

The prefix oracle verified 3,000 Outbox ACKs and Bronze identities, 597 Gold sequences across 345 receipt intervals and exactly 1,194 memberships. Its own `full_drain=false`; therefore it does not certify the full 3,000-event pipeline drain. Restart resumed same binding from Gold change cursor 693 to 695, with 23.969 s to progress. Isolated ClickHouse outage held cursor at 837 while failing, then advanced to 862 with the same binding after 37.331 s stop-to-progress. These are targeted safety/recovery cases, not sustained recovery/headroom objectives.

Stage B correctness, actual transaction crash barriers, invalidation races and isolation passed as documented in Core acceptance; this says nothing by itself about sustainable whole-path capacity. Existing broad tests report 472 pass, 50 skips and one inherited missing Gold4 runbook failure in the selected regression group; architecture run reports 117 pass and 10 inherited failures. Do not present that architecture run as clean.

## Historical context

| Phase | Retained result | Valid interpretation |
|---|---|---|
| Phase 1 Raw V1 | 6,000 observations / 1,200 simulation seconds, about 15.57 Raw deliveries/s; zero final Kafka lag; isolated Raw-focused stack | Producer/Raw workload observation, not Raw max or full-path capacity. |
| Phase 2 final `ffbed382b2` | `de/artifacts/bronze_v1/acceptance/phase2-measured-ffbed382b2/report.json`: 600 events; 14 samples; catch-up 12.3217934 s; total 42.2592266 s. | Independently read tracked JSON; finite post-source catch-up, not λ/μ or sustainable throughput. |
| Phase 2 prior `70f64d12e4` | `de/artifacts/bronze_v1/acceptance/phase2-measured-70f64d12e4/report.json`: 600 events; 23 samples; catch-up 37.3013543 s; total 73.9950448 s. | Finite catch-up under a different stack/profile; not a capacity sweep. |
| Phase 2 independent hardening `bc3c691c4a` | `de/artifacts/bronze_v1/acceptance/independent-hardening/hardening-5383496c/phase2-measured-bc3c691c4a/report.json`: 600 expected/Outbox ACK/Raw/canonical/lineage rows; 14 samples; catch-up 15.8129024 s; total 49.3353630 s. | Separate finite hardening trial with source verification; not comparable as a continuous rate. |
| Phase 2 sampled difference | `.../phase2-measured-ffbed382b2/samples.json`: for each row compute `raw.raw_deliveries - bronze.processing_status_counts.COMPLETED`; max **415** at elapsed 29.94s (2,030 minus 1,615). | Samples may observe different transaction boundaries; this is a sampled counter difference, not necessarily an exact simultaneous durable backlog. It does not establish Bronze maximum service capacity. |
| Phase 3 | A34 explicitly optional; later 600-event source oracle and eventual cursor 2,200, obligations reconciled | Correctness and finite backlog recovery; no sustained capacity certification. The ~2.637s→0.002115s query improvement is one indexed pending-obligation query, not a whole-stage profile. |
| Phase 4A-Pre | no whole path capacity result; five-event early attempt around 14–15s and 0.25 obs/s focused test path | Fixture-specific throughput symptom only. |
| Phase 4A-Core | 4.946 obs/s offered; Silver 0.960 seq/s; Bronze→Silver backlog growth 4.109 seq/s | Current retained diagnostic clearly insufficient at this offered workload. |

All cited Phase 2 report/sample paths are confirmed by `git ls-files`; reports were parsed directly and the maximum sample difference was recalculated as described above. These are finite 120-second simulation workloads. Phase 1–3 runs have different implementation/environment boundaries.

## SUMO workload profiles

Normal source defaults: `SUMO_STEP_LENGTH=0.01` simulation seconds (`Visualize/configuration/model_parameters.yaml`), `PUBLISH_INTERVAL=1.0` simulation seconds, participants `A,B,C,D,M`, and `OBSERVATION_INTERVAL_SEC=1.0` simulation seconds. Scenario changes may request extra captures. V1 publish and Kafka Outbox flags default off and must be explicitly recorded as enabled for the benchmark. Wall pacing is separate. Core's benchmark overrides the step to 1.0s, materially different from normal operation.

| Profile | Definition and use |
|---|---|
| W1 Historical Comparable | Reproduce Core's 1.0s diagnostic step, 600 simulation seconds, five participants, same payload/topology/scenario/seed/pacing and isolated stack. Matched before/after only; not intended-operation certification. |
| W2 Intended Operation | Normal 0.01s step, G0-approved capture/cache/publish settings, participants/scenario, and normal co-resident stack. Current expectation near five observations/wall-second at 1x is only a hypothesis until measured with capture counts and simulation pacing. |
| W3 Controlled Stress | Reproducibly increase offered work using actual accelerated SUMO and/or clearly labeled deterministic component fixtures. Keep declared cadence, payload and semantics; component fixtures do not certify full E2E capacity. |

Report steps/s, simulation seconds/wall second, scheduled cycles, completed captures, event rows/cycle, bytes/s, Outbox inserts/ACK/pending, Kafka ACK and each stage's unit. A run whose source slows below the approved pacing is not a passing offered-load trial.

## Measurement limitations

- Core used a one-second SUMO step; normal YAML config is 0.01s. It is a diagnostic profile, not W2.
- The report retains collision and detector-fingerprint warnings. The observed workload needs a clean semantic/fidelity review and matched normal configuration.
- Some regression tests ran concurrently and could contend for resources.
- The measured path is isolated DE/SUMO and not the complete normal stack with Projector, Orion-LD, MongoDB, Spring, PostgreSQL and Dashboard.
- Producer completed 3,000 but end counters show Raw 2,991, Bronze 2,945, Silver/Gold 544; independent later prefix oracle is only 597 and `full_drain=false`.
- No nested Silver operation profile, no individual stage CH attribution, no exact Silver applied-at timestamp to Gold ACK, no backlog-age distribution by stage, no sustained invalidation churn, no Gold work capacity with a faster source, and no completed-work drain evidence.
- 229 samples enable trend inspection, not a controlled maximum-capacity claim. Do not invent confidence intervals without retained raw sample reanalysis and trial methodology.

## Verdict

**Current measured capacity is insufficient** for the offered 4.95 observation/s profile because the Bronze-to-Silver sequence backlog grew at about 4.11/s. Silver is the first confirmed backlog bottleneck. Gold window work is the highest-priority downstream unknown. Raw/Bronze/producer observed progress near offered rates in this interval does not establish spare capacity or headroom. No latency SLO, supported operating workload, recovery objective or resource envelope is approved.
