# Phase 4A-Core post-Pre source and document audit

Audit baseline: branch `codex/phase-1-v1-raw`, commit `9adc6fc7d4dd6ecada8b01b6d53b6ffe2d3b6a1b` (`feat(de): implement Silver canonical incremental feed`). This is a source/document review, not a new runtime acceptance run. The Pre implementation and evidence at that commit are authoritative for this planning update. Gold Stage B has not run; whole-pipeline capacity is `NOT MEASURED`; production operational readiness is not established.

## Confirmed upstream source behavior

| Finding | Source/evidence |
|---|---|
| Phase 3's reader accepts a run/digest/time query, pages complete bundles by event key and opaque token, and `read_complete_scan()` accumulates a tuple. It exposes no production sequence checkpoint. | `de/silver/canonical_reader.py:61-103,303-406`. Preserve those APIs for compatibility, but limit full scan to bounded fixture/smoke use. |
| The additive feed is public on `CanonicalSilverReader`/`CanonicalReadSession`; request/response DTOs, strict limits and retryability live in Silver-owned modules. | `de/silver/incremental_contract.py:29-205,208-245`; `de/silver/incremental_reader.py:40-145,446-458`. |
| Discovery binds source identity and distinguishes cold bootstrap from unchanged resume. Cold history continuity is streamed; same-binding resume verifies cursor/next prefix rather than rescanning all history. | `de/silver/incremental_reader.py:93-162`; `de/silver/source_binding.py` source identity/allocator verifier. No journal prune/rewind is assumed. |
| Silver verifies each sequence from Bronze journal and identity, Silver per-sequence obligations, invalidation linkage, archive identity and canonical ClickHouse evidence. It never treats current `silver_v1_event_validity` alone as historical proof. | `de/silver/incremental_reader.py:163-425`; Bronze journal DDL in `de/bronze/checkpoint_store.py:295-343`; applied obligations in `de/silver/checkpoint_store.py` and `de/silver/processor.py:290-420`. |
| Effective dispositions are `BUNDLE_VALID`, `SUPERSEDED_BY_CONFLICT`, `CONFLICTED`; a changed effective disposition at a later boundary may be legitimate while source transition identity remains immutable. | `de/silver/incremental_reader.py:344-408`; `pre/contract-design.md` §10. Missing/corrupt evidence and gaps do not produce a success disposition. |
| Each bounded page captures `Hc,Hi,Sc,Si`; uses `Pc=min(Hc,Sc)`, `Pi=min(Hi,Si)`; limits count/bytes/deadlines during processing; final-checks included evidence; unrelated appends above captured H are deferred. Receipts use JCS/SHA-256. | `de/silver/incremental_reader.py:446-579`; `de/silver/incremental_contract.py:208-245`; `de/feed_read_budget.py:7-43`; normative `pre/contract-design.md` §10. |
| Feed errors are exact API contract. Retryable codes are only `SOURCE_LAG`, `PREFIX_BLOCKED`, `INVALIDATION_LAG`, `BOUNDARY_STALE`, `DEADLINE_EXCEEDED`, `AUTHORITY_UNAVAILABLE`, `CLICKHOUSE_UNAVAILABLE`. Non-retryable outcomes include binding/rewind/retention/schema/integrity/gap/transition/linkage/version/cursor/request faults and `ENTRY_TIMEOUT`/`RECORD_TOO_LARGE`; caller must repair or explicitly rebind. | `de/silver/incremental_contract.py:14-18,29-36`; `de/silver/incremental_reader.py:45-91,365-405,510-553`. Never map a non-retryable failure to a different retryable alias. |
| Change and invalidation feeds are independent. Invalidation has fixed requested ceiling and receipt; a pending CH mirror can still carry authoritative local suppression. The legacy `read_invalidation_page()` remains compatible. | `de/silver/incremental_reader.py:409-579`; `de/silver/canonical_reader.py:186-255`; `pre/contract-design.md` §10. |
| Bootstrap finds journaled runs, not event-free registered runs. Source replacement, rewind and retention gaps fail closed. | `de/silver/incremental_contract.py:85-102`; `de/silver/incremental_reader.py:93-162`; `pre/contract-design.md` §10. |

## Existing Gold call path and gaps

`de/gold_runtime/main.py:21-64` acquires the store/lock, constructs `GoldProcessor` and runs the existing entrypoint. `GoldProcessor` (`processor.py:182-220,269-355,373-412,468-825`) polls direct Silver rows, schedules legacy windows, invokes `GoldTransformationEngine`, persists business outputs through `GoldClickHouseRepository`, and then advances per-table fact cursors. Its loop sleeps after each cycle. This cannot serve as the approved Pre feed consumer and must be refactored, not paralleled.

`de/gold_runtime/silver_readers.py:161-440` owns direct ClickHouse Silver fact/dimension queries and fact cursor bounds. This bypasses Silver's canonical validity/receipt boundary and is retired from the active Core path. `repositories.py:168-451` combines CH schema checks, Silver reads and Gold business writes; split/refactor to Gold SQLite processing metadata and no Core business output. `cursor.py` orders by `(processed_at, topic, partition, offset, hash)` and must not drive Pre feed progress.

`checkpoint_store.py:25-88,160-580` has checkpoint/window/work/lease/migration tables, SQLite WAL/FULL sync, transaction/CAS, and immutable terminal work/window behavior. It is a useful base, but current APIs do not atomically store a Pre receipt, all sequence dispositions, suppression/membership/work and both independent feed cursors. Add indexed additive V1 state and transaction methods; use separate V1 paths. Keep immutable acknowledged history and represent corrections with a new dirty generation/obligation.

`window_scheduler.py:84-256` provides `window_start_for()` epoch alignment and half-open membership candidates but also watermark/eligibility semantics coupled to zero lateness. `de/gold/windowing.py:27-49` aligns epoch-zero 60/300 memberships but embeds scenario in `WindowKey`, which conflicts with scenario-neutral work identity. Refactor the pure identity/membership path, define precision and run boundaries, and disable publication finality. `context_builder.py:146-350` maps old source rows, quality and cadence-derived `expected_rows_for`; retain nullable/provenance mapping only and do not use cadence to declare completeness.

`config.py:32-46,166-230,265-294` fixes zero lateness/delay and max revision 1, permits physical Silver table source configuration, points V1-default paths at `de/artifacts/gold/`, and defaults poll=2s/max windows=1. These are legacy settings, not approved V1 policy. `processor.py` fixed waits and one-window cap are not throughput decisions. `revisions.py` applies the one-revision cap and is retired from publication policy. Refactor config to approved `gold_v1` paths, feed bounds, fairness/backoff and explicit resource limits.

`replay.py` provides namespace isolation helpers but its current manifest is coupled to V2 table/window scope; refactor to immutable Pre binding/boundary and current-effective-state replay. `dimensions.py` and Gold dimension builders represent business dimensions; defer those outputs, retaining only run/topology identity needed for work. `metrics.py`/`health_api.py` expose existing processor state but need feed/backlog/resource dimensions and independent foundation readiness. `instance_lock.py` lease/file-lock patterns may be reused after path/ownership review.

## Module disposition inventory

Classifications apply to the active Core path; retained legacy code/tests are not evidence of V1 behavior.

| Modules | Classification | Core disposition |
|---|---|---|
| `de/gold_runtime/silver_readers.py` | RETIRE FROM ACTIVE PATH | No direct fact/dimension SQL; replace with the Silver public feed adapter. Preserve only bounded compatibility tests as needed. |
| `de/gold_runtime/checkpoint_store.py` | REFACTOR | Reuse SQLite CAS/lease/storage discipline; add isolated schema, binding/receipt/transition/cursors/suppression/membership/dirty/recompute/replay transactions and indexes. |
| `de/gold_runtime/processor.py` | REFACTOR | Replace legacy poll→business transform→CH write cycle with bounded feed/work coordinator, fair scheduling and durable acknowledgments. |
| `de/gold_runtime/window_scheduler.py`; `de/gold/windowing.py` | REFACTOR | Pure 60s/300s simulation-time tumbling candidates/membership, scenario-neutral identity; no zero-lateness finality. |
| `de/gold_runtime/context_builder.py` | REFACTOR | Adapt canonical bundle fields, preserve null/quality/scenario/run identity; retire cadence-derived completeness. |
| `de/gold_runtime/cursor.py` | RETIRE FROM ACTIVE PATH | Fact cursor cannot represent canonical global sequence. A receipt verification helper may be REUSED only if separated from the old cursor semantics. |
| `de/gold_runtime/processing_ledger.py` | REFACTOR | Reuse canonical hashing/idempotency patterns for immutable work intent/receipt; remove CH business-output ledger from active Core. |
| `de/gold_runtime/revisions.py` | RETIRE FROM ACTIVE PATH | Legacy latest/revision-one publication behavior is not approved; use dirty-generation/recompute work state. |
| `de/gold_runtime/replay.py` | REFACTOR | Freeze Pre boundary/binding and isolate SQLite/lock/namespace; current-effective only. |
| `de/gold_runtime/repositories.py` | REFACTOR | Remove source reads and Core mart writers. Retain CH only for explicitly justified infrastructure needs; no 005 writes. |
| `de/gold_runtime/dimensions.py` | DEFER TO PHASE 4B | Business dimension candidates deferred; retain only immutable run/topology identity required to key processing. |
| `de/gold_runtime/instance_lock.py` | REUSE / REFACTOR | Reuse one-owner lease and file locking with V1 path, fencing and replay isolation. |
| `de/gold_runtime/config.py` | REFACTOR | V1 paths, source contract versions, bounded resource controls, no physical Silver table allowlist, no inherited finality/cadence defaults. |
| `de/gold_runtime/metrics.py`, `health_api.py` | REFACTOR | Add stage lag/backlog/age, receipts, dirty queue, retries, source binding, resource and foundation-vs-analytics readiness. |
| `de/gold_runtime/main.py` | REFACTOR IN PLACE | Keep current module/entrypoint; start one owner with graceful shutdown and no V2 business path. |
| `de/gold/input_models.py`, `models.py`, `contracts.py` | REFACTOR | Keep source-neutral processing identity/input where useful; preserve nullable availability/provenance. Defer business result contracts. |
| `de/gold/engine.py`, `aggregators/`, `builders/`, `calculators/` | DEFER TO PHASE 4B | No KPI, priority, congestion, mart or business aggregation in Core. |
| `de/gold/canonicalization.py`, `deduplication.py`, `lineage.py`, `validation.py`, `quality.py` | REUSE / REFACTOR | Reuse only pure utilities after proving they match canonical source semantics; no legacy fact-table lineage or invented quality/default policy. |
| `de/gold/latest_selector.py` | RETIRE FROM ACTIVE PATH | Business latest-result selection is deferred. |
| `de/gold/aggregators/*`, `builders/*`, `calculators/*` | DEFER TO PHASE 4B | Preserve historical tests/source as legacy; not called by active Core. |
| `de/migrations/005_create_gold_m1.sql` | PRESERVE AS LEGACY | Do not modify or delete tables/data; no Core writes. |
| `docker-compose.yml` existing `de-gold-runtime`, `de/scripts/migrate_clickhouse.py` | REFACTOR IN PLACE after review | One service and existing entrypoint; selected legacy migration 005 remains untouched; no new CH migration absent approval. |
| Existing `de/tests/gold/`, `de/tests/gold_runtime/`, architecture/integration tests | REUSE / REFACTOR / PRESERVE AS LEGACY | Add Stage B acceptance to existing suites; retain historical behavior tests where relevant; never blanket-skip regressions. |

## Pre evidence reviewed and its limits

`pre/acceptance-result.md`, all Pre design/decision/plan/acceptance/status/capacity docs, and committed evidence under `pre/evidence/` were reviewed. Evidence reports Stage A 15 passed; Phase 3 local 334 passed and 32 legacy/opt-in skips (retained XML: 366 total); real CH/replay 20 passed; real schema/reader 3 passed. Broad architecture comparison is 126 cases, 10 failures in current vs 11 baseline: ten unchanged failures remain and one prior Silver IO allowlist failure resolved; no new failure. These are historical recorded runs, not reruns in this task.

Focused capacity in `evidence/capacity.json` is a replay/apply + Silver feed + test-only SQLite consumer measurement: 150 applied/acknowledged in 600s (0.25/s); total 255 sequences/bundles, 221 receipts, 5,255,148 bytes; controlled backlog 30 after consumer pause, drained in 28.23s; feed+test-ack p50/p95/p99 828.1/949.0/1,152.3ms; 3,519 CH queries and 127,863 SQLite statements. It does not include the complete live SUMO→Kafka→Raw→Bronze→Silver→actual Gold path; the pause was not a producer burst and measured rate is not maximum sustainable rate. `capacity-capture.xml` took 1,027.41s, pytest exit 0; independent oracle exit 0. Whole-pipeline capacity remains `NOT MEASURED`.

`pre/evidence/crash-replay.json` records test-harness exit 73 before transaction (old cursor retained) and 74 after commit (cursor and local receipt persisted) for both streams; it is not a Gold SQLite Stage B result. `receipt-sample.json`, `capacity-input.json`, `stage-a.xml`, `phase3-local.xml`, `phase3-real.xml`, `phase3-live-host.xml`, and `architecture-comparison.json` were checked. No evidence was modified.

The ten inherited broad architecture failures remain tracked, unchanged from baseline: `test_bronze_forbidden_imports`, `test_bronze_package_compliant`, `test_canonical_base_is_kafka_only_default_runtime`, `test_default_compose_render_is_exact_kafka_runtime`, `test_final_manifest_json_matches_static_service_contract`, `test_operator_tools_have_no_raw_v1_replay_entrypoint`, `test_raw_v1_reference_allowlist_is_repository_wide_and_exact`, `test_rollback_compose_render_has_full_activation_contract`, `test_server_does_not_depend_on_kafka_packages`, and `test_top_level_compose_allowlist_is_exactly_one`. The baseline's `test_only_plan3_modules_import_io_stack` failure was resolved; no new failure was recorded. Do not report the entire repository architecture suite as passing.

## Document mismatches corrected by this plan revision

- The old Core docs still called Pre a proposal and left P4-D10/P4-D30 and B4A-01/02 open; this review marks the Silver contract implemented/Stage A accepted and keeps actual Gold Stage B pending.
- The old implementation sequence retained Option B, production full scans, whole-scan receipt/staging and O(N) ordinary polling as active alternatives. The revised plan makes Option A the only production path and confines `read_complete_scan()` to bounded fixtures/compatibility/smoke.
- Old acceptance cases relied on legacy opaque tokens/error aliases and stated scanning rather than receipt-per-page. The new matrix tests the exact public DTO/JCS contract, real Gold transaction boundary and stage distinctions.
- Earlier documents framed window durations as configurable/proposed, allowed scenario-keyed windows and left finality policy open for Core. This revision selects pure 60s/300s tumbling, scenario-neutral identity, completeness UNKNOWN and finality DISABLED.
- Prior path/state language described the V1 `gold_v1` path as pending approval; it is approved, while runtime config and isolation implementation remain pending. Migration 005 stays preserved.
- Prior performance prose could imply sustained end-to-end evidence from the Pre harness. This revision preserves the exact focused benchmark and explicitly says whole-pipeline capacity and production readiness are not measured/passed.

## Review limitations

No application tests, containers, live endpoints or performance workloads were run for this documentation task. All outcome claims above are attributed to committed Pre evidence. Referenced source line numbers are at the audit baseline and may shift as Core code changes.
