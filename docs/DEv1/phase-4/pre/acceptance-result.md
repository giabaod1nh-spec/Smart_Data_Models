# Phase 4A-Pre Stage A acceptance results

Approved base `a2f3ac6b98c7ca325213b617e603825481202023` with scoped implementation changes in the worktree. Windows, Python 3.14.4 and ClickHouse 24.3. Evidence here refers to the corresponding Stage A commands and isolated fixtures.

## Execution record

| Gate | Status | Evidence |
|---|---|---|
| Stage A correctness and JCS vectors | PASS | [stage-a.xml](evidence/stage-a.xml), exit 0, 15 passed. Ten cases use isolated real Bronze/Silver SQLite authorities and ClickHouse 24.3.18.7. Five cover the literal JCS vectors and frozen request type. |
| Phase 3 local regressions | PASS | [phase3-local.xml](evidence/phase3-local.xml), exit 0, 334 passed, 32 opt-in/legacy skips. The real ClickHouse and replay regressions are tracked separately. |
| Phase 3 real ClickHouse and replay regressions | PASS | [phase3-real.xml](evidence/phase3-real.xml), exit 0, 20 passed. The initial run found that reinserting a timezone-naive UTC driver value on this host shifted it seven hours. V1 insertion now binds those values explicitly as UTC. Identical duplicate handling and rejection of divergent facts were both reverified. |
| Phase 3 real schema and legacy reader smoke checks | PASS | [phase3-live-host.xml](evidence/phase3-live-host.xml), exit 0, 3 passed. The disposable Windows fixture used a new explicit physical source binding. A first attempt to read checkpoint files bound inside Linux correctly rejected the host path. |
| Silver architecture and diff validation | PASS | The scoped architecture suite passed with local regression. `git diff --check` passed. |
| Broad historical architecture inventory | FAIL | [Baseline comparison](evidence/architecture-comparison.json). Ten unrelated legacy Compose, Raw/Bronze boundary and Spring inventory assertions fail on both the approved base and this implementation. The previous Silver IO allowlist failure is resolved. These historical failures are outside the Silver incremental contract gate. |
| Focused feed capacity | PASS | [capacity-capture.xml](evidence/capacity-capture.xml), exit 0, 1 passed in 1,027.41 s. [Independent revalidation](evidence/capacity.json) confirms 255 sequences over 600 seconds steady, 120 seconds of backlog buildup and 300 seconds recovery. |
| Whole-pipeline capacity | NOT_RUN | Whole-pipeline measurement status is `NOT MEASURED`. |

## Stage A case coverage

The names below are case identifiers in the Stage A JUnit report. The focused capacity revalidation additionally reads all retained receipt and journal rows.

| Required case | Status | Evidence |
|---|---|---|
| Strict request types, limits, unknown version, frozen DTO and payload | PASS | `live_prefix_receipts_late_multi_run_and_byte_limits`, `sequence_projection_and_request_immutability`. |
| Empty Bronze/Silver, caught up, Gold behind Silver, exhausted Silver prefix, and `has_more` | PASS | `live_prefix_receipts_late_multi_run_and_byte_limits`. |
| `BUNDLE_VALID`, superseded historical acceptance, explicit conflict and blocked first sequence | PASS | `conflict_blocks_then_converges_and_acknowledges_independently`, `conflict_before_materialization_and_invalidation_lag`. |
| Unsafe later sequence truncates the page, including when invalidation trails | PASS | `conflict_before_materialization_and_invalidation_lag`. |
| Unrelated change append and included-event conflict after capture | PASS | `live_prefix_receipts_late_multi_run_and_byte_limits`, `conflict_during_read_and_binding_gaps`. |
| Conflict after receipt, durable acknowledgments and convergence | PASS | `conflict_blocks_then_converges_and_acknowledges_independently` replays the earlier receipt and advances independent cursors after actual subprocess exits. |
| Late event, run interleaving, distinct generation, topology and manifest | PASS | `live_prefix_receipts_late_multi_run_and_byte_limits`. |
| Cold sequence-zero initialization, same-binding resume without full scan, rewind and replacement | PASS | `conflict_before_materialization_and_invalidation_lag`, `retained_source_replacement_rewind_and_resume`. |
| Sequence gaps at bootstrap and within a page | PASS | `conflict_during_read_and_binding_gaps`. |
| Missing real ClickHouse fact, corrupt frozen evidence, archive fault, illegal transition | PASS | `missing_clickhouse_fact_illegal_transition_and_archive_fault`, `entry_timeouts_and_authority_faults_produce_no_receipt`. |
| Invalidation fixed ceiling, lag, empty, pending mirror without physical row, append and legacy `SNAPSHOT_STALE` | PASS | `invalidation_continues_under_unrelated_append`, `conflict_blocks_then_converges_and_acknowledges_independently`. |
| Invalidation identity mutation at final check and invalid linkage | PASS | `invalidation_link_and_final_identity_faults_fail_closed`. |
| Count/byte caps, actual oversized-entry fault, page deadline before and after safe prefix | PASS | `live_prefix_receipts_late_multi_run_and_byte_limits`, `normal_page_deadline_can_return_only_the_verified_prefix`. |
| Entry timeout, unavailable authority during read and final verification, no receipt on error | PASS | `entry_timeouts_and_authority_faults_produce_no_receipt`. |
| RFC 8785 exact bytes, SHA-256, receipt ID, Unicode, null, numeric forms and entry order | PASS | Four literal `frozen_jcs_vectors` cases and signed-64 sequence projection. |
| Consumer crash before and after change and invalidation commits, watermark ordering, duplicates and changed binding | PASS | `conflict_blocks_then_converges_and_acknowledges_independently`. Exit codes 73 and 74 delimit rollback and committed restart. [Crash and replay evidence](evidence/crash-replay.json) records the asserted intermediate sequences and the retained final cursors and receipts. |
| Sustained progress, controlled backlog, resource envelope and independent records | PASS | [capacity.json](evidence/capacity.json) and [receipt-sample.json](evidence/receipt-sample.json). Full-envelope bytes and each acknowledged canonical fact were revalidated. |
| Production Gold work transactions, dirty-generation fencing, migration and cutover | NOT_RUN | Core Stage B. |
| Pre-existing live Compose health and readiness endpoints | NOT_RUN | A live Silver process deployment is outside this isolated Stage A fixture. Local health truth tables and real Silver worker/reader tests ran separately. |

## Focused capacity

The first attempt selected a five-second wall cadence. Replaying one five-intersection cycle through the existing Bronze and Silver apply paths took about 14–15 seconds during that attempt. The five-second profile could not produce its selected arrival rate, so the attempt was stopped. The final full-duration profile replayed 51 genuine cycles from the retained Phase 3 A01 Outbox capture at five observations per 20 wall seconds. [Input fingerprints](evidence/capacity-input.json) identify the ordered payloads, run manifest and topology catalog. This profile measures the Bronze/Silver apply path, Silver reader and local durable consumer.

| Measurement | Result |
|---|---:|
| Steady applied and durably acknowledged | 150 each in 600 s (0.25 entries/s) |
| Sampled steady backlog, early and late maxima | 0 and 0 at group completion sampling granularity |
| Backlog after two-minute consumer pause | 30 |
| Backlog drain after restarting consumption | 28.23 s while replay continued |
| End of run | 255 sequences and bundles, 221 receipts, 5,255,148 bytes, exact oracle agreement |
| Consumer peak RSS | 62.9 MiB of 512 MiB |
| Producer fixture process sampled peak RSS | 146.4 MiB |
| ClickHouse server memory tracking sample | 1,260,765,604 bytes, across server activity |
| Consumer elapsed CPU / read and write I/O | 25.02 s / 311,178,755 and 17,537,189 bytes |
| Consumer SQLite statements / ClickHouse queries | 127,863 / 3,519, including repeat polls, receipt acknowledgment and final evidence checks |
| Page read plus local acknowledgment p50 / p95 / p99 | 828.1 / 949.0 / 1,152.3 ms |
| Max page bytes / encoded bytes per second across all phases | 585,688 / 5,152.1 |

**Verdicts:** `SUSTAINABLE AT TESTED WORKLOAD` and `BURST RECOVERY VERIFIED`. An earlier complete collection reported a false oracle mismatch because the assertion compared tuples with `sqlite3.Row` objects. The corrected verifier passed on that retained data. The final run repeated the entire 17-minute profile with exit 0 from pytest and a separate exit 0 from offline fixture verification. Verification checked every receipt digest, transition hash, exact interval and canonical bundle. Peak sustainable arrival rate and whole-pipeline operational readiness remain unmeasured.

## Reproduction

Use a dedicated local ClickHouse instance or permission to create isolated test databases. Install `de/requirements.txt` and `psutil`. The retained Outbox and run archives used for capacity are read only. Choose a fresh evidence directory for each capacity trial.

```powershell
$env:SILVER_LIVE_ACCEPTANCE = '1'
python -m pytest de/tests/silver/test_incremental_contract.py de/tests/silver/test_incremental_reader.py -k 'not focused_feed_capacity' --junitxml=.pytest_cache/phase4pre-stage-a-final.xml

python -m pytest de/tests/silver/test_real_crash_v1.py de/tests/silver/test_real_invalidation_recovery_v1.py de/tests/silver/test_real_lifecycle_v1.py de/tests/silver/test_real_lineage_v1.py de/tests/silver/test_real_migration_v1.py de/tests/silver/test_real_quality_v1.py de/tests/silver/test_real_replay_boundary_v1.py --junitxml=.pytest_cache/phase4pre-phase3-real-final.xml

Remove-Item Env:SILVER_LIVE_ACCEPTANCE
python -m pytest tests/architecture/test_silver_architecture.py de/tests/bronze de/tests/silver --ignore=de/tests/silver/test_incremental_reader.py --junitxml=.pytest_cache/phase4pre-phase3-local-final.xml

$env:SILVER_LIVE_ACCEPTANCE = '1'
$env:SILVER_FEED_CAPACITY = '1'
$run = ".pytest_cache/phase4pre-capacity-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
$env:SILVER_FEED_EVIDENCE = $run
python -m pytest de/tests/silver/test_incremental_reader.py::test_focused_feed_capacity --basetemp="$run-temp" --junitxml="$run.xml"
python -m de.tests.silver.feed_capacity_oracle "$run-temp/test_focused_feed_capacity0" $run
```

The retained local `applied.jsonl`, `consumer.jsonl`, `schedule.json` and isolated SQLite files support the offline verifier. The committed evidence omits disposable DBs, full observation envelopes and per-poll logs.

## Release scope

Stage A correctness and focused feed capacity passed. Operational readiness evidence covers the tested Silver read and test-consumer path. Phase 4A-Core owns deployed Gold behavior, state isolation and operational readiness. Whole-pipeline sustained capacity is `NOT MEASURED`.
