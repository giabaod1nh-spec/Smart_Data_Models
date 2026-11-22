# Phase 3 Silver V1 runtime acceptance result

Date: 2026-11-22 (Asia/Bangkok). Branch `codex/phase-1-v1-raw`, baseline HEAD `fe33bda0ab23aba50127cac499101321f38d3e4b`; implementation is in the preserved dirty worktree, not a new commit. Evidence root: `de/artifacts/silver_v1/`. This result supersedes historical pending statements while retaining them as chronological evidence.

## Runtime result

Actual producer run `phase3-silver-20261122-a01`, generation4, completed120 cycles for A/B/C/D/M. Producer/Outbox→Kafka→Raw→Bronze oracle verified600/600. Whole-run Silver verifier exited0/PASS:600 source/600 VALID,600 independently verified with errors[], canonical600 intersection+2040 traffic+2040 signal=4680 facts. Boundary2200/0 unchanged before/after. Recorded producer-source oracle is hash-linked; it was not presented as a new producer rerun. Report: `acceptance/phase3-silver-20261122-a01/silver-certification.json`.

Final actual snapshot01:24:54UTC:READY, both lags0, cursor2200/0, all25960 obligations RECONCILED, no pending/quarantine/fault, lifecycle current. All2200 historical/current events VALID. Exactly one existing Silver service runs `python -m de.silver.main`. All30 Silver runtime/adapter/migration files match worktree SHA256 (`acceptance/runtime-code-parity.json`). The image includes runtime code; latest test/docs edits were supplied as test fixtures and are preserved in worktree.

## Assertion closure

The following assertions were checked against implementation, exact test assertions and actual artifacts, not inferred from aggregate green counts. JUnit paths are relative to evidence root; associated per-fixture JSON reports retain IDs, counts and inventories.

| ID | Result and evidence |
|---|---|
| A01 | PASS — actual600/4680 source/fact oracle and canonical scan; whole-run report above and producer source-oracle.json. |
| A02 | PASS — subset preserves full digest-keyed topology while run/binding/event scope remains participant-only; subset-topology/topology-late-worker JUnit. |
| A03 | PASS — exact three approaches, no fourth filler; worker and real quality/owner fixtures. |
| A04 | PASS — distinct retained run generations/digests and cross-identity rejection (archive-multigeneration JUnit); real lifecycle COMPLETE/INCOMPLETE/STOPPED, outage/restart/idempotence (acceptance/real-lifecycle-junit.xml, integrated suite). |
| A05 | PASS — archive digest mismatch and zero-output worker failure; archive/topology JUnit. |
| A06 | PASS — missing expected direction yields exact missing list, no obligations/validity/facts/cursor advancement; topology-late-worker JUnit. |
| A07 | PASS — unexpected direction yields exact unexpected list, no widened topology or valid output; same suite. |
| A08 | PASS — missing manifest/catalog is retryable; malformed JSON/nonobject/identity mismatch is integrity failure; repeated historical lookup with no active fallback; archive-file-failure and archive-multigeneration JUnit. |
| A09 | PASS — real lifecycle cache/registry/manifest outage and recovery, bounded poll retry state/readiness; source startup outage retains cursor and recovers; integrated-v1-acceptance JUnit. |
| A10 | PASS — all core zero values survive actual CH and remain measured; real-quality JUnit. |
| A11 | PASS — NULL+UNAVAILABLE round trip and JSON status preserved; real-quality JUnit. |
| A12 | PASS — NULL+NOT_APPLICABLE remains distinct; same suite. |
| A13 | PASS — exact direction JSON key-presence equality and absent optional fields across successive events; same suite and independent run oracle. |
| A14 | PASS — partial count produces NULL/PARTIAL_OR_UNAVAILABLE; complete zero sum remains0; same suite. |
| A15 | PASS — absent optional state stays NULL/absent, explicit false stays0; same suite. |
| A16 | PASS — independent East/South/West availability and quality flags; same suite. |
| A17 | PASS — event-local scenario sequence preserved in all three families; archived run metadata remains separate; real-quality fixture and fact/oracle mapping. |
| A18 | PASS — actual R1/R2 owner deliveries, unchanged boundaries/physical fact content/primary R1 provenance, two-delivery lineage; pre/post duplicate replay hashes equal. Explicit identity-sensitive/timestamp-independent hash test. real-lineage-replay-reader, real-replay-conflict-abort, replay-hash-identity JUnit. |
| A19 | PASS — journal1,2,3 retains cycles4,9,2 with all VALID identities and exact fact population; topology-late-worker JUnit. |
| A20 | PASS — actual source conflict before consumption yields zero conflicted-event facts, INVALIDATED and cursors3/1; unaffected event retained; real-canonical-race/real-replay-conflict-abort fixtures. |
| A21 | PASS — stable real multi-page complete bundles; exact physical duplicate collapse; differing content rejected; source conflict during page rejected; changed boundary/token/filter/key/page-size refused; limits0/101 fail and1/100 pass. real-canonical-race, reader-filter-key-size, canonical-reader-focused JUnit and actual A01 scan. |
| A22 | PASS — local suppression before failed CH mirror; feed exposes pending mirror, canonical lag gates prevent partial page; real before-insert/lost-response invalidation fixtures plus invalidation-feed JUnit. |
| A23 | PASS — abrupt subprocess exit73 after freeze,insert,partial traffic fact,acknowledgement,finalize; reopen snapshots PENDING/cursor0 or VALID/cursor1; exact recovery to complete bundles/cursor2; real-abrupt-crash JUnit/reports. |
| A24 | PASS — partial directional CH output recovers from frozen rows; unresolved outputs block cursor; differing content durably faults and is not retried after reopen; real-partial-fact-crash and integrated recovery suites. |
| A25 | PASS — lost insert response reconciles without repeat logical insert, including actual CH tombstone restart; real-invalidation-recovery and integrated recovery suites. |
| A26 | PASS — simultaneous independent-connection CAS has one winner; empty/contiguous/gapped/allocator/link/path/namespace/schema checks; append above fixed highwater left for next pass. concurrent-cas-fixed-boundary JUnit; retained journal/source/RO-mount inventory and operational no-prune contract in handover. |
| A27 | PASS — both independent cursor gates, authority/unknown/content errors, four transient attempts maximum, stale staged-page discard and bounded four scan attempts, SOURCE_CHURN exhaustion and fresh completion. reader-independent-cursor-gates/filter-key-size JUnit, actual CH/source race and A01 complete scan. |
| A28 | PASS — actual stable repeated replay and benign lineage growth yield equal hash; source conflict mid-materialization or before completion persists ABORTED/SOURCE_BOUNDARY_CHANGED; real-replay-conflict-abort JUnit. |
| A29 | PASS — actual all-main-CH and main-SQLite sorted full-row hashes/counts unchanged across completed,resumed and second-ID replay; separate state/targets/locks; abrupt restart tests. real-replay-live-inventory and runtime-isolation-current JUnit/reports. |
| A30 | PASS — one in-place V1 executable/service, retired V2 loop; correct profiles/dependencies/mounts/allowlists; old V2 data retained. compose-current/runtime-isolation JUnit, runtime inventory and actual V2 sentinels. |
| A31 | PASS — real clean and populated historical V2 databases:006/007 then008 twice; all20 main/replay tables exact types; V2 DDL/row hashes identical. acceptance/real-migration-junit.xml and p3_migration_*.json; actual migration logs exit0. |
| A32 | PASS — exact planned three-profile Compose up command exited0; migration006/007/008 exit0, real upstream Raw/Bronze, one healthy Silver owner, RO/RW mounts and no newly started Gold/V2 runtime; compose-runtime-inventory.jsonl,migration-runtime.log,runtime-latest.json. |
| A33 | PASS — public Python bundle/query/token/error/retry/feed interfaces; no HTTP reader service; actual SELECT-only CH account reads successfully and rejects INSERT; Phase4 mappings/ownership documented; reader suites and real-canonical-race fixture. |
| A34 | OPTIONAL — no sustained capacity claim. Actual resource samples/query latency evidence retained; not a mandatory exit gate. |

## Tests and retained limitations

Wide Silver regression:272 passed,32 skipped,0 failed (`silver-wide-final-regression-junit.xml`). `acceptance/wide-regression-skips.json` classifies every skip:26 opt-in actual cases each linked to a separate passing JUnit,5 historical V2 evidence-format checks,1 retired V2 executable batch module. Integrated opt-in69passed; abrupt crash5passed; actual migration2passed; latest reader9passed and hash identity1passed separately. Counts belong to overlapping suites and must not be summed as unique tests. Earlier failed runs remain recorded with their corrected causes.

No commits, pushes, reset/clean or destructive V2 database operations were performed. Historical V2 definitions/sentinels were tested in retained isolated upgrade databases. External privileged file rewriting cannot be prevented by SQLite binding: preserve the same Bronze authority, full journals and WAL files under the documented operational no-prune/no-rewind rule. Gold consumers configure SELECT-only credentials and consume canonical bundles/feed; raw CH facts are audit storage. No multi-page distributed snapshot, arbitrary historical as-of replay, starvation-free scan or capacity SLA is claimed. Phase4 Gold/Spring/Dashboard implementation is outside this completed implementation scope.
