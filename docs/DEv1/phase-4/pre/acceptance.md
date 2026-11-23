# Phase 4A-Pre acceptance and evidence matrix

**Purpose:** define evidence required before the Silver incremental source contract can be used for production Gold progress. This is a plan, not a test run; no measurements or pass claims are made here.

**Final staging:** Cases below are classified as Stage A Pre contract acceptance, Stage B Phase 4A-Core Gold integration, or pipeline benchmark. Stage A must run before Gold Core exists and uses a minimal test-only durable consumer. Current end-to-end capacity is `NOT MEASURED`; see [capacity-and-backpressure.md](capacity-and-backpressure.md).

## Independent oracle and evidence package

Tests must not use the new API as their only expected-value source. Build an independent fixture oracle from:

1. Bronze SQLite `v1_sequence`, `v1_changes`, `v1_identity`, and `v1_invalidations`, read directly by test-only code.
2. Silver persisted per-sequence obligations and invalidation provenance, read through a test-only store adapter or SQL fixture inspection.
3. ClickHouse materialized source rows and obligation reconciliation where applicable.
4. Immutable archived run manifests/topology identity.

The oracle independently derives ordered sequence transitions and expected disposition, then compares exact `(sequence, disposition, event, revision, evidence hash)` coverage and full bundle content. It must detect a skipped row, duplicate, wrong current revision, false valid bundle, false conflict, invalidation-link mismatch, or receipt/digest mismatch. Preserve fixture DBs, query logs, API requests/responses, receipts, cursors, crash-injection point, and resource samples with each run.

## Correctness matrix

| Scenario | Setup and independent oracle | Required evidence | Pass criteria | Failure criteria |
|---|---|---|---|---|
| ACCEPTED then CONFLICTED | Accepted seq `a`, then conflict seq `c` for same event; inspect raw transition and invalidation journals plus Silver obligations. Exercise conflict both within and above captured `Hc`. | Entries, bundles, receipt, invalidation page, Gold cursor/dirty state. | If `c <= Hc`, `a` is `SUPERSEDED_BY_CONFLICT` with no bundle and `c` is `CONFLICTED`; if conflict races above H, stale or later invalidation repairs without publication. | Any superseded payload returned as `BUNDLE_VALID`; absent sequence; conflict without linked invalidation; ack with no durable suppression/dirty intent. |
| Missing/corrupted evidence | Remove/alter a required CH source row, frozen obligation, archive object, hash, or journal linkage in isolated fixtures. | Typed error and unchanged cursors; raw corruption and reconciliation log. | Fails closed; no success receipt or progress; no fabricated skip disposition. | Partial receipt, bundle missing facts yet accepted, silent quarantine/skip, cursor advance. |
| Concurrent source append | Pause between H capture and page completion; append unrelated event above H. Separately append conflict for an included event. | Allocator values before/after, page range, final identity checks, receipt/error. | Unrelated append excluded and next batch sees it; relevant transition makes batch stale or is represented as superseded within H. | Batch silently expands past frozen H, gaps, duplicate inclusion, or receipt describes moving H. |
| Late events | Insert event IDs sorting before/after already seen event IDs while assigning later journal sequences. | Raw journal order and batch pages. | All later sequences discovered independent of event ID order. | Event-key pagination misses late lower-sorting ID. |
| Multiple runs | Interleave sequences from several archived runs/generations in one namespace. | Journal oracle, immutable archive identities, emitted bundles. | Global cursor covers all runs; each bundle carries correct run/generation/digest/manifest. | Run filter skips sequence or aliases reused run ID. |
| Source replacement | Resume saved nonzero cursor after changing source file/path/schema identity or replacing a DB with same namespace. | Prior/new binding, typed error, cursor storage. | Fails `SOURCE_BINDING_CHANGED`; no reset or receipt; explicit new generation required. | Silent adoption of replacement or cursor reset. |
| Invalidation races | Race independent invalidation append/consumption around change receipt and Gold work intent, in both transaction orders. | Both cursors, receipt Hi, local dirty generation, retry/recompute records. | Idempotent eventual suppression/recompute; no acknowledged dirty work lost. | Change ack advances while required invalidation is unrepresented; race leaves clean stale output. |
| Crash/restart | Inject crash before Gold transaction, during rollback, after commit, and after receipt persistence attempt. | Durable DB snapshots, cursor and work records before/after restart. | Before commit replays old cursor; after commit resumes new cursor with complete work; duplicate replay harmless. | Cursor moves without all dispositions/work; duplicate work corrupts state; lost intent. |
| Sequence gap | Delete/omit a journal row or mismatch allocator and max sequence, including gap at bootstrap and within page. | Allocator, raw rows, verification response. | Detects exact gap and refuses bootstrap/receipt; cursor unchanged. | Uses `max(sequence)` as proof of continuity or skips gap. |
| Bounded memory | Run increasing retained history with fixed batch and bundle caps; sample process RSS and staging files. | Workload, per-batch bytes, peak RSS, temp storage, code path evidence. | Memory is bounded by configured in-flight limits and fixed overhead; no O(history) accumulation. | RSS/staging grows with total history or caps are bypassed. |
| Sustained throughput/backlog | Run representative producer and consumer for owner-selected duration and volume; include realistic bundle sizes and conflict rate. | Environment, workload seed, input/output counts, entries/sec, bytes/sec, CPU, I/O, SQLite/CH query counts and latency, RSS, queue/backlog time series. | Exact oracle remains equal; resource caps respected; backlog is stable or drains at owner-approved target rate. | Silent loss/duplication, unbounded resource use, persistent backlog growth against agreed target, or incomplete evidence. |
| Phase 3 regression | Run relevant Bronze/Silver tests and migrations against untouched Phase 3 behavior, including journal worker, source binding, canonical reader, obligations, lineage, quality and real invalidation recovery. | Test command/version and complete output, migration checks, before/after schema comparison. | Existing Phase 3 behavior and semantic authority preserved. | Any regression, weakened reconciliation, changed existing token/page semantics without approved compatibility plan. |

## Resource protocol

Do not claim an SLO before measuring. Before P4Pre-08, owners must specify representative retained-history size, event and bundle size distribution, producer rate, conflict rate, available CPU/memory/disk, acceptable batch caps, duration, and required drain/backlog rate. Record:

- records and encoded bytes per second, total records and bytes;
- maximum and percentile batch sizes and receipt sizes;
- peak and time-series RSS, temporary staging bytes, CPU and I/O;
- SQLite/ClickHouse call counts and latency distributions;
- producer highwater, Silver applied cursor, Gold cursor, backlog depth and slope;
- retries, stale batches, gap/corruption failures, and invalidation lag.

Pass requires bounds and owner targets documented before the run and met in the run. If targets are missing, the correctness suite may pass, but production readiness remains **not certified**.

## Release gate

The contract is acceptable for production Gold source progress only after:

1. P4Pre-01 decisions and exact receipt serialization are owner-approved.
2. Every correctness scenario above passes against independent raw-store oracles.
3. All successful batches prove contiguous exact disposition coverage and all failures leave cursors unchanged.
4. Memory/resource evidence proves fixed-cap bounded behavior; measured sustained throughput/backlog meets owner-approved target.
5. Phase 3 regression suite passes without authority or semantic weakening.
6. The eventual-consistency race boundary, no-prune assumption, event-free-run discovery limitation, and source-binding reset procedure are accepted and documented.

Until then, Phase 4A may continue its separately approved foundation work, but production source cursor advancement and activation remain blocked.

## Verified applied prefix acceptance additions — Stage A

| Scenario | Setup and independent oracle | Required result |
|---|---|---|
| Silver behind live Bronze high-water | Set `Hc=1200, Sc=1100`, consumer after=1000; compare Bronze allocator/journal and Silver cursor directly. | API returns bounded exact prefix ending `<=1100` without waiting for Hc; unrelated appends above captured H are deferred. |
| Conflict in Bronze, not yet Silver-applied | Current identity points to conflict sequence `c>Sc` or linked invalidation `i>Si`; oracle reads both journals and local obligations. | Truncate before affected ACCEPTED sequence; zero prior entries yields retryable `PREFIX_BLOCKED`, no bundle/skip/receipt for blocked row. |
| Change applied but invalidation lags | Construct `Sc` advanced and `Si` behind a linked invalidation. | Unaffected accepted entries progress after identity checks; known-conflicted event cannot be superseded until conflict+invalidation obligations applied. Receipt Pi is finite; harness consumes through Pi before change ack. |
| ACCEPTED becomes CONFLICTED during page | Pause after boundary capture and inject conflict/invalidation at each verification stage. | If conflict/link/obligation is within safe prefixes, emit superseded plus conflict dispositions; else stale/truncate; never emit old accepted bundle. |
| Conflict after receipt | Inject after final validation but before harness ack. | Saved receipt bytes replay identically; newer boundary may return superseded/conflicted state; invalidation converges idempotently. Document unavoidable race; no distributed atomicity claim. |
| CH invalidation mirror pending | Bronze SQLite and Silver local invalidation obligations durable while mirror status pending. | Invalidation feed delivers suppression with `mirror_status=PENDING`; no bundle for conflicted event. |
| Continuous unrelated append | Keep producer appending while test consumer requests successive batches. | Cursor progresses on each verified prefix while `Hc` moves; no requirement `Sc==Hc`; sequence oracle finds no omissions. |
| Permanent Silver lag | Freeze Silver cursor while Bronze continues appending. | API delivers only through Sc then reports lag/block; no cursor leap. Benchmark capacity verdict is `CAPACITY INSUFFICIENT` if agreed target cannot recover. |
| Oversized or slow sequence | Lower page/single-entry byte cap below a real canonical bundle; separately inject a slow archive/CH evidence lookup beyond the per-entry deadline. | Return `RECORD_TOO_LARGE` or `ENTRY_TIMEOUT` with exact sequence/event and observed size/elapsed operation; no receipt or cursor advance past it. An unchanged retry cannot report success or loop as ordinary retry forever. |

## Acceptance stages and performance verdict

**Stage A — Contract correctness PASS** uses the real Silver public API, real Bronze/Silver authority fixtures, independent journal oracle, actual ClickHouse reconciliation where bundle facts require it, and a minimal test-only durable consumer for receipt/cursor crash replay. It covers cold bootstrap from zero, ordinary resume without full-history scan, run interleaving, source replacement, sequence gaps, effective-state evolution, bounded receipts, deterministic canonical serialization and failure no-progress. Gold application schemas and production Gold checkpoints are not dependencies.

**Stage B — Phase 4A-Core** covers actual Gold SQLite cursor, atomic receipt/work-intent commit, independent invalidation cursor, window membership, dirty-generation CAS, recompute obligations, replay isolation and integration recovery. It starts after Pre acceptance and is not a circular Pre release gate.

Report separate outcomes: **Correctness PASS/FAIL**; **Capacity** one of `SUSTAINABLE AT TESTED WORKLOAD`, `BURST RECOVERY VERIFIED`, `CAPACITY INSUFFICIENT`, `INCONCLUSIVE`, `NOT MEASURED`; and **Operational Readiness PASS/FAIL**. Correctness can pass while capacity fails. No throughput pass with paused producer, skipped/slow events, cursor-only progress, disabled reconciliation, undocumented dataset reduction, mock-only Gold, no backlog trend, mean-only latency or a short non-steady run. Zero queue does not prove all obligations/invalidation/window work is complete.

Pipeline scenarios A-H, full metric names, local CPU/RAM/disk constraints, Kafka retention headroom, SQLite WAL/checkpoint growth, query amplification, tail latency, catch-up targets, and owner capacity worksheet are defined in `capacity-and-backpressure.md`. A test-only consumer measures Silver API capacity only, never actual Gold runtime end-to-end performance.
