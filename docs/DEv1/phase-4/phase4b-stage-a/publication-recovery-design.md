# Business publication, revision and recovery design

## Separate state dimensions

Do not overload Core `COMPLETE` as business `PUBLISHED`.

1. **Source completeness:** expected observations and per-metric coverage are evaluated against approved schedule/population.
2. **Core work:** work generation and verified source proof complete for the recorded input fence.
3. **Business result:** deterministic metric calculations and cross-grain rollups complete for pinned metric versions.
4. **Business revision:** monotonic per logical result key, tied to Core generation, topology, definition set and proof identity.
5. **Publication:** staged rows verified; one durable manifest/fence marks an entire compatible result family eligible.
6. **Serving eligibility:** query selects current published manifest only if generation/source fence is not suppressed by a known invalidation.

Candidate lifecycle: `NOT_ELIGIBLE → COMPUTING → STAGED → VERIFIED → PUBLISHED`; failure → `FAILED` with no visibility; source invalidation/generation change → `SUPPRESSED/REPROCESSING`; corrected output creates a new revision. Partial metric availability can be published only if approved metric-specific quality policy allows it, with explicit per-metric states and no blending of old/new rows.

## Atomic visibility across SQLite and ClickHouse

There is no distributed transaction. Proposed protocol:

1. Under local SQLite transaction and current owner/generation fence, create deterministic result identity and `STAGING` manifest with expected output grains/count, proof identity and definition versions.
2. Compute output bytes/rows deterministically from one verified input proof. Record digest and stage immutable rows in ClickHouse with deterministic insert token/row identity where supported. Retry ambiguous inserts idempotently; never assume a timeout means failure.
3. Read back by immutable output identity and verify row count, keys, values/digest and proof lineage independently of the aggregation code.
4. In a SQLite transaction, recheck generation, suppression, invalidation cursor/fence and owner lease; mark manifest `PUBLISHED` and advance current-effective pointer only if every expected grain verifies.
5. Serving resolves exactly one manifest/revision and filters all products by it. It never independently takes latest revision from each table.

Crash after ClickHouse insert but before manifest publish leaves invisible staged rows. Recovery re-verifies and publishes if fences still hold, otherwise marks them orphaned/suppressed for retention cleanup. Crash after SQLite publish returns the same effective manifest. If source invalidation arrives later, suppression is durable locally before new work; serving must consult the effective manifest/fence so invalidated revision is not selected after Gold observes the invalidation. The design cannot promise instantaneous source-to-serving suppression before Silver invalidation is observed.

## Failure matrix

| Failure point | Durable state | May serve? | Recovery and expected result |
|---|---|---|---|
| Before stage manifest | No business obligation | Existing prior published revision only if its fence remains valid | Retry current generation from verified source |
| After `STAGING`, before CH write | Manifest only | No new result | Resume same deterministic ID or fail it; no duplicate revision |
| CH insert rejected | Manifest + partial/none rows | No staged result | Retry bounded insert; verify all expected rows |
| CH accepted, client timed out | Possibly all rows; outcome ambiguous | No new result | Query deterministic identity, compare expected digest, then continue or repair |
| Some grains inserted | Partial rows + `STAGING` | No new result family | Insert missing rows, verify whole family; never expose individual grain early |
| Read-back/validation mismatch | Staged rows + failed validation | No new result | Quarantine result identity, preserve evidence, recompute only after cause resolved |
| Crash before publication transaction | Verified rows, unpublished manifest | No new result | Recheck generation/fence then publish or suppress |
| Crash after publication transaction | Published manifest and pointer | Yes, all products under same manifest | Idempotent restart returns same revision |
| Invalidation observed before publication | Suppression/fence durable | No invalidated result | Discard/stage retained data as orphan; recompute new generation |
| Invalidation arrives after publication | Existing published manifest then new suppression | Once suppression is durable, prior result unavailable/current status reprocessing | Create corrected revision; never blend products |
| Replay run | Isolated namespace/database/lock | No live result | Replay produces deterministic isolated manifest; no live writes |

## Required invariants and oracle

All direction/intersection/network rows served together must carry one publication manifest, run generation, topology, definition set and business revision. A missing metric cannot borrow its value from an older revision. Use idempotent result IDs; conflicting output for same ID is an integrity fault. Replay state is isolated and V2 is untouched. An independent source-derived oracle checks expected slot set, values, row identities, digests and effective selection. Crash tests inspect both SQLite manifests and ClickHouse physical rows plus serving results.

