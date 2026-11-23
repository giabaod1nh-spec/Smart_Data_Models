# Phase 4A-Pre implementation status

Baseline `a2f3ac6b98c7ca325213b617e603825481202023` on `codex/phase-1-v1-raw`. The owner approved Option A and the initial resource limits. [Contract v1](contract-design.md#10-normative-contract-v1) is the public specification.

| Package | Implementation | Verification |
|---|---|---|
| P4Pre-01 | Approved source strategy, v1 bounds and independent acknowledgment ordering recorded. | Contract and literal receipt vectors in `contract-design.md`. |
| P4Pre-02 | Additive methods on `CanonicalSilverReader` and `CanonicalReadSession`. Frozen DTOs and typed errors in `de/silver/incremental_contract.py`. | Stage A request, DTO and legacy compatibility cases. |
| P4Pre-03 | Consecutive journal point reads, current Bronze identity, Silver obligations, archive and ClickHouse reconciliation. | Real Stage A authority fixture and independent SQLite journal and obligation oracle. |
| P4Pre-04 | Captured allocator and applied boundaries, final checks, RFC 8785 and exact receipt/envelope sizing. | Literal JCS vectors and concurrent append/conflict cases. |
| P4Pre-05 | Cold discovery verifies the retained source before cursor zero. Identical-binding resume checks the next requested prefix. | Multi-run, topology digest, late sequence, rewind, gap and replacement cases. |
| P4Pre-06 | Count, byte and time budgets in feed reads, with ClickHouse evidence result and execution ceilings. | Exact byte and time limit cases and focused load profile. |
| P4Pre-07 | Independent fixed-ceiling invalidation feed and test-only SQLite receipt/cursor probe. | Invalidation append/mirror cases and subprocess crash before and after each acknowledgment. |
| P4Pre-08 | Real ClickHouse Stage A suite and reproducible paced Outbox replay profile. | See [acceptance-result.md](acceptance-result.md) for actual gate status. |

## Phase 4A-Core handover

- Open a Silver `CanonicalReadSession` with its approved read credentials. Import the request and response types and `canonical_bytes` / `batch_digest` from `de.silver.incremental_contract`.
- Call `discover_canonical_source(SourceDiscoveryRequest("live"))` for a new source. Persist the binding and both cursors at zero after successful discovery. For an existing binding pass its saved ID and retain its acknowledged cursors. A source replacement requires explicit recovery.
- Call `read_canonical_changes_page(CanonicalChangePageRequest(...))` using the last committed exclusive change cursor. Verify receipt version, binding, interval, byte length, entry count and digest. Commit only the actual returned interval.
- Call `read_canonical_invalidations_page(CanonicalInvalidationPageRequest(...))` with the independent exclusive invalidation cursor and fixed ceiling. Before acknowledging a change receipt, acknowledge invalidations through that receipt's `safe_invalidation_through_sequence`. Preserve local suppression when the mirror status is pending.
- Use Gold-local atomic transactions for receipt and cursor acknowledgment, suppression before work creation, and dirty-generation fencing. Core owns the one active runtime and isolated `de/artifacts/gold_v1/` checkpoint and lock. The probe writes test state under the test directory.

## Measurement scope

The feed profile drives the real Bronze and Silver apply paths using genuine retained V1 observation bytes and an isolated ClickHouse database. Its consumer is a separate process. It measures the Silver feed and durable test acknowledgment, with independent local resource sampling. The whole pipeline sustained capacity remains `NOT MEASURED`.
