# Gold Business Layer architecture

## Invariants inherited from Phase 4A

Use the public Silver V1 canonical read contract and verified bundle. Preserve source binding, change and independent invalidation cursors, contiguous cursor progression, receipt and bundle hash verification, durable local ACK/work obligations, exact membership, run/topology isolation, work generation, immutable completed-work identity, dirty-window fencing, bounded reads/work quanta, restart recovery and replay namespace isolation. Never query Silver/ Bronze physical tables or private journals from business aggregation. Never use Orion/Projector, active-run state, or legacy Gold V2 as historical authority.

Core's acceptance proves these mechanisms for its tested scope, but not business completeness, definition validity, business publication, cross-mart visibility or Performance Acceptance.

## Data flow proposal

```text
Silver public discovery + verified receipts/bundles
        │
        ▼
Gold Core durable ACK, membership, generation and invalidation fences
        │
        ▼
One bounded verified bundle traversal per eligible Core work proof
        │  derive reusable business sufficient statistics and exact queue sample state
        ▼
Business result staged under (run, topology, window, generation, definition set)
        │
        ▼
Validate expected population + independent quality checks + deterministic digest
        │
        ▼
ClickHouse immutable revision rows + SQLite durable publication manifest/fence
        │
        ▼
Gold serving views/queries select one current-effective complete revision
        │
        └── historical Stage B API; no Orion or active run dependency
```

This is a design proposal only. Choose a single coordinator/store boundary after failure and visibility semantics are reviewed. Do not publish when G-COV or G-REV is unresolved.

## Aggregation alternatives

| Option | Correctness/recovery | Cost and limitations | Recommendation |
|---|---|---|---|
| A. Aggregate during verified materialization | Reuses the exact member proof, generation and bundle; deterministic recompute from current source | CPU per bundle; exact P95 needs bounded values or exact order state; requires Core extension/interface and failure tests | Preferred starting point, conditional on source contract and memory bound |
| B. Bounded verified staging | Can stage bundles/statistics and resume bounded processing | Adds storage, lifecycle and cleanup; durable payload retention duplicates Silver and must revalidate current-effective status | Use only if measured CPU/transaction boundary needs decoupling |
| C. Reusable incremental business state across generations/windows | Potentially less recompute | Late changes/invalidation make subtract/replace correctness complex; two durations have distinct memberships; cache must be generation fenced | Do not adopt without measured need and proof of exact invalidation behavior |
| Separate business reread per KPI/chart | Familiar but repeated source traversal | N canonical reads per metric, amplifies latency and authority load | Reject |

Implement a pure deterministic accumulator, independent oracle, bounded state and versioned metric registry first. At completion of a Core proof, process every verified bundle once and fan out values into all approved accumulators. Exact queue P95 should retain at most one window's samples per direction/work identity; estimated current topology memory is small (e.g. 17×300 values for one 300s region), but generic bound must derive from archived participant/approach counts and maximum eligible slots. Never retain whole history in RAM.

## Window identity discrepancy

Core currently includes scenario in its window identity and produces work/memberships accordingly. Business Spec wants scenario-neutral window identity with scenario provenance on each event. Preserve existing Core behavior until owner decides whether to (a) approve a Core amendment, (b) treat scenario-specific Core proofs as source partitions and deterministically combine them into a scenario-neutral business window using verified memberships, or (c) revise business requirement. Each option has edge cases when scenario changes mid-window and must pass membership/generation oracle. Do not silently alter Core.

## Resource and transaction boundaries

Keep Silver page limits and Core work quantum. Accumulate one member at a time; do not cache raw bundles between generations without a source binding/fence. Stage deterministic outputs and manifest separately from current-effective visibility. Avoid a distributed transaction assumption between SQLite and ClickHouse. ClickHouse inserts are retryable/ambiguous; SQLite controls eligibility only after staged row verification. See `publication-recovery-design.md`.

