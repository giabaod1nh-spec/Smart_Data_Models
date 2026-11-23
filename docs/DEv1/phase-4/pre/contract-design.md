# Phase 4A-Pre — Silver V1 to Gold V1 canonical incremental contract

**Status:** proposed for owner review; planning only  
**Evidence baseline:** Phase 3 commit `443f40e3ddde0f9cf657785b3fbf1851281897ea`  
**Scope:** a Silver-owned source contract for the future Gold V1 runtime. This is not a claim of a cross-database snapshot or transaction.

## 1. Contract objective

Gold needs a durable way to discover every Silver-materialized source change, including changes across retained runs, late event IDs, and conflict transitions. Silver remains responsible for canonicalization and semantic verification. Gold must never read Bronze journals or Silver checkpoint tables directly.

The proposed contract is a namespace-scoped, change-sequence feed. Each returned batch covers an exact, exclusive/inclusive sequence interval and assigns exactly one verified disposition to every sequence in it. A receipt describes that bounded batch and its source boundary; Gold persists the receipt and downstream work atomically in its own state store.

## 2. Observed behavior and limits

`CanonicalSilverReader` currently accepts a query with explicit namespace, run ID, topology digest, and simulation-time bounds. It pages valid bundles ordered by event ID and returns an opaque continuation token. It checks Bronze and Silver highwaters/applied cursors around a page, and a complete scan stages all results in memory before returning. It does not expose a global source-sequence cursor, run discovery, or a public durable receipt.

The Silver worker consumes contiguous Bronze change and invalidation journals independently. It persists per-sequence frozen output obligations, and its current event-validity table stores only the event's current `VALID` or `INVALIDATED` state. Bronze stores current identity state separately from append-only `v1_changes` and independent `v1_invalidations`. On conflict, current identity moves from `ACCEPTED` to `CONFLICTED`; the point read of the accepted revision then returns no observation. Consequently, a current `silver_v1_event_validity` row cannot prove that every historical accepted sequence was valid or materialized.

Source binding currently verifies source identity, schema/version, allocator-derived highwaters, and contiguous history at bind/recovery. Bronze page reads use the maximum journal sequence as their effective latest sequence; the reader itself does not prove the allocator/history invariant. The new Silver contract must verify it rather than assume it. Existing archive-integrity failures fault processing; they are not an approved skip disposition.

## 3. Public API shape

The following names and fields are the proposed exact public contract to freeze in P4Pre-01. They are additive; existing `CanonicalReadQuery`, page-token and complete-scan APIs retain their current behavior.

```python
class CanonicalSilverChangeSource:
    def discover_canonical_source(self, request: SourceDiscoveryRequest
                                  ) -> SourceDiscovery: ...
    def read_canonical_changes_page(self, request: CanonicalChangePageRequest
                                   ) -> CanonicalChangePage: ...
    def read_canonical_invalidations_page(self, request: CanonicalInvalidationPageRequest
                                         ) -> CanonicalInvalidationPage: ...
```

The API is Silver-owned and is the only production source contract Gold uses. `discover` does not authorize starting at the latest cursor: a new consumer receives a cold-bootstrap boundary whose `first_sequence` is zero and whose full retained history has been verified. If Gold has a saved cursor, it supplies the saved binding and resumes from that exclusive cursor. A source replacement returns a typed fatal binding error and requires explicit new namespace/generation bootstrap.

`read_canonical_changes_page` is namespace-global, not filtered to a run. Every journal sequence must be represented, including a change for a run Gold would otherwise not query. A bundle contains immutable run identity (run ID, generation, topology digest, manifest identity), event identity and revision, canonical payload/lineage/quality facts, and source evidence hashes. Per-run subscriptions may be layered above this global stream only if they preserve the global cursor and disposition coverage.

### DTOs

`CanonicalChangePage` is the single bounded/frozen unit. The next-page continuation is the exclusive integer `after_change_sequence=prior.through_change_sequence`; there is no client-decoded token and no promise of one snapshot across several requests. Every request captures a fresh finite boundary. `boundary_id` is `"sha256:" + SHA256(canonical_json({contract_version, source_binding_id, Hc, Hi, Sc, Si}))`; `receipt_id` additionally binds this page's actual interval and ordered entries.

`CanonicalChangePage` contains:

- namespace and stable `source_binding_id`;
- exclusive `after_change_sequence` and inclusive `through_change_sequence`;
- captured Bronze change and invalidation allocator highwaters;
- Silver applied change and invalidation cursors observed for the boundary;
- source/schema/reader/contract versions;
- ordered entries, entry count, encoded byte count, and batch digest;
- a `CanonicalBatchReceipt` with `status=OK` only after exact coverage and final validation.

Each entry has the source sequence, event key, source change kind, immutable transition evidence, a typed disposition, and either a canonical bundle or a reason/evidence reference. The digest covers canonical serialization of the ordered entries and receipt boundary. It detects accidental mismatch; it is not a signature or proof against a compromised source.

For current Bronze V1, `source_transition_hash` is SHA-256 over canonical JSON of the exact immutable `v1_changes` fields `{namespace, change_sequence, event_id, revision, change_type, previous_state, new_state, content_hash, invalidation_id}`, with absent previous state/invalidation ID encoded as null. `created_at` remains source evidence but is excluded from semantic identity hash. A source schema change that alters this field set requires a new contract version.

The independent `CanonicalInvalidationPage` has its own exclusive/inclusive invalidation sequence interval, captured invalidation highwater, binding/version, conflict identities, and receipt. It is never implied by the change receipt.

### Per-sequence dispositions

1. **`BUNDLE_VALID`** — the sequence is an accepted transition that remains the current accepted revision at the batch's verified source boundary; Bronze identity matches its accepted sequence/revision/hash; Silver's durable obligation for that sequence is reconciled; archive/run identity is verified; and the complete canonical bundle passes reconciliation.
2. **`SUPERSEDED_BY_CONFLICT`** — the sequence was accepted, then a later conflict sequence for the same event is present within the captured change highwater. The entry names that later sequence and linked invalidation. It carries no bundle. Silver must verify both transitions and their linkage. This explicitly preserves sequence coverage without presenting a no-longer-current accepted fact as valid.
3. **`CONFLICTED`** — the sequence itself is the explicit conflict transition, linked one-to-one to the independent invalidation journal and to Silver's frozen invalidation obligation. It carries no bundle.

There is no generic `SKIPPED`, `QUARANTINED`, or “invalid” success status. If Silver later introduces a durable, sequence-bound quarantine authority, it requires a separately approved contract version and evidence requirements. Missing/corrupt evidence, gap, illegal transition, absent obligation, archive corruption, stale binding, and bound overflow are errors that produce no receipt and no cursor progress.

## 4. Historical validity and ACCEPTED → CONFLICTED

The feed reconstructs disposition from append-only Bronze transitions and Silver's per-sequence frozen obligations, not from the single current event-validity row. For each accepted sequence, Silver checks whether a later conflict for that same event lies at or below the frozen change highwater. If so, it returns `SUPERSEDED_BY_CONFLICT`; otherwise it can return `BUNDLE_VALID` only after current identity and all materialized evidence agree. The matching conflict sequence is separately emitted as `CONFLICTED` and its invalidation is independently consumed.

This design reports current-effective canonical facts plus complete transition coverage. It does not promise historical as-of payload availability: Bronze's current point read only returns a currently accepted matching revision. Historical accepted facts already superseded by conflict are represented as a disposition, not reconstructed as valid bundles. An accepted sequence whose conflict lies after the captured boundary can be included only if the final live identity check still sees that acceptance; otherwise the batch is stale and must be retried.

## 5. Frozen batch and consistency boundary

For each request, Silver captures Bronze allocator highwaters `Hc`/`Hi` and its applied cursors `Sc`/`Si`. The initial candidate change ceiling is `min(Hc, Sc)`, and the independently deliverable invalidation ceiling is `min(Hi, Si)`. Silver then shortens the change ceiling at the first sequence whose effective disposition cannot be verified using the captured prefix plus current-identity guard. It does not require `Sc == Hc` or `Si == Hi`. Exact safe-prefix rules are normative in the final revision section below. Silver applies entry, encoded-byte and processing-deadline limits while reading/serializing; it never builds an unbounded result and checks the byte cap only afterward.

The captured highwaters are frozen boundaries for the request, not a cross-store snapshot. Silver reads SQLite and ClickHouse evidence under their existing consistency mechanisms, then validates the source binding, exact sequence coverage, obligations, and relevant current identities before issuing the receipt. Silver does not require live allocator highwaters to remain equal to `Hc`/`Hi`: producer appends above those boundaries are deferred to the next batch. A change to an event included in the batch that makes its claimed disposition no longer true causes a stale-batch error and no receipt. A newly appended conflict above `Hc` can therefore invalidate a not-yet-acknowledged accepted bundle; the independent feed and subsequent read repair it, and Phase 4A metadata remains unpublished.

There is no guarantee that all Bronze SQLite and ClickHouse reads reflect one global transaction-time snapshot. A receipt states exactly which boundaries and checks were performed; it must not be described as an arbitrary historical snapshot.

## 6. Cold bootstrap, run discovery, and source binding

Cold bootstrap begins at sequence zero and verifies every retained change and invalidation sequence through captured highwaters, plus allocator agreement and available Silver obligations. This is a one-time streaming full-history verification for a new binding; it is not repeated on each poll or ordinary restart. The first delivered batch may be bounded; bootstrap does not require one in-memory full-history result. Runs are discovered from verified journal entries and their immutable archive identity. A run with no journaled events is not discoverable from this feed and needs a separately specified run-registration source if Gold must represent empty runs.

`source_binding_id` is stable for the same namespace and configured Bronze identity: `"sha256:" + lowercase_hex(SHA256(canonical_json({namespace, canonical_source_path, file_identity:[st_dev,st_ino], schema_fingerprint, bronze_version})))`. Sequence highwaters are receipt fields, not part of this stable ID. A saved cursor can resume only with the identical binding. Rewind, replacement, schema change, missing retained history, or allocator/journal mismatch fails closed. Existing Phase 3 operational assumptions—no journal pruning or rewind—remain required unless a future retention protocol is approved.

## 7. Bounds and liveness

The contract bounds entries and serialized bytes per batch, and defines a maximum single bundle/evidence size. The Gold consumer holds only one batch (or bounded durable staging) at a time. The Silver implementation must use streaming/page reads instead of `read_complete_scan`'s unbounded list. Oversized individual records are a hard error; a future chunked-bundle contract is required to support them.

Change-sequence reads are proportional to new journal entries plus bounded evidence checks. A separate invalidation cursor avoids conflating change coverage with invalidation acknowledgment. Liveness requires the Silver worker to continue applying through captured highwaters and the producer to retain journal history. No latency or throughput SLO is asserted here; the owner must set workload and resource budgets before sustained-load acceptance.

## 8. Ownership and acknowledgment

**Silver owns:** Bronze source binding verification; sequence continuity and transition validation; canonical bundle and disposition authority; run/archive checks; applied-obligation verification; bounded batch assembly; frozen boundary and receipt; independent invalidation feed; stale-source detection and typed errors.

**Gold owns:** its durable change and invalidation cursors; idempotent receipt storage; per-sequence downstream work intent/acknowledgment; local transaction that records all batch dispositions and advances the change cursor; invalidation suppression/dirty work and invalidation cursor advancement; retry and backpressure policy. Gold acknowledges only after downstream effects are durably represented in its own store. No remote Silver acknowledgment write is required.

## 9. Error and replay rules

Gap, corrupted evidence, source replacement, unsupported contract version, stale included identity, missing obligation, invalid transition, or per-record overflow returns a typed failure and no success receipt. Gold leaves its cursor unchanged and retries only after the cause is repaired or an owner-approved new binding/generation is established. A successful batch can be replayed by requesting the same exclusive cursor; the API may see a newer frozen boundary, so Gold deduplicates by namespace, binding, sequence, disposition, and content hash. It must reject a different disposition/hash for an already committed sequence under the same binding.

See `decisions.md` for the alternative analysis, `plan.md` for implementation packages, and `acceptance.md` for executable evidence gates.

## Final architecture revision — normative additions

This section supersedes any earlier draft sentence requiring `Sc >= Hc` or `Si >= Hi` before every batch. Its meanings take precedence over illustrative wording above.

### Exact request and response contract

Public API methods:

```python
discover_canonical_source(SourceDiscoveryRequest) -> SourceDiscovery
read_canonical_changes_page(CanonicalChangePageRequest) -> CanonicalChangePage
read_canonical_invalidations_page(CanonicalInvalidationPageRequest) -> CanonicalInvalidationPage
```

Request DTOs (all IDs are nonempty strings; sequences are nonnegative integers; booleans are not accepted as integers):

```text
SourceDiscoveryRequest:
  namespace: str
  expected_source_binding_id: str | None  # None only for a new binding

CanonicalChangePageRequest:
  namespace: str
  after_change_sequence: int               # exclusive
  expected_source_binding_id: str
  max_entries: int                         # 1..100; initial proposed cap
  max_encoded_bytes: int                   # positive; owner-frozen cap
  max_processing_ms: int                   # page wall-clock budget; owner-frozen cap
  max_entry_processing_ms: int             # per-sequence evidence deadline; owner-frozen cap

CanonicalInvalidationPageRequest:
  namespace: str
  after_invalidation_sequence: int          # exclusive
  through_invalidation_sequence: int        # inclusive immutable ceiling
  expected_source_binding_id: str
  max_entries: int                          # 1..100
  max_encoded_bytes: int                    # owner-frozen cap
  max_processing_ms: int                    # owner-frozen cap
  max_entry_processing_ms: int              # per-invalidation deadline; owner-frozen cap
```

The `100` ceiling matches existing reader page limits as a conservative initial API ceiling, not a proven performance optimum. Byte, per-entry and time caps are not selected until P4Pre-01 records owner-approved measured values. `SourceDiscovery` advertises the server maxima including `max_single_entry_bytes` and `max_entry_processing_ms_cap`; requests above a server maximum are rejected. Implementations enforce actual encoded bytes incrementally and include receipt overhead in the limit.

`SourceDiscovery` fields: `contract_version`, `namespace`, stable `source_binding_id`, `source_binding` (canonical source path/file identity/schema fingerprint/Bronze version), captured `bronze_change_high_water`, `bronze_invalidation_high_water`, `silver_change_applied_sequence`, `silver_invalidation_applied_sequence`, `verified_change_prefix`, `verified_invalidation_prefix`, server caps `max_entries_cap`, `max_encoded_bytes_cap`, `max_single_entry_bytes`, `max_processing_ms_cap`, `max_entry_processing_ms_cap`, `next_change_sequence=0`, `next_invalidation_sequence=0`, `bootstrap_required`, and `run_discovery_scope="journaled-runs-only"`. A new binding gets `bootstrap_required=true` and no consumer cursor initialized until the full streaming bootstrap check succeeds. An unchanged saved binding gets `bootstrap_required=false`; ordinary resume verifies static binding identity, cursor ranges and recent/next-prefix continuity, not all history. Any unsupported schema, binding replacement, lost retained history or rewind fails closed.

`CanonicalChangePage` fields: `contract_version`, `source_binding_id`, `after_change_sequence`, `through_change_sequence`, `captured_bronze_change_high_water`, `captured_bronze_invalidation_high_water`, `captured_silver_change_applied_sequence`, `captured_silver_invalidation_applied_sequence`, `safe_invalidation_through_sequence`, `verified_change_prefix`, ordered `entries`, `entry_count`, `encoded_bytes`, `boundary_id`, `has_more`, `status`, and `receipt | None`. `through_change_sequence` is the last sequence actually included; it may never mean the requested maximum or captured Hc unless the response covers every sequence up to that value. `has_more=true` means another source sequence remained within `min(captured Hc,captured Sc)` at that receipt boundary; it does not promise the next sequence is safe to emit, so a later call can return `PREFIX_BLOCKED`. Data appended after capture does not change it.

`CanonicalInvalidationPage` carries the independent invalidation interval, captured Bronze/Silver boundaries, `through_invalidation_sequence`, ordered invalidation entries, count/bytes, same binding and version, `has_more`, and its own receipt. It returns only durable Silver invalidation obligations through the requested frozen `through` ceiling. ClickHouse mirror status may be `PENDING`; that does not erase Bronze/Silver local invalidation authority or block suppression-feed delivery.

Exact invalidation response fields are `contract_version`, `source_binding_id`, `after_invalidation_sequence`, `through_invalidation_sequence`, `requested_through_invalidation_sequence`, `captured_bronze_invalidation_high_water`, `captured_silver_invalidation_applied_sequence`, ordered `entries`, `entry_count`, `encoded_bytes`, `has_more`, `status`, and `receipt | None`. Each entry contains `invalidation_sequence`, `invalidation_id`, `event_id`, `semantic_revision`, `previous_immutable_envelope_hash`, `conflicting_immutable_envelope_hash`, `source_transition_hash`, verified run/generation/topology/manifest identity, and `mirror_status`. Page coverage is exactly `(after, through]`. If requested through is above `min(Hi,Si)`, the service clamps to the safe available prefix only when it can return a nonempty contiguous prefix; if no sequence is available, it returns retryable `INVALIDATION_LAG` with no receipt. Empty equal-cursor response has no receipt and no progress.

Entry immutable transition fields: `change_sequence`, `event_id`, `semantic_revision`, `change_type`, `previous_state`, `new_state`, `source_transition_hash`, `source_invalidation_id | None`. These values are derived from Bronze append-only journal and cannot change for a binding. Boundary-dependent fields: `effective_disposition`, `superseding_change_sequence | None`, `invalidation_sequence | None`, `canonical_bundle | None`, `bundle_hash | None`, and `evidence_status`. These can legitimately differ at a newer boundary. No run ID is used to filter the namespace-global sequence, but every bundle/disposition includes verified run ID, generation, topology digest and immutable manifest identity.

Disposition values remain exactly `BUNDLE_VALID`, `SUPERSEDED_BY_CONFLICT`, `CONFLICTED`. A superseded accepted transition is a correct effective disposition, not journal corruption. Genuine disagreement in immutable journal fields, hashes, identity linkage or invalidation provenance is `SOURCE_INTEGRITY_ERROR` and stops progress.

### Verified applied prefix and safety algorithm

At request start Silver reads a consistent Bronze SQLite allocator boundary and Silver-local applied cursors, checking the existing static source binding. Let `Hc, Hi, Sc, Si` be those four values. Require `0 <= Sc <= Hc` and `0 <= Si <= Hi`; violations are `SOURCE_INTEGRITY_ERROR`, except a source rewind/replacement is `SOURCE_BINDING_CHANGED`. Define change ceiling `Pc=min(Hc,Sc)` and invalidation ceiling `Pi=min(Hi,Si)`. These are upper bounds, not proof for every item.

Starting at `s=after_change_sequence+1`, Silver processes sequences in strict order through `Pc`, checking each sequence and serializing each complete entry before reading the next. For each accepted transition, Silver validates immutable transition evidence, its durable per-sequence obligations and canonical ClickHouse facts. It also reads the current Bronze identity and Silver-local current validity:

- Current Bronze identity still `ACCEPTED` at this exact sequence/revision, Silver local validity is `VALID` for the same sequence/revision, and reconciled fact/archive obligations agree: `BUNDLE_VALID`.
- Current Bronze identity is `CONFLICTED` at `c <= Sc`, the exact conflict transition is present and linked, its invalidation sequence `i <= Si`, Silver local validity is `INVALIDATED`, and all local invalidation obligations match: `SUPERSEDED_BY_CONFLICT` for accepted `s`. The conflict sequence `c` itself will have its own `CONFLICTED` entry when the page reaches it.
- Current Bronze identity points to a conflict with `c > Sc`, or its invalidation is not applied through `Si`, or Silver validity is inconsistent/incomplete: stop before `s`; return any already completed contiguous safe prefix. If zero entries precede it, return `PREFIX_BLOCKED` with no receipt and retryable lag metadata. Do not emit bundle, skip, or superseded disposition on Silver's behalf before Silver has applied the transition/obligation.
- Current identity has advanced beyond captured `Hc`, or changes during final verification: abort the entire page with `BOUNDARY_STALE`, no receipt. This is producer churn across the frozen boundary, not permanent immutable-journal corruption.
- Explicit conflict sequence is eligible only if the paired invalidation is within `Pi`, Silver has durable local invalidation obligation and local validity is `INVALIDATED`; otherwise truncate before that conflict sequence.

After each candidate entry, enforce `max_entries`, encoded-byte limit including envelope/receipt overhead, page wall-clock budget and per-entry evidence deadline. If a completed entry fits but the page budget is exhausted before starting another, return the already serialized nonempty safe prefix with `has_more=true`. If one entry exceeds `max_single_entry_bytes`, return actionable `RECORD_TOO_LARGE`; if its evidence work alone exceeds `max_entry_processing_ms`, return `ENTRY_TIMEOUT` with event/sequence and no receipt. Both require changed capacity/configuration or repaired source/dependency before retry, never skip or loop unchanged forever. A page deadline before first entry returns retryable `DEADLINE_EXCEEDED` only when no individual entry timeout occurred; caller may lower page size or inspect dependency. An entry count of zero is a successful `EMPTY` no-op only when `after == Hc == Sc` and no known unsafe included event blocks the prefix; it has `through=after`, `has_more=false`, `receipt=None`, and no cursor progress. If `after > Hc`, return `CURSOR_AHEAD`; if `after > Sc`, return permanent `SILVER_CURSOR_REGRESSION`; if `after == Sc < Hc`, return retryable `SOURCE_LAG` with no receipt. These are not empty successes.

Before issuing a nonempty receipt, Silver rechecks static binding, verifies the exact journal interval and all included effective identities/obligations again. It requires end-boundary allocators and applied cursors to be greater than or equal to their captured values; any rewind/regression fails closed. It intentionally does not require live Hc/Hi/Sc/Si to remain equal to captured values. New unrelated appends or applied progress above capture are deferred. A newly appended conflict touching an included event before final check makes the response stale. A conflict appended after receipt is an unavoidable cross-store race, repaired by the separate invalidation stream; Phase 4A does not publish it.

### Receipt identity and serialization

`CanonicalBatchReceipt` exact fields: `contract_version`, `receipt_kind` (`CHANGE` or `INVALIDATION`), `namespace`, `source_binding_id`, `requested_after_sequence`, `returned_after_sequence`, `returned_through_sequence`, `captured_bronze_change_high_water`, `captured_bronze_invalidation_high_water`, `captured_silver_change_applied_sequence`, `captured_silver_invalidation_applied_sequence`, `safe_invalidation_through_sequence` (change receipt only; null for invalidation receipt), `entry_count`, `encoded_bytes`, `batch_digest`, and `receipt_id`. Receipt records the checks/boundaries actually observed; it does not claim a distributed snapshot.

Canonical bytes use UTF-8 JSON Canonicalization Scheme (RFC 8785): Unicode strings normalized to NFC before encoding, lexicographically ordered object keys, no insignificant whitespace, arrays in sequence order, shortest round-trip binary64 number representation, no NaN/Infinity, and explicit `null` for absent optional fields. Bundle objects use the canonical fact field set and deterministic array ordering. `batch_digest` is lowercase SHA-256 hex over canonical JSON of `{contract_version, receipt_kind, namespace, source_binding_id, requested_after_sequence, returned_after_sequence, returned_through_sequence, captured_bronze_change_high_water, captured_bronze_invalidation_high_water, captured_silver_change_applied_sequence, captured_silver_invalidation_applied_sequence, safe_invalidation_through_sequence, entry_count, ordered entries}`; `encoded_bytes`, `batch_digest`, and `receipt_id` are excluded from digest input. `encoded_bytes` equals final canonical response byte length, including receipt/digest/ID and its own decimal field; compute it by serializing with a candidate count until byte length equals the encoded integer. `receipt_id="sha256:" + batch_digest`. This is an integrity/idempotency identifier, not a source signature.

For a fixed saved response, receipt bytes and digest replay identically. A new read under a later boundary may return a different effective disposition and a different receipt for the same transition. Gold/test consumer treats that as legitimate only when the earlier receipt was not durably acknowledged; a different immutable transition hash for a committed sequence under the same binding is a permanent integrity error. API version change that changes canonical fields requires a new `contract_version`; old receipts remain verifiable with their recorded version, while resume under an unsupported version returns `UNSUPPORTED_CONTRACT_VERSION` and does not reset cursors.

### Status and error classification

| Code/status | Retryable | Receipt/progress | Meaning/action |
|---|---:|---|---|
| `OK`, `EMPTY` | n/a | Nonempty receipt for `OK`; `EMPTY` has no receipt and no cursor progress | Exact verified prefix / no available applied prefix. |
| `PREFIX_BLOCKED`, `SOURCE_LAG`, `INVALIDATION_LAG` | yes | No receipt when zero safe entries; cursor unchanged | Silver must apply/reconcile the blocking sequence; retry same cursor. |
| `DEADLINE_EXCEEDED`, `AUTHORITY_UNAVAILABLE`, `CLICKHOUSE_UNAVAILABLE` | yes | No receipt if no complete entry; already serialized safe prefix may return only if final validation completed | Bounded retry/backoff; no skip. |
| `BOUNDARY_STALE` | yes | No receipt | Included event changed across request; retry from same Gold cursor with fresh boundary. |
| `SOURCE_BINDING_CHANGED`, `SOURCE_REWOUND`, `RETENTION_GAP` | no | No receipt | Stop. Owner-led new binding/bootstrap or restore history; never auto-reset. |
| `SOURCE_INTEGRITY_ERROR`, `CANONICAL_EVIDENCE_MISSING`, `INVALID_TRANSITION`, `INVALIDATION_LINK_MISMATCH` | no | No receipt | Repair source/evidence through governed recovery; no disposition invented. |
| `RECORD_TOO_LARGE` | no until remedied | No receipt containing that record; cursor does not advance past it | Report exact seq/event/observed bytes/cap. Owner raises cap after resource review or approves a new chunked bundle contract. Never retry unchanged limits forever. |
| `ENTRY_TIMEOUT` | no until remedied | No receipt; cursor unchanged | One sequence exceeded per-entry evidence deadline; report exact seq/event/elapsed operation and dependency. Increase approved cap or repair source/dependency, do not skip. |
| `INVALID_REQUEST`, `UNSUPPORTED_CONTRACT_VERSION`, `CURSOR_AHEAD`, `SILVER_CURSOR_REGRESSION` | no | No receipt | Caller/config/version error or Silver applied cursor regressed behind an acknowledged consumer; correct/restore without reset. |

### Race-condition table

| Race/state | Safe behavior |
|---|---|
| Bronze appends unrelated changes while Silver lags | Deliver `(Gold, min(Hc,Sc)]` up to entry/byte/time cap; later appends wait next call. No `Sc==Hc` gate. |
| Conflict exists in Bronze before read but Silver has not applied it | Current identity reveals conflict. Truncate before the affected accepted transition; if no prior safe entry, `PREFIX_BLOCKED`; no stale bundle. |
| Silver change cursor advanced but invalidation cursor lags | `Pc` can progress for unaffected events. Any accepted transition whose current state is conflicted cannot be superseded until matching conflict and invalidation are applied through Sc/Si. Change receipt includes `Pi`; Gold must consume invalidations through that frozen `Pi` before acknowledging the change receipt. |
| ACCEPTED becomes CONFLICTED during page | If conflict is within captured/applied prefixes and all obligations agree before final check, emit superseded + conflict dispositions; otherwise stale or truncate before accepted sequence. Never emit old accepted bundle. |
| Conflict appended above captured Hc after boundary capture | If observed before final check for included identity, abort `BOUNDARY_STALE`; if committed after final check/receipt, later invalidation repairs Gold. No atomicity claim. |
| Invalidation durable locally but ClickHouse mirror pending | Silver invalidation API may emit durable suppression with `mirror_status=PENDING`; do not wait for mirror to suppress. Canonical fact bundle still requires all expected CH fact rows reconciled. |
| Producer keeps appending unrelated rows | Hc is frozen for each request; finite page and cursor progress continue even when live Hc keeps rising. |
| Silver permanently lags | Gold can consume safe prefix below Sc; at first unresolved sequence it stops without skipping. Health becomes degraded by lag/age. Buffering does not cure a μ<λ condition; capacity intervention is required. |

### Deterministic examples

Compact examples omit opaque binding details and full canonical bundle fields; they illustrate exact boundary fields/status semantics.

```json
{"contract_version":"1","status":"OK","source_binding_id":"bnd-7","after_change_sequence":100,"through_change_sequence":135,"captured_bronze_change_high_water":200,"captured_silver_change_applied_sequence":150,"safe_invalidation_through_sequence":42,"entry_count":35,"has_more":true,"entries":[{"change_sequence":101,"change_type":"ACCEPTED","transition_hash":"…","effective_disposition":"BUNDLE_VALID","bundle_hash":"…"},{"change_sequence":135,"change_type":"ACCEPTED","transition_hash":"…","effective_disposition":"BUNDLE_VALID","bundle_hash":"…"}],"receipt":{"requested_after_sequence":100,"returned_through_sequence":135,"batch_digest":"sha256…","receipt_id":"sha256:…"}}
```

Empty applied-prefix response: `{"status":"EMPTY","after_change_sequence":150,"through_change_sequence":150,"captured_bronze_change_high_water":200,"captured_silver_change_applied_sequence":150,"entry_count":0,"has_more":false,"receipt":null}`. Cursor remains 150.

Conflict at a later boundary: saved receipt at `Hc=100` has transition 100 as `BUNDLE_VALID`; a later page at `Hc=101, Sc>=101, Si>=linked_i` returns transition 100 as `SUPERSEDED_BY_CONFLICT` with `superseding_change_sequence=101`, then transition 101 as `CONFLICTED`. Its immutable transition hash is unchanged; the new receipt digest differs because effective disposition/boundary differ.

Stale-boundary response: if source identity for an included sequence advances to a conflicting sequence above captured H before final validation, return `{"error_code":"BOUNDARY_STALE","retryable":true,"receipt":null,"cursor_advance":false}` and reread from the same Gold cursor. This is producer churn, not permanent immutable-journal corruption.

### Bootstrap, recovery and acknowledgment boundary

Cold bootstrap performs streaming full-history continuity/linkage verification once at sequence zero. Successful `SourceDiscovery` is persisted by a test harness/Gold with change cursor 0 and invalidation cursor 0. Resume with unchanged binding validates static identity, cursor values against allocator, and exact next-page continuity; it does not rescan all history. For unchanged cursor with missing/corrupt next row, return `RETENTION_GAP`/`SOURCE_INTEGRITY_ERROR`. Historical reconciliation/backfill is a distinct owner-triggered operation and never masquerades as normal polling.

For each change receipt, Gold's eventual implementation must first durably apply the independent invalidation stream at least through that receipt's frozen `safe_invalidation_through_sequence`, then atomically record all change entries/work intents and advance its own change cursor in Gold SQLite. The invalidation cursor is committed atomically with its suppression/dirty obligations in separate local transactions. This is a future Phase 4A-Core requirement, not a Pre test dependency. The Phase 4A-Pre test-only durable harness proves API receipt/cursor replay only and is not represented as production Gold.
