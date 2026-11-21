# Phase 3 design decisions

Status: proposed decisions for implementation review. Source: Master Plan §3/§5/Phase 3; current V1 Bronze source and handover; observation/topology contracts; current Silver implementation. No application code changed.

## D3-01 — Bronze semantic authority is the incremental source

**Problem.** Current Silver reads V2 ClickHouse entity tables. V1 Bronze owns semantic classification in SQLite and publishes append-only changes and invalidations.

**Options.** (A) scan ClickHouse observations; (B) consume SQLite change journal and use ClickHouse only as reconciled content mirror; (C) reclassify Raw deliveries in Silver.

**Selected.** B. Silver opens Bronze state read-only, consumes bounded `changes` and `invalidations` pages with a frozen high-water, and owns separate Silver cursors/processing state. Bronze's `current()` is consulted before materialization/serving. Require a new additive Bronze atomic bootstrap + point lookup API before relying on it; it does not currently exist.

**Rationale / tradeoffs.** This preserves Bronze's sole semantic authority and accepts late conflict transitions. It creates a local SQLite read dependency and requires V1-specific adapter tests. A source outage stops cursor advancement and degrades readiness.

**Impact.** P3-01 first; Bronze owns interface implementation/approval, Silver owns adapter. Source evidence: `de/bronze/checkpoint_store.py` (`V1SemanticReader.read_page`, `current`, lineage/provenance), `de/bronze/clickhouse_repository.py:read_valid_observations`, Phase 2 handover.

## D3-02 — Journal high-water and bootstrap are explicit

**Problem.** Separate current scans and page calls are not an atomic snapshot, and `read_valid_observations` is not as-of pagination.

**Options.** Replay entire retained journal from sequence 0; atomically snapshot current accepted identities plus high-water; consume CH pages and patch conflicts afterward.

**Selected.** For first live bootstrap, new Bronze API returns accepted current state and `changes` high-water from one SQLite transaction. Silver materializes that frozen set in bounded batches, then consumes changes strictly after that high-water. Normal startup resumes Silver's own durable cursor. Bootstrap restart retains frozen high-water and obligations. No “begin at latest” shortcut.

**Tradeoffs.** Additive API and possibly long initial catch-up; deterministic and restartable, without claiming Bronze time travel.

**Impact.** P3-01/P3-06. Source evidence: each Bronze page has its own read transaction; CH page is current-state only.

## D3-03 — Archive is the only topology authority

**Problem.** Producer configuration can change after a run; current topology cannot interpret old events safely.

**Options.** Current topology; event direction keys alone; run archive verified by manifest and V1 digest.

**Selected.** Archive keyed by run ID, immutable verified context keyed by `(simulationRunId, runGeneration, topologyDigest)`. Use `contracts.topology.v1_digest.topology_digest`; match manifest identity, generation, digest, participants and exact catalog approaches. No active-config fallback.

**Tradeoffs.** A missing archive pauses/retries that event; malformed or inconsistent archive blocks/quarantines as integrity failure. Archive retention becomes an operational dependency.

**Impact.** P3-02. Evidence: `run_manifest.py`, `v1_digest.py`, topology contract and event contract.

## D3-04 — Three V1 fact tables, two dimensions and one binding

**Problem.** V2 entity tables and current dimensions encode unrelated entities, four-way assumptions, defaults, Camera and run lifecycle.

**Options.** Copy V2 SQL; add a second silver_v1 implementation tree; refactor existing Python modules and add additive V1 tables.

**Selected.** Refactor `de/silver/` in place and add V1-only tables in migration 008. Facts: traffic direction, signal direction, intersection state. Dimensions: run and topology; binding associates a run generation with a topology digest. No scenario/intersection/approach dimension until an actual consumer needs one; scenario and intersection remain values from event/catalog.

**Tradeoffs.** Retire some V2 module functions and rewrite V2-coupled tests; keep historical tables intact. JSON payload columns preserve supported optional fields without inventing analytics columns.

**Impact.** P3-03/P3-04. Evidence: Master Plan fixed grains; V1 metric contract; current source inventory.

## D3-05 — Preserve value, availability and absence independently

**Problem.** V2 fact builders turn absence into zero and omit availability reason.

**Options.** Fill defaults; store nullable metric only; store nullable metric plus per-metric availability and source payload.

**Selected.** For required V1 metrics store nullable numeric value and availability code (`MEASURED`, `UNAVAILABLE`, `NOT_APPLICABLE`); optional absent fields remain absent in raw direction payload and are not converted to a sentinel. Persist canonical direction/intersection payload JSON and availability JSON for all supported fields. A measured numeric 0 stays `0`. Derive aggregate intersection total only when constituent values required by its definition are complete; otherwise NULL and explicit quality state.

**Tradeoffs.** Wider rows and stricter schema tests; Gold receives per-metric coverage inputs without Phase 3 implementing Gold formulas.

**Impact.** P3-03/P3-04. Evidence: `V1_METRIC_CONTRACT.md`, JSON contract via `contracts/traffic_observation.py`; `fact_builders.py` currently defaults missing waiting/arrival to zero.

## D3-06 — Event validity is a durable Silver-owned state; facts are immutable mirrors

**Problem.** Bronze can change ACCEPTED to CONFLICTED after facts were written. ClickHouse MergeTree inserts and async DELETE mutations cannot alone guarantee retraction.

**Options.** Async DELETE; versioned state table in CH alone; Silver SQLite validity authority plus explicit CH invalidation publication, with Gold-facing canonical reader consulting Silver validity; rebuild-only.

**Selected.** Maintain Silver SQLite per-event state (`PENDING`, `VALID`, `INVALIDATED`) and a durable invalidation obligation in the same transaction as advancing the consumed invalidation item. Publish an append-only invalidation row to CH and reconcile it; checkpoint the Bronze invalidation sequence only after the durable local state and required publication obligation are frozen. Canonical Gold-facing Silver reader must filter by Silver's SQLite validity state, never by fact row presence, and overlay invalidation outbox state so a conflicted event is suppressed as soon as Silver has durably observed it, even if CH mirror publication is pending. Keep original fact rows immutable for audit. Gold receives explicit tombstone identity/reason/revision and recomputes affected windows in Phase 4.

**Correctness boundary.** Before Silver can read a new Bronze conflict, it cannot know it exists. Once detected by the authority read, it first marks the event INVALIDATED locally; canonical reads immediately suppress it. If publication fails, invalidation backlog/readiness reports pending and retry recovers. Bronze `current()` and journal remain authoritative for whether event is currently accepted/conflicted; on any source outage or status mismatch, canonical reader fails closed for affected events. Gold must consume the canonical reader contract, not query raw facts directly.

**Example.** Event `E`, run `R`, digest `D`, intersection `B`, direction `north`, cycle 12 was accepted. Silver emits fact `(R,D,E,B,north)` and records validity VALID. Bronze later appends `CONFLICTED` plus invalidation `I`. Silver reads `I`, checks `current(E)=CONFLICTED`, atomically changes E to INVALIDATED and freezes tombstone; Gold-facing read excludes all direction and intersection rows whose `event_id=E`; CH tombstone insert retries until content-reconciled. The other directions from E are suppressed too because the logical observation is the conflict unit.

**Tradeoffs.** Requires Gold's Phase 4 source adapter to read the read-only Silver SQLite validity state/canonical service, and requires preserving that state directory. This is appropriate for the single-owner local Docker project; it avoids reliance on mutation timing. It is not a distributed multi-host design.

**Impact.** P3-05/P3-06 and mandatory Phase 4 handover. Evidence: Master Plan conflict invariant; Bronze monotonic conflict and invalidation source in checkpoint_store.

## D3-07 — Frozen obligations, reconcile, then checkpoint

**Problem.** ClickHouse writes are not transactional with Silver SQLite; lost responses and partial batches can occur.

**Options.** checkpoint before inserts; infer success from IDs; frozen expected rows + positive content reconciliation + CAS.

**Selected.** Persist per-change frozen obligations and hash in SQLite before inserts. Reconcile each fact/dimension/validity/invalidation row by namespace and full content identity after write or uncertain response. Mark local intent complete only after every obligation exists exactly; checkpoint CAS only after all page obligations and validity updates are durable.

**Tradeoffs.** More SQLite metadata and CH read traffic, but repeatable crash recovery. Integrity mismatch faults closed, not “best effort”.

**Impact.** P3-05/P3-06. Reuse current checkpoint CAS, batch ledger patterns only after semantics review.

## D3-08 — Replay freezes event scope and stays physically isolated

**Problem.** Current Silver replay uses ClickHouse V2 offset windows; Bronze reader pages are current-state, not historical snapshots.

**Options.** pretend offsets imply as-of state; run ordinary processor in a replay namespace; bounded run-scoped bootstrap at declared Bronze high-water and isolated targets/state.

**Selected.** Manifest freezes simulationRunId, source namespace, `changes`/`invalidations` cutoffs, verified archive identity and replay ID. Bootstrap from an explicit Bronze state snapshot at those cutoffs only if Bronze can supply it; otherwise replay is limited to current accepted state at captured high-water and must state that cutoff (no as-of claim). Use `replay:<id>` SQLite file and CH `_replay` tables. Never advance or mutate live namespace.

**Tradeoffs.** Bounded semantics, no arbitrary historical state reconstruction. Re-run with same frozen manifest must reconcile idempotently.

**Impact.** P3-06. Evidence: Bronze current reader no time travel; current replay manifest and namespace guard.

## D3-09 — Additive migration and separate runtime profile

**Problem.** Existing 004 is V2 and migration selector has no Silver V1 target.

**Selected.** `008_create_silver_v1.sql` is the next sequence after 007; `IF NOT EXISTS`, additive `silver_*_v1` names, idempotent repeat; add selector such as `--v1-silver` that applies only required V1 tables/migrations and never executes historical V2 mutation. Separate `de-migrate-v1-silver` then `de-silver-v1` under `v1-silver`; one shared ClickHouse.

**Impact.** P3-07; validate clean install, V2 sentinel, repeat migration and worker schema gate.

## Approval and open design

No architectural choice is left silently open. Review is requested for D3-06's requirement that Phase 4 consume the canonical Silver reader and its SQLite validity overlay. Recommended default is approval because a ClickHouse-only asynchronous tombstone cannot make pending invalidation safe. Bronze point lookup/bootstrap (D3-01/02) is an interface dependency owned upstream, not a request to change Phase 2 logic during this planning task.

