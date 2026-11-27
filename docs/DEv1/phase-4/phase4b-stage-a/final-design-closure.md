# Final Implementation Plan Candidate — owner review

**PLAN FROZEN – READY FOR SCOPED IMPLEMENTATION AUTHORIZATION. FULL INTEGRATION BLOCKED. OPERATIONAL ACCEPTANCE PENDING.** Owner architectural directions and D-A14 selection are recorded below. The authoritative amendment is pending.

| Decision / package | Evidence and options | Recommended mechanism, costs and residual risk | Approval / acceptance |
|---|---|---|---|
| D-A01 scope | Master Plan governance, Business Spec B4B, Stage A requirements | Core Gold direction/intersection/network, 60/300, quality, publication, reader, recovery and comparable experiments. B/C own API/UI, Phase 5 operational integration. Optional research stays separate. | Owner approves scope and any Master Plan amendment; existing Stage A acceptance traced. |
| D-A05 schedule / A-P03 | E-01 nominal grid drift; public Silver lacks schedule/outcome authority | Use deterministic producer lattice + sparse outcome evidence. Bare reconstruction cannot distinguish stopped producer from missing observations. Received rows forbidden. Metadata/receipt extension costs bounded bytes and adds source change approval. | SUMO/source + Silver + Gold owners; FA-01…07 and independent archive/cycle/lifecycle oracle. |
| D-A02/03/04/08 / A-P04 | E-01/02 source probes and E-03 math oracle | Adopt weighted speed, stopped ratio and load as candidate definitions. Queue method-qualified, no mixing. Versioned occupancy correction before pressure calibration. Keep raw semantics changes separate. | Metric registry and source owners approve subsets. Calibration and experiment comparability remain gates. |
| D-A11/15 / A-I02/I03 | Single SQLite lease and E-03 sizing floor | Use atomic seal, durable result reuse and separate CH writes. Versioned canonical JSON candidate, no compression until measured. Count/byte reservations include coexisting projection and result. | Approve codec/hard limits, migration rehearsal and FA-08…25. |
| D-A10/11 / A-I04 | SQLite authority with immutable CH rows | Gold reader library/service boundary owns read-only access to both stores and the fence handshake. Stage B calls one Gold query port. Logical interface Query(selector, run, scope, duration, time bounds, metric set, cursor) → rows, completeness, versions, revision IDs, epoch token or explicit unavailable/errors. Keep binding mechanism local in isolated acceptance. Deployment adapter/IPC and credentials need separate review before integration. | No new persistent authority. Race/replica ambiguity and exact rows via FA-15…20. |
| D-A15 / A-I06 | Frozen public Core replay rejects moving boundary | Isolated replay/backfill with separate Core/compute/publication predicates. Retain source through oldest outstanding proof/backfill range. Concurrent historical replay needs separately approved immutable public-compatible capture. | FA-23/26…30, retention and cleanup owner sign-off. |
| D-A14 / A-I01…07 | Existing paced gate ENVIRONMENT BLOCKED; E-03 isolated sizing | Owner selected Conditional Entry. Existing gate remains in effect pending authoritative amendment and package sign-offs. No live activation. Integrated acceptance and G-PERF remain open. | Authoritative owner amendment to Phase 4A gate/governance before any A-I start. |

## Upstream prerequisites

**UP-01 schedule and lifecycle.** Producer owns immutable schedule and contiguous settled horizon, Outbox links successful slot outcome to its canonical event. Silver validates provenance and exposes additive versioned metadata on public contract. No historical reconstruction without independently sufficient archive evidence. Rollback disables new-version publication, retains archived evidence and existing V1 readers. D-A05, FA-01…07.

**UP-02 source metric semantics.** Version normalization for inbound getter percent and outbound ratio, validate measurement method/geometry and quality. Fix four paths under source owner approval, preserve old versions, separate E2 and fallback where semantics differ. Qualify all required fields and affected Realtime/AI consumers. Golden vectors, live getter tests and canonical→Silver fixtures required. D-A03/04, SA-05…08. Queue needs versioned method evidence and oracle. This prerequisite scopes affected metrics rather than the entire independent registry.

## Conditional Entry selected by owner

See [D-A14 authoritative amendment proposal](conditional-entry-proposal.md). No authorization is effective yet.

## Readiness by dimension

Architectural choices are frozen. Upstream feasibility and production resource approval remain open. Isolated work may proceed only after the effective amendment and applicable approvals. Full integration and operational acceptance remain blocked. See [evidence](preparation-result.md), [budgets](resource-sizing.md), [roadmap](plan.md).
## Freeze record — owner selections

Approved principles: public Silver contract and existing Core invariants, single verified traversal per window, bounded slot reducer, atomic completion/obligation/sealed projection, additive SQLite state with a single writer, versioned canonical JSON, bounded compute and publication, durable result, immutable ClickHouse rows and SQLite sole authority, generation fencing and fenced Gold reader, separate completion predicates, reservation/admission/backpressure and live-first scheduling, finalized 60/300 tumbling and direction/intersection/network marts. OPT-01/02 deferred. No new infrastructure proposed.

UP-01 B+C and boundary linkage, UP-02A/B and official readiness, scheduler and codec specifications are the frozen handoff. Verification and scope approvals remain pending. [UP-01](up-01-implementation-plan.md), [UP-02](up-02-implementation-plan.md), [D-A14](conditional-entry-proposal.md), [state/codec](state-schema.md).
