# Phase 3 blockers and dependencies

> Runtime acceptance closure (2026-11-22): A01–A33 evidence was reviewed; actual600-event run/canonical4680 facts PASS. See [acceptance-result.md](acceptance-result.md) for assertion-by-assertion evidence, test counts and operating limitations. Historical design/pending statements below are preserved chronology and are superseded by that closure record.


## Current dependency status — 2026-11-22

No external implementation blocker is currently identified. The missing Bronze-owned exact-current point-read listed below has been implemented in BronzeClickHouseRepository.read_valid_observation and verified through actual owner/CH integration, conflicts and source races. Source schema/binding, complete retained1..H journals, RO mounts and the operational no-prune rule are recorded in acceptance/source-retention-binding.json, source-retention-mounts.json and handover.md. Privileged external rewriting is outside that operational guarantee and must not be performed beneath a bound source. Remaining work is acceptance completion and evidence audit, not the absent-API blocker described in the preserved baseline below. A01 has reached600VALID; its independent whole-run verifier is running.


Status as of 2026-11-21. This file lists external contracts that block dependent acceptance, not normal engineering work packages.

## Genuine prerequisite before P3-01 materialization

1. **Bronze exact-current accepted-event point read is absent.** `V1SemanticReader` in `de/bronze/checkpoint_store.py` exposes current identity, bounded journal pages, delivery lineage and invalidation provenance, but not `read_valid_observation(namespace,event_id,expected_revision)`. The Bronze-owned additive read must return the exact envelope/content hash only when current identity is ACCEPTED at that revision and existing canonical ClickHouse content reconciliation succeeds. It fails closed for conflict, revision/content mismatch, missing mirror or unavailable authority. No atomic bootstrap API is needed.
2. **No-prune source contract must be verified and bound.** P3-01 must prove both journals are contiguous from sequence 1 through their transactional allocators (or valid empty at H=0), bind the configured source path/namespace/schema fingerprint, and obtain the operational guarantee that history will not be pruned for the source lifetime. A gap or unproven guarantee blocks starting at cursor 0; do not start at latest or assume completeness.

## Sequenced implementation dependencies (not external blockers)

- P3-05 implements the complete-only reader after P3-01 binding/point-read prerequisites; both applied change and invalidation cursors must cover captured source high-waters.
- P3-06 implements replay only after source verification and the P3-05 validity contract; replay refuses if Bronze advances from its frozen boundary.
- P3-02 lifecycle polling uses the existing read-only archive registry and Silver-owned known-run index. It does not require producer lifecycle events or producer changes.
- Phase 3 P3-08 must deliver the implemented/tested complete-only Python reader, invalidation feed, token/error contract, mappings and acceptance evidence. Gold V1 adoption, window policy, Spring and Dashboard changes are Phase 4 implementation requirements and do not block Phase 3 exit.

## Non-blocking status

Phase 2 final certification remains owned by Phase 2. Its status is not a Phase 3 design blocker; P3-01 must verify the current source schema, APIs and no-prune contract it actually uses. Do not restart broad Phase 2 certification as part of this plan review.

No Orion-LD, Projector, new broker/database, historical snapshot subsystem, HTTP reader service, or separate Silver V1 package/runtime is required. Compose profile/migration wiring and Python reader implementation are Phase 3 work packages, not external blockers.
