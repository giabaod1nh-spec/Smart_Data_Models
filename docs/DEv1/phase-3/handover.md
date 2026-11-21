# Phase 3 implementation kickoff

**State:** audit and plan are proposed for review. No Phase 3 code is implemented or certified.

## Approved source baseline

- Branch: `codex/phase-1-v1-raw`
- HEAD: `cd20886545114b0bbc60227fb7f3a74193c629cd`
- Current local tree is authoritative. Preserve nine modified files listed in `audit.md`; latest Phase 2 regression is reported 186/186, but final certification of current changes is pending.
- Keep V2 migrations 001–005 and all existing data/artifacts. Do not reset, clean, stash or overwrite local modifications.

## Required contracts before Silver reads

- Canonical V1 event: `contracts/observation/V1_CONTRACT.md`, `V1_METRIC_CONTRACT.md`, `contracts/traffic_observation.py` and schema artifact actually used by code (do not assume the filename suggested in historical text exists).
- Topology: `contracts/topology/v1_digest.py` and archived manifest/catalog under `<archive-root>/runs/<simulationRunId>/`.
- Bronze authority: read-only `V1SemanticReader`; journal `read_page` with fixed high-water; invalidation provenance/lineage; Bronze CH canonical content reconciliation.
- Before P3-01 bootstrap/lookup: add and test Bronze point-read of accepted envelope/revision and an atomic bootstrap snapshot paired with high-water. These APIs are currently absent; do not silently substitute current CH pagination as time travel.

## Work packages

P3-01 adapter; P3-02 archive resolver; P3-03 additive V1 schema and fact builders; P3-04 dimensions/availability/quality; P3-05 validity/invalidation/reconciliation; P3-06 checkpoints/restart/replay; P3-07 Compose/health/migrations; P3-08 actual end-to-end acceptance and Phase 4 handover. Detailed file lists, interfaces, test cases, evidence paths and PASS criteria are in `plan.md`.

## First implementation task

Start with P3-02 resolver fixtures and P3-01 interface contract review. In P3-01, verify current local Bronze source and close the explicitly identified point-lookup/bootstrap gap with its Bronze owner before adapting Silver. Add tests proving read-only access, frozen high-water, accepted vs conflicted current state, missing mirror and source outage. Do not change Bronze during Phase 3 without a separately authorized/interface-owned task.

## Commands and evidence

Capture baseline before implementation:

```powershell
git branch --show-current
git rev-parse HEAD
git status --short
git diff --stat
rg --files de/silver de/tests/silver contracts/observation contracts/topology
```

Use workspace-local test basetemp and evidence roots. Proposed runtime profile: `docker compose --profile v1-silver up -d --build de-silver-v1`; migration selector and service names are proposed until implemented. Do not execute as if present. Store actual acceptance evidence in `de/artifacts/silver_v1/acceptance/<run-id>/`: junit XML, schema/migration inventories, source-to-output oracle, archive hashes, lineage/invalidation reports, health/cursor/resource series, replay isolation snapshots.

## Current risks and stop conditions

- Bronze final local certification pending; confirm interface contract before integration.
- Bronze point lookup and atomic bootstrap are missing from inspected implementation.
- Gold must consume validity-aware canonical Silver reader; raw ClickHouse fact tables alone are not correct after an invalidation.
- Stop implementation if a source identity/hash, archive generation/digest, lineage, journal sequence, or output content cannot be positively reconciled. Never default null to zero, use active topology for a historical run, advance over an unresolved intent, or call an unexecuted gate PASS.
- Stop before destructive V2 changes, Phase 4/5 implementation, or changes outside approved Phase 3 documentation/source scope.

