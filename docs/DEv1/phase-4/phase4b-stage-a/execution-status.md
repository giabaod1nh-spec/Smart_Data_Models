# Stage A execution status

Reviewed baseline and initial local/remote: `05c592d30faa9ace6cdb589d0a771f7031f01a0a`, branch `codex/phase-1-v1-raw`, clean working tree/index.

## Approval record

On 2026-11-27 (Asia/Bangkok), the requester confirmed both the project owner and Performance Acceptance owner roles and approved the frozen D-A14 exception. The effective amendment is in [Phase 4A Performance Acceptance](../performance/acceptance-result.md). The requester separately confirmed authority and approval for UP-01 (source, contract and Silver) and UP-02A/B in the frozen isolated scope. Official source adoption, G-MART/G-REV design approvals where applicable, full-entry performance and operational gates are separate.

## Implementation and evidence

| Package | Current status | Evidence / next dependency |
|---|---|---|
| A-I01a | Implemented, verified on isolated fixtures | Pure schedule/slot reducer and independent metrics in `de/gold/business.py`; `python -m pytest de/tests/gold/test_stage_a_fixture_oracle.py -q`: 12 passed. Fixture oracle covers both durations, three grains, quality, weighted rollups, direction P95 and network max, missing/conflict/waiting/invalid, deterministic bytes and member cap. No official canonical input or publication. |
| UP-01 | Authorized, pending implementation/verification | Frozen schedule and separate P/S/G watermarks remain the required contract. |
| UP-02A/B | Authorized, implementation underway | Qualify source changes and compatibility before claiming readiness. |
| A-I02 | Isolated SQLite scope eligible under D-A14 | Next Gold package: additive rehearsal, reserve/CAS, atomic handoff and recovery on disposable SQLite. |
| A-I03 | Pending | Isolated failure paths require A-I02 and the applicable G-MART/G-REV design approvals. |
| A-I04–07, full integration | Pending respective gates | Existing authoritative full-entry minimum gate, source/metric acceptances, representative workload and continuous business-to-Gold proof. |

Host Python lacked the repository-pinned `rfc8785==0.1.4`. Tests use the unchanged dependency installed in ignored `.pytest_cache/phase4b-deps` (add to PYTHONPATH). No live benchmark or production migration ran. Phase 4A Performance Acceptance remains ENVIRONMENT BLOCKED; this fixture result does not apply to PA-01–12.
