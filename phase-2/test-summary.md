# Phase 2 verification

On 2026-11-18, the focused Phase 1/2 contract and runtime tests passed: **47 passed**. The command covered `contracts/tests/test_observation_v1.py`, `tests/architecture/test_phase2_canonical_cycle.py`, `tests/architecture/test_phase1_observation_boundary.py`, and `Visualize/tests/test_average_speed_precision.py`. The bundled pytest warned that the existing `asyncio_mode` config option is unknown; no test failed.

Headless SUMO runs succeeded with V1 enabled and disabled. The enabled 61-second simulation generated `phase-2/fixtures/current-sumo-cycle.json` at simulation time **60.04 s**, completed step **6004**, sampling sequence **61**, run ID `aecf4e8b-ac2c-459b-a50e-aaa7f4d2b4b4`. It contains five valid observations for A/B/C/D/M. The measured approach vehicle counts were 4/7/2/10/2 respectively. The fixture test validates all five events and rebuilds the exact event collection from the saved canonical snapshot.

The run archive for that fixture is `Visualize/artifacts/runs/aecf4e8b-ac2c-459b-a50e-aaa7f4d2b4b4/network_topology_catalog.json` in the local workspace. Its V1 projection digest is recorded in the fixture. `Visualize/artifacts/runs` is ignored by Git, while the generated topology catalog used to verify the digest is tracked.

SUMO emitted the existing `Detector expected_network_hash mismatch` warning during both V1 and V2-only runs. It did not prevent the topology preflight, catalog verification, simulation, or V1 validation.
