# SUMO TraCI Backend (Visualize)

Pipeline: **SUMO → TraCI → SumoBackend → Snapshot → entity_generator → Orion** (+ Control API).

Governance: see [`docs/adr.md`](docs/adr.md), [`docs/data_dictionary.md`](docs/data_dictionary.md), [`docs/parameter_catalog.md`](docs/parameter_catalog.md), [`docs/out_of_scope.md`](docs/out_of_scope.md).

Parameters SSOT: immutable `ParameterRegistry` (`configuration/model_parameters.yaml` → validate → freeze). Runtime motorcycle PCE **0.24** (Chu & Sano 2003); TCVN 0.30 is reference-only. Each run writes `artifacts/runs/<id>/run_manifest.json`.

Package layout (Phase 2.6 / 2.6C): implementations live under `observation/`, `actuators/`, `runtime/`, `context_engine/`, `simulation/`, `api/`, `integration/orion/`, `configuration/`, `topology/`, `app/`. Root keeps only justified facades (`traci_runner.py`, `config.py`, `control_api.py`, `model_params.py`) — see [`docs/migration_2_6c_shim_cleanup.md`](docs/migration_2_6c_shim_cleanup.md).

## Requirements

1. Eclipse SUMO (`SUMO_HOME`, `bin` on PATH)
2. Python 3.10+
3. Optional: Orion-LD at `ORION_URL`

```powershell
$env:SUMO_HOME = "D:\SUMO"
$env:PATH = "$env:SUMO_HOME\bin;$env:PATH"
cd Visualize
pip install -r requirements.txt
```

## Run

```powershell
# GUI, no Orion
python traci_runner.py --gui --no-orion

# Headless smoke 30s
python traci_runner.py --no-gui --no-orion --no-api --max-sim-time 30

# All nodes A–D + Control API :9090
$env:PUBLISH_NODES = "A,B,C,D"
python traci_runner.py --gui --no-orion

# With Orion
$env:ORION_URL = "http://localhost:1026"
python traci_runner.py --gui
```

## Mapping

| NGSI | SUMO TLS |
|------|----------|
| A | J1 |
| B | J2 |
| C | J3 |
| D | J4 |

Lane convention (ADR-002): `_0=right`, `_1=through`, `_2=left`.

## Control API

| Method | Path |
|--------|------|
| GET | `/health`, `/stats`, `/scenario`, `/snapshot/{id}`, `/trip-records`, `/network-state`, `/overlays`, `/intersections/{id}/state` |
| POST | `/scenario` (compat), `/demand-profile`, `/overlays`, `/control-mode`, `/phase`, `/green-duration` |
| DELETE | `/overlays/{id}` |

Port: `CONTROL_API_PORT` (default 9090). Mutations use CommandQueue (ADR-005).  
Hybrid demand docs: `docs/scenario_catalog.md`, `docs/demo_guide.md`.

## Tests

```powershell
pytest tests/ -v
```

## Tools

- `tools/regen_net.ps1` — netconvert from nod/edg
- `tools/generate_rou.py` — weighted demand baseline (ADR-007 experimental)
- `tools/generate_topology_catalog.py` — `generated/network_topology_catalog.json`
- `tools/generate_detectors.py` — E1/E2 XML
- `tools/generate_catalogs.py` — parameter/metric/entity catalogs + `parameter_catalog.json`
- `tools/check_magic_numbers.py` — AST magic-number gate
- `tools/generate_health_report.py` — `docs/health_report.md`
- `tools/demo_scenarios.ps1` — demand/overlay Control API demo

## Version

See `VERSION` file. Phase 2 layer docs: `docs/tick_lifecycle.md`, `docs/layer_field_ownership.md`.
