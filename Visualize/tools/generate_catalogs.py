#!/usr/bin/env python3
"""Generate parameter/metric/entity catalogs from ParameterRegistry."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

VISUALIZE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(VISUALIZE))

from configuration.model_params import get_registry  # noqa: E402


def _md_table(rows: list[dict], cols: list[str]) -> str:
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    lines = [header, sep]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)


def main() -> None:
    reg = get_registry(reload=True)
    docs = VISUALIZE / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    params = reg.list_parameters()
    catalog_json = {
        "schema_version": reg.schema_version,
        "generated_at": generated_at,
        "config_hash": reg.config_hash,
        "profile_id": reg.profile_id,
        "profile_version": reg.profile_version,
        "parameters": params,
    }
    json_path = docs / "parameter_catalog.json"
    json_path.write_text(
        json.dumps(catalog_json, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    md_rows = []
    for p in params:
        md_rows.append(
            {
                "key": p["key"],
                "value": p["value"],
                "unit": p.get("unit"),
                "source_type": p.get("source_type"),
                "calibrated": p.get("calibrated"),
                "description": (p.get("description") or "").replace("\n", " ").strip()[:120],
            }
        )
    param_md = f"""# Parameter Catalog

Auto-generated from ParameterRegistry. Do not hand-edit values.

- **schema_version:** `{reg.schema_version}`
- **profile_id:** `{reg.profile_id}`
- **config_hash:** `{reg.config_hash}`
- **generated_at:** `{generated_at}`

## Runtime parameters

{_md_table(md_rows, ["key", "value", "unit", "source_type", "calibrated", "description"])}

## Notes

- Motorcycle runtime PCE is **0.24** (Chu & Sano 2003, ACADEMIC).
- TCVN motorcycle **0.30** is reference-only and is **not** `runtime_default`.
- Container PCE **2.50** maps to canonical `articulated_proxy` (DESIGN_DECISION), not TCVN articulated 4.00.
"""
    (docs / "parameter_catalog.md").write_text(param_md, encoding="utf-8")

    metrics = reg.metrics_provenance()
    m_rows = []
    for name, meta in sorted(metrics.items()):
        m_rows.append(
            {
                "metric": name,
                "source_type": meta.get("source_type"),
                "derivation_kind": meta.get("derivation_kind", ""),
                "note": (meta.get("note") or meta.get("description") or meta.get("semantic_name") or ""),
            }
        )
    metric_md = f"""# Metric Catalog

Auto-generated metric provenance from ParameterRegistry.

- **config_hash:** `{reg.config_hash}`
- **generated_at:** `{generated_at}`

{_md_table(m_rows, ["metric", "source_type", "derivation_kind", "note"])}

## Semantics

- Field `density` is a **Traffic Load Class** from Approach PCU Count bins (not PCU/km).
- Camera `confidence` is a **PLACEHOLDER** (always 1.0 in simulation).
"""
    (docs / "metric_catalog.md").write_text(metric_md, encoding="utf-8")

    entity_md = f"""# Entity Catalog

NGSI-LD entities published by Visualize (field names unchanged — Strategy C).

| Entity | Type | Key properties (unchanged) | Notes |
| --- | --- | --- | --- |
| Intersection | Intersection | congestionLevel, averageSpeed, occupancyRate, pcuEquivalent | Aggregates from approaches |
| Camera | Camera | confidence, trafficStatus, vehicleCount | confidence=1.0 PLACEHOLDER |
| TrafficLight | TrafficLight | signalPhase, remainingTime | SUMO TLS mapping |
| VehicleSensor | VehicleSensor | pcuEquivalent, density, queueLength, occupancyRate | density = Traffic Load Class |

- **schema_version:** `{reg.schema_version}`
- **generated_at:** `{generated_at}`
"""
    (docs / "entity_catalog.md").write_text(entity_md, encoding="utf-8")
    print(f"Wrote catalogs under {docs} hash={reg.config_hash[:12]}")


if __name__ == "__main__":
    main()
