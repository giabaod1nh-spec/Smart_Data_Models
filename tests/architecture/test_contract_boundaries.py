from __future__ import annotations

from arch_utils import collect_imports, iter_py_files, read_text
from ownership_matrix import (
    EVENT_CONTRACT_VERSION,
    PROJECTOR_PACKAGES,
    REPO_ROOT,
)


def test_event_contract_version_in_schemas():
    schema = (
        REPO_ROOT
        / "contracts"
        / "events"
        / "traffic-entity-event-v2.schema.json"
    )
    text = read_text(schema)
    assert f'"{EVENT_CONTRACT_VERSION}"' in text


def test_entity_contract_version_file():
    ver = read_text(REPO_ROOT / "contracts" / "VERSION").strip()
    assert ver == "1.0.0"


def test_webhook_uses_legacy_notification_lineage():
    init = read_text(REPO_ROOT / "de" / "webhook" / "__init__.py")
    assert "Notification Delivery 1.0.0" in init


def test_projector_uses_event_to_entity_path_not_analytics_services():
    """Primary evidence: no forbidden analytics/service imports (not keyword scan)."""
    forbidden = (
        "de.bronze",
        "de.kafka_raw",
        "sklearn",
        "tensorflow",
        "torch",
        "analytics",
        "prediction",
    )
    hits = []
    for p in iter_py_files(PROJECTOR_PACKAGES):
        for imported in collect_imports(p):
            for bad in forbidden:
                if imported == bad or imported.startswith(bad + "."):
                    hits.append(f"{p}:{imported}")
    assert hits == [], hits
