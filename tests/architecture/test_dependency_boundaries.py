from __future__ import annotations

from pathlib import Path

from arch_utils import collect_imports, iter_py_files
from ownership_matrix import (
    BRONZE_FORBIDDEN_IMPORTS,
    BRONZE_PACKAGES,
    KAFKA_PRODUCER_PACKAGES,
    PROJECTOR_PACKAGES,
    RAW_CONSUMER_PACKAGES,
    REPO_ROOT,
    WEBHOOK_PACKAGES,
)


def test_no_projector_raw_cycle():
    proj_imports = set()
    for p in iter_py_files(PROJECTOR_PACKAGES):
        proj_imports |= collect_imports(p)
    raw_imports = set()
    for p in iter_py_files(RAW_CONSUMER_PACKAGES):
        raw_imports |= collect_imports(p)
    assert not any(i.startswith("de.kafka_raw") for i in proj_imports)
    assert not any(i.startswith("integration.projector") for i in raw_imports)


def test_server_does_not_depend_on_kafka_packages():
    pom = (REPO_ROOT / "server" / "pom.xml").read_text(encoding="utf-8").lower()
    assert "kafka" not in pom
    assert "confluent" not in pom
    assert "clickhouse" not in pom


def test_webhook_does_not_import_kafka_producer():
    for p in iter_py_files(WEBHOOK_PACKAGES):
        imports = collect_imports(p)
        assert not any(i.startswith("integration.kafka") for i in imports)
        assert not any(i.startswith("confluent_kafka") for i in imports)


def test_bronze_package_compliant():
    bronze = REPO_ROOT / "de" / "bronze"
    assert bronze.is_dir(), "K-7 Bronze package must exist"
    for p in iter_py_files(BRONZE_PACKAGES):
        imports = collect_imports(p)
        for forbidden in BRONZE_FORBIDDEN_IMPORTS:
            assert not any(
                i == forbidden or i.startswith(f"{forbidden}.") for i in imports
            ), f"{p} forbidden import {forbidden}"


def test_sumo_kafka_path_no_webhook_clickhouse():
    hits = []
    for p in iter_py_files(KAFKA_PRODUCER_PACKAGES):
        text = p.read_text(encoding="utf-8")
        imports = collect_imports(p)
        if any(i.startswith("de.webhook") for i in imports):
            hits.append(f"{p}: webhook import")
        if "clickhouse_connect" in imports or "clickhouse_driver" in imports:
            hits.append(f"{p}: clickhouse import")
        if "raw_ngsi_notifications" in text:
            hits.append(f"{p}: raw_ngsi_notifications reference")
    assert hits == [], hits
