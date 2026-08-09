from __future__ import annotations

from arch_utils import scan_forbidden_imports
from ownership_matrix import (
    BRONZE_FORBIDDEN_IMPORTS,
    BRONZE_PACKAGES,
    KAFKA_PRODUCER_FORBIDDEN_IMPORTS,
    KAFKA_PRODUCER_PACKAGES,
    PROJECTOR_FORBIDDEN_IMPORTS,
    PROJECTOR_PACKAGES,
    RAW_CONSUMER_PACKAGES,
    RAW_FORBIDDEN_IMPORTS,
)


def test_projector_forbidden_imports():
    hits = scan_forbidden_imports(PROJECTOR_PACKAGES, PROJECTOR_FORBIDDEN_IMPORTS)
    assert hits == [], hits


def test_raw_consumer_forbidden_imports():
    hits = scan_forbidden_imports(RAW_CONSUMER_PACKAGES, RAW_FORBIDDEN_IMPORTS)
    assert hits == [], hits


def test_kafka_producer_forbidden_imports():
    hits = scan_forbidden_imports(KAFKA_PRODUCER_PACKAGES, KAFKA_PRODUCER_FORBIDDEN_IMPORTS)
    assert hits == [], hits


def test_bronze_forbidden_imports():
    hits = scan_forbidden_imports(BRONZE_PACKAGES, BRONZE_FORBIDDEN_IMPORTS)
    assert hits == [], hits
