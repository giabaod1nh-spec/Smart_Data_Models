"""Projector analytics boundary — import/call primary; keyword scan = warning only."""
from __future__ import annotations

import warnings

from arch_utils import collect_imports, iter_py_files, read_text
from ownership_matrix import PROJECTOR_PACKAGES


FORBIDDEN_ANALYTICS_IMPORTS = (
    "de.bronze",
    "de.kafka_raw",
    "de.webhook",
    "clickhouse_connect",
    "sklearn",
    "tensorflow",
    "torch",
    "statsmodels",
)


def test_projector_no_analytics_service_imports():
    hits = []
    for p in iter_py_files(PROJECTOR_PACKAGES):
        for imported in collect_imports(p):
            for bad in FORBIDDEN_ANALYTICS_IMPORTS:
                if imported == bad or imported.startswith(bad + "."):
                    hits.append(f"{p}:{imported}")
    assert hits == [], hits


def test_projector_keyword_scan_warning_only():
    """Keyword hits are informational — congestion may appear in valid entity payloads."""
    keywords = ("aggregat", "congestion", "predict", "bronze")
    warnings_found = []
    for p in iter_py_files(PROJECTOR_PACKAGES):
        text = read_text(p).lower()
        for kw in keywords:
            if kw in text:
                warnings_found.append(f"{p.name}:{kw}")
    if warnings_found:
        warnings.warn(
            "Projector keyword scan (non-blocking): " + ", ".join(warnings_found),
            stacklevel=1,
        )
    # Never fail on keywords alone
    assert True
