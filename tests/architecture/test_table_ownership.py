from __future__ import annotations

from arch_utils import iter_py_files, read_text
from ownership_matrix import (
    BRONZE_ALLOWED_TABLES,
    BRONZE_FORBIDDEN_TABLES,
    BRONZE_PACKAGES,
    KAFKA_PRODUCER_PACKAGES,
    PROJECTOR_PACKAGES,
    RAW_ALLOWED_TABLES,
    RAW_CONSUMER_PACKAGES,
    RAW_FORBIDDEN_TABLES,
    REPO_ROOT,
)


def test_raw_consumer_only_writes_owned_tables():
    text = "\n".join(read_text(p) for p in iter_py_files(RAW_CONSUMER_PACKAGES))
    for bad in RAW_FORBIDDEN_TABLES:
        assert bad not in text, f"Raw consumer must not touch {bad}"
    # At least one owned table referenced
    assert any(t in text for t in RAW_ALLOWED_TABLES)


def test_projector_no_clickhouse_tables():
    text = "\n".join(read_text(p) for p in iter_py_files(PROJECTOR_PACKAGES))
    for t in RAW_ALLOWED_TABLES | RAW_FORBIDDEN_TABLES:
        assert t not in text, f"Projector must not reference table {t}"


def test_producer_no_clickhouse_writes():
    text = "\n".join(read_text(p) for p in iter_py_files(KAFKA_PRODUCER_PACKAGES))
    assert "clickhouse" not in text.lower()
    assert "kafka_raw_events" not in text


def test_bronze_only_reads_raw_and_writes_bronze_tables():
    text = "\n".join(read_text(p) for p in iter_py_files(BRONZE_PACKAGES))
    for bad in BRONZE_FORBIDDEN_TABLES:
        assert bad not in text, f"Bronze must not touch {bad}"
    assert any(t in text for t in BRONZE_ALLOWED_TABLES)
    assert "confluent_kafka" not in text
    assert "integration.kafka" not in text
