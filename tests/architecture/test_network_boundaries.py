from __future__ import annotations

from arch_utils import iter_py_files, read_text
from ownership_matrix import PROJECTOR_PACKAGES, RAW_CONSUMER_PACKAGES, REPO_ROOT


def test_raw_has_http_ready():
    text = read_text(REPO_ROOT / "de" / "kafka_raw" / "health_api.py")
    assert "/ready" in text
    assert "/health" in text


def test_projector_tool_exposes_health_ready():
    text = read_text(REPO_ROOT / "Visualize" / "tools" / "projector_live_consumer.py")
    assert "/health" in text
    assert "/ready" in text
    assert "PROJECTOR_TARGET_NAMESPACE" in text


def test_traci_kafka_independent_of_publish_orion_block():
    """Regression for AL-001: kafka_wanted init must not be nested solely under publish_orion."""
    text = read_text(REPO_ROOT / "Visualize" / "app" / "traci_runner.py")
    assert "kafka_wanted" in text
    assert "Kafka-only path" in text or "kafka_wanted" in text
