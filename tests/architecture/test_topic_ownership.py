from __future__ import annotations

from arch_utils import iter_py_files, read_text
from ownership_matrix import (
    KAFKA_PRODUCER_PACKAGES,
    MAIN_TOPIC,
    PROJECTOR_PACKAGES,
    RAW_CONSUMER_PACKAGES,
    REPO_ROOT,
)


def test_main_topic_consistent():
    for roots in (KAFKA_PRODUCER_PACKAGES, RAW_CONSUMER_PACKAGES):
        text = "\n".join(read_text(p) for p in iter_py_files(roots))
        assert MAIN_TOPIC in text or "entity-events.v2" in text


def test_kafka_init_creates_expected_topics():
    init = (REPO_ROOT / "docker" / "kafka-init.sh").read_text(encoding="utf-8")
    assert "traffic.entity-events.v2" in init


def test_projector_consumes_main_topic_default():
    tool = REPO_ROOT / "Visualize" / "tools" / "projector_live_consumer.py"
    text = tool.read_text(encoding="utf-8")
    assert MAIN_TOPIC in text
