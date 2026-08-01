from __future__ import annotations

from arch_utils import read_text
from ownership_matrix import COMPOSE_BASE


def test_bronze_processor_single_replica_in_compose():
    text = read_text(COMPOSE_BASE)
    assert "de-bronze-processor:" in text
    block = text.split("de-bronze-processor:")[1].split("\n  ")[0:20]
    section = "de-bronze-processor:" + "\n  ".join(block)
    assert "8092" in section
    assert "replicas: 1" in section or "container_name: de-bronze-processor" in section
