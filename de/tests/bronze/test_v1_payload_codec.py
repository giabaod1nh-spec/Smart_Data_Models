import json
from pathlib import Path

import pytest

from de.bronze.payload_codec import decode_v1_observation
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator


def test_v1_original_bytes_and_integrity():
    event = json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    value = json.dumps(event).encode()
    record = V1Record("traffic.observations.v1", 0, 0,
                      f"{event['simulationRunId']}:{event['intersectionId']}".encode(),
                      value, topic_identity="uuid")
    classified = V1RawValidator().classify(record)
    assert classified.destination == "RAW"
    raw = V1ClickHouseRepository(None).materialize(classified)
    parsed, original = decode_v1_observation(raw)
    assert parsed == event and original == value
    raw["value_bytes_base64"] = "!!!"
    with pytest.raises(ValueError):
        decode_v1_observation(raw)


def test_v1_rejects_changed_stored_byte_hash():
    event = json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    record = V1Record("traffic.observations.v1", 0, 1,
                      f"{event['simulationRunId']}:{event['intersectionId']}".encode(),
                      json.dumps(event).encode(), topic_identity="uuid")
    raw = V1ClickHouseRepository(None).materialize(V1RawValidator().classify(record))
    raw["payload_bytes_hash"] = "0" * 64
    with pytest.raises(ValueError, match="byte hash"):
        decode_v1_observation(raw)
