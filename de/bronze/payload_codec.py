"""Decode Raw payload_stored + payload_encoding."""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
from typing import Any, Dict, Tuple

from de.bronze.models import RawRow
from contracts.traffic_observation import validate_observation, canonical_json, observation_key
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import build_immutable_envelope_hash


def decode_payload(raw: RawRow) -> Tuple[Dict[str, Any], str]:
    """Return (parsed_event_dict, raw_text)."""
    enc = (raw.payload_encoding or "utf8").lower()
    if enc == "base64":
        text = base64.b64decode(raw.payload_stored).decode("utf-8", errors="replace")
    else:
        text = raw.payload_stored
    body = json.loads(text)
    if not isinstance(body, dict):
        raise ValueError("JSON root must be object")
    return body, text


def decode_v1_observation(raw: dict) -> tuple[dict, bytes]:
    """Verify the original Raw transport, then validate the shared V1 contract."""
    try:
        if raw["value_is_null"] or raw["key_is_null"]:
            raise ValueError("confirmed V1 observation has null transport value/key")
        value = base64.b64decode(raw["value_bytes_base64"], validate=True)
        key = base64.b64decode(raw["key_bytes_base64"], validate=True)
        headers = json.loads(raw["headers_json"])
        if not isinstance(headers, list):
            raise ValueError("invalid ordered headers")
        decoded_headers = tuple((str(name), None if is_null else base64.b64decode(encoded, validate=True))
                                for name, is_null, encoded in headers)
        transport = V1Record(raw["topic"], int(raw["partition"]), int(raw["offset"]),
                             key, value, decoded_headers, raw["broker_timestamp_ms"],
                             int(raw["broker_timestamp_type"]), raw["topic_identity"])
        if transport.raw_ingestion_id != raw["raw_ingestion_id"]:
            raise ValueError("Raw ingestion identity mismatch")
        if transport.physical_record_hash != raw["physical_record_hash"]:
            raise ValueError("Raw transport hash mismatch")
        if hashlib.sha256(value).hexdigest() != raw["payload_bytes_hash"]:
            raise ValueError("Raw source byte hash mismatch")
        text = value.decode("utf-8", errors="strict")
        event = json.loads(text, parse_constant=lambda v: (_ for _ in ()).throw(ValueError("non-finite JSON")))
        if not isinstance(event, dict):
            raise ValueError("V1 observation root must be object")
        validate_observation(event)
        if key.decode("utf-8", errors="strict") != observation_key(event):
            raise ValueError("V1 Kafka key mismatch")
        if raw["payload_text"] != text:
            raise ValueError("Raw decoded text mismatch")
        if raw["immutable_envelope_hash"] != build_immutable_envelope_hash(event):
            raise ValueError("immutable envelope mismatch")
        if raw["payload_hash"] != event["payloadHash"]:
            raise ValueError("payload hash metadata mismatch")
        if raw["envelope_json"] != canonical_json({k: event[k] for k in (
            "eventId", "eventType", "schemaVersion", "simulationRunId", "intersectionId",
            "scenarioId", "cycleSequence", "simulationStep", "simulationTimeSeconds",
            "capturedAt", "topologyDigest", "payloadHash")}):
            raise ValueError("Raw immutable envelope JSON mismatch")
        return event, value
    except (KeyError, TypeError, UnicodeError, json.JSONDecodeError, binascii.Error) as exc:
        raise ValueError("confirmed Raw V1 integrity failure") from exc
