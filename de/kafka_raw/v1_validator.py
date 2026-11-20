"""Shared V1 wire validation with lossless base64 transport evidence."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from contracts.traffic_observation import canonical_json, validate_observation, observation_key
from de.kafka_raw.v1_models import V1Record, V1ClassifiedRecord, digest, encoded

ENVELOPE_FIELDS = ("eventId", "eventType", "schemaVersion", "simulationRunId",
 "intersectionId", "scenarioId", "cycleSequence", "simulationStep",
 "simulationTimeSeconds", "capturedAt", "topologyDigest", "payloadHash")
VALIDATOR_VERSION = "v1-wire-1-envelope-1-transport-1"

def build_immutable_envelope_hash(event: dict) -> str:
    return digest(canonical_json({k: event[k] for k in ENVELOPE_FIELDS}).encode())

class V1RawValidator:
    def classify(self, record: V1Record) -> V1ClassifiedRecord:
        now = datetime.now(timezone.utc)
        row = dict(record.transport(), raw_ingestion_id=record.raw_ingestion_id,
                   physical_record_hash=record.physical_record_hash,
                   payload_bytes_hash=digest(record.value_bytes or b""),
                   consumed_at=now, validator_version=VALIDATOR_VERSION,
                   migration_version="006-v1-raw", payload_text=None,
                   envelope_json=None, immutable_envelope_hash=None, event_id=None,
                   payload_hash=None, simulation_run_id=None, intersection_id=None)
        stage = "TRANSPORT"
        try:
            if record.value_bytes is None:
                raise ValueError("NULL_VALUE")
            stage = "DECODE"
            row["payload_text"] = record.value_bytes.decode("utf-8")
            stage = "JSON_PARSE"
            event = json.loads(row["payload_text"],
                               parse_constant=lambda v: (_ for _ in ()).throw(ValueError("NON_FINITE")))
            if not isinstance(event, dict):
                raise ValueError("NOT_OBJECT")
            stage = "EVENT_TYPE"
            if event.get("eventType") != "IntersectionObservation":
                raise ValueError("INVALID_EVENT_TYPE")
            stage = "SCHEMA_VERSION"
            if type(event.get("schemaVersion")) is not int or event["schemaVersion"] != 1:
                raise ValueError("INVALID_SCHEMA_VERSION")
            stage = "CONTRACT"
            validate_observation(event)
            stage = "KAFKA_KEY"
            if record.key_bytes is None or record.key_bytes.decode("utf-8") != observation_key(event):
                raise ValueError("INVALID_KAFKA_KEY")
            row.update(event_id=event["eventId"], payload_hash=event["payloadHash"],
                       simulation_run_id=event["simulationRunId"], intersection_id=event["intersectionId"],
                       envelope_json=canonical_json({k: event[k] for k in ENVELOPE_FIELDS}),
                       immutable_envelope_hash=build_immutable_envelope_hash(event))
            return V1ClassifiedRecord("RAW", row)
        except (ValueError, TypeError, KeyError, UnicodeError) as exc:
            code = {"DECODE": "NON_UTF8", "JSON_PARSE": "INVALID_JSON",
                    "CONTRACT": "INVALID_CONTRACT", "KAFKA_KEY": "INVALID_KAFKA_KEY"}.get(stage, str(exc))
            row.update(failed_at=now, failure_stage=stage, error_code=code, error_detail=str(exc))
            return V1ClassifiedRecord("QUARANTINE", row, stage, code, str(exc))

