"""G0 Contract Freeze — JSON Schema, OpenAPI, enum/error parity."""
from __future__ import annotations

import json
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"

try:
    import jsonschema
except ImportError:  # pragma: no cover
    jsonschema = None

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _load(name: str) -> dict:
    with (ROOT / name).open(encoding="utf-8") as fh:
        return json.load(fh)


def _fixture(name: str) -> dict:
    with (FIXTURES / name).open(encoding="utf-8") as fh:
        return json.load(fh)


COMMAND_TYPES = {
    "FORCE_PHASE",
    "SET_GREEN_DURATION",
    "SET_SCENARIO",
    "SET_DEMAND_PROFILE",
    "ADD_OVERLAY",
    "REMOVE_OVERLAY",
    "SET_CONTROL_MODE",
    "EMERGENCY_PREEMPTION",
}

LIFECYCLE = {
    "RECEIVED", "VALIDATED", "QUEUED", "APPLYING", "COMPLETED",
    "FAILED", "EXPIRED", "UNKNOWN_OUTCOME",
}
DISPATCH = {"PENDING", "DISPATCHING", "ACCEPTED", "FAILED", "UNKNOWN"}
EXECUTION = {
    "NOT_STARTED", "QUEUED", "EXECUTING", "TRANSITIONING",
    "APPLIED_AT_SUMO", "FAILED_AT_RUNTIME",
}
OBSERVATION = {
    "NOT_REQUESTED", "PENDING", "CONFIRMED", "MISMATCH",
    "TIMED_OUT", "UNAVAILABLE", "NOT_OBSERVABLE",
}
ERROR_CODES = {
    "INVALID_COMMAND", "INVALID_TARGET", "INVALID_PHASE", "INVALID_DURATION",
    "INVALID_SCENARIO", "STALE_RUN", "SIMULATION_NOT_RUNNING", "RESOURCE_BUSY",
    "QUEUE_FULL", "COMMAND_EXPIRED", "RUNTIME_RESTARTED", "UNKNOWN_OUTCOME",
    "TRACI_OPERATION_FAILED", "UPSTREAM_UNAVAILABLE", "OBSERVATION_TIMEOUT",
    "OBSERVATION_MISMATCH", "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_REQUEST",
    "COMMAND_NOT_FOUND",
}


@unittest.skipUnless(jsonschema is not None, "jsonschema not installed")
class ContractFreezeG0Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.command_schema = _load("control-command-v1.schema.json")
        cls.status_schema = _load("control-command-status-v1.schema.json")
        cls.error_schema = _load("control-error-v1.schema.json")
        cls.validator_cmd = jsonschema.Draft202012Validator(cls.command_schema)
        cls.validator_status = jsonschema.Draft202012Validator(cls.status_schema)
        cls.validator_error = jsonschema.Draft202012Validator(cls.error_schema)

    def test_meta_schema_valid(self):
        jsonschema.Draft202012Validator.check_schema(self.command_schema)
        jsonschema.Draft202012Validator.check_schema(self.status_schema)
        jsonschema.Draft202012Validator.check_schema(self.error_schema)

    def test_valid_force_phase_fixture(self):
        doc = _fixture("valid_force_phase.json")
        self.validator_cmd.validate(doc)

    def test_invalid_missing_expected_run_id(self):
        doc = _fixture("invalid_missing_expected_run_id.json")
        with self.assertRaises(jsonschema.ValidationError):
            self.validator_cmd.validate(doc)

    def test_valid_status_fixture(self):
        doc = _fixture("valid_status_idle.json")
        self.validator_status.validate(doc)

    def test_no_cancelled_in_status(self):
        lifecycle = self.status_schema["properties"]["lifecycleStatus"]["enum"]
        self.assertNotIn("CANCELLED", lifecycle)

    def test_no_kafka_stages_in_observation(self):
        obs = self.status_schema["properties"]["observationStatus"]["enum"]
        for bad in ("PUBLISHED_TO_OUTBOX", "PUBLISHED_TO_KAFKA", "PROJECTED_TO_ORION"):
            self.assertNotIn(bad, obs)

    def test_command_enum_parity(self):
        schema_types = set(self.command_schema["properties"]["commandType"]["enum"])
        self.assertEqual(schema_types, COMMAND_TYPES)

    def test_status_enum_parity(self):
        self.assertEqual(set(self.status_schema["properties"]["lifecycleStatus"]["enum"]), LIFECYCLE)
        self.assertEqual(set(self.status_schema["properties"]["dispatchStatus"]["enum"]), DISPATCH)
        self.assertEqual(set(self.status_schema["properties"]["executionStatus"]["enum"]), EXECUTION)
        self.assertEqual(set(self.status_schema["properties"]["observationStatus"]["enum"]), OBSERVATION)

    def test_error_enum_parity(self):
        codes = set(self.error_schema["properties"]["code"]["enum"])
        self.assertEqual(codes, ERROR_CODES)
        self.assertIn("COMMAND_NOT_FOUND", codes)
        self.assertNotIn("COMMAND_CANCELLED", codes)

    def test_additional_properties_false(self):
        self.assertFalse(self.command_schema["additionalProperties"])
        self.assertFalse(self.status_schema["additionalProperties"])
        self.assertFalse(self.error_schema["additionalProperties"])

    def test_expected_run_id_required_non_empty(self):
        req = self.command_schema["required"]
        self.assertIn("expectedRunId", req)
        prop = self.command_schema["properties"]["expectedRunId"]
        self.assertGreaterEqual(prop.get("minLength", 0), 1)


@unittest.skipUnless(yaml is not None, "pyyaml not installed")
class OpenApiG0Test(unittest.TestCase):
    def test_openapi_yaml_parses(self):
        path = ROOT / "control-command-v1.openapi.yaml"
        with path.open(encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        self.assertEqual(doc["openapi"], "3.1.0")
        self.assertIn("/api/control/commands", doc["paths"])
        self.assertIn("post", doc["paths"]["/api/control/commands"])
        self.assertIn("get", doc["paths"]["/api/control/commands/{commandId}"])


if __name__ == "__main__":
    unittest.main()
