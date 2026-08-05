"""Dependency-free contract smoke tests; no runtime or network access."""
import json
import pathlib
import unittest


ROOT = pathlib.Path(__file__).parents[1]


class ControlContractSmokeTest(unittest.TestCase):
    def load(self, name):
        with (ROOT / name).open(encoding="utf-8") as fh:
            return json.load(fh)

    def test_json_artifacts_parse(self):
        for name in (
            "control-command-v1.schema.json",
            "control-command-status-v1.schema.json",
            "control-error-v1.schema.json",
        ):
            self.assertIsInstance(self.load(name), dict)

    def test_command_envelope_is_closed_and_required(self):
        schema = self.load("control-command-v1.schema.json")
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["required"], [
            "contractVersion", "commandId", "commandType", "target", "payload",
            "expectedRunId", "idempotencyKey", "requestedAt", "expiresAt", "source",
        ])
        self.assertEqual(schema["properties"]["source"]["const"], "DASHBOARD")
        self.assertEqual(schema["properties"]["contractVersion"]["const"], "1.0")

    def test_status_has_separate_dimensions_and_no_cancellation(self):
        schema = self.load("control-command-status-v1.schema.json")
        self.assertFalse(schema["additionalProperties"])
        self.assertNotIn("CANCELLED", schema["properties"]["lifecycleStatus"]["enum"])
        for field in ("lifecycleStatus", "dispatchStatus", "executionStatus", "observationStatus"):
            self.assertIn(field, schema["required"])

    def test_error_contains_stable_command_not_found(self):
        schema = self.load("control-error-v1.schema.json")
        self.assertIn("COMMAND_NOT_FOUND", schema["properties"]["code"]["enum"])


if __name__ == "__main__":
    unittest.main()
