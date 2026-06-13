import json
import unittest

from model_utils import train_model
from telemetry_handler import decode_telemetry_message, process_telemetry


class TelemetryHandlerTests(unittest.TestCase):
    def setUp(self):
        self.model = train_model(random_state=3)

    def test_process_telemetry_returns_none_for_normal_sample(self):
        payload = {"motorTemp": 85.0, "vibration": 0.3, "energyUse": 110.0, "craneId": "crane001"}
        alert_payload, error = process_telemetry(payload, self.model)
        self.assertIsNone(alert_payload)
        self.assertIsNone(error)

    def test_process_telemetry_returns_alert_for_extreme_sample(self):
        payload = {"motorTemp": 200.0, "vibration": 5.0, "energyUse": 500.0, "craneId": "crane001"}
        alert_payload, error = process_telemetry(payload, self.model)
        self.assertIsNone(error)
        self.assertIsNotNone(alert_payload)
        self.assertEqual(alert_payload["assetId"], "crane001")
        self.assertEqual(alert_payload["alertType"], "PREDICTIVE_MAINTENANCE_REQUIRED")
        self.assertIn("telemetry", alert_payload)

    def test_decode_telemetry_message_rejects_invalid_json(self):
        alert_payload, error = decode_telemetry_message(b"{not-json", self.model)
        self.assertIsNone(alert_payload)
        self.assertEqual(error, "json")

    def test_decode_telemetry_message_rejects_missing_fields(self):
        alert_payload, error = decode_telemetry_message(
            json.dumps({"motorTemp": 85}).encode("utf-8"),
            self.model,
        )
        self.assertIsNone(alert_payload)
        self.assertEqual(error, "payload")

    def test_decode_telemetry_message_accepts_valid_payload(self):
        payload = json.dumps(
            {"motorTemp": 85.0, "vibration": 0.3, "energyUse": 110.0, "craneId": "crane001"}
        ).encode("utf-8")
        alert_payload, error = decode_telemetry_message(payload, self.model)
        self.assertIsNone(error)
        self.assertIsNone(alert_payload)

    def test_process_telemetry_rejects_non_numeric_field(self):
        payload = {"motorTemp": "hot", "vibration": 0.3, "energyUse": 110.0}
        with self.assertRaises(ValueError):
            process_telemetry(payload, self.model)


if __name__ == "__main__":
    unittest.main()
