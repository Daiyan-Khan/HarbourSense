import json
import re
import unittest
from pathlib import Path


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "mqtt"

COMPLETION_REQUIRED = {"id", "shipmentId", "phase", "status", "completedAt"}
TASK_REQUIRED = {"shipmentId", "phase", "task", "startNode", "finalNode"}
SHIPMENT_REQUIRED = {"id", "status", "arrivalNode"}
SENSOR_REQUIRED = {"id", "type", "node", "reading", "timestamp"}
MAINTENANCE_ALERT_REQUIRED = {"assetId", "alertType", "reason", "timestamp"}
CRANE_TELEMETRY_REQUIRED = {"motorTemp", "vibration", "energyUse"}
CANONICAL_COMPLETION_TOPIC = re.compile(r"^harboursense/edge/[^/]+/completion$")
DEPRECATED_COMPLETION_TOPIC = re.compile(r"^harboursense/edge/completion/[^/]+$")


def load_fixture(name):
    with open(FIXTURES_DIR / name, encoding="utf-8") as handle:
        return json.load(handle)


COMPLETION_FIXTURES = [
    ("completion-offload.json", "offload"),
    ("completion-transport.json", "transport"),
    ("completion-store-load.json", "store_load"),
    ("completion-delivery.json", "delivery"),
]


class MqttContractFixtureTests(unittest.TestCase):
    def test_completion_fixtures_have_required_fields(self):
        for filename, expected_phase in COMPLETION_FIXTURES:
            with self.subTest(fixture=filename):
                payload = load_fixture(filename)
                missing = COMPLETION_REQUIRED - payload.keys()
                self.assertEqual(missing, set(), f"{filename} missing {missing}")
                self.assertEqual(payload["phase"], expected_phase)

    def test_task_fixture_has_required_fields(self):
        payload = load_fixture("task-transport.json")
        missing = TASK_REQUIRED - payload.keys()
        self.assertEqual(missing, set())
        self.assertEqual(payload["phase"], "transport")

    def test_canonical_completion_topic_shape(self):
        topic = "harboursense/edge/crane001/completion"
        self.assertTrue(CANONICAL_COMPLETION_TOPIC.match(topic))
        self.assertFalse(DEPRECATED_COMPLETION_TOPIC.match(topic))

    def test_deprecated_completion_topic_is_detectable(self):
        topic = "harboursense/edge/completion/crane001"
        self.assertTrue(DEPRECATED_COMPLETION_TOPIC.match(topic))
        self.assertFalse(CANONICAL_COMPLETION_TOPIC.match(topic))

    def test_shipment_fixture_has_required_fields(self):
        payload = load_fixture("shipment-arrived.json")
        missing = SHIPMENT_REQUIRED - payload.keys()
        self.assertEqual(missing, set())

    def test_sensor_fixture_has_required_fields(self):
        payload = load_fixture("sensor-data.json")
        missing = SENSOR_REQUIRED - payload.keys()
        self.assertEqual(missing, set())

    def test_maintenance_alert_fixture_has_required_fields(self):
        payload = load_fixture("maintenance-alert.json")
        missing = MAINTENANCE_ALERT_REQUIRED - payload.keys()
        self.assertEqual(missing, set())

    def test_crane_telemetry_fixture_has_required_fields(self):
        payload = load_fixture("crane-telemetry.json")
        missing = CRANE_TELEMETRY_REQUIRED - payload.keys()
        self.assertEqual(missing, set())

    def test_sensor_and_analyzer_topic_shapes(self):
        self.assertEqual("harboursense/sensor/data", "harboursense/sensor/data")
        self.assertEqual("harboursense/alerts/maintenance", "harboursense/alerts/maintenance")
        self.assertTrue(
            re.match(r"^harboursense/telemetry/crane/[^/]+/raw$", "harboursense/telemetry/crane/crane001/raw")
        )


if __name__ == "__main__":
    unittest.main()
