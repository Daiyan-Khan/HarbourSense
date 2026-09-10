"""Unit checks for analyzer fencing/idempotency; not broker recovery tests."""
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python-backend"))
from demo_runtime import CURRENT
from demo_analyzer import handle_demo_telemetry
from model_utils import train_model


class Collection:
    def __init__(self):
        self.documents = {}

    async def find_one(self, query):
        return self.documents.get(query["_id"])

    async def update_one(self, query, update, upsert=False):
        document = self.documents.setdefault(query["_id"], {"_id": query["_id"], **update.get("$setOnInsert", {})})
        document.update(update.get("$set", {}))


class DemoAnalyzerTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = train_model()

    async def asyncSetUp(self):
        self.context = SimpleNamespace(run_id="a" * 32, checkpoint=AsyncMock(), db=SimpleNamespace(
            demoInbox=Collection(), craneTelemetry=Collection(), maintenanceAlerts=Collection()))
        self.token = CURRENT.set(self.context)
        self.mqtt = SimpleNamespace(publish=AsyncMock())
        self.payload = {"runId": self.context.run_id, "eventId": "test-1", "craneId": "crane001",
                        "motorTemp": 200, "vibration": 5, "energyUse": 500}

    async def asyncTearDown(self):
        CURRENT.reset(self.token)

    async def test_old_run_has_no_model_or_storage_effect(self):
        self.payload["runId"] = "b" * 32
        self.assertFalse(await handle_demo_telemetry(self.payload, None, self.context, self.mqtt))
        self.assertFalse(self.context.db.craneTelemetry.documents)
        self.assertFalse(self.context.db.maintenanceAlerts.documents)
        self.mqtt.publish.assert_not_called()

    async def test_duplicate_sample_does_not_duplicate_alert_or_publication(self):
        self.assertTrue(await handle_demo_telemetry(self.payload, self.model, self.context, self.mqtt))
        self.assertFalse(await handle_demo_telemetry(self.payload, self.model, self.context, self.mqtt))
        self.assertEqual(len(self.context.db.craneTelemetry.documents), 1)
        self.assertEqual(len(self.context.db.maintenanceAlerts.documents), 1)
        self.assertEqual(self.mqtt.publish.await_count, 2)
        analyzed = json.loads(self.mqtt.publish.await_args_list[0].args[1])
        self.assertEqual(analyzed["eventId"], "test-1-analyzed")
        self.assertIn("analysis", analyzed)

    async def test_failed_publication_retries_with_same_storage_identity(self):
        self.mqtt.publish.side_effect = RuntimeError("connection lost")
        with self.assertRaises(RuntimeError):
            await handle_demo_telemetry(self.payload, self.model, self.context, self.mqtt)
        self.assertFalse(self.context.db.demoInbox.documents)
        self.mqtt.publish.side_effect = None
        await handle_demo_telemetry(self.payload, self.model, self.context, self.mqtt)
        self.assertEqual(len(self.context.db.craneTelemetry.documents), 1)
        self.assertEqual(len(self.context.db.maintenanceAlerts.documents), 1)
        self.assertIn("test-1", self.context.db.demoInbox.documents)

    async def test_healthy_samples_are_scored_and_saved_without_alert(self):
        self.payload.update(motorTemp=85, vibration=.3, energyUse=110)
        await handle_demo_telemetry(self.payload, self.model, self.context, self.mqtt)
        self.assertFalse(self.context.db.maintenanceAlerts.documents)
        saved = self.context.db.craneTelemetry.documents["test-1"]
        self.assertFalse(saved["analysis"]["anomalous"])
        self.mqtt.publish.assert_awaited_once()
