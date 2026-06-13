import importlib
import json
import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


def install_dependency_stubs():
    motor_module = types.ModuleType("motor")
    motor_asyncio_module = types.ModuleType("motor.motor_asyncio")
    motor_asyncio_module.AsyncIOMotorClient = object
    motor_module.motor_asyncio = motor_asyncio_module
    sys.modules.setdefault("motor", motor_module)
    sys.modules.setdefault("motor.motor_asyncio", motor_asyncio_module)

    bson_module = types.ModuleType("bson")

    class ObjectId:
        def __str__(self):
            return "stub-object-id"

    timestamp_module = types.ModuleType("bson.timestamp")

    class Timestamp:
        def __init__(self, time=0):
            self.time = time

    int64_module = types.ModuleType("bson.int64")

    class Int64(int):
        pass

    bson_module.ObjectId = ObjectId
    bson_module.timestamp = timestamp_module
    timestamp_module.Timestamp = Timestamp
    int64_module.Int64 = Int64
    sys.modules.setdefault("bson", bson_module)
    sys.modules.setdefault("bson.timestamp", timestamp_module)
    sys.modules.setdefault("bson.int64", int64_module)

    pymongo_module = types.ModuleType("pymongo")
    operations_module = types.ModuleType("pymongo.operations")

    class UpdateOne:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    operations_module.UpdateOne = UpdateOne
    pymongo_module.operations = operations_module
    sys.modules.setdefault("pymongo", pymongo_module)
    sys.modules.setdefault("pymongo.operations", operations_module)

    aiomqtt_module = types.ModuleType("aiomqtt")

    class MqttError(Exception):
        pass

    aiomqtt_module.Client = object
    aiomqtt_module.MqttError = MqttError
    sys.modules.setdefault("aiomqtt", aiomqtt_module)

    sensor_analyzer_module = types.ModuleType("sensor_analyzer")

    class SensorAnalyzer:
        def __init__(self, db):
            self.db = db

    sensor_analyzer_module.SensorAnalyzer = SensorAnalyzer
    sys.modules.setdefault("sensor_analyzer", sensor_analyzer_module)


install_dependency_stubs()
manager = importlib.import_module("manager")


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    async def to_list(self, length):
        return self.docs


class FakeCollection:
    def __init__(self, docs):
        self.docs = []
        for doc in docs:
            self.docs.append(dict(doc))

    def _matches(self, doc, query):
        for key, value in query.items():
            if isinstance(value, dict) and "$in" in value:
                if doc.get(key) not in value["$in"]:
                    return False
            elif isinstance(value, dict) and "$ne" in value:
                if doc.get(key) == value["$ne"]:
                    return False
            elif doc.get(key) != value:
                return False
        return True

    async def find_one(self, query):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    def find(self, query=None):
        query = query or {}
        return FakeCursor([doc for doc in self.docs if self._matches(doc, query)])

    async def update_one(self, query, update, upsert=False):
        for doc in self.docs:
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                return
        if upsert:
            new_doc = dict(query)
            for key, value in update.get("$set", {}).items():
                new_doc[key] = value
            self.docs.append(new_doc)


class FakeMqttClient:
    def __init__(self):
        self.published = []

    async def publish(self, topic, payload):
        self.published.append((topic, payload))


class FakeTaskAssigner:
    def __init__(self):
        self.calls = []

    async def assign_maintenance_task(self, node, db, mqtt_client, severity=None):
        self.calls.append({"node": node, "severity": severity})


class MaintenanceAlertHelperTests(unittest.TestCase):
    def test_resolve_maintenance_target_node_prefers_asset_location(self):
        alert = {"assetId": "crane001", "telemetry": {"node": "B4"}}
        self.assertEqual(manager.resolve_maintenance_target_node(alert, "A1"), "A1")

    def test_resolve_maintenance_target_node_uses_telemetry_node(self):
        alert = {"assetId": "crane001", "telemetry": {"node": "C3"}}
        self.assertEqual(manager.resolve_maintenance_target_node(alert), "C3")

    def test_resolve_maintenance_target_node_falls_back_to_asset_id(self):
        alert = {"assetId": "crane001"}
        self.assertEqual(manager.resolve_maintenance_target_node(alert), "crane001")


class MaintenanceAlertHandlerTests(unittest.IsolatedAsyncioTestCase):
    def make_alert_payload(self, asset_id="crane001"):
        return {
            "assetId": asset_id,
            "alertType": "PREDICTIVE_MAINTENANCE_REQUIRED",
            "reason": "Anomalous motor telemetry detected by EdgeAnalyzer.",
            "timestamp": "2026-06-13T10:00:00Z",
            "telemetry": {"craneId": asset_id, "motorTemp": 120, "vibration": 1.2, "energyUse": 180},
        }

    def make_db(self, edge_docs=None, alert_docs=None):
        return types.SimpleNamespace(
            edgeDevices=FakeCollection(edge_docs or [
                {"id": "crane001", "type": "crane", "taskPhase": "idle", "currentLocation": "A1"},
                {"id": "robot001", "type": "robot", "taskPhase": "idle", "currentLocation": "E5"},
            ]),
            maintenanceAlerts=FakeCollection(alert_docs or [
                {
                    "assetId": "crane001",
                    "alertType": "PREDICTIVE_MAINTENANCE_REQUIRED",
                    "resolved": False,
                    "timestamp": "2026-06-13T10:00:00Z",
                }
            ]),
        )

    async def test_resolve_maintenance_node_uses_asset_current_location(self):
        db = self.make_db()
        node = await manager.resolve_maintenance_node(db, self.make_alert_payload())
        self.assertEqual(node, "A1")

    async def test_handle_maintenance_alert_assigns_task_for_asset(self):
        db = self.make_db()
        mqtt_client = FakeMqttClient()
        task_assigner = FakeTaskAssigner()
        payload = json.dumps(self.make_alert_payload()).encode("utf-8")

        await manager.handle_maintenance_alert(db, mqtt_client, task_assigner, payload)

        self.assertEqual(len(task_assigner.calls), 1)
        self.assertEqual(task_assigner.calls[0]["node"], "A1")
        alert = await db.maintenanceAlerts.find_one({"assetId": "crane001"})
        self.assertTrue(alert.get("assignmentTriggered"))

    async def test_handle_maintenance_alert_skips_when_assignment_already_triggered(self):
        db = self.make_db(alert_docs=[
            {
                "assetId": "crane001",
                "resolved": False,
                "assignmentTriggered": True,
                "timestamp": "2026-06-13T10:00:00Z",
            }
        ])
        task_assigner = FakeTaskAssigner()
        payload = json.dumps(self.make_alert_payload()).encode("utf-8")

        await manager.handle_maintenance_alert(db, FakeMqttClient(), task_assigner, payload)

        self.assertEqual(task_assigner.calls, [])

    async def test_handle_maintenance_alert_skips_when_active_maintenance_exists(self):
        db = self.make_db(edge_docs=[
            {"id": "crane001", "type": "crane", "taskPhase": "idle", "currentLocation": "A1"},
            {
                "id": "robot001",
                "type": "robot",
                "taskPhase": "assigned",
                "currentLocation": "E5",
                "task": {"phase": "maintenance", "finalNode": "A1", "shipmentId": "repair_A1_1"},
            },
        ])
        task_assigner = FakeTaskAssigner()
        payload = json.dumps(self.make_alert_payload()).encode("utf-8")

        await manager.handle_maintenance_alert(db, FakeMqttClient(), task_assigner, payload)

        self.assertEqual(task_assigner.calls, [])

    async def test_handle_maintenance_alert_ignores_invalid_json(self):
        db = self.make_db()
        task_assigner = FakeTaskAssigner()

        await manager.handle_maintenance_alert(db, FakeMqttClient(), task_assigner, b"not-json")

        self.assertEqual(task_assigner.calls, [])

    async def test_maintenance_assignment_active_detects_non_idle_robot(self):
        db = self.make_db(edge_docs=[
            {
                "id": "robot001",
                "type": "robot",
                "taskPhase": "en_route_start",
                "task": {"phase": "maintenance", "finalNode": "C3"},
            }
        ])

        self.assertTrue(await manager.maintenance_assignment_active(db, "C3"))
        self.assertFalse(await manager.maintenance_assignment_active(db, "A1"))


if __name__ == "__main__":
    unittest.main()
