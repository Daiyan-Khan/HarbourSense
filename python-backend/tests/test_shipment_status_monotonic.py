import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock


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
    operations_module.UpdateOne = type("UpdateOne", (), {})
    pymongo_module.operations = operations_module
    sys.modules.setdefault("pymongo", pymongo_module)
    sys.modules.setdefault("pymongo.operations", operations_module)

    aiomqtt_module = types.ModuleType("aiomqtt")
    aiomqtt_module.Client = object
    aiomqtt_module.MqttError = Exception
    sys.modules.setdefault("aiomqtt", aiomqtt_module)

    sensor_analyzer_module = types.ModuleType("sensor_analyzer")

    class SensorAnalyzer:
        def __init__(self, db):
            self.db = db

    sensor_analyzer_module.SensorAnalyzer = SensorAnalyzer
    sys.modules.setdefault("sensor_analyzer", sensor_analyzer_module)


install_dependency_stubs()

import importlib

import task_assigner

manager = importlib.import_module("manager")
handle_shipment_update = manager.handle_shipment_update


class ShipmentStatusMonotonicTests(unittest.IsolatedAsyncioTestCase):
    def test_max_status_never_regresses(self):
        self.assertEqual(task_assigner.max_status("delivered", "arrived"), "delivered")
        self.assertEqual(task_assigner.max_status("arrived", "offloaded"), "offloaded")
        self.assertEqual(task_assigner.max_status("transported", "storing"), "storing")

    def test_normalize_shipment_current_node_rejects_processing(self):
        shipment = {"warehouseAssigned": "B4", "destination": "D2", "currentNode": "processing"}
        self.assertEqual(
            task_assigner.normalize_shipment_current_node("processing", shipment),
            "B4",
        )

    def test_can_assign_store_allows_transported_with_warehouse_assigned(self):
        assigner = task_assigner.TaskAssigner(db=None, mqtt_client=None, analyzer=None, graph=task_assigner.GRAPH_LIST)
        shipment = {
            "status": "transported",
            "currentNode": "processing",
            "destination": "B4",
            "warehouseAssigned": "B4",
        }
        self.assertTrue(assigner._can_assign_store(shipment, "B4"))

    async def test_reconcile_shipment_status_bumps_from_assigned_edges(self):
        db = types.SimpleNamespace(
            shipments=types.SimpleNamespace(
                update_one=AsyncMock(),
            )
        )
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=None, graph=task_assigner.GRAPH_LIST)
        shipment = {
            "id": "shipment_27",
            "status": "arrived",
            "assignedEdges": [
                {"edgeId": "truck_d", "phase": "delivery", "completedAt": "2026-06-12T10:00:00Z"},
            ],
        }
        result = await assigner._reconcile_shipment_status(shipment)
        self.assertEqual(result, "delivered")
        db.shipments.update_one.assert_awaited_once()

    async def test_handle_shipment_update_applies_monotonic_status(self):
        db = types.SimpleNamespace(
            shipments=types.SimpleNamespace(
                find_one=AsyncMock(return_value={
                    "id": "shipment_27",
                    "status": "stored",
                    "assignedEdges": [],
                    "warehouseAssigned": "B4",
                }),
                update_one=AsyncMock(),
            )
        )
        task_assigner_stub = types.SimpleNamespace(
            graph=task_assigner.parse_graph(task_assigner.GRAPH_LIST),
            mqtt_client=None,
        )
        message = MagicMock()
        message.topic = "harboursense/shipments/shipment_27"
        message.payload = b'{"status":"arrived","currentNode":"processing","destination":"B4","createdAt":"2026-06-12T09:00:00Z"}'

        await handle_shipment_update(db, task_assigner_stub, None, message)

        update_call = db.shipments.update_one.await_args
        set_fields = update_call.args[1]["$set"]
        self.assertEqual(set_fields["status"], "stored")
        self.assertEqual(set_fields["currentNode"], "B4")


if __name__ == "__main__":
    unittest.main()
