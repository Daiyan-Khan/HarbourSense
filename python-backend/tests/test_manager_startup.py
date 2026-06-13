import importlib
import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


def install_manager_dependency_stubs():
    """Keep manager startup tests independent from optional runtime packages."""
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


install_manager_dependency_stubs()
manager = importlib.import_module("manager")


class ManagerGraphStartupTests(unittest.TestCase):
    def test_empty_raw_graph_raises_seed_missing_error(self):
        with self.assertRaisesRegex(manager.GraphSeedMissingError, "Graph missing or empty"):
            manager.build_graph_from_raw_nodes([], lambda value: value)

    def test_single_nodes_document_is_flattened(self):
        graph = manager.build_graph_from_raw_nodes(
            [
                {
                    "nodes": [
                        {"id": "A1", "neighbors": {"E": "A2"}, "type": "dock"},
                        {"id": "A2", "neighbors": {"W": "A1"}, "type": "route_point"},
                    ]
                }
            ],
            lambda value: value,
        )

        self.assertEqual(sorted(graph), ["A1", "A2"])
        self.assertEqual(graph["A1"]["type"], "dock")

    def test_docs_without_node_ids_raise_seed_structure_error(self):
        with self.assertRaisesRegex(manager.GraphSeedMissingError, "no usable node id"):
            manager.build_graph_from_raw_nodes([{"name": "A1"}], lambda value: value)


if __name__ == "__main__":
    unittest.main()
