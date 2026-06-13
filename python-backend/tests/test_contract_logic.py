import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


def install_dependency_stubs():
    """Keep pure helper imports independent from optional service packages."""
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

    timestamp_module.Timestamp = Timestamp
    int64_module = types.ModuleType("bson.int64")

    class Int64(int):
        pass

    int64_module.Int64 = Int64
    bson_module.ObjectId = ObjectId
    bson_module.timestamp = timestamp_module
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


install_dependency_stubs()

import task_assigner
import traffic_analyzer


class FakeAnalyzer:
    def __init__(self, predicted_loads=None):
        self._predicted_loads = predicted_loads or {}
        self.analysis_triggers = []
        self.planner = self

    def get_predicted_loads(self):
        return self._predicted_loads

    def get_current_loads(self):
        return {}

    def get_route_congestion(self):
        return {}

    def compute_path(self, start, end, node_loads, route_congestion, predicted_loads=None):
        return [start] if start == end else [start, end]

    async def analyze_metrics(self, triggered_by=""):
        self.analysis_triggers.append(triggered_by)


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    async def to_list(self, length):
        return self.docs


class FakeCollection:
    def __init__(self, docs):
        self.docs = {doc["id"]: doc for doc in docs}

    async def find_one(self, query):
        doc_id = query.get("id")
        if doc_id is not None:
            return self.docs.get(doc_id)
        for doc in self.docs.values():
            if all(doc.get(key) == value for key, value in query.items()):
                return doc
        return None

    def find(self, query=None):
        query = query or {}
        matches = []
        for doc in self.docs.values():
            matched = True
            for key, value in query.items():
                if isinstance(value, dict) and "$in" in value:
                    matched = doc.get(key) in value["$in"]
                elif doc.get(key) != value:
                    matched = False
                    break
            if matched:
                matches.append(doc)
        return FakeCursor(matches)

    async def update_one(self, query, update, upsert=False, array_filters=None):
        doc_id = query.get("id")
        doc = self.docs.get(doc_id)
        if doc is None and upsert:
            doc = {"id": doc_id}
            self.docs[doc_id] = doc
        if doc is None:
            return

        for key, value in update.get("$set", {}).items():
            if key == "assignedEdges.$[entry].completedAt":
                wanted = array_filters[0] if array_filters else {}
                wanted_edge = wanted.get("entry.edgeId")
                wanted_phase = wanted.get("entry.phase")
                for entry in doc.get("assignedEdges", []):
                    if entry.get("edgeId") == wanted_edge and entry.get("phase") == wanted_phase:
                        entry["completedAt"] = value
            else:
                doc[key] = value
        for key, value in update.get("$inc", {}).items():
            doc[key] = doc.get(key, 0) + value
        for key, value in update.get("$addToSet", {}).items():
            items = doc.setdefault(key, [])
            if value not in items:
                items.append(value)
        for key, value in update.get("$push", {}).items():
            doc.setdefault(key, []).append(value)


class FakeDb:
    def __init__(self):
        self.edgeDevices = FakeCollection([
            {
                "id": "forklift_1",
                "type": "forklift",
                "taskPhase": "completing",
                "shipmentId": "shipment_1",
                "assignedShipment": "shipment_1",
                "currentLocation": "B4",
                "finalNode": "B4",
                "task": {"shipmentId": "shipment_1", "phase": "store_load", "finalNode": "B4"},
            }
        ])
        self.shipments = FakeCollection([
            {
                "id": "shipment_1",
                "status": "storing",
                "assignedEdges": [
                    task_assigner.make_assigned_edge("forklift_1", "store_load", assigned_at="assigned", completed_at=None)
                ],
            }
        ])
        self.graph = FakeCollection([
            {"id": "B4", "type": "warehouse", "currentOccupancy": 0}
        ])


class TrafficAnalyzerHelperTests(unittest.TestCase):
    def test_parse_graph_keeps_neighbors_type_and_capacity(self):
        graph = traffic_analyzer.parse_graph(
            [
                {"id": "A1", "neighbors": {"E": "A2"}, "type": "dock", "capacity": 8},
                {"id": "A2", "neighbors": {"W": "A1"}, "type": "route_point"},
            ]
        )

        self.assertEqual(graph["A1"]["neighbors"], {"E": "A2"})
        self.assertEqual(graph["A1"]["type"], "dock")
        self.assertEqual(graph["A2"]["capacity"], 5)

    def test_route_planner_computes_path_and_respects_blocked_nodes(self):
        graph = traffic_analyzer.parse_graph(
            [
                {"id": "A1", "neighbors": {"E": "A2", "S": "B1"}},
                {"id": "A2", "neighbors": {"W": "A1", "S": "B2"}},
                {"id": "B1", "neighbors": {"N": "A1", "E": "B2"}},
                {"id": "B2", "neighbors": {"N": "A2", "W": "B1"}},
            ]
        )
        planner = traffic_analyzer.SmartRoutePlanner(graph, blocked_nodes={"A2"})

        self.assertEqual(planner.compute_path("A1", "B2", {}, {}), ["A1", "B1", "B2"])
        self.assertEqual(planner.get_neighbors("A1"), [("B1", 1.0)])

    def test_route_planner_scrubs_invalid_start_and_safe_float_values(self):
        graph = traffic_analyzer.parse_graph(
            [
                {"id": "A1", "neighbors": {"E": "A2"}},
                {"id": "A2", "neighbors": {"W": "A1"}},
            ]
        )
        planner = traffic_analyzer.SmartRoutePlanner(graph)

        self.assertEqual(planner.safe_float({"bad": "shape"}, 2.5), 2.5)
        self.assertEqual(planner.safe_float("3.5"), 3.5)
        self.assertEqual(planner.compute_path(None, "A2", {}, {}), ["A1", "A2"])
        self.assertTrue(traffic_analyzer.is_missing_node("Null"))
        self.assertTrue(traffic_analyzer.is_missing_node(None))


class TaskAssignerHelperTests(unittest.IsolatedAsyncioTestCase):
    def make_assigner(self, graph_data, predicted_loads=None):
        return task_assigner.TaskAssigner(
            db=object(),
            mqtt_client=None,
            analyzer=FakeAnalyzer(predicted_loads),
            graph=graph_data,
        )

    def test_parse_graph_adds_default_warehouse_occupancy(self):
        graph = task_assigner.parse_graph(
            [
                {"id": "B4", "neighbors": {}, "type": "warehouse", "capacity": 3},
                {"id": "A1", "neighbors": {}, "type": "dock"},
            ]
        )

        self.assertEqual(graph["B4"]["currentOccupancy"], 0)
        self.assertEqual(graph["A1"]["currentOccupancy"], 0)

    def test_is_warehouse_uses_loaded_graph(self):
        assigner = self.make_assigner(
            [
                {"id": "W1", "neighbors": {}, "type": "warehouse"},
                {"id": "B4", "neighbors": {}, "type": "route_point"},
            ]
        )

        self.assertTrue(assigner._is_warehouse("W1"))
        self.assertFalse(assigner._is_warehouse("B4"))

    def test_phase_chain_matches_shared_contract(self):
        assigner = self.make_assigner(task_assigner.GRAPH_LIST)

        self.assertEqual(assigner._next_phase("offload"), "transport")
        self.assertEqual(assigner._next_phase("transport"), "store_move")
        self.assertEqual(assigner._next_phase("store_move"), "store_load")
        self.assertEqual(assigner._next_phase("store_load"), "delivery")
        self.assertEqual(assigner._next_phase("delivery"), "completed")
        self.assertEqual(assigner._next_phase("unexpected"), "idle")

    def test_assigned_edge_helpers_accept_canonical_and_legacy_entries(self):
        canonical = task_assigner.make_assigned_edge("truck_tempo_1", "transport", assigned_at="now")
        legacy = "truck_tempo_1:transport"

        self.assertTrue(task_assigner.assigned_edge_matches(canonical, phase="transport", edge_id="truck_tempo_1"))
        self.assertTrue(task_assigner.assigned_edge_matches(legacy, phase="transport", edge_id="truck_tempo_1"))
        self.assertTrue(task_assigner.has_assigned_phase([canonical], "transport"))
        self.assertEqual(task_assigner.status_after_phase("store_load"), "stored")
        self.assertEqual(task_assigner.status_after_phase("delivery"), "delivered")

    async def test_select_warehouse_prefers_available_nearby_capacity(self):
        graph_data = [
            {"id": "C3", "neighbors": {}, "type": "berth"},
            {"id": "B4", "neighbors": {}, "type": "warehouse", "capacity": 3, "currentOccupancy": 3},
            {"id": "D2", "neighbors": {}, "type": "warehouse", "capacity": 2, "currentOccupancy": 0},
            {"id": "E5", "neighbors": {}, "type": "warehouse", "capacity": 35, "currentOccupancy": 0},
        ]
        assigner = self.make_assigner(graph_data, predicted_loads={"B4": 0, "D2": 0, "E5": 0})

        self.assertEqual(await assigner._select_warehouse("C3", "shipment-1"), "D2")

    async def test_assign_stage_device_uses_graph_seed_locations(self):
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([
                {
                    "id": "crane_seeded",
                    "type": "crane",
                    "taskPhase": "idle",
                    "currentLocation": "B5",
                    "speed": 4,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_seeded",
                    "status": "arrived",
                    "currentNode": "C1",
                    "assignedEdges": [],
                }
            ]),
        )
        graph_data = [
            {"id": "B5", "neighbors": {}, "type": "dock"},
            {"id": "C1", "neighbors": {}, "type": "dock"},
        ]
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=graph_data)

        assigned = await assigner._assign_stage_device(
            "shipment_seeded",
            "offload",
            {"shipmentId": "shipment_seeded", "destNode": "C1"},
        )

        self.assertEqual(assigned, "crane_seeded")
        edge = db.edgeDevices.docs["crane_seeded"]
        shipment = db.shipments.docs["shipment_seeded"]
        self.assertEqual(edge["taskPhase"], "en_route_start")
        self.assertEqual(edge["startNode"], "C1")
        self.assertTrue(task_assigner.has_assigned_phase(shipment["assignedEdges"], "offload"))

    async def test_store_load_completion_is_idempotent(self):
        db = FakeDb()
        analyzer = FakeAnalyzer()
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=analyzer, graph=task_assigner.GRAPH_LIST)
        payload = {
            "shipmentId": "shipment_1",
            "phase": "store_load",
            "location": "B4",
            "completedAt": "completed",
        }

        await assigner.handle_completion("forklift_1", payload)
        await assigner.handle_completion("forklift_1", payload)

        shipment = db.shipments.docs["shipment_1"]
        edge = db.edgeDevices.docs["forklift_1"]
        warehouse = db.graph.docs["B4"]
        self.assertEqual(shipment["status"], "stored")
        self.assertEqual(shipment["assignedEdges"][0]["completedAt"], "completed")
        self.assertEqual(warehouse["currentOccupancy"], 1)
        self.assertEqual(edge["taskPhase"], "idle")
        self.assertIsNone(edge["shipmentId"])

    async def test_offload_completion_duplicate_does_not_regress_status(self):
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([
                {
                    "id": "crane001",
                    "type": "crane",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_offload",
                    "assignedShipment": "shipment_offload",
                    "currentLocation": "C3",
                    "finalNode": "C3",
                    "task": {"shipmentId": "shipment_offload", "phase": "offload", "finalNode": "C3"},
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_offload",
                    "status": "arrived",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="assigned", completed_at=None)
                    ],
                }
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=task_assigner.GRAPH_LIST)
        payload = {
            "shipmentId": "shipment_offload",
            "phase": "offload",
            "location": "C3",
            "completedAt": "first",
        }

        await assigner.handle_completion("crane001", payload)
        await assigner.handle_completion("crane001", payload)

        shipment = db.shipments.docs["shipment_offload"]
        self.assertEqual(shipment["status"], "offloaded")
        self.assertEqual(len(shipment["assignedEdges"]), 1)
        self.assertEqual(shipment["assignedEdges"][0]["completedAt"], "first")

    async def test_transport_completion_duplicate_keeps_single_assignment(self):
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_transport",
                    "assignedShipment": "shipment_transport",
                    "currentLocation": "D2",
                    "finalNode": "D2",
                    "task": {"shipmentId": "shipment_transport", "phase": "transport", "finalNode": "D2"},
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_transport",
                    "status": "offloaded",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("truck_tempo_1", "transport", assigned_at="assigned", completed_at=None)
                    ],
                }
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=task_assigner.GRAPH_LIST)
        payload = {
            "shipmentId": "shipment_transport",
            "phase": "transport",
            "location": "D2",
            "completedAt": "transported-at",
        }

        await assigner.handle_completion("truck_tempo_1", payload)
        await assigner.handle_completion("truck_tempo_1", payload)

        shipment = db.shipments.docs["shipment_transport"]
        self.assertEqual(shipment["status"], "transported")
        self.assertEqual(len(shipment["assignedEdges"]), 1)


if __name__ == "__main__":
    unittest.main()
