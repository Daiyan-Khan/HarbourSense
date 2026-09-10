import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock


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

    class ReturnDocument:
        AFTER = 1
        BEFORE = 0

    operations_module.UpdateOne = UpdateOne
    pymongo_module.operations = operations_module
    pymongo_module.ReturnDocument = ReturnDocument
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
from edge_view import split_edge_document


def build_split_edge_collections(edge_docs):
    runtimes = []
    assignments = []
    for doc in edge_docs:
        assignment, runtime = split_edge_document(doc)
        runtimes.append(runtime)
        assignments.append(assignment)
    return runtimes, assignments


def build_test_db(edge_docs=None, shipments=None, graph=None, **extra):
  class TestDb:
    def __getitem__(self, key):
      return getattr(self, key)

  db = TestDb()
  edge_docs = edge_docs or []
  runtimes, assignments = build_split_edge_collections(edge_docs) if edge_docs else ([], [])
  db.edgeRuntime = FakeCollection(runtimes)
  db.edgeAssignments = FakeCollection(assignments)
  db.edgeDevices = FakeCollection(edge_docs)
  db.shipments = FakeCollection(shipments or [])
  db.graph = FakeCollection(graph or [])
  for key, value in extra.items():
    setattr(db, key, value)
  return db


class ContractDb:
    """Test DB with automatic edgeDevices -> edgeRuntime + edgeAssignments split."""

    def __init__(self, **kwargs):
        edge_coll = kwargs.pop("edgeDevices", None)
        if edge_coll is not None:
            edge_docs = list(edge_coll.docs.values())
            runtimes, assignments = build_split_edge_collections(edge_docs)
            kwargs["edgeRuntime"] = FakeCollection(runtimes)
            kwargs["edgeAssignments"] = FakeCollection(assignments)
            kwargs["edgeDevices"] = edge_coll
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)


def merged_edge_doc(db, edge_id):
    from edge_view import merge_edge_snapshot

    runtime = db.edgeRuntime.docs.get(edge_id) or db.edgeDevices.docs.get(edge_id)
    assignment = db.edgeAssignments.docs.get(edge_id) or {"id": edge_id}
    return merge_edge_snapshot(assignment, runtime) if runtime else None


class FakeAnalyzer:
    def __init__(self, predicted_loads=None):
        self._predicted_loads = predicted_loads or {}
        self.analysis_triggers = []
        graph = traffic_analyzer.parse_graph(traffic_analyzer.GRAPH_LIST)
        self.planner = traffic_analyzer.SmartRoutePlanner(graph)

    def get_predicted_loads(self):
        return self._predicted_loads

    def get_current_loads(self):
        return {}

    def get_route_congestion(self):
        return {}

    async def analyze_metrics(self, triggered_by=""):
        self.analysis_triggers.append(triggered_by)


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self.docs):
            raise StopAsyncIteration
        doc = self.docs[self._index]
        self._index += 1
        return doc

    async def to_list(self, length):
        return list(self.docs)


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
        for key in update.get("$unset", {}):
            doc.pop(key, None)
        for key, value in update.get("$inc", {}).items():
            doc[key] = doc.get(key, 0) + value
        for key, value in update.get("$addToSet", {}).items():
            items = doc.setdefault(key, [])
            if value not in items:
                items.append(value)
        for key, value in update.get("$push", {}).items():
            doc.setdefault(key, []).append(value)

    async def find_one_and_update(self, query, update, return_document=None, upsert=False):
        doc_id = query.get("id")
        doc = self.docs.get(doc_id)
        if doc is None:
            return None
        for key, value in query.items():
            if key == "id":
                continue
            if doc.get(key) != value:
                return None
        await self.update_one({"id": doc_id}, update, upsert=upsert)
        return self.docs.get(doc_id)

    async def count_documents(self, query=None):
        query = query or {}
        count = 0
        for doc in self.docs.values():
            matched = True
            for key, value in query.items():
                if doc.get(key) != value:
                    matched = False
                    break
            if matched:
                count += 1
        return count


class FakeDb:
    def __init__(self, edge_docs=None):
        default_edges = edge_docs if edge_docs is not None else [
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
        ]
        runtimes, assignments = build_split_edge_collections(default_edges)
        self.edgeRuntime = FakeCollection(runtimes)
        self.edgeAssignments = FakeCollection(assignments)
        self.edgeDevices = FakeCollection(default_edges)
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

    def __getitem__(self, key):
        return getattr(self, key)


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

    def test_normalize_node_id_strips_hyphens_and_nulls(self):
        self.assertEqual(traffic_analyzer.normalize_node_id("A-1"), "A1")
        self.assertIsNone(traffic_analyzer.normalize_node_id("Null"))

    def test_validate_path_adjacency_rejects_teleport_hop(self):
        graph = traffic_analyzer.parse_graph(traffic_analyzer.GRAPH_LIST)
        planner = traffic_analyzer.SmartRoutePlanner(graph)
        self.assertIsNone(traffic_analyzer.validate_path_adjacency(["A1", "B4"], planner))

    def test_validate_path_adjacency_accepts_manhattan_route(self):
        graph = traffic_analyzer.parse_graph(traffic_analyzer.GRAPH_LIST)
        planner = traffic_analyzer.SmartRoutePlanner(graph)
        path = ["A1", "A2", "B2", "B3", "B4"]
        self.assertEqual(traffic_analyzer.validate_path_adjacency(path, planner), path)

    def test_greedy_fallback_returns_none_not_teleport(self):
        graph = traffic_analyzer.parse_graph([
            {"id": "A1", "neighbors": {"E": "A2"}},
            {"id": "A2", "neighbors": {"W": "A1"}},
            {"id": "B4", "neighbors": {}},
        ])
        planner = traffic_analyzer.SmartRoutePlanner(graph)
        result = planner._greedy_fallback("A1", "B4", {}, {})
        self.assertIsNone(result)

    def test_compute_path_never_returns_non_adjacent_two_node_path(self):
        graph = traffic_analyzer.parse_graph([
            {"id": "A1", "neighbors": {"E": "A2"}},
            {"id": "A2", "neighbors": {"W": "A1"}},
            {"id": "B4", "neighbors": {}},
        ])
        planner = traffic_analyzer.SmartRoutePlanner(graph)
        path = planner.compute_path("A1", "B4", {}, {})
        if path and len(path) == 2:
            self.assertIsNotNone(traffic_analyzer.validate_path_adjacency(path, planner))
        else:
            self.assertIsNone(path)


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
        db = ContractDb(
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
        edge = merged_edge_doc(db, "crane_seeded")
        shipment = db.shipments.docs["shipment_seeded"]
        self.assertEqual(edge["taskPhase"], "idle")
        self.assertEqual(edge["startNode"], "C1")
        self.assertGreater(len(edge.get("pendingPath") or []), 1)
        self.assertNotIn("path", edge)
        self.assertEqual(edge["pendingPath"][0], "B5")
        self.assertEqual(edge["pendingPath"][-1], "C1")
        self.assertTrue(task_assigner.has_assigned_phase(shipment["assignedEdges"], "offload"))

    async def test_offload_crane_at_dock_gets_assigned_without_path(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane_dock",
                    "type": "crane",
                    "taskPhase": "idle",
                    "currentLocation": "A1",
                    "speed": 4,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_dock",
                    "status": "arrived",
                    "currentNode": "A1",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_dock",
            "offload",
            {"shipmentId": "shipment_dock", "destNode": "A1"},
        )

        self.assertEqual(assigned, "crane_dock")
        edge = merged_edge_doc(db, "crane_dock")
        self.assertEqual(edge["taskPhase"], "idle")
        self.assertEqual(edge.get("pendingPath"), [])

    async def test_offload_skips_second_crane_when_phase_in_flight(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane002",
                    "type": "crane",
                    "taskPhase": "en_route_start",
                    "currentLocation": "C3",
                    "shipmentId": "shipment_1",
                    "assignedShipment": "shipment_1",
                },
                {
                    "id": "crane003",
                    "type": "crane",
                    "taskPhase": "idle",
                    "currentLocation": "E4",
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_1",
                    "status": "arrived",
                    "currentNode": "C3",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane002", "offload", assigned_at="t", completed_at=None),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_1",
            "offload",
            {"shipmentId": "shipment_1", "destNode": "C3"},
        )

        self.assertIsNone(assigned)
        self.assertEqual(len(db.shipments.docs["shipment_1"]["assignedEdges"]), 1)

    async def test_reconcile_stale_idle_edges_clears_orphan_bindings(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_delivery_1",
                    "type": "truck_delivery",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                    "shipmentId": "shipment_75",
                    "task": {"phase": "transport", "shipmentId": "shipment_75"},
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_75",
                    "status": "stored",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "truck_delivery_1", "delivery", assigned_at="t", completed_at="done"
                        ),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        shipments_by_id = {s["id"]: s for s in db.shipments.docs.values()}
        edges = [merged_edge_doc(db, eid) for eid in db.edgeRuntime.docs]

        cleared = await assigner._reconcile_stale_idle_edges(edges, shipments_by_id)

        self.assertEqual(cleared, 1)
        edge = merged_edge_doc(db, "truck_delivery_1")
        self.assertIsNone(edge.get("shipmentId"))
        self.assertIn(edge.get("task"), (None, "idle"))

    async def test_offload_remote_crane_gets_path_to_dock(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane002",
                    "type": "crane",
                    "taskPhase": "idle",
                    "currentLocation": "C3",
                    "speed": 4,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_remote",
                    "status": "arrived",
                    "currentNode": "A1",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_remote",
            "offload",
            {"shipmentId": "shipment_remote", "destNode": "A1"},
        )

        self.assertEqual(assigned, "crane002")
        edge = merged_edge_doc(db, "crane002")
        self.assertEqual(edge["taskPhase"], "idle")
        pending_path = edge.get("pendingPath") or []
        self.assertGreater(len(pending_path), 1)
        self.assertEqual(pending_path[0], "C3")
        self.assertEqual(pending_path[-1], "A1")
        task = edge.get("task") or {}
        self.assertEqual(task.get("destNode"), "A1")
        self.assertIn("routeRevision", task)

    async def test_assign_task_preserves_valid_computed_path(self):
        """Path validation must not replace a valid multi-hop route with a teleport shortcut."""
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "robot_path",
                    "type": "robot",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                    "speed": 7,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_path",
                    "status": "transported",
                    "currentNode": "D2",
                    "destination": "D2",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        task_details = {
            "shipmentId": "shipment_path",
            "phase": "store_move",
            "requiredPlace": "D2",
            "finalNode": "D2",
            "startNode": "D2",
        }

        await assigner.assign_task("robot", task_details, edge_id="robot_path")

        edge = merged_edge_doc(db, "robot_path")
        pending_path = edge.get("pendingPath")
        self.assertIsNotNone(pending_path)
        self.assertGreater(len(pending_path), 1)
        self.assertEqual(pending_path[0], "B4")
        validated = traffic_analyzer.validate_path_adjacency(pending_path, assigner.analyzer.planner)
        self.assertEqual(validated, pending_path)

    async def test_transport_assign_uses_pickup_as_required_place(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "B1",
                    "speed": 15,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_pickup",
                    "status": "offloaded",
                    "currentNode": "A1",
                    "arrivalNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        await assigner._try_assign_transport("shipment_pickup")
        task = merged_edge_doc(db, "truck_tempo_1").get("task") or {}
        self.assertEqual(task.get("requiredPlace"), "A1")
        self.assertEqual(task.get("destNode"), "B4")
        self.assertEqual(task.get("finalNode"), "B4")

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
        edge = merged_edge_doc(db, "forklift_1")
        warehouse = db.graph.docs["B4"]
        self.assertEqual(shipment["status"], "stored")
        self.assertEqual(shipment["assignedEdges"][0]["completedAt"], "completed")
        self.assertEqual(warehouse["currentOccupancy"], 1)
        self.assertIsNone(db.edgeAssignments.docs["forklift_1"].get("shipmentId"))

    async def test_offload_completion_duplicate_does_not_regress_status(self):
        db = ContractDb(
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

    async def test_offload_completion_clears_offload_queued_flag(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane001",
                    "type": "crane",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_7",
                    "assignedShipment": "shipment_7",
                    "currentLocation": "A1",
                    "finalNode": "A1",
                    "task": {"shipmentId": "shipment_7", "phase": "offload", "finalNode": "A1"},
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_7",
                    "status": "arrived",
                    "offloadQueued": True,
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="assigned", completed_at=None)
                    ],
                }
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=task_assigner.GRAPH_LIST)
        assigner._try_assign_transport = AsyncMock(return_value=None)

        await assigner.handle_completion(
            "crane001",
            {
                "shipmentId": "shipment_7",
                "phase": "offload",
                "location": "A1",
                "completedAt": "done",
            },
        )

        shipment = db.shipments.docs["shipment_7"]
        self.assertEqual(shipment["status"], "offloaded")
        self.assertFalse(shipment.get("offloadQueued"))

    async def test_reconcile_queue_flags_clears_stale_offload_queued(self):
        db = ContractDb(
            edgeDevices=FakeCollection([]),
            shipments=FakeCollection([
                {
                    "id": "shipment_7",
                    "status": "offloaded",
                    "offloadQueued": True,
                    "transportQueued": True,
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="t", completed_at="done"),
                    ],
                }
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=task_assigner.GRAPH_LIST)
        shipment = db.shipments.docs["shipment_7"]

        changed = await assigner._reconcile_queue_flags(shipment)

        self.assertTrue(changed)
        self.assertFalse(shipment.get("offloadQueued"))
        self.assertTrue(shipment.get("transportQueued"))

    async def test_transport_completion_assigns_store_move_immediately(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_chain",
                    "assignedShipment": "shipment_chain",
                    "currentLocation": "D2",
                    "finalNode": "D2",
                    "task": {"shipmentId": "shipment_chain", "phase": "transport", "finalNode": "D2"},
                },
                {
                    "id": "robot001",
                    "type": "robot",
                    "taskPhase": "idle",
                    "currentLocation": "D2",
                    "speed": 7,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_chain",
                    "status": "offloaded",
                    "destination": "D2",
                    "warehouseAssigned": "D2",
                    "currentNode": "D2",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("truck_tempo_1", "transport", assigned_at="assigned", completed_at=None),
                    ],
                }
            ]),
            graph=FakeCollection([
                {"id": "D2", "type": "warehouse", "capacity": 25, "currentOccupancy": 0},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        payload = {
            "shipmentId": "shipment_chain",
            "phase": "transport",
            "location": "D2",
            "completedAt": "transported-at",
        }

        await assigner.handle_completion("truck_tempo_1", payload)

        shipment = db.shipments.docs["shipment_chain"]
        self.assertIn(shipment["status"], ("transported", "storing"))
        self.assertTrue(task_assigner.has_assigned_phase(shipment["assignedEdges"], "store_move"))
        robot = merged_edge_doc(db, "robot001")
        self.assertEqual(robot["shipmentId"], "shipment_chain")
        self.assertEqual(robot["task"]["phase"], "store_move")
        self.assertIn("assignmentEpoch", robot["task"])

    async def test_store_move_completion_assigns_store_load_immediately(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "robot001",
                    "type": "robot",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_store",
                    "assignedShipment": "shipment_store",
                    "currentLocation": "B4",
                    "finalNode": "B4",
                    "task": {"shipmentId": "shipment_store", "phase": "store_move", "finalNode": "B4"},
                },
                {
                    "id": "forklift_001",
                    "type": "forklift",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                    "speed": 5,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_store",
                    "status": "storing",
                    "destination": "B4",
                    "currentNode": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("robot001", "store_move", assigned_at="assigned", completed_at=None),
                    ],
                }
            ]),
            graph=FakeCollection([
                {"id": "B4", "type": "warehouse", "capacity": 35, "currentOccupancy": 0},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        await assigner.handle_completion(
            "robot001",
            {
                "shipmentId": "shipment_store",
                "phase": "store_move",
                "location": "B4",
                "completedAt": "move-done",
            },
        )

        shipment = db.shipments.docs["shipment_store"]
        self.assertTrue(task_assigner.has_assigned_phase(shipment["assignedEdges"], "store_load"))
        forklift = merged_edge_doc(db, "forklift_001")
        self.assertEqual(forklift["shipmentId"], "shipment_store")

    async def test_store_load_completion_assigns_delivery_when_applicable(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "forklift_001",
                    "type": "forklift",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_deliver",
                    "assignedShipment": "shipment_deliver",
                    "currentLocation": "B4",
                    "finalNode": "B4",
                    "task": {"shipmentId": "shipment_deliver", "phase": "store_load", "finalNode": "B4"},
                },
                {
                    "id": "truck_delivery_1",
                    "type": "truck_delivery",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                    "speed": 12,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_deliver",
                    "status": "storing",
                    "destination": "B4",
                    "currentNode": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("robot001", "store_move", assigned_at="t", completed_at="done"),
                        task_assigner.make_assigned_edge("forklift_001", "store_load", assigned_at="assigned", completed_at=None),
                    ],
                }
            ]),
            graph=FakeCollection([
                {"id": "B4", "type": "warehouse", "capacity": 35, "currentOccupancy": 0},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        await assigner.handle_completion(
            "forklift_001",
            {
                "shipmentId": "shipment_deliver",
                "phase": "store_load",
                "location": "B4",
                "completedAt": "stored-at",
            },
        )

        shipment = db.shipments.docs["shipment_deliver"]
        self.assertEqual(shipment["status"], "stored")
        self.assertTrue(task_assigner.has_assigned_phase(shipment["assignedEdges"], "delivery"))
        truck = merged_edge_doc(db, "truck_delivery_1")
        self.assertEqual(truck["shipmentId"], "shipment_deliver")

    async def test_assign_stage_device_includes_assignment_epoch(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "B1",
                    "assignmentEpoch": 100,
                    "speed": 15,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_epoch",
                    "status": "offloaded",
                    "currentNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        await assigner._assign_stage_device(
            "shipment_epoch",
            "transport",
            {
                "shipmentId": "shipment_epoch",
                "startNode": "A1",
                "finalNode": "B4",
                "pickupNode": "A1",
                "requiredPlace": "A1",
            },
        )

        edge = merged_edge_doc(db, "truck_tempo_1")
        self.assertGreater(edge["assignmentEpoch"], 100)
        self.assertEqual(edge["task"]["assignmentEpoch"], edge["assignmentEpoch"])
        self.assertEqual(edge["taskPhase"], "idle")
        self.assertNotIn("path", edge)
        self.assertEqual(edge.get("progressToNext", 0), 0)

    async def test_transport_completion_duplicate_keeps_single_assignment(self):
        db = ContractDb(
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

    def test_max_status_helpers(self):
        self.assertEqual(task_assigner.max_status("delivered", "arrived"), "delivered")
        self.assertEqual(task_assigner.max_status("offloaded", "transported"), "transported")

    def test_can_assign_store_with_processing_current_node(self):
        assigner = self.make_assigner(task_assigner.GRAPH_LIST)
        shipment = {
            "status": "transported",
            "currentNode": "processing",
            "destination": "B4",
            "warehouseAssigned": "B4",
        }
        self.assertTrue(assigner._can_assign_store(shipment, "B4"))

    def test_normalize_shipment_current_node_maps_processing_to_warehouse(self):
        shipment = {"warehouseAssigned": "D2", "destination": "B4"}
        self.assertEqual(
            task_assigner.normalize_shipment_current_node("processing", shipment),
            "D2",
        )

    def test_pickup_node_for_transport_uses_arrival_when_current_at_warehouse(self):
        assigner = self.make_assigner(task_assigner.GRAPH_LIST)
        shipment = {
            "currentNode": "B4",
            "arrivalNode": "A1",
            "status": "offloaded",
        }
        self.assertEqual(assigner._pickup_node_for_transport(shipment), "A1")

    async def test_offload_completion_assigns_transport_immediately(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane001",
                    "type": "crane",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_1",
                    "assignedShipment": "shipment_1",
                    "currentLocation": "A1",
                    "finalNode": "A1",
                    "task": {"shipmentId": "shipment_1", "phase": "offload", "requiredPlace": "A1", "finalNode": "A1"},
                },
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "B1",
                    "speed": 15,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_1",
                    "status": "arrived",
                    "currentNode": "A1",
                    "arrivalNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="assigned", completed_at=None),
                    ],
                }
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        payload = {
            "shipmentId": "shipment_1",
            "phase": "offload",
            "location": "A1",
            "completedAt": "2026-06-14T10:00:00Z",
        }

        await assigner.handle_completion("crane001", payload)

        shipment = db.shipments.docs["shipment_1"]
        self.assertEqual(shipment["status"], "offloaded")
        self.assertTrue(task_assigner.has_assigned_phase(shipment["assignedEdges"], "transport"))
        edge = merged_edge_doc(db, "truck_tempo_1")
        self.assertEqual(edge["taskPhase"], "idle")
        self.assertEqual(edge["shipmentId"], "shipment_1")
        self.assertTrue(edge.get("pendingPath") or edge.get("task", {}).get("path"))

    async def test_try_assign_transport_when_current_node_drifted_to_warehouse(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "B1",
                    "speed": 15,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_drift",
                    "status": "offloaded",
                    "currentNode": "B4",
                    "arrivalNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="assigned", completed_at="done"),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._try_assign_transport("shipment_drift")

        self.assertEqual(assigned, "truck_tempo_1")
        edge = merged_edge_doc(db, "truck_tempo_1")
        self.assertEqual(edge["shipmentId"], "shipment_drift")

    async def test_transport_from_warehouse_routes_via_dock_pickup(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                    "speed": 15,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_loop",
                    "status": "offloaded",
                    "currentNode": "B4",
                    "arrivalNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="t", completed_at="done"),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._try_assign_transport("shipment_loop")

        self.assertEqual(assigned, "truck_tempo_1")
        edge = merged_edge_doc(db, "truck_tempo_1")
        pending_path = edge.get("pendingPath") or edge.get("task", {}).get("path") or []
        self.assertIn("A1", pending_path)
        self.assertEqual(pending_path[0], "B4")
        self.assertEqual(pending_path[-1], "B4")

    async def test_reconcile_superseded_offload_releases_duplicate_cranes(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane002",
                    "type": "crane",
                    "taskPhase": "en_route_start",
                    "currentLocation": "C3",
                    "shipmentId": "shipment_1",
                    "assignedShipment": "shipment_1",
                },
                {
                    "id": "crane003",
                    "type": "crane",
                    "taskPhase": "en_route_start",
                    "currentLocation": "E4",
                    "shipmentId": "shipment_1",
                    "assignedShipment": "shipment_1",
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_1",
                    "status": "offloaded",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("crane001", "offload", assigned_at="t", completed_at="done"),
                        task_assigner.make_assigned_edge("crane002", "offload", assigned_at="t", completed_at=None),
                        task_assigner.make_assigned_edge("crane003", "offload", assigned_at="t", completed_at=None),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        shipment = db.shipments.docs["shipment_1"]
        edges_by_id = {e["id"]: e for e in db.edgeDevices.docs.values()}

        changed = await assigner._reconcile_superseded_assignments(shipment, edges_by_id)

        self.assertTrue(changed)
        self.assertTrue(all(e.get("completedAt") for e in shipment["assignedEdges"]))
        self.assertIsNone(db.edgeAssignments.docs["crane002"].get("shipmentId"))
        self.assertIsNone(db.edgeAssignments.docs["crane003"].get("shipmentId"))

    async def test_store_move_assigns_when_warehouse_over_capacity(self):
        graph_data = [
            {"id": "D2", "neighbors": {"N": "C2"}, "type": "warehouse", "capacity": 25, "currentOccupancy": 63, "x": 3, "y": 1},
            {"id": "E5", "neighbors": {"W": "E4"}, "type": "warehouse", "capacity": 35, "currentOccupancy": 0, "x": 4, "y": 4},
        ]
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "robot001",
                    "type": "robot",
                    "taskPhase": "idle",
                    "currentLocation": "E5",
                    "speed": 7,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_72",
                    "status": "transported",
                    "destination": "D2",
                    "warehouseAssigned": "D2",
                    "currentNode": "D2",
                    "assignedEdges": [],
                    "storeQueued": True,
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=graph_data,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_72",
            "store_move",
            {"shipmentId": "shipment_72", "pickupNode": "D2", "finalNode": "D2", "subPhase": "move"},
        )

        self.assertEqual(assigned, "robot001")
        self.assertTrue(task_assigner.has_assigned_phase(
            db.shipments.docs["shipment_72"]["assignedEdges"], "store_move"
        ))

    async def test_reconcile_stale_assignment_backfills_completed_at(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "crane001",
                    "type": "crane",
                    "taskPhase": "idle",
                    "currentLocation": "A1",
                    "shipmentId": None,
                    "assignedShipment": None,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_stale",
                    "status": "arrived",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "crane001", "offload", assigned_at="assigned", completed_at=None
                        ),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db, mqtt_client=None, analyzer=None, graph=task_assigner.GRAPH_LIST
        )
        shipment = db.shipments.docs["shipment_stale"]
        edges_by_id = {edge["id"]: edge for edge in db.edgeDevices.docs.values()}

        changed = await assigner._reconcile_stale_assignments(shipment, edges_by_id)

        self.assertTrue(changed)
        entry = db.shipments.docs["shipment_stale"]["assignedEdges"][0]
        self.assertIsNotNone(entry.get("completedAt"))
        self.assertEqual(db.shipments.docs["shipment_stale"]["status"], "offloaded")

    async def test_handle_completion_marks_legacy_assigned_edge(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_legacy",
                    "currentLocation": "D2",
                    "finalNode": "D2",
                    "task": {"shipmentId": "shipment_legacy", "phase": "transport"},
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_legacy",
                    "status": "offloaded",
                    "assignedEdges": ["truck_tempo_1:transport"],
                }
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=task_assigner.GRAPH_LIST
        )
        await assigner.handle_completion(
            "truck_tempo_1",
            {"shipmentId": "shipment_legacy", "phase": "transport", "location": "D2", "completedAt": "done"},
        )
        shipment = db.shipments.docs["shipment_legacy"]
        self.assertEqual(shipment["status"], "transported")
        self.assertTrue(task_assigner.phase_complete(shipment["assignedEdges"], "transport"))

    async def test_delivery_completion_decrements_warehouse_occupancy(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_delivery_1",
                    "type": "truck_delivery",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_delivery",
                    "assignedShipment": "shipment_delivery",
                    "currentLocation": "E5",
                    "finalNode": "E5",
                    "task": {"shipmentId": "shipment_delivery", "phase": "delivery", "finalNode": "E5"},
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_delivery",
                    "status": "stored",
                    "destination": "B4",
                    "warehouseAssigned": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "truck_delivery_1", "delivery", assigned_at="assigned", completed_at=None
                        ),
                    ],
                }
            ]),
            graph=FakeCollection([
                {"id": "B4", "type": "warehouse", "capacity": 3, "currentOccupancy": 2},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db, mqtt_client=None, analyzer=FakeAnalyzer(), graph=task_assigner.GRAPH_LIST
        )
        await assigner.handle_completion(
            "truck_delivery_1",
            {
                "shipmentId": "shipment_delivery",
                "phase": "delivery",
                "location": "E5",
                "completedAt": "done",
            },
        )
        shipment = db.shipments.docs["shipment_delivery"]
        warehouse = db.graph.docs["B4"]
        self.assertEqual(shipment["status"], "delivered")
        self.assertEqual(warehouse["currentOccupancy"], 1)

    async def test_sync_warehouse_occupancy_clamps_runaway_counter(self):
        db = ContractDb(
            shipments=FakeCollection([
                {"id": "s1", "status": "stored", "destination": "B4"},
                {"id": "s2", "status": "storing", "destination": "B4"},
            ]),
            graph=FakeCollection([
                {"id": "B4", "type": "warehouse", "capacity": 3, "currentOccupancy": 11},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=None,
            graph=[{"id": "B4", "type": "warehouse", "capacity": 3, "currentOccupancy": 11}],
        )
        await assigner._sync_warehouse_occupancy()
        # Only stored shipments occupy slots; storing (awaiting store_load) does not.
        self.assertEqual(db.graph.docs["B4"]["currentOccupancy"], 1)
        self.assertEqual(assigner.graph["B4"]["currentOccupancy"], 1)

    async def test_assign_stage_device_persists_no_idle_device_diagnostic(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_busy",
                    "type": "truck_tempo",
                    "taskPhase": "assigned",
                    "currentLocation": "C5",
                    "shipmentId": "shipment_other",
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_waiting",
                    "status": "offloaded",
                    "currentNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_waiting",
            "transport",
            {
                "shipmentId": "shipment_waiting",
                "startNode": "A1",
                "finalNode": "B4",
                "pickupNode": "A1",
                "requiredPlace": "B4",
            },
        )
        self.assertIsNone(assigned)
        shipment = db.shipments.docs["shipment_waiting"]
        diagnostic = shipment.get("workflowDiagnostic", {})
        self.assertEqual(diagnostic.get("blockerCode"), "NO_IDLE_DEVICE")

    async def test_assign_stage_device_persists_no_valid_path_diagnostic(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_isolated",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "Z9",
                    "speed": 10,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_no_path",
                    "status": "offloaded",
                    "currentNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_no_path",
            "transport",
            {
                "shipmentId": "shipment_no_path",
                "startNode": "A1",
                "finalNode": "B4",
                "pickupNode": "A1",
                "requiredPlace": "B4",
            },
        )
        self.assertIsNone(assigned)
        shipment = db.shipments.docs["shipment_no_path"]
        diagnostic = shipment.get("workflowDiagnostic", {})
        self.assertEqual(diagnostic.get("blockerCode"), "NO_VALID_PATH")

    async def test_assign_stage_device_skips_transport_when_no_path_to_pickup(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_isolated",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "Z9",
                    "speed": 10,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_no_pickup_path",
                    "status": "offloaded",
                    "currentNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_no_pickup_path",
            "transport",
            {
                "shipmentId": "shipment_no_pickup_path",
                "startNode": "A1",
                "finalNode": "B4",
                "pickupNode": "A1",
                "requiredPlace": "A1",
            },
        )
        self.assertIsNone(assigned)
        edge = merged_edge_doc(db, "truck_isolated")
        self.assertEqual(edge.get("taskPhase"), "idle")

    async def test_assign_stage_device_publishes_route_command_on_assign(self):
        mqtt = AsyncMock()
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_tempo_1",
                    "type": "truck_tempo",
                    "taskPhase": "idle",
                    "currentLocation": "C5",
                    "speed": 10,
                }
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_route",
                    "status": "offloaded",
                    "currentNode": "A1",
                    "destination": "B4",
                    "assignedEdges": [],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=mqtt,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

        assigned = await assigner._assign_stage_device(
            "shipment_route",
            "transport",
            {
                "shipmentId": "shipment_route",
                "startNode": "A1",
                "finalNode": "B4",
                "pickupNode": "A1",
                "requiredPlace": "B4",
            },
        )
        self.assertEqual(assigned, "truck_tempo_1")
        topics = [call.args[0] for call in mqtt.publish.await_args_list]
        self.assertTrue(any(t.endswith("/route") for t in topics))
        edge = merged_edge_doc(db, "truck_tempo_1")
        self.assertEqual(edge["task"]["requiredPlace"], "B4")


class RouteHelpersIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_or_publish_route_stashes_pending_path_mid_transit(self):
        import route_helpers

        edge_doc = {
            "id": "truck_1",
            "currentLocation": "B1",
            "progressToNext": 45,
            "activeHop": {"from": "B1", "to": "B2"},
            "routeRevision": 2,
        }
        db = ContractDb(
            edgeDevices=FakeCollection([edge_doc]),
        )
        mqtt = AsyncMock()
        analyzer = FakeAnalyzer()
        new_path = ["B1", "B2", "C2", "D2"]

        ok = await route_helpers.queue_or_publish_route(db, mqtt, "truck_1", new_path, analyzer)
        self.assertTrue(ok)
        mqtt.publish.assert_not_awaited()
        updated = merged_edge_doc(db, "truck_1")
        self.assertEqual(updated.get("pendingPath"), ["B1", "B2", "C2", "D2"])
        self.assertEqual(updated.get("routeRevision"), 3)


class StoreLoadWorkflowFixTests(unittest.IsolatedAsyncioTestCase):
    def test_pending_store_phases_includes_storing_with_incomplete_store_move(self):
        """Monitor bucket must include storing shipments stuck mid-store_move."""
        shipments = [
            {
                "id": "shipment_stuck",
                "status": "storing",
                "assignedEdges": [
                    task_assigner.make_assigned_edge("robot001", "store_move", assigned_at="t"),
                ],
            },
            {
                "id": "shipment_done_move",
                "status": "storing",
                "assignedEdges": [
                    task_assigner.make_assigned_edge(
                        "robot001", "store_move", assigned_at="t", completed_at="done"
                    ),
                ],
            },
            {
                "id": "shipment_transported",
                "status": "transported",
                "assignedEdges": [],
            },
        ]
        pending = task_assigner.sort_shipments_fifo([
            s for s in shipments
            if s.get("status") in ("transported", "transporting", "storing")
            and not task_assigner.phase_complete(s.get("assignedEdges", []), "store_load")
        ])
        ids = {s["id"] for s in pending}
        self.assertIn("shipment_stuck", ids)
        self.assertIn("shipment_done_move", ids)
        self.assertIn("shipment_transported", ids)

    async def test_lost_store_move_assignment_cleared_for_retry(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "robot001",
                    "type": "robot",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_stuck",
                    "status": "storing",
                    "destination": "B4",
                    "currentNode": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("robot001", "store_move", assigned_at="t"),
                    ],
                },
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        shipment = db.shipments.docs["shipment_stuck"]
        edges_by_id = {
            eid: merged_edge_doc(db, eid) for eid in db.edgeRuntime.docs
        }
        shipments_by_id = {s["id"]: s for s in db.shipments.docs.values()}
        changed = await assigner._reconcile_lost_assignments(
            shipment, edges_by_id, shipments_by_id
        )
        self.assertTrue(changed)
        self.assertFalse(
            task_assigner.has_inflight_phase(shipment["assignedEdges"], "store_move")
        )

    def test_sort_shipments_fifo_oldest_first(self):
        shipments = [
            {"id": "shipment_3", "createdAt": "2026-06-12T10:04:00Z"},
            {"id": "shipment_1", "createdAt": "2026-06-12T10:00:00Z"},
            {"id": "shipment_2", "createdAt": "2026-06-12T10:02:00Z"},
        ]
        ordered = task_assigner.sort_shipments_fifo(shipments)
        self.assertEqual([s["id"] for s in ordered], ["shipment_1", "shipment_2", "shipment_3"])

    async def test_store_load_not_blocked_by_storing_backlog(self):
        graph_data = [
            {"id": "E5", "neighbors": {"W": "E4"}, "type": "warehouse", "capacity": 35, "currentOccupancy": 0, "x": 4, "y": 4},
        ]
        storing_docs = []
        for i in range(1, 25):
            storing_docs.append({
                "id": f"shipment_{i}",
                "status": "storing",
                "destination": "E5",
                "currentNode": "E5",
                "createdAt": f"2026-06-12T10:{i:02d}:00Z",
                "assignedEdges": [
                    task_assigner.make_assigned_edge("robot001", "store_move", assigned_at="t", completed_at="done"),
                ],
            })
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "forklift_001",
                    "type": "forklift",
                    "taskPhase": "idle",
                    "currentLocation": "E5",
                    "speed": 5,
                },
            ]),
            shipments=FakeCollection(storing_docs),
            graph=FakeCollection([
                {"id": "E5", "type": "warehouse", "capacity": 35, "currentOccupancy": 0},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=graph_data,
        )

        reserved = await assigner._count_warehouse_reservations("E5")
        self.assertEqual(reserved, 0)

        assigned = await assigner._try_assign_next(
            "shipment_1",
            "store_load",
            db.shipments.docs["shipment_1"],
        )
        self.assertEqual(assigned, "forklift_001")
        self.assertTrue(
            task_assigner.has_assigned_phase(
                db.shipments.docs["shipment_1"]["assignedEdges"], "store_load"
            )
        )

    async def test_store_move_completion_refreshes_diagnostic_for_store_load(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "robot001",
                    "type": "robot",
                    "taskPhase": "completing",
                    "shipmentId": "shipment_24",
                    "assignedShipment": "shipment_24",
                    "currentLocation": "E5",
                    "finalNode": "E5",
                    "task": {"shipmentId": "shipment_24", "phase": "store_move", "finalNode": "E5"},
                },
                {
                    "id": "forklift_001",
                    "type": "forklift",
                    "taskPhase": "idle",
                    "currentLocation": "E5",
                    "speed": 5,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_24",
                    "status": "storing",
                    "destination": "E5",
                    "currentNode": "E5",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge("robot001", "store_move", assigned_at="assigned", completed_at=None),
                    ],
                    "workflowDiagnostic": {
                        "nextPhase": "store_move",
                        "blockerCode": "ASSIGNED",
                        "blockerMessage": "Assigned robot001 for store_move",
                    },
                }
            ]),
            graph=FakeCollection([
                {"id": "E5", "type": "warehouse", "capacity": 35, "currentOccupancy": 0},
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=[{"id": "E5", "neighbors": {"W": "E4"}, "type": "warehouse", "capacity": 35, "currentOccupancy": 0}],
        )

        await assigner.handle_completion(
            "robot001",
            {
                "shipmentId": "shipment_24",
                "phase": "store_move",
                "location": "E5",
                "completedAt": "move-done",
            },
        )

        shipment = db.shipments.docs["shipment_24"]
        diag = shipment.get("workflowDiagnostic") or {}
        self.assertEqual(diag.get("nextPhase"), "store_load")
        self.assertNotIn("store_move", diag.get("blockerMessage", ""))

    async def test_reconcile_does_not_clear_active_store_load_when_stale_entry_exists(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "forklift_001",
                    "type": "forklift",
                    "taskPhase": "assigned",
                    "currentLocation": "B4",
                    "shipmentId": "shipment_4",
                    "assignedShipment": "shipment_4",
                    "task": {"phase": "store_load", "shipmentId": "shipment_4"},
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_26",
                    "status": "storing",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "forklift_001", "store_load", assigned_at="2026-01-01T10:00:00", completed_at=None
                        ),
                    ],
                },
                {
                    "id": "shipment_4",
                    "status": "storing",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "forklift_001", "store_load", assigned_at="2026-06-14T10:19:00", completed_at=None
                        ),
                    ],
                },
            ]),
            graph=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db, mqtt_client=None, analyzer=None, graph=task_assigner.GRAPH_LIST
        )
        edges_by_id = {edge["id"]: edge for edge in db.edgeDevices.docs.values()}
        shipments_by_id = {s["id"]: s for s in db.shipments.docs.values()}

        changed_26 = await assigner._reconcile_stale_assignments(
            db.shipments.docs["shipment_26"], edges_by_id, shipments_by_id
        )
        changed_4 = await assigner._reconcile_stale_assignments(
            db.shipments.docs["shipment_4"], edges_by_id, shipments_by_id
        )

        self.assertFalse(changed_26)
        self.assertFalse(changed_4)
        self.assertIsNone(db.shipments.docs["shipment_4"]["assignedEdges"][0].get("completedAt"))
        self.assertTrue(
            assigner._phase_in_flight(
                db.shipments.docs["shipment_4"], "store_load", edges_by_id, shipments_by_id
            )
        )

    async def test_same_node_store_load_uses_empty_path(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "forklift_001",
                    "type": "forklift",
                    "taskPhase": "idle",
                    "currentLocation": "B4",
                    "shipmentId": None,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_4",
                    "status": "storing",
                    "destination": "B4",
                    "currentNode": "B4",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "robot001", "store_move", assigned_at="t", completed_at="done"
                        ),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        assigned = await assigner._assign_stage_device(
            "shipment_4",
            "store_load",
            {
                "shipmentId": "shipment_4",
                "pickupNode": "B4",
                "finalNode": "B4",
                "requiredPlace": "B4",
                "subPhase": "load",
            },
        )
        self.assertEqual(assigned, "forklift_001")
        edge = merged_edge_doc(db, "forklift_001")
        task = edge.get("task") or {}
        self.assertEqual(task.get("path"), [])

    async def test_same_node_delivery_uses_empty_path(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_delivery_1",
                    "type": "truck_delivery",
                    "taskPhase": "idle",
                    "currentLocation": "E5",
                    "shipmentId": None,
                },
            ]),
            shipments=FakeCollection([
                {
                    "id": "shipment_4",
                    "status": "stored",
                    "destination": "E5",
                    "currentNode": "E5",
                    "assignedEdges": [
                        task_assigner.make_assigned_edge(
                            "forklift_001", "store_load", assigned_at="t", completed_at="done"
                        ),
                    ],
                }
            ]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db,
            mqtt_client=None,
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )
        assigned = await assigner._assign_stage_device(
            "shipment_4",
            "delivery",
            {
                "shipmentId": "shipment_4",
                "startNode": "E5",
                "finalNode": "E5",
                "pickupNode": "E5",
                "requiredPlace": "E5",
                "destNode": "E5",
            },
        )
        self.assertEqual(assigned, "truck_delivery_1")
        edge = merged_edge_doc(db, "truck_delivery_1")
        task = edge.get("task") or {}
        self.assertEqual(task.get("path"), [])
        self.assertEqual(edge.get("pendingPath"), [])

    async def test_reconcile_stuck_same_node_delivery_clears_stale_task_path(self):
        db = ContractDb(
            edgeDevices=FakeCollection([
                {
                    "id": "truck_delivery_1",
                    "type": "truck_delivery",
                    "taskPhase": "en_route_start",
                    "currentLocation": "E5",
                    "startNode": None,
                    "finalNode": None,
                    "path": [],
                    "pendingPath": [],
                    "shipmentId": "shipment_4",
                    "task": {
                        "phase": "delivery",
                        "shipmentId": "shipment_4",
                        "startNode": "E5",
                        "finalNode": "E5",
                        "path": ["E5", "D5", "E5"],
                    },
                },
            ]),
            shipments=FakeCollection([]),
        )
        assigner = task_assigner.TaskAssigner(
            db=db, mqtt_client=None, analyzer=None, graph=task_assigner.GRAPH_LIST
        )
        edges_by_id = {edge["id"]: merged_edge_doc(db, edge["id"]) for edge in db.edgeRuntime.docs.values()}
        changed = await assigner._reconcile_stuck_same_node_delivery(edges_by_id)
        self.assertTrue(changed)
        edge = merged_edge_doc(db, "truck_delivery_1")
        self.assertEqual(edge.get("startNode"), "E5")
        self.assertEqual(edge.get("finalNode"), "E5")
        self.assertEqual(edge.get("task", {}).get("path"), [])


if __name__ == "__main__":
    unittest.main()
