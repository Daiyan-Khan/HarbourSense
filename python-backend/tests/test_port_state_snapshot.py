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

    bson_module.ObjectId = ObjectId
    sys.modules.setdefault("bson", bson_module)

    aiomqtt_module = types.ModuleType("aiomqtt")
    aiomqtt_module.Client = object
    aiomqtt_module.MqttError = Exception
    sys.modules.setdefault("aiomqtt", aiomqtt_module)

    pymongo_module = types.ModuleType("pymongo")

    class ReturnDocument:
        AFTER = 1
        BEFORE = 0

    pymongo_module.ReturnDocument = ReturnDocument
    sys.modules.setdefault("pymongo", pymongo_module)

    pymongo_module = types.ModuleType("pymongo")

    class ReturnDocument:
        AFTER = 1
        BEFORE = 0

    pymongo_module.ReturnDocument = ReturnDocument
    sys.modules.setdefault("pymongo", pymongo_module)


install_dependency_stubs()

import port_state


def shipment(shipment_id, status, assigned_edges=None, **extra):
    doc = {
        "id": shipment_id,
        "status": status,
        "assignedEdges": assigned_edges or [],
    }
    doc.update(extra)
    return doc


class ClassifyPendingBacklogTests(unittest.TestCase):
    def test_pending_arrived_without_offload(self):
        shipments = [shipment("s1", "arrived")]
        backlog = port_state.classify_pending_backlog(shipments)
        self.assertEqual(backlog["pending_arrived"], shipments)
        self.assertEqual(backlog["pending_offloaded"], [])

    def test_offloaded_waits_for_transport_assignment(self):
        shipments = [shipment("s2", "offloaded")]
        backlog = port_state.classify_pending_backlog(shipments)
        self.assertEqual(backlog["pending_offloaded"], shipments)

    def test_transported_still_pending_when_store_move_incomplete(self):
        edges = [{"edgeId": "robot1", "phase": "store_move", "completedAt": None}]
        shipments = [shipment("s3", "transported", edges)]
        backlog = port_state.classify_pending_backlog(shipments)
        self.assertEqual(backlog["pending_transported"], shipments)

    def test_transported_not_pending_when_store_move_complete(self):
        edges = [{"edgeId": "robot1", "phase": "store_move", "completedAt": "2026-01-01T00:00:00"}]
        shipments = [shipment("s3b", "transported", edges)]
        backlog = port_state.classify_pending_backlog(shipments)
        self.assertEqual(backlog["pending_transported"], [])

    def test_storing_pending_store_load_after_move(self):
        edges = [{"edgeId": "robot1", "phase": "store_move", "completedAt": "2026-01-01T00:00:00"}]
        shipments = [shipment("s4", "storing", edges)]
        backlog = port_state.classify_pending_backlog(shipments)
        self.assertEqual(backlog["pending_storing"], shipments)
        self.assertEqual(backlog["pending_transported"], [])

    def test_stored_pending_delivery(self):
        shipments = [shipment("s5", "stored")]
        backlog = port_state.classify_pending_backlog(shipments)
        self.assertEqual(backlog["pending_stored"], shipments)

    def test_uses_canonical_status_not_transporting_alias(self):
        """Backlog keys off transported/storing/stored — not legacy transporting."""
        transporting = shipment("legacy", "transporting")
        transported = shipment("canonical", "transported")
        backlog = port_state.classify_pending_backlog([transporting, transported])
        self.assertEqual(backlog["pending_transported"], [transported])
        self.assertNotIn("legacy", [s["id"] for s in backlog["pending_transported"]])


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

    async def to_list(self, length=None):
        return list(self.docs)


class FakeCollection:
    def __init__(self, docs):
        self.docs = list(docs)

    async def count_documents(self, query=None):
        return len(self.docs)

    def find(self, query=None):
        query = query or {}
        if query.get("status", {}).get("$ne") == "delivered":
            return FakeCursor([d for d in self.docs if d.get("status") != "delivered"])
        if query.get("resolved") is False:
            return FakeCursor([d for d in self.docs if not d.get("resolved")])
        return FakeCursor(self.docs)


class FakeDb:
    def __init__(self, shipments, edges, alerts=None, graph=None):
        from edge_view import split_edge_document

        self.shipments = FakeCollection(shipments)
        runtimes, assignments = [], []
        for edge in edges:
            assignment, runtime = split_edge_document(edge)
            runtimes.append(runtime)
            assignments.append(assignment)
        self.edgeRuntime = FakeCollection(runtimes)
        self.edgeAssignments = FakeCollection(assignments)
        self.edgeDevices = FakeCollection(edges)
        self.sensorAlerts = FakeCollection(alerts or [])
        self.graph = FakeCollection(graph if graph is not None else [])

    def __getitem__(self, key):
        return getattr(self, key)


class BuildPortStateSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_pending_counts_match_monitor_buckets(self):
        shipments = [
            shipment("a1", "arrived"),
            shipment("o1", "offloaded"),
            shipment("t1", "transported"),
            shipment("st1", "storing", [{"edgeId": "r1", "phase": "store_move", "completedAt": "x"}]),
            shipment("sd1", "stored"),
            shipment("d1", "delivered"),
        ]
        edges = [
            {"id": "crane1", "type": "crane", "taskPhase": "idle"},
            {"id": "truck1", "type": "truck_tempo", "taskPhase": "assigned", "currentLocation": "A1"},
        ]
        db = FakeDb(shipments, edges)
        snapshot = await port_state.build_port_state_snapshot(db)

        self.assertEqual(snapshot["pending_counts"]["pending_arrived"], 1)
        self.assertEqual(snapshot["pending_counts"]["pending_offloaded"], 1)
        self.assertEqual(snapshot["pending_counts"]["pending_transported"], 1)
        self.assertEqual(snapshot["pending_counts"]["pending_storing"], 1)
        self.assertEqual(snapshot["pending_counts"]["pending_stored"], 1)
        self.assertEqual(snapshot["active_shipments"], 5)
        self.assertEqual(snapshot["edge_state_counts"]["idle"], 1)
        self.assertEqual(snapshot["idle_by_type"]["crane"], 1)

    async def test_snapshot_status_counts_include_transported_not_transporting(self):
        shipments = [
            shipment("s1", "transported"),
            shipment("s2", "transporting"),
        ]
        db = FakeDb(shipments, [])
        snapshot = await port_state.build_port_state_snapshot(db)
        self.assertEqual(snapshot["status_counts"].get("transported"), 1)
        self.assertEqual(snapshot["status_counts"].get("transporting"), 1)
        self.assertEqual(snapshot["pending_counts"]["pending_transported"], 1)

    async def test_snapshot_includes_shipment_and_idle_edge_diagnostics(self):
        shipments = [
            shipment("a1", "arrived", currentNode="A1"),
        ]
        edges = [
            {"id": "crane1", "type": "crane", "taskPhase": "idle", "currentLocation": "A1"},
            {"id": "truck1", "type": "truck_tempo", "taskPhase": "assigned", "currentLocation": "A1"},
        ]
        graph = [
            {"id": "A1", "type": "dock", "capacity": 8, "currentOccupancy": 1},
        ]
        db = FakeDb(shipments, edges, graph=graph)
        snapshot = await port_state.build_port_state_snapshot(db)

        self.assertIn("shipment_diagnostics", snapshot)
        self.assertIn("idle_edge_diagnostics", snapshot)
        self.assertEqual(len(snapshot["shipment_diagnostics"]), 1)
        self.assertEqual(snapshot["shipment_diagnostics"][0]["shipmentId"], "a1")
        self.assertEqual(snapshot["shipment_diagnostics"][0]["nextPhase"], "offload")
        self.assertEqual(len(snapshot["idle_edge_diagnostics"]), 1)
        self.assertEqual(snapshot["idle_edge_diagnostics"][0]["edgeId"], "crane1")
        self.assertIn("generatedAt", snapshot)


if __name__ == "__main__":
    unittest.main()
