"""
Bounded manager MQTT→Mongo integration tests.

Exercises manager.handle_completion (the MQTT completion entry point) with an
in-memory Mongo test double and a real TaskAssigner. No live broker required.
"""

import importlib
import json
import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "mqtt"
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

import task_assigner

manager = importlib.import_module("manager")


def load_fixture(name):
    with open(FIXTURES_DIR / name, encoding="utf-8") as handle:
        return json.load(handle)


def parse_completion_device_id(topic):
    """Mirror mqtt_handler routing for canonical completion topics."""
    if topic.startswith("harboursense/edge/") and topic.endswith("/completion"):
        return topic.split("/")[2]
    return None


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


class FakeAnalyzer:
    def __init__(self):
        self.analysis_triggers = []

    def get_predicted_loads(self):
        return {}

    def get_current_loads(self):
        return {}

    def get_route_congestion(self):
        return {}

    async def analyze_metrics(self, triggered_by=""):
        self.analysis_triggers.append(triggered_by)


class FakeMqttClient:
    def __init__(self):
        self.published = []

    async def publish(self, topic, payload):
        self.published.append((topic, payload))


def make_edge(device_id, device_type, phase, shipment_id, location):
    return {
        "id": device_id,
        "type": device_type,
        "taskPhase": "completing",
        "shipmentId": shipment_id,
        "assignedShipment": shipment_id,
        "currentLocation": location,
        "finalNode": location,
        "task": {"shipmentId": shipment_id, "phase": phase, "finalNode": location},
    }


def make_shipment(shipment_id, status, phase, edge_id, current_node="C3"):
    return {
        "id": shipment_id,
        "status": status,
        "currentNode": current_node,
        "assignedEdges": [
            task_assigner.make_assigned_edge(edge_id, phase, assigned_at="assigned", completed_at=None)
        ],
    }


async def route_completion_message(db, task_assigner, topic, raw_payload, mqtt_client=None):
    """Simulate mqtt_handler completion branch without a live broker."""
    device_id = parse_completion_device_id(topic)
    if device_id is None:
        raise ValueError(f"Unsupported completion topic: {topic}")
    task_payload = json.loads(raw_payload.decode("utf-8"))
    await manager.handle_completion(db, task_assigner, device_id, task_payload, mqtt_client)


class CompletionTopicRoutingTests(unittest.TestCase):
    def test_parse_completion_device_id_matches_canonical_topic(self):
        topic = "harboursense/edge/crane001/completion"
        self.assertEqual(parse_completion_device_id(topic), "crane001")

    def test_parse_completion_device_id_rejects_deprecated_topic(self):
        topic = "harboursense/edge/completion/crane001"
        self.assertIsNone(parse_completion_device_id(topic))


class ManagerCompletionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def make_assigner(self, db):
        return task_assigner.TaskAssigner(
            db=db,
            mqtt_client=FakeMqttClient(),
            analyzer=FakeAnalyzer(),
            graph=task_assigner.GRAPH_LIST,
        )

    async def test_offload_fixture_completion_mutates_shipment_and_edge(self):
        fixture = load_fixture("completion-offload.json")
        shipment_id = fixture["shipmentId"]
        device_id = fixture["id"]
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([make_edge(device_id, "crane", "offload", shipment_id, fixture["location"])]),
            shipments=FakeCollection([make_shipment(shipment_id, "arrived", "offload", device_id)]),
            graph=FakeCollection([]),
        )
        assigner = self.make_assigner(db)
        topic = f"harboursense/edge/{device_id}/completion"

        await route_completion_message(db, assigner, topic, json.dumps(fixture).encode("utf-8"))

        shipment = db.shipments.docs[shipment_id]
        edge = db.edgeDevices.docs[device_id]
        self.assertEqual(shipment["status"], "offloaded")
        self.assertEqual(shipment["currentNode"], fixture["location"])
        self.assertEqual(shipment["assignedEdges"][0]["completedAt"], fixture["completedAt"])
        self.assertEqual(edge["taskPhase"], "idle")
        self.assertIsNone(edge["shipmentId"])
        self.assertEqual(edge["currentLocation"], fixture["location"])

    async def test_transport_fixture_completion_advances_shipment(self):
        fixture = load_fixture("completion-transport.json")
        shipment_id = fixture["shipmentId"]
        device_id = fixture["id"]
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([make_edge(device_id, "truck_tempo", "transport", shipment_id, fixture["location"])]),
            shipments=FakeCollection([make_shipment(shipment_id, "offloaded", "transport", device_id, fixture["location"])]),
            graph=FakeCollection([]),
        )
        assigner = self.make_assigner(db)
        topic = f"harboursense/edge/{device_id}/completion"

        await route_completion_message(db, assigner, topic, json.dumps(fixture).encode("utf-8"))

        shipment = db.shipments.docs[shipment_id]
        edge = db.edgeDevices.docs[device_id]
        self.assertEqual(shipment["status"], "transported")
        self.assertEqual(edge["taskPhase"], "idle")

    async def test_store_load_fixture_completion_updates_warehouse_occupancy(self):
        fixture = load_fixture("completion-store-load.json")
        shipment_id = fixture["shipmentId"]
        device_id = fixture["id"]
        warehouse = fixture["location"]
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([make_edge(device_id, "forklift", "store_load", shipment_id, warehouse)]),
            shipments=FakeCollection([make_shipment(shipment_id, "storing", "store_load", device_id, warehouse)]),
            graph=FakeCollection([{"id": warehouse, "type": "warehouse", "currentOccupancy": 0}]),
        )
        assigner = self.make_assigner(db)
        topic = f"harboursense/edge/{device_id}/completion"

        await route_completion_message(db, assigner, topic, json.dumps(fixture).encode("utf-8"))

        shipment = db.shipments.docs[shipment_id]
        warehouse_doc = db.graph.docs[warehouse]
        self.assertEqual(shipment["status"], "stored")
        self.assertEqual(shipment["deliveryStatus"], "pending")
        self.assertEqual(shipment["storageCompleteAt"], fixture["completedAt"])
        self.assertEqual(warehouse_doc["currentOccupancy"], 1)

    async def test_delivery_fixture_completion_sets_delivered_status(self):
        fixture = load_fixture("completion-delivery.json")
        shipment_id = fixture["shipmentId"]
        device_id = fixture["id"]
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([make_edge(device_id, "truck_delivery", "delivery", shipment_id, fixture["location"])]),
            shipments=FakeCollection([make_shipment(shipment_id, "stored", "delivery", device_id, fixture["location"])]),
            graph=FakeCollection([]),
        )
        assigner = self.make_assigner(db)
        topic = f"harboursense/edge/{device_id}/completion"

        await route_completion_message(db, assigner, topic, json.dumps(fixture).encode("utf-8"))

        shipment = db.shipments.docs[shipment_id]
        self.assertEqual(shipment["status"], "delivered")
        self.assertEqual(shipment["deliveryStatus"], "completed")
        self.assertEqual(shipment["deliveryCompleteAt"], fixture["completedAt"])

    async def test_duplicate_offload_fixture_via_manager_is_idempotent(self):
        fixture = load_fixture("completion-offload.json")
        shipment_id = fixture["shipmentId"]
        device_id = fixture["id"]
        db = types.SimpleNamespace(
            edgeDevices=FakeCollection([make_edge(device_id, "crane", "offload", shipment_id, fixture["location"])]),
            shipments=FakeCollection([make_shipment(shipment_id, "arrived", "offload", device_id)]),
            graph=FakeCollection([]),
        )
        assigner = self.make_assigner(db)
        topic = f"harboursense/edge/{device_id}/completion"
        payload = json.dumps(fixture).encode("utf-8")

        await route_completion_message(db, assigner, topic, payload)
        await route_completion_message(db, assigner, topic, payload)

        shipment = db.shipments.docs[shipment_id]
        self.assertEqual(shipment["status"], "offloaded")
        self.assertEqual(len(shipment["assignedEdges"]), 1)
        self.assertEqual(shipment["assignedEdges"][0]["completedAt"], fixture["completedAt"])


if __name__ == "__main__":
    unittest.main()
