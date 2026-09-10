import importlib
import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


def install_main_dependency_stubs():
    fastapi_module = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def __init__(self, *args, **kwargs):
            pass

        def add_middleware(self, *args, **kwargs):
            return None

        def get(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    FastAPI.post = FastAPI.get
    response_module = types.ModuleType('fastapi.responses')
    response_module.StreamingResponse = lambda *args, **kwargs: None
    sys.modules.setdefault('fastapi.responses', response_module)
    fastapi_module.FastAPI = FastAPI
    fastapi_module.HTTPException = HTTPException
    middleware_module = types.ModuleType("fastapi.middleware")
    cors_module = types.ModuleType("fastapi.middleware.cors")
    cors_module.CORSMiddleware = object
    middleware_module.cors = cors_module
    sys.modules.setdefault("fastapi", fastapi_module)
    sys.modules.setdefault("fastapi.middleware", middleware_module)
    sys.modules.setdefault("fastapi.middleware.cors", cors_module)

    sklearn_module = types.ModuleType("sklearn")
    ensemble_module = types.ModuleType("sklearn.ensemble")

    class IsolationForest:
        def __init__(self, *args, **kwargs):
            pass

        def fit_predict(self, data):
            return [1 for _ in data]

    ensemble_module.IsolationForest = IsolationForest
    sklearn_module.ensemble = ensemble_module
    sys.modules.setdefault("sklearn", sklearn_module)
    sys.modules.setdefault("sklearn.ensemble", ensemble_module)

    numpy_module = types.ModuleType("numpy")
    numpy_module.array = lambda data: data
    sys.modules.setdefault("numpy", numpy_module)

    bson_module = types.ModuleType("bson")

    class ObjectId:
        def __init__(self, value="object-id"):
            self.value = value

        def __str__(self):
            return self.value

    bson_module.ObjectId = ObjectId
    sys.modules.setdefault("bson", bson_module)

    motor_module = types.ModuleType("motor")
    motor_asyncio_module = types.ModuleType("motor.motor_asyncio")

    class AsyncIOMotorClient:
        def __init__(self, *args, **kwargs):
            pass

        def __getitem__(self, name):
            return object()

    motor_asyncio_module.AsyncIOMotorClient = AsyncIOMotorClient
    motor_module.motor_asyncio = motor_asyncio_module
    sys.modules.setdefault("motor", motor_module)
    sys.modules.setdefault("motor.motor_asyncio", motor_asyncio_module)


install_main_dependency_stubs()
main = importlib.import_module("main")


class AsyncListCursor:
    def __init__(self, docs):
        self.docs = list(docs)
        self.index = 0

    def sort(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    async def to_list(self, length=None):
        return list(self.docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.docs):
            raise StopAsyncIteration
        doc = self.docs[self.index]
        self.index += 1
        return doc

    async def to_list(self, length=None):
        return list(self.docs)


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    async def count_documents(self, query=None):
        return len(self.docs)

    def find(self, query=None):
        query = query or {}
        filtered = list(self.docs)
        status_filter = query.get("status")
        if isinstance(status_filter, dict) and "$ne" in status_filter:
            filtered = [doc for doc in filtered if doc.get("status") != status_filter["$ne"]]
        if query.get("resolved") is False:
            filtered = [doc for doc in filtered if not doc.get("resolved")]
        return AsyncListCursor(filtered)


class ReadApiDb:
    def __init__(self):
        from edge_view import split_edge_document

        edge_doc = {"id": "crane1", "type": "crane", "taskPhase": "idle", "currentLocation": "A1"}
        assignment, runtime = split_edge_document(edge_doc)
        self.shipments = FakeCollection([
            {
                "id": "shipment_1",
                "status": "transported",
                "currentNode": "B4",
                "updatedAt": "2026-06-12T10:00:00Z",
            }
        ])
        self.edgeRuntime = FakeCollection([runtime])
        self.edgeAssignments = FakeCollection([assignment])
        self.edgeDevices = FakeCollection([edge_doc])
        self.sensorAlerts = FakeCollection([
            {
                "id": "alert_1",
                "node": "A1",
                "type": "temperature",
                "severity": "high",
                "resolved": False,
                "timestamp": "2026-06-12T10:05:00Z",
            }
        ])
        self.maintenanceAlerts = FakeCollection([
            {
                "assetId": "crane001",
                "alertType": "PREDICTIVE_MAINTENANCE_REQUIRED",
                "resolved": False,
                "timestamp": "2026-06-12T10:06:00Z",
            }
        ])
        self.graph = FakeCollection([
            {"id": "A1", "type": "dock", "capacity": 8, "currentOccupancy": 0},
        ])

    def __getitem__(self, key):
        return getattr(self, key)


class ReadApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_shipments_returns_sanitized_documents(self):
        original_db = main.db
        main.db = ReadApiDb()
        try:
            response = await main.get_shipments()
        finally:
            main.db = original_db

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["id"], "shipment_1")
        self.assertEqual(response[0]["status"], "transported")

    async def test_get_sensor_alerts_filters_unresolved(self):
        original_db = main.db
        main.db = ReadApiDb()
        try:
            response = await main.get_sensor_alerts()
        finally:
            main.db = original_db

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["node"], "A1")

    async def test_get_maintenance_alerts_returns_records(self):
        original_db = main.db
        main.db = ReadApiDb()
        try:
            response = await main.get_maintenance_alerts()
        finally:
            main.db = original_db

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["assetId"], "crane001")

    async def test_get_port_state_returns_diagnostic_snapshot(self):
        original_db = main.db
        main.db = ReadApiDb()
        try:
            response = await main.get_port_state()
        finally:
            main.db = original_db

        self.assertIn("shipment_diagnostics", response)
        self.assertIn("idle_edge_diagnostics", response)
        self.assertIn("pending_counts", response)
        self.assertEqual(response["active_shipments"], 1)


if __name__ == "__main__":
    unittest.main()
