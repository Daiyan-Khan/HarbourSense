import importlib
import sys
import types
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))


def install_main_dependency_stubs():
    """Keep API error tests independent from optional runtime packages."""
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


try:
    import pymongo  # Load its real BSON dependencies before optional-package stubs.
except ImportError:
    pass

install_main_dependency_stubs()
main = importlib.import_module("main")


class ServerSelectionTimeoutError(Exception):
    pass


class FailingAsyncCursor:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise ServerSelectionTimeoutError(
            "host.docker.internal:27017: [Errno 101] Network is unreachable, Timeout: 5.0s"
        )


class FailingCollection:
    async def count_documents(self, query=None):
        raise ServerSelectionTimeoutError('database unavailable')

    def find(self, *args, **kwargs):
        return FailingAsyncCursor()


class EmptyAsyncCursor:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class EmptyCollection:
    def find(self, *args, **kwargs):
        return EmptyAsyncCursor()


class FailingDb:
    def __getitem__(self, key):
        return FailingCollection()

    def __init__(self):
        self.edgeDevices = FailingCollection()


class EmptyGraphDb:
    def __init__(self):
        self.graph = EmptyCollection()


class ReadyGraphCollection:
    async def count_documents(self, query):
        return 80


class ReadyDb:
    def __init__(self):
        self.graph = ReadyGraphCollection()

    async def command(self, name):
        return {"ok": 1}


class ApiDatabaseErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_health_reports_seeded_database(self):
        original_db = main.db
        main.db = ReadyDb()
        try:
            response = await main.health_ready()
        finally:
            main.db = original_db

        self.assertEqual(response["status"], "ready")
        self.assertEqual(response["database"], "reachable")
        self.assertTrue(response["graphSeeded"])
        self.assertEqual(response["graphNodes"], 80)

    async def test_edges_returns_sanitized_database_unavailable_error(self):
        original_db = main.db
        original_logged_operations = set(main.DB_UNAVAILABLE_LOGGED_OPERATIONS)
        main.db = FailingDb()
        main.DB_UNAVAILABLE_LOGGED_OPERATIONS.add("edge devices")
        try:
            with self.assertRaises(main.HTTPException) as context:
                await main.get_edges()
        finally:
            main.db = original_db
            main.DB_UNAVAILABLE_LOGGED_OPERATIONS = original_logged_operations

        self.assertEqual(context.exception.status_code, 503)
        self.assertEqual(context.exception.detail["error"], "database_unavailable")
        self.assertIn("MONGO_URI", context.exception.detail["message"])
        self.assertIn("network reachability", context.exception.detail["message"])
        self.assertNotIn("host.docker.internal", str(context.exception.detail))
        self.assertNotIn("Errno 101", str(context.exception.detail))

    async def test_graph_empty_collection_returns_seed_missing_error(self):
        original_db = main.db
        original_graph_logged = main.GRAPH_SEED_MISSING_LOGGED
        main.db = EmptyGraphDb()
        main.GRAPH_SEED_MISSING_LOGGED = True
        try:
            with self.assertRaises(main.HTTPException) as context:
                await main.get_graph()
        finally:
            main.db = original_db
            main.GRAPH_SEED_MISSING_LOGGED = original_graph_logged

        self.assertEqual(context.exception.status_code, 503)
        self.assertEqual(context.exception.detail["error"], "graph_seed_missing")
        self.assertIn("graph collection is empty", context.exception.detail["message"])
        self.assertIn("seed", context.exception.detail["message"])


if __name__ == "__main__":
    unittest.main()
