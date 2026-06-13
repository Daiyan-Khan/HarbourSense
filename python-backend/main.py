import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
import numpy as np
from bson import ObjectId  # Import this for type checking
import math
from backend_config import MongoConfigError, get_mongo_database, get_mongo_settings


logger = logging.getLogger(__name__)
MONGO_RUNTIME_ERROR_NAMES = {
    "AutoReconnect",
    "ConfigurationError",
    "ConnectionFailure",
    "NetworkTimeout",
    "ServerSelectionTimeoutError",
}
DB_UNAVAILABLE_LOGGED_OPERATIONS = set()
GRAPH_SEED_MISSING_LOGGED = False
GRAPH_SEED_MISSING_MESSAGE = (
    "MongoDB graph collection is empty. Run the seed harness or graph insert script "
    "for the port.graph collection before starting HarbourSense."
)


def is_mongo_runtime_error(exc):
    """Return True for MongoDB connectivity/configuration failures without importing PyMongo in tests."""
    current = exc
    while current is not None:
        if current.__class__.__name__ in MONGO_RUNTIME_ERROR_NAMES:
            return True
        current = getattr(current, "__cause__", None) or getattr(current, "__context__", None)
    return False


def raise_database_unavailable(operation, exc):
    if operation not in DB_UNAVAILABLE_LOGGED_OPERATIONS:
        DB_UNAVAILABLE_LOGGED_OPERATIONS.add(operation)
        logger.warning(
            "MongoDB unavailable during %s. Error type: %s",
            operation,
            exc.__class__.__name__,
        )
    else:
        logger.debug("MongoDB still unavailable during %s.", operation)
    raise HTTPException(
        status_code=503,
        detail={
            "error": "database_unavailable",
            "message": (
                f"MongoDB is unavailable while reading {operation}. Check MONGO_URI, "
                "MONGO_DB_NAME, network reachability, and MONGO_SERVER_SELECTION_TIMEOUT_MS."
            ),
        },
    ) from None


def raise_internal_api_error(operation, exc):
    logger.exception("Unhandled backend error during %s. Error type: %s", operation, exc.__class__.__name__)
    raise HTTPException(
        status_code=500,
        detail={
            "error": "internal_error",
            "message": f"Unable to read {operation}. Check backend logs for the sanitized error type.",
        },
    ) from None


def raise_graph_seed_missing(operation):
    global GRAPH_SEED_MISSING_LOGGED
    if not GRAPH_SEED_MISSING_LOGGED:
        GRAPH_SEED_MISSING_LOGGED = True
        logger.warning("%s while reading %s", GRAPH_SEED_MISSING_MESSAGE, operation)
    raise HTTPException(
        status_code=503,
        detail={
            "error": "graph_seed_missing",
            "message": GRAPH_SEED_MISSING_MESSAGE,
        },
    ) from None


def fix_mongo_ids(document):
    """Recursively convert ObjectId to str in MongoDB documents."""
    if isinstance(document, list):
        return [fix_mongo_ids(doc) for doc in document]
    elif isinstance(document, dict):
        new_doc = {}
        for key, value in document.items():
            if key == "_id" and isinstance(value, ObjectId):
                new_doc[key] = str(value)
            else:
                new_doc[key] = fix_mongo_ids(value) if isinstance(value, (dict, list)) else value
        return new_doc
    else:
        return document
def sanitize_for_json(data):
    """Recursively convert NaN to None."""
    if isinstance(data, list):
        return [sanitize_for_json(item) for item in data]
    elif isinstance(data, dict):
        return {key: sanitize_for_json(value) for key, value in data.items()}
    elif isinstance(data, float) and math.isnan(data):
        return None # Convert NaN to null (None in Python)
    else:
        return data
app = FastAPI()

# CORS setup to allow frontend requests
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "*"  # Optional for testing; remove in production
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection follows the Compose/.env contract.
try:
    mongo_settings = get_mongo_settings()
    db = get_mongo_database(mongo_settings)
except MongoConfigError as exc:
    raise RuntimeError(f"Invalid MongoDB configuration for FastAPI startup: {exc}") from None
# Health check endpoint
@app.get("/")
async def read_root():
    return {"message": "HarbourSense Python Backend is running"}


@app.get("/health/live")
async def health_live():
    return {"status": "alive"}


@app.get("/health/ready")
async def health_ready():
    operation = "readiness"
    try:
        await db.command("ping")
        graph_count = await db.graph.count_documents({})
        if graph_count == 0:
            raise_graph_seed_missing(operation)
        return {
            "status": "ready",
            "database": "reachable",
            "graphSeeded": True,
            "graphNodes": graph_count,
        }
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)

@app.get('/analyze_sensors')
async def analyze_sensors(sensor_type: str = 'all', window_mins: int = 30):
    operation = "sensor analysis"
    try:
        query = {} if sensor_type == 'all' else {'type': sensor_type}
        start_time = datetime.now() - timedelta(minutes=window_mins)
        query['timestamp'] = {'$gt': start_time}

        cursor = db.sensorData.find(query).sort('timestamp', -1)
        data = []
        docs = []
        async for doc in cursor:
            reading = float(doc.get('reading', 0))  # Assume numeric for simplicity
            data.append([reading])
            docs.append(doc)

        if not data:
            return {'status': 'no data'}

        # Simple anomaly detection
        model = IsolationForest(contamination=0.1)
        labels = model.fit_predict(np.array(data))

        alerts = []
        for doc, label in zip(docs, labels):
            alert = label < 0  # Anomaly
            suggestion = 'repair' if alert and doc['type'] == 'vibration' else 'monitor'
            alerts.append({'id': doc['id'], 'alert': alert, 'suggestion': suggestion, 'node': doc['node']})

        await db.sensorAlerts.insert_many(alerts)  # Store for decision maker
        return {'status': 'analyzed', 'alerts': len(alerts)}
    except Exception as exc:
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)

@app.get("/api/edges")
async def get_edges():
    operation = "edge devices"
    try:
        # 1. Fetch the raw data from MongoDB
        edges = [doc async for doc in db.edgeDevices.find()]
        
        # 2. Fix the MongoDB ObjectIDs
        fixed_edges = fix_mongo_ids(edges)
        
        # 3. Sanitize the data to remove NaN values
        sanitized_edges = sanitize_for_json(fixed_edges)
        
        # 4. Return the clean, JSON-compliant data
        return sanitized_edges

    except Exception as exc:
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)

@app.get("/api/sensors")
async def get_sensors():
    operation = "sensor readings"
    try:
        sensors = [doc async for doc in db.sensorData.find().sort("timestamp", -1).limit(100)]  # Latest sensors
        return fix_mongo_ids(sensors)  # Returns list with id, node, reading, type, etc.
    except Exception as exc:
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)


@app.get("/api/shipments")
async def get_shipments(limit: int = 50):
    operation = "shipments"
    bounded_limit = max(1, min(limit, 200))
    try:
        cursor = db.shipments.find().sort("updatedAt", -1).limit(bounded_limit)
        shipments = [doc async for doc in cursor]
        return fix_mongo_ids(shipments)
    except Exception as exc:
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)


@app.get("/api/alerts/sensor")
async def get_sensor_alerts(limit: int = 50, unresolved_only: bool = True):
    operation = "sensor alerts"
    bounded_limit = max(1, min(limit, 200))
    query = {"resolved": False} if unresolved_only else {}
    try:
        cursor = db.sensorAlerts.find(query).sort("timestamp", -1).limit(bounded_limit)
        alerts = [doc async for doc in cursor]
        return fix_mongo_ids(alerts)
    except Exception as exc:
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)


@app.get("/api/alerts/maintenance")
async def get_maintenance_alerts(limit: int = 50, unresolved_only: bool = True):
    operation = "maintenance alerts"
    bounded_limit = max(1, min(limit, 200))
    query = {"resolved": False} if unresolved_only else {}
    try:
        cursor = db.maintenanceAlerts.find(query).sort("timestamp", -1).limit(bounded_limit)
        alerts = [doc async for doc in cursor]
        return fix_mongo_ids(alerts)
    except Exception as exc:
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)

@app.get("/api/graph")
async def get_graph():
    operation = "graph nodes"
    try:
        cursor = db.graph.find()  # Fetch all node documents
        all_nodes = {}
        async for doc in cursor:
            node_id = doc.get('id')
            if node_id:
                doc.pop('_id', None)  # Remove internal Mongo field for clean JSON
                all_nodes[node_id] = doc
        if not all_nodes:
            raise_graph_seed_missing(operation)
        return {"nodes": all_nodes}
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        if is_mongo_runtime_error(exc):
            raise_database_unavailable(operation, exc)
        raise_internal_api_error(operation, exc)

