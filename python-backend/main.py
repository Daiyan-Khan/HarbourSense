from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
import numpy as np
from bson import ObjectId  # Import this for type checking

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

# MongoDB connection (rest of your code remains the same)
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port
# Health check endpoint
@app.get("/")
async def read_root():
    return {"message": "HarbourSense Python Backend is running"}

@app.get('/analyze_sensors')
async def analyze_sensors(sensor_type: str = 'all', window_mins: int = 30):
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

@app.get("/api/edges")
async def get_edges():
    edges = [doc async for doc in db.edgeDevices.find()]
    return fix_mongo_ids(edges)  # Returns list of edges with id, currentLocation, task, speed, nextNode, etc.

@app.get("/api/sensors")
async def get_sensors():
    sensors = [doc async for doc in db.sensorData.find().sort("timestamp", -1).limit(100)]  # Latest sensors
    return fix_mongo_ids(sensors)  # Returns list with id, node, reading, type, etc.

@app.get("/api/graph")
async def get_graph():
    cursor = db.graph.find()  # Fetch all node documents
    all_nodes = {}
    async for doc in cursor:
        node_id = doc.get('id')
        if node_id:
            doc.pop('_id', None)  # Remove internal Mongo field for clean JSON
            all_nodes[node_id] = doc
    return {"nodes": all_nodes}

