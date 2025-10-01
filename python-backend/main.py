# main.py - HarbourSense Python Backend

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

# MongoDB connection (use your URI; store securely in env vars for production)
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port  # 'port' database

# Health check endpoint
@app.get("/")
async def read_root():
    return {"message": "HarbourSense Python Backend is running"}

# Endpoint to pull and return all sensor data
@app.get("/sensors")
async def get_sensors():
    sensor_cursor = db.sensorData.find()
    sensors = []
    async for sensor in sensor_cursor:
        sensor["_id"] = str(sensor["_id"])  # Convert ObjectId to string for JSON
        sensors.append(sensor)
    return sensors

# Endpoint to pull and return all edges data
@app.get("/edges")
async def get_edges():
    edges_cursor = db.edges.find()
    edges = []
    async for edge in edges_cursor:
        edge["_id"] = str(edge["_id"])  # Convert ObjectId to string for JSON
        edges.append(edge)
    return edges

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