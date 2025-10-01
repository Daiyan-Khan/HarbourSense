from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from sklearn.ensemble import IsolationForest
import numpy as np
from datetime import datetime, timedelta

app = FastAPI()

# MongoDB connection (match your main.py)
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port

# Health check
@app.get("/")
async def read_root():
    return {"message": "Sensor Analyzer is running"}

# Analysis endpoint
@app.get("/analyze_sensors")
async def analyze_sensors(sensor_type: str = 'all', window_mins: int = 30):
    query = {} if sensor_type == 'all' else {'type': sensor_type}
    start_time = datetime.now() - timedelta(minutes=window_mins)
    query['timestamp'] = {'$gt': start_time}

    cursor = db.sensorData.find(query).sort('timestamp', -1)
    data = []
    docs = []
    async for doc in cursor:
        reading = float(doc.get('reading', 0))
        data.append([reading])
        docs.append(doc)

    if not data:
        return {'status': 'no data'}

    model = IsolationForest(contamination=0.1)
    labels = model.fit_predict(np.array(data))

    alerts = []
    for doc, label in zip(docs, labels):
        alert = label < 0
        suggestion = 'repair' if alert and doc.get('type') == 'vibration' else 'monitor'
        
        # Environmental logic: Suggest reroute for high wind/humidity (customize thresholds)
        humidity = float(doc.get('humidity', 0))
        wind = float(doc.get('wind', 0))
        if wind > 20 or humidity > 80:  # E.g., wind in km/h, humidity in %
            suggestion = 'reroute'  # For ships/cranes to avoid risky areas
        
        alerts.append({'id': doc.get('id'), 'alert': alert, 'suggestion': suggestion, 'node': doc.get('node')})

    await db.sensorAlerts.insert_many(alerts)
    return {'status': 'analyzed', 'alerts': len(alerts)}
