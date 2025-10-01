from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

app = FastAPI()

# MongoDB connection
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port

@app.get("/assign_tasks")
async def assign_tasks():
    # Fetch data
    edges = [doc async for doc in db.edges.find()]
    alerts = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    traffic_doc = await db.trafficData.find_one(sort=[('timestamp', -1)])
    traffic_congestion = traffic_doc.get('routeCongestion', {}) if traffic_doc else {}

    assignments = []
    for edge in edges:
        task = edge.get('task', 'idle')
        alert = next((a for a in alerts if a['node'] == edge['currentLocation']), None)

        if alert and edge.get('priority', 0) > 5:
            task = alert.get('suggestion', 'monitor')  # e.g., repair, reroute
        else:
            route_key = f"{edge['currentLocation']}-{edge.get('nextNode', '')}"
            congestion = traffic_congestion.get(route_key, {'level': 'low'})
            if congestion['level'] == 'high' and edge.get('type') != 'crane':
                task = 'reroute'

        assignments.append({'id': edge['id'], 'task': task})

    # Store assignments
    await db.tasks.insert_one({'timestamp': datetime.utcnow(), 'assignments': assignments})
    return {'status': 'assigned', 'assignments': assignments}
