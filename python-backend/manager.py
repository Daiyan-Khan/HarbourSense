from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

app = FastAPI()

# MongoDB connection
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port

# Priority mapping (customize as needed; higher number = higher priority)
priority_map = {
    'repair': 10,
    'reroute': 7,
    'transport': 5,
    'monitor': 3,
    'idle': 1
    # Add more tasks here, e.g., 'offload': 6
}

@app.get("/manage_edges")
async def manage_edges():
    # Fetch data
    edges = [doc async for doc in db.edges.find()]
    alerts = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    traffic_doc = await db.trafficData.find_one(sort=[('timestamp', -1)])
    tasks_doc = await db.tasks.find_one(sort=[('timestamp', -1)])
    traffic_congestion = traffic_doc.get('routeCongestion', {}) if traffic_doc else {}
    tasks = tasks_doc.get('assignments', []) if tasks_doc else []

    commands = []
    for edge in edges:
        current_task = edge.get('task', 'idle')
        alert = next((a for a in alerts if a['node'] == edge['currentLocation']), None)
        route_key = f"{edge['currentLocation']}-{edge.get('nextNode', '')}"
        congestion = traffic_congestion.get(route_key, {'level': 'low'})
        assigned_task = next((t for t in tasks if t['id'] == edge['id']), {'task': 'idle'})

        # Decision logic: Prioritize alerts, then congestion, then assigned tasks
        if alert and edge.get('priority', 0) > 5:
            new_task = alert.get('suggestion', 'repair')
        elif congestion['level'] == 'high':
            new_task = 'reroute'
        else:
            new_task = assigned_task['task']

        # Update priority based on new task
        new_priority = priority_map.get(new_task, 1)  # Default to 1 if task not in map

        # Check idle/active status
        status = 'active' if new_task != 'idle' else 'idle'

        commands.append({
            'id': edge['id'],
            'status': status,
            'current_task': current_task,
            'new_task': new_task,
            'new_priority': new_priority,  # Added for visibility
            'location': edge['currentLocation'],
            'next_node': edge.get('nextNode'),
            'congestion_level': congestion['level']
        })

        # Update edge in DB with task and priority
        await db.edges.update_one({'id': edge['id']}, {'$set': {'task': new_task, 'priority': new_priority}})

    return {'status': 'managed', 'commands': commands}
