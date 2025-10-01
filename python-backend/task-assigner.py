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
    # Fetch data including graph
    edges = [doc async for doc in db.edges.find()]
    alerts = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    traffic_doc = await db.trafficData.find_one(sort=[('timestamp', -1)])
    graph_doc = await db.graph.find_one()  # Assumes graph stored here

    traffic_congestion = traffic_doc.get('routeCongestion', {}) if traffic_doc else {}
    graph_nodes = graph_doc.get('nodes', {}) if graph_doc else {}  # e.g., {"A1": {"neighbors": {"E": "A2", "S": "B1"}, "type": "entry_gate", ...}}

    assignments = []

    # Placeholder: Fetch or simulate shipments waiting at nodes (e.g., from a shipments collection)
    # For example: shipments_at_nodes = {node: True if shipment waiting else False}
    shipments_at_nodes = {}  # TODO: Populate, e.g., await db.shipments.find() and map to nodes

    for edge in edges:
        task = edge.get('task', 'idle')
        current_location = edge['currentLocation']
        alert = next((a for a in alerts if a['node'] == current_location), None)

        # Priority 1: Handle alerts for high-priority edges
        if alert and edge.get('priority', 0) > 5:
            task = alert.get('suggestion', 'monitor')  # e.g., repair, reroute

        # Priority 2: Coordinate with other edges/shipments (e.g., assign idle truck to transport)
        elif edge.get('type') == 'truck' and task == 'idle' and shipments_at_nodes.get(current_location, False):
            # Get valid neighbors from graph
            neighbors = graph_nodes.get(current_location, {}).get('neighbors', {})
            if neighbors:
                # Prefer warehouse if available among neighbors
                warehouse_node = next((n for n in neighbors.values() if graph_nodes.get(n, {}).get('type') == 'warehouse'), None)
                next_node = warehouse_node or next(iter(neighbors.values()))  # Fallback to first valid neighbor
                task = 'transport'
                # Update edge's nextNode to valid one
                await db.edges.update_one({'id': edge['id']}, {'$set': {'nextNode': next_node}})

        # Priority 3: Handle congestion-based reroute
        else:
            route_key = f"{current_location}-{edge.get('nextNode', '')}"
            congestion = traffic_congestion.get(route_key, {'level': 'low'})
            if congestion['level'] == 'high' and edge.get('type') != 'crane':
                task = 'reroute'
                # Optional: Reroute to a low-congestion valid neighbor
                neighbors = graph_nodes.get(current_location, {}).get('neighbors', {})
                low_congestion_node = next((n for n in neighbors.values() if traffic_congestion.get(f"{current_location}-{n}", {}).get('level') == 'low'), None)
                if low_congestion_node:
                    await db.edges.update_one({'id': edge['id']}, {'$set': {'nextNode': low_congestion_node}})

        assignments.append({'id': edge['id'], 'task': task})

    # Store assignments
    await db.tasks.insert_one({'timestamp': datetime.utcnow(), 'assignments': assignments})
    return {'status': 'assigned', 'assignments': assignments}
