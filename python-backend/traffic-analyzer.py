from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

app = FastAPI()

# MongoDB connection
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port

@app.get("/analyze_traffic")
async def analyze_traffic():
    # Fetch graph and capacities from MongoDB
    graph_doc = await db.graph.find_one()
    route_caps_doc = await db.routeCapacities.find_one()

    graph = graph_doc.get('nodes', {}) if graph_doc else {}
    route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}

    # Fetch edges
    edges = [doc async for doc in db.edges.find()]

    # Calculate loads
    route_load = {}
    node_traffic = {}
    for edge in edges:
        route_key = f"{edge['currentLocation']}-{edge.get('nextNode', '')}"
        route_load[route_key] = route_load.get(route_key, 0) + 1
        node_traffic[edge['currentLocation']] = node_traffic.get(edge['currentLocation'], 0) + 1

    # Dynamic congestion per route (ratio and level)
    route_congestion = {}
    for route_key, load in route_load.items():
        capacity = route_capacities.get(route_key, 3)  # Default 3
        ratio = load / capacity if capacity > 0 else 0
        if ratio < 0.5:
            level = 'low'
        elif ratio < 0.8:
            level = 'medium'
        else:
            level = 'high'
        route_congestion[route_key] = {'ratio': ratio, 'level': level}

    # Node congestion (kept simple, but can be dynamic too if nodes have capacities)
    node_congestion = {node: 'high' if load > 5 else 'low' for node, load in node_traffic.items()}

    # Suggestions with updates
    suggestions = []
    for edge in edges:
        current_node = edge['currentLocation']
        neighbors = graph.get(current_node, {}).get('neighbors', {})  # {neighbor: dist}

        best_node = None
        best_eta = float('inf')
        for neighbor, dist in neighbors.items():
            route_key = f"{current_node}-{neighbor}"
            congestion = route_congestion.get(route_key, {'ratio': 0, 'level': 'low'})
            if congestion['ratio'] >= 1:  # Avoid fully congested
                continue

            speed = edge.get('speed', 10)
            eta = dist / speed

            if eta < best_eta:
                best_eta = eta
                best_node = neighbor

        # Fallback
        if not best_node and neighbors:
            best_node = next(iter(neighbors))
            best_eta = neighbors[best_node] / edge.get('speed', 10)

        # Update edge in DB
        await db.edges.update_one(
            {'id': edge['id']},
            {'$set': {'nextNode': best_node, 'eta': best_eta}}
        )

        suggestions.append({
            'edgeId': edge['id'],
            'currentLocation': current_node,
            'nextNode': best_node,
            'eta': best_eta,
            'congestionRatio': route_congestion.get(f"{current_node}-{best_node}", {}).get('ratio', 0),
            'congestionLevel': route_congestion.get(f"{current_node}-{best_node}", {}).get('level', 'low')
        })

    # Store results
    await db.trafficData.insert_one({
        'timestamp': datetime.utcnow(),
        'routeLoad': route_load,
        'nodeTraffic': node_traffic,
        'routeCongestion': route_congestion,
        'nodeCongestion': node_congestion,
        'suggestions': suggestions
    })

    return {'status': 'analyzed', 'suggestions': suggestions}
