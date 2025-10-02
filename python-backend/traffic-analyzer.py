from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import heapq
from math import inf

app = FastAPI()

# MongoDB connection
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port

# Route Planner Integration (simplified class for dynamic path computation)
class RoutePlanner:
    def __init__(self, graph):
        self.graph = graph

    def compute_path(self, start, end, node_loads, route_congestion, capacity_threshold=0.8, predicted_loads={}):
        distances = {node: inf for node in self.graph['nodes']}
        distances[start] = 0
        previous = {node: None for node in self.graph['nodes']}
        pq = [(0, start)]

        while pq:
            dist, current = heapq.heappop(pq)
            if dist > distances[current]:
                continue
            neighbors = self.graph['nodes'].get(current, {}).get('neighbors', {})
            for neighbor, weight in neighbors.items():
                # Adjust weight based on congestion, node load, and predicted future load (from edge ETAs)
                route_key = f"{current}-{neighbor}"
                congestion_ratio = route_congestion.get(route_key, {'ratio': 0})['ratio']
                current_load = node_loads.get(neighbor, 0)
                predicted_load = predicted_loads.get(neighbor, 0)  # Predicted arrivals
                load_factor = (current_load + predicted_load) / capacity_threshold
                adjusted_weight = weight * (1 + congestion_ratio + load_factor)  # Penalize potential congestion
                alt = dist + adjusted_weight
                if alt < distances[neighbor]:
                    distances[neighbor] = alt
                    previous[neighbor] = current
                    heapq.heappush(pq, (alt, neighbor))

        # Reconstruct path (shortest time/distance with congestion prevention)
        path = []
        current = end
        while current is not None:
            path.append(current)
            current = previous[current]
        path.reverse()
        return path if path and path[0] == start else None

@app.get("/analyze_traffic")
async def analyze_traffic():
    # Fetch graph and capacities from MongoDB
    graph_doc = await db.graph.find_one()
    route_caps_doc = await db.routeCapacities.find_one()

    graph = graph_doc.get('nodes', {}) if graph_doc else {}
    route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}
    # Fetch anomalies from sensorAlerts
    anomalies = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    anomaly_penalties = {a['node']: 5 if a['severity'] == 'high' else 2 for a in anomalies}  # Penalty scores

    # Fetch edges (now including posted ETA, journeyTime, finalNode for predictions)
    edges = [doc async for doc in db.edges.find()]

    # Calculate current loads
    route_load = {}
    node_loads = {}
    predicted_loads = {}  # For congestion prediction
    for edge in edges:
        route_key = f"{edge['currentLocation']}-{edge.get('nextNode', '')}"
        route_load[route_key] = route_load.get(route_key, 0) + 1
        node_loads[edge['currentLocation']] = node_loads.get(edge['currentLocation'], 0) + 1

        # Predict future loads based on ETA and finalNode
        if 'eta' in edge and 'nextNode' in edge:
            predicted_loads[edge['nextNode']] = predicted_loads.get(edge['nextNode'], 0) + 1  # Arriving soon
        if 'finalNode' in edge and 'journeyTime' in edge:
            if edge['journeyTime'] < 60:  # Predict near-term arrival to final
                predicted_loads[edge['finalNode']] = predicted_loads.get(edge['finalNode'], 0) + 1

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

    # Node congestion (dynamic, assuming node capacities in graph)
    node_congestion = {}
    for node, load in node_loads.items():
        capacity = graph.get(node, {}).get('capacity', 5)  # Default 5, fetch from graph if available
        ratio = load / capacity if capacity > 0 else 0
        level = 'high' if ratio > 0.8 else 'low'
        node_congestion[node] = {'ratio': ratio, 'level': level}

    # Intelligent suggestions: Integrate with RoutePlanner for dynamic path recommendations
    suggestions = []
    route_planner = RoutePlanner(graph)  # Initialize planner with graph

    for edge in edges:
        current_node = edge['currentLocation']
        destination = edge.get('destinationNode', current_node)  # Assume a destination if not set
        neighbors = graph.get(current_node, {}).get('neighbors', {})

        # Compute intelligent path using congestion data and predictions (shortest time/distance with prevention)
        suggested_path = route_planner.compute_path(current_node, destination, node_loads, route_congestion, predicted_loads=predicted_loads)

        # Fallback to best single-hop if full path not computable
        if not suggested_path or len(suggested_path) < 2:
            best_node = None
            best_eta = float('inf')
            for neighbor, dist in neighbors.items():
                route_key = f"{current_node}-{neighbor}"
                congestion = route_congestion.get(route_key, {'ratio': 0, 'level': 'low'})
                if congestion['level'] == 'high':
                    continue  # Skip highly congested routes

                speed = edge.get('speed', 10)
                eta = dist / speed * (1 + congestion['ratio'])  # Adjust ETA for congestion
                if eta < best_eta:
                    best_eta = eta
                    best_node = neighbor

            # Ultimate fallback
            if not best_node and neighbors:
                best_node = next(iter(neighbors))
                best_eta = neighbors[best_node] / edge.get('speed', 10)

            suggested_path = [current_node, best_node] if best_node else None
        else:
            best_node = suggested_path[1]
            best_eta = sum(graph['nodes'][suggested_path[i]]['neighbors'][suggested_path[i+1]] for i in range(len(suggested_path)-1)) / edge.get('speed', 10)

        # Update edge in DB with suggested nextNode and ETA
        await db.edges.update_one(
            {'id': edge['id']},
            {'$set': {'nextNode': best_node, 'eta': best_eta, 'suggestedPath': suggested_path}}
        )

        # Append suggestion with traffic insights
        route_key = f"{current_node}-{best_node}"
        suggestions.append({
            'edgeId': edge['id'],
            'currentLocation': current_node,
            'suggestedNextNode': best_node,
            'suggestedPath': suggested_path,
            'eta': best_eta,
            'congestionRatio': route_congestion.get(route_key, {'ratio': 0})['ratio'],
            'congestionLevel': route_congestion.get(route_key, {'level': 'low'})['level'],
            'nodeCongestionAtNext': node_congestion.get(best_node, {'level': 'low'})['level']
        })

    # Store results
    await db.trafficData.insert_one({
        'timestamp': datetime.utcnow(),
        'routeLoad': route_load,
        'nodeTraffic': node_loads,
        'routeCongestion': route_congestion,
        'nodeCongestion': node_congestion,
        'suggestions': suggestions
    })

    return {'status': 'analyzed', 'suggestions': suggestions}
