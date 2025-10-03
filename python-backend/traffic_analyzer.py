from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import heapq
from math import inf
import string  # For letter-to-index conversion

app = FastAPI()

# MongoDB connection
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port

# Helper: Convert node like 'A9' to (y, x) coordinates (A=0, 9=9)
def node_to_coords(node):
    if len(node) < 2:
        return None
    letter = node[0].upper()
    number = node[1:]
    if letter not in string.ascii_uppercase or not number.isdigit():
        return None
    y = ord(letter) - ord('A')
    x = int(number)
    return (y, x)

# Enhanced Route Planner with coordinate awareness and blocked node filtering
class SmartRoutePlanner:  # Merged from traffic_analyzer.py (superior for real-time traffic analysis)
    def __init__(self, graph, blocked_nodes=None):
        self.graph = graph  # Assume graph is already the dict of nodes (e.g., {'A1': {...}, 'B2': {...}})
        self.blocked = set(blocked_nodes or [])

    def get_neighbors(self, node):
        if node in self.blocked: return []
        return [(k, v) for k, v in self.graph.get(node, {}).get('neighbors', {}).items() if k not in self.blocked]

    def compute_path(self, start, end, node_loads, route_congestion, predicted_loads, capacity_threshold=0.8):
        distances = {node: inf for node in self.graph}  # Fix: Iterate directly over self.graph (nodes dict)
        distances[start] = 0
        previous = {node: None for node in self.graph}  # Fix: Same here
        pq = [(0, start)]
        while pq:
            dist, current = heapq.heappop(pq)
            if dist > distances[current]: continue
            neighbors = self.get_neighbors(current)
            for neighbor, weight in neighbors:
                route_key = f"{current.replace('-', '--')}-{neighbor.replace('-', '--')}"
                start_coords = node_to_coords(current)
                neigh_coords = node_to_coords(neighbor)
                grid_penalty = abs(start_coords[0] - neigh_coords[0]) + abs(start_coords[1] - neigh_coords[1]) if start_coords and neigh_coords else 0
                congestion_ratio = route_congestion.get(route_key, {'ratio': 0})['ratio']
                current_load = node_loads.get(neighbor, 0)
                predicted_load = predicted_loads.get(neighbor, 0)
                load_factor = (current_load + predicted_load) / capacity_threshold
                adjusted_weight = weight * (1 + congestion_ratio + load_factor) + grid_penalty
                alt = dist + adjusted_weight
                if alt < distances[neighbor]:
                    distances[neighbor] = alt
                    previous[neighbor] = current
                    heapq.heappush(pq, (alt, neighbor))
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

    # Define blocked nodes (e.g., docks, non-route points) - adapt based on your graph
    blocked_nodes = ['C3', 'dock_A1', 'loading_zone']  # Example: block specific docks or areas

    # Fetch edges (now including posted ETA, journeyTime, finalNode for predictions)
    edges = [doc async for doc in db.edgeDevices.find()]  # Assuming 'edgeDevices' collection

    # Calculate current loads
    route_load = {}
    node_loads = {}
    predicted_loads = {}  # For congestion prediction
    for edge in edges:
        if not edge or not isinstance(edge, dict):  # Null/dict check
            continue
        # Idle-only check per workflow (skip if not idle)
        if edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            continue
        current_loc = edge.get('currentLocation', 'na').replace('->', '-')  # Standardize, safe default
        next_node = edge.get('nextNode', "Null").replace('->', '-')  # Use string placeholder, standardize
        if next_node == "Null":  # Handle placeholder as no next node
            continue
        route_key = f"{current_loc}-{next_node}"
        route_load[route_key] = route_load.get(route_key, 0) + 1
        node_loads[current_loc] = node_loads.get(current_loc, 0) + 1

        # Predict future loads based on ETA and finalNode with string checks
        if 'eta' in edge and edge.get('eta') != "N/A" and next_node != "Null":
            predicted_loads[next_node] = predicted_loads.get(next_node, 0) + 1  # Arriving soon
        if 'finalNode' in edge and 'journeyTime' in edge:
            if edge.get('finalNode') != "None" and edge.get('journeyTime') != "N/A" and edge['journeyTime'] < 60:  # Near-term with string checks
                final_node = edge.get('finalNode', 'na').replace('->', '-')  # Standardize, safe
                predicted_loads[final_node] = predicted_loads.get(final_node, 0) + 1

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

    # Intelligent suggestions: Integrate with SmartRoutePlanner for dynamic path recommendations
    suggestions = []
    route_planner = SmartRoutePlanner(graph, blocked_nodes)  # Initialize with blocking

    for edge in edges:
        if not edge or not isinstance(edge, dict) or edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            continue  # Idle-only
        current_node = edge.get('currentLocation', 'na').replace('->', '-')  # Standardize, safe
        destination_val = edge.get('finalNode', "None").replace('->', '-')  # Use string placeholder, standardize
        if destination_val == "None":
            destination = current_node  # Safe default to current if placeholder
        else:
            destination = destination_val
        neighbors = route_planner.get_neighbors(current_node)  # Filtered neighbors

        # Compute intelligent path using congestion data, predictions, and blocking (shortest with prevention)
        suggested_path = route_planner.compute_path(current_node, destination, node_loads, route_congestion, predicted_loads=predicted_loads)

        # Fallback to best single-hop if full path not computable (e.g., due to blocking)
        if not suggested_path or len(suggested_path) < 2:
            best_node = "Null"
            best_eta = "N/A"
            for neighbor, dist in neighbors.items():
                neighbor = neighbor.replace('->', '-')  # Standardize
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
            if best_node == "Null" and neighbors:
                best_node = next(iter(neighbors)).replace('->', '-')  # Standardize
                best_eta = neighbors[best_node] / edge.get('speed', 10)

            suggested_path = [current_node, best_node] if best_node != "Null" else None
        else:
            best_node = suggested_path[1]
            best_eta = sum(graph['nodes'][suggested_path[i]]['neighbors'][suggested_path[i+1]] for i in range(len(suggested_path)-1)) / edge.get('speed', 10)

        # Update edge in DB with suggested nextNode and ETA (using placeholders if needed)
        await db.edgeDevices.update_one(
            {'id': edge['id']},
            {'$set': {'nextNode': best_node if best_node != "Null" else "Null", 'eta': best_eta if best_eta != "N/A" else "N/A", 'suggestedPath': suggested_path}}
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
