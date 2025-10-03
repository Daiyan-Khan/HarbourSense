from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import heapq
from math import inf
import string  # For letter-to-index conversion
import logging


app = FastAPI()


# Logging setup (consistent with manager.py)
logger = logging.getLogger("TrafficAnalyzer")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("traffic_log.txt", mode="a")
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


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


# Function to parse graph list into dict format
def parse_graph(graph_list):
    graph = {}
    for node in graph_list:
        node_id = node.get('id')
        if node_id:
            graph[node_id] = {
                'neighbors': {dir: neighbor for dir, neighbor in node.get('neighbors', {}).items()},
                'type': node.get('type', 'route_point'),
                'capacity': node.get('capacity', 5)  # Default capacity
            }
    print(graph)
    return graph


# Example graph list (replace with actual data or load dynamically if needed)
GRAPH_LIST = [
    {"id": "A1", "neighbors": {"E":"A2","S":"B1"}, "type":"dock", "capacity":8},
    {"id": "A2", "neighbors": {"W":"A1","E":"A3","S":"B2"}, "type":"route_point", "capacity":7},
    {"id": "A3", "neighbors": {"W":"A2","E":"A4","S":"B3"}, "type":"route_point", "capacity":6},
    {"id": "A4", "neighbors": {"W":"A3","E":"A5","S":"B4"}, "type":"control_office", "capacity":3},
    {"id": "A5", "neighbors": {"W":"A4","S":"B5"}, "type":"route_point", "capacity":7},
    {"id": "B1", "neighbors": {"N":"A1","E":"B2","S":"C1"}, "type":"route_point", "capacity":7},
    {"id": "B2", "neighbors": {"N":"A2","W":"B1","E":"B3","S":"C2"}, "type":"route_point", "capacity":6},
    {"id": "B3", "neighbors": {"N":"A3","W":"B2","E":"B4","S":"C3"}, "type":"route_point", "capacity":7},
    {"id": "B4", "neighbors": {"N":"A4","W":"B3","E":"B5","S":"C4"}, "type":"warehouse", "capacity":3},
    {"id": "B5", "neighbors": {"N":"A5","W":"B4","S":"C5"}, "type":"route_point", "capacity":6},
    {"id": "C1", "neighbors": {"N":"B1","E":"C2","S":"D1"}, "type":"route_point", "capacity":7},
    {"id": "C2", "neighbors": {"N":"B2","W":"C1","E":"C3","S":"D2"}, "type":"route_point", "capacity":6},
    {"id": "C3", "neighbors": {"N":"B3","W":"C2","E":"C4","S":"D3"}, "type":"berth", "capacity":2},
    {"id": "C4", "neighbors": {"N":"B4","W":"C3","E":"C5","S":"D4"}, "type":"route_point", "capacity":7},
    {"id": "C5", "neighbors": {"N":"B5","W":"C4","S":"D5"}, "type":"route_point", "capacity":6},
    {"id": "D1", "neighbors": {"N":"C1","E":"D2","S":"E1"}, "type":"route_point", "capacity":7},
    {"id": "D2", "neighbors": {"N":"C2","W":"D1","E":"D3","S":"E2"}, "type":"warehouse", "capacity":2},
    {"id": "D3", "neighbors": {"N":"C3","W":"D2","E":"D4","S":"E3"}, "type":"route_point", "capacity":6},
    {"id": "D4", "neighbors": {"N":"C4","W":"D3","E":"D5","S":"E4"}, "type":"route_point", "capacity":7},
    {"id": "D5", "neighbors": {"N":"C5","W":"D4","S":"E5"}, "type":"route_point", "capacity":6},
    {"id": "E1", "neighbors": {"N":"D1","E":"E2"}, "type":"exit_gate", "capacity":8},
    {"id": "E2", "neighbors": {"N":"D2","W":"E1","E":"E3"}, "type":"route_point", "capacity":7},
    {"id": "E3", "neighbors": {"N":"D3","W":"E2","E":"E4"}, "type":"route_point", "capacity":6},
    {"id": "E4", "neighbors": {"N":"D4","W":"E3","E":"E5"}, "type":"berth", "capacity":3},
    {"id": "E5", "neighbors": {"N":"D5","W":"E4"}, "type":"warehouse", "capacity":35}
]


# Enhanced Route Planner with coordinate awareness and blocked node filtering
class SmartRoutePlanner:
    def __init__(self, graph_list, blocked_nodes=None):
        self.graph = parse_graph(graph_list)  # Parse the provided list into dict
        self.blocked = set(blocked_nodes or [])


    def get_neighbors(self, node):
        if node in self.blocked: return []
        return [(k, 1) for k in self.graph.get(node, {}).get('neighbors', {}).values()]  # Assume weight 1 for simplicity


    def compute_path(self, start, end, node_loads, route_congestion, predicted_loads, capacity_threshold=0.8):
        logger.info(f"Computing path from {start} to {end}")
        if start not in self.graph or end not in self.graph:
            logger.warning(self.graph)
            return None
        distances = {node: inf for node in self.graph}
        distances[start] = 0
        previous = {node: None for node in self.graph}
        if start == end:
            logger.info(f"No path needed: Start and end are the same ({start})")
            return [start]  # Return a trivial path


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
        if path and path[0] == start:
            logger.info(f"Path found: {path}")
            return path
        else:
            logger.warning(f"No path found from {start} to {end}")
            return None


@app.get("/analyze_traffic")
async def analyze_traffic():
    logger.info("Starting traffic analysis")
    # Fetch graph and capacities from MongoDB (cache if frequent calls)
    graph_doc = await db.graph.find_one()
    route_caps_doc = await db.routeCapacities.find_one()


    graph = parse_graph(GRAPH_LIST)  # Use parsed graph (fallback or primary)
    if graph_doc:
        # Merge or override with DB if needed
        db_graph = graph_doc.get('nodes', {})
        graph.update(db_graph)
    route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}


    # Fetch anomalies from sensorAlerts
    anomalies = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    anomaly_penalties = {a['node']: 5 if a['severity'] == 'high' else 2 for a in anomalies}  # Penalty scores


    # Define blocked nodes (e.g., docks, non-route points) - adapt based on your graph
    blocked_nodes = ['C3', 'dock_A1', 'loading_zone']  # Example: block specific docks or areas


    # Fetch edges (now including posted ETA, journeyTime, finalNode for predictions)
    edges = [doc async for doc in db.edgeDevices.find()]  # Assuming 'edgeDevices' collection


    # Calculate current loads (single pass optimization)
    route_load = {}
    node_loads = {}
    predicted_loads = {}  # For congestion prediction
    for edge in edges:
        if not edge or not isinstance(edge, dict):  # Null/dict check
            logger.warning("Skipping invalid edge document")
            continue
        # Idle-only check per workflow (skip if not idle)
        if edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            logger.debug(f"Skipping non-idle edge {edge.get('id')}: task={edge.get('task')}, phase={edge.get('taskPhase')}")
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
    route_planner = SmartRoutePlanner(GRAPH_LIST, blocked_nodes)  # Initialize with parsing


    # Bulk updates preparation
    bulk_ops = []


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
            min_eta = inf  # For finding the best
            for neighbor, dist in neighbors:
                neighbor = neighbor.replace('->', '-')  # Standardize
                route_key = f"{current_node}-{neighbor}"
                congestion = route_congestion.get(route_key, {'ratio': 0, 'level': 'low'})
                if congestion['level'] == 'high':
                    continue  # Skip highly congested routes


                speed = edge.get('speed', 10)
                eta = dist / speed * (1 + congestion['ratio'])  # Adjust ETA for congestion
                if eta < min_eta:
                    min_eta = eta
                    best_node = neighbor


            # Ultimate fallback
            if best_node == "Null" and neighbors:
                best_neighbor, best_dist = next(iter(neighbors))
                best_node = best_neighbor.replace('->', '-')  # Standardize
                best_eta = best_dist / edge.get('speed', 10)
            else:
                best_eta = min_eta if min_eta != inf else "N/A"


            suggested_path = [current_node, best_node] if best_node != "Null" else None
        else:
            best_node = suggested_path[1]
            best_eta = sum(1 for i in range(len(suggested_path)-1)) / edge.get('speed', 10)  # Simple unit weight ETA


        # Prepare bulk update
        bulk_ops.append({
            'filter': {'id': edge['id']},
            'update': {'$set': {'nextNode': best_node if best_node != "Null" else "Null", 'eta': best_eta if best_eta != "N/A" else "N/A", 'suggestedPath': suggested_path}}
        })


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


    # Execute bulk updates if any
    if bulk_ops:
        try:
            result = await db.edgeDevices.bulk_write([UpdateOne(op['filter'], op['update']) for op in bulk_ops])
            logger.debug(f"Bulk update: {result.modified_count} edges updated")
        except Exception as e:
            logger.error(f"Bulk update failed: {e}")


    # Store results
    await db.trafficData.insert_one({
        'timestamp': datetime.utcnow(),
        'routeLoad': route_load,
        'nodeTraffic': node_loads,
        'routeCongestion': route_congestion,
        'nodeCongestion': node_congestion,
        'suggestions': suggestions
    })


    logger.info("Traffic analysis completed successfully")
    return {'status': 'analyzed', 'suggestions': suggestions}
