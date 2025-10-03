import logging
from datetime import datetime
import heapq
from math import inf
import string

# Logging setup
logger = logging.getLogger("TrafficAnalyzer")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("traffic_log.txt", mode="a")
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Conversion function for MongoDB extended JSON/BSON numbers
def convert_bson_numbers(obj):
    if isinstance(obj, dict):
        if "$numberInt" in obj:
            return int(obj["$numberInt"])
        elif "$numberLong" in obj:
            return int(obj["$numberLong"])
        else:
            return {k: convert_bson_numbers(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson_numbers(item) for item in obj]
    else:
        return obj

class SmartRoutePlanner:
    def __init__(self, graph, blocked_nodes=None):
        self.graph = graph or {}
        self.blocked = set(blocked_nodes or [])

    def get_neighbors(self, node):
        if node in self.blocked or node not in self.graph:
            return []
        neighbors = self.graph[node].get("neighbors", {})
        return [(neighbor, 1.0) for neighbor in neighbors.values() if neighbor not in self.blocked]  # Default float weight

    def compute_path(self, start, end, node_loads, route_congestion, predicted_loads, capacity_threshold=0.8):
        if start == end:
            return [start]
        if not self.graph or start not in self.graph or end not in self.graph:
            logger.warning(f"Cannot compute path: Graph empty or missing start/end nodes ({start} to {end})")
            return None

        distances = {node: inf for node in self.graph}
        distances[start] = 0
        previous = {node: None for node in self.graph}
        pq = [(0, start)]

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

        while pq:
            dist, current = heapq.heappop(pq)
            if dist > distances[current]:
                continue
            for neighbor, weight in self.get_neighbors(current):
                weight = float(weight)  # Force to float to prevent TypeError
                route_key = f"{current.replace('-', '--')}-{neighbor.replace('-', '--')}"
                start_coords = node_to_coords(current)
                neigh_coords = node_to_coords(neighbor)
                grid_penalty = abs(start_coords[0] - neigh_coords[0]) + abs(start_coords[1] - neigh_coords[1]) if start_coords and neigh_coords else 0
                congestion_ratio = route_congestion.get(route_key, {"ratio": 0})["ratio"]
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

async def analyze_traffic(db):
    logger.info("Starting traffic analysis")
    graph_doc = await db.graph.find().to_list(None)
    graph_clean = convert_bson_numbers(graph_doc)
    graph = {node['id']: node for node in graph_clean if 'id' in node}

    if not graph:
        logger.error("No nodes loaded for traffic analysis - Skipping")
        return {}

    route_caps_doc = await db.routeCapacities.find_one() or {'capacities': {}}
    route_capacities = route_caps_doc['capacities']

    anomalies = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    blocked_nodes = [a['node'] for a in anomalies if a['severity'] == 'high']

    edges = [doc async for doc in db.edgeDevices.find()]

    route_load = {}
    node_loads = {}
    predicted_loads = {}
    for edge in edges:
        if not isinstance(edge, dict) or edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            continue
        current_loc = edge.get('currentLocation', 'na').replace('-', '--')
        next_node = edge.get('nextNode', "Null").replace('-', '--')
        if next_node == "Null":
            continue
        route_key = f"{current_loc}-{next_node}"
        route_load[route_key] = route_load.get(route_key, 0) + 1
        node_loads[current_loc] = node_loads.get(current_loc, 0) + 1

        if 'eta' in edge and edge['eta'] != "N/A" and next_node != "Null":
            predicted_loads[next_node] = predicted_loads.get(next_node, 0) + 1
        if 'finalNode' in edge and edge['finalNode'] != "None" and 'journeyTime' in edge and edge['journeyTime'] != "N/A" and edge['journeyTime'] < 60:
            final_node = edge['finalNode'].replace('-', '--')
            predicted_loads[final_node] = predicted_loads.get(final_node, 0) + 1

    route_congestion = {}
    for route_key, load in route_load.items():
        capacity = route_capacities.get(route_key, 3)
        ratio = load / capacity if capacity > 0 else 0
        level = 'low' if ratio < 0.5 else 'medium' if ratio < 0.8 else 'high'
        route_congestion[route_key] = {'ratio': ratio, 'level': level}

    # Insert analysis to DB (optional - adjust as needed)
    await db.trafficAnalysis.insert_one({
        'timestamp': datetime.now(),
        'route_congestion': route_congestion,
        'node_loads': node_loads,
        'predicted_loads': predicted_loads
    })

    logger.info("Traffic analysis completed")
    return route_congestion, node_loads, predicted_loads
