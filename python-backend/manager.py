import asyncio
import json
import logging
import time
from motor.motor_asyncio import AsyncIOMotorClient
import paho.mqtt.client as mqtt
from task_assigner import TaskAssigner
from datetime import datetime
import heapq
from math import inf
import string
from traffic_analyzer import SmartRoutePlanner, analyze_traffic  # Import the class and function
# ------------------- Logging Setup -------------------
logger = logging.getLogger("HarbourSenseManager")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.pool").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
logging.getLogger("pymongo.server_selection").setLevel(logging.WARNING)
logging.getLogger("pymongo.cursor").setLevel(logging.WARNING)
logging.getLogger("pymongo.command").setLevel(logging.WARNING)

# File handler
file_handler = logging.FileHandler("log.txt", mode="a")
file_handler.setLevel(logging.DEBUG)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Attach handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ------------------------------------------------------

# Debounce map for handling frequent MQTT updates from edges
last_processed = {}
DEBOUNCE_DELAY = 1

# Global event loop reference
loop = None

# ------------------- Helper Functions -------------------
def node_to_coords(node):
    if len(node) != 2:
        return None
    letter, number = node[0].upper(), node[1:]
    if letter not in string.ascii_uppercase or not number.isdigit():
        return None
    y = ord(letter) - ord("A")
    x = int(number)
    return (y, x)

class SmartRoutePlanner:
    def __init__(self, graph, blocked_nodes=None):
        self.graph = graph
        self.blocked = set(blocked_nodes or [])

    def get_neighbors(self, node):
        if node in self.blocked:
            return []
        return [
            (neighbor, 1)  # Assume weight 1
            for neighbor in self.graph.get(node, {}).get("neighbors", {}).values()
            if neighbor not in self.blocked
        ]

    def compute_path(
        self,
        start,
        end,
        node_loads,
        route_congestion,
        predicted_loads,
        capacity_threshold=0.8,
    ):
        if start == end:
            return [start]
        if start not in self.graph or end not in self.graph:
            logger.warning(f"Graph missing start/end nodes: {start} to {end}")
            return None
        distances = {node: inf for node in self.graph}
        distances[start] = 0
        previous = {node: None for node in self.graph}
        pq = [(0, start)]

        while pq:
            dist, current = heapq.heappop(pq)
            if dist > distances[current]:
                continue
            neighbors = self.get_neighbors(current)
            for neighbor, weight in neighbors:
                route_key = f"{current.replace('-', '--')}-{neighbor.replace('-', '--')}"
                start_coords = node_to_coords(current)
                neigh_coords = node_to_coords(neighbor)
                grid_penalty = (
                    abs(start_coords[0] - neigh_coords[0])
                    + abs(start_coords[1] - neigh_coords[1])
                    if start_coords and neigh_coords
                    else 0
                )
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

# ------------------- Traffic Analyzer -------------------
# Assuming route_capacities is fetched or defined
async def analyze_traffic(db):
    graph_doc = await db.graph.find_one()
    raw_nodes = graph_doc.get('nodes', []) if graph_doc else []
    graph = {node['id']: node for node in raw_nodes if 'id' in node}
    logger.debug(f"Loaded {len(graph)} graph nodes for analysis: {list(graph.keys())}")

    if not graph:
        logger.error("No nodes loaded for traffic analysis - Skipping")
        return {}

    route_caps_doc = await db.routeCapacities.find_one()
    route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}

    anomalies = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    blocked_nodes = [a['node'] for a in anomalies if a['severity'] == 'high']  # Example blocking

    edges = [doc async for doc in db.edgeDevices.find()]

    route_load = {}
    node_loads = {}
    predicted_loads = {}
    for edge in edges:
        if not isinstance(edge, dict):
            logger.warning("Skipping invalid edge in analysis")
            continue
        if edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
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

    node_congestion = {}
    for node, load in node_loads.items():
        capacity = graph.get(node, {}).get('capacity', 5)
        ratio = load / capacity if capacity > 0 else 0
        level = 'high' if ratio > 0.8 else 'low'
        node_congestion[node] = {'ratio': ratio, 'level': level}

    suggestions = []
    route_planner = SmartRoutePlanner(graph, blocked_nodes)
    for edge in edges:
        if not isinstance(edge, dict) or edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            continue
        current_node = edge.get('currentLocation', 'na').replace('-', '--')
        destination_val = edge.get('finalNode', "None")
        destination = current_node if destination_val == "None" else destination_val.replace('-', '--')
        suggested_path = route_planner.compute_path(
            current_node, destination, node_loads, route_congestion, predicted_loads
        )
        suggestions.append({'edgeId': edge.get('id', 'unknown'), 'suggestedPath': suggested_path})

    await db.trafficData.insert_one({
        'timestamp': datetime.utcnow(),
        'routeLoad': route_load,
        'nodeTraffic': node_loads,
        'routeCongestion': route_congestion,
        'nodeCongestion': node_congestion,
        'suggestions': suggestions
    })

    return {
        'route_congestion': route_congestion,
        'node_loads': node_loads,
        'predicted_loads': predicted_loads,
        'suggestions': suggestions
    }

# ------------------- Shipment Watcher -------------------
async def shipment_watcher(db, shipment_manager):
    while True:
        waiting_shipments = await db.shipments.find(
            {"status": {"$in": ["waiting", "processing"]}}
        ).to_list(None)

        for shipment in waiting_shipments:
            await shipment_manager.process_shipment_steps(shipment)

        await asyncio.sleep(5)  # check every 5s

# ------------------- Periodic Traffic Analysis -------------------
async def periodic_traffic_analysis(db):
    while True:
        await analyze_traffic(db)
        await asyncio.sleep(30)  # Every 30 seconds

# ------------------- Setup Entry -------------------
async def setup():
    global loop
    loop = asyncio.get_running_loop()

    uri = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
    client = AsyncIOMotorClient(uri)

    db = client["port"]
    logger.info("Connected to MongoDB cluster and using database 'port'")

    mqtt_client = mqtt.Client(client_id="manager", protocol=mqtt.MQTTv311)
    mqtt_client.tls_set(
        ca_certs="../certs/AmazonRootCA1.pem",
        certfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt",
        keyfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key",
    )
    mqtt_client.connect("a1dghi6and062t-ats.iot.us-east-1.amazonaws.com", 8883, 60)
    mqtt_client.loop_start()
    logger.info("MQTT client connected to AWS IoT Core and loop started")

    raw_nodes = await db.graph.find().to_list(None)  # Loads all node documents
    if not raw_nodes:
        logger.error("Graph missing or empty in DB - cannot proceed")
        return

    graph = {}
    for node in raw_nodes:
        node_id = node.get('id')
        if node_id:
            # Optional: Handle any BSON conversions if needed (usually not, as Motor handles it)
            if isinstance(node.get('capacity'), dict) and '$numberInt' in node['capacity']:
                node['capacity'] = int(node['capacity']['$numberInt'])
            graph[node_id] = node
        else:
            logger.warning(f"Skipping invalid node without 'id': {node}")

    logger.info(f"Loaded graph with {len(graph)} nodes")
    task_assigner = TaskAssigner(db, mqtt_client, graph)
    route_planner = SmartRoutePlanner(graph)

    # Define ShipmentManager inline here
    class ShipmentManager:
        def __init__(self, db, task_assigner):
            self.db = db
            self.task_assigner = task_assigner

        async def process_shipment_steps(self, shipment):
            # Example implementation: Assign task to transport shipment
            if shipment.get('status') == 'waiting':
                task_details = {
                    'task': 'transport shipment',
                    'startNode': shipment.get('arrivalNode'),
                    'destinationNode': 'C3'  # Example destination; adjust as needed
                }
                assigned = await self.task_assigner.assign_task('truck', task_details)
                if assigned:
                    await self.db.shipments.update_one(
                        {'id': shipment['id']},
                        {'$set': {'status': 'assigned'}}
                    )
                    logger.info(f"Shipment {shipment['id']} assigned for transport")
                else:
                    logger.warning(f"Failed to assign shipment {shipment['id']}")

    shipment_manager = ShipmentManager(db, task_assigner)

    loop.create_task(shipment_watcher(db, shipment_manager))
    loop.create_task(periodic_traffic_analysis(db))

    logger.info("Manager setup completed, entering monitoring loop...")

    await asyncio.gather(
        task_assigner.monitor_and_assign(),
    )

if __name__ == "__main__":
    asyncio.run(setup())
