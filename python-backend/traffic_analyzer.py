import logging
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import aiomqtt
import json
import bson
from bson import ObjectId
from bson.int64 import Int64
import heapq
from math import inf  # For infinite distances (float('inf') alternative)
from motor.motor_asyncio import AsyncIOMotorClient  # If needed for DB in analyzer
from pymongo.operations import UpdateOne  # FIXED: Correct import for bulk ops

def convert_bson_numbers(obj):
    """
    Recursively converts BSON types (e.g., ObjectId to str, Int64 to int) for JSON serialization.
    Handles dicts, lists, and primitives.
    """
    if isinstance(obj, dict):
        return {key: convert_bson_numbers(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson_numbers(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)  # Convert ObjectId to hex string
    elif isinstance(obj, Int64):
        return int(obj)  # Convert Int64 to Python int (safe for most JSON uses)
    elif isinstance(obj, bson.timestamp.Timestamp):
        return obj.time  # Convert to Unix timestamp
    else:
        return obj  # Primitives stay as-is

import string  # For node_to_coords

logger = logging.getLogger("TrafficAnalyzer")  # Consistent with your setup

def node_to_coords(node):  # Add if missing
    if len(node) < 2:
        return None
    letter = node[0].upper()
    number = node[1:]
    if letter not in string.ascii_uppercase or not number.isdigit():
        return None
    y = ord(letter) - ord('A')
    x = int(number)
    return (y, x)

# Parse graph list into dict format (fallback if DB empty)
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
    logger.debug(f"Parsed graph with {len(graph)} nodes")
    return graph

# Example GRAPH_LIST (use as fallback; load from DB primarily)
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

class SmartRoutePlanner:
    def __init__(self, graph, blocked_nodes=None):
        # Parse if str (BSON/JSON from DB)
        if isinstance(graph, str):
            try:
                self.flat_graph = json.loads(graph)
                logger.warning("Graph parsed from str")
            except json.JSONDecodeError:
                logger.error("Invalid graph str; empty dict")
                self.flat_graph = {}
        else:
            self.flat_graph = graph or {}
        
        # Assume flat {'A1': {'neighbors': {...}}} or {'nodes': {...}}; adapt to flat
        if 'nodes' in self.flat_graph:
            self.flat_graph = self.flat_graph['nodes']
        else:
            logger.debug("Using flat graph (no 'nodes' key)")
        
        self.blocked = set(blocked_nodes or [])
        logger.debug(f"Graph keys sample: {list(self.flat_graph.keys())[:3]}")

    def get_neighbors(self, node):
        if node in self.blocked:
            return []
        
        # Safe access to node data
        node_data = self.flat_graph.get(node, {}) if isinstance(self.flat_graph, dict) else {}
        if isinstance(node_data, str):
            try:
                node_data = json.loads(node_data)
            except:
                logger.error(f"Invalid node data str for {node}")
                return []
        
        neighbors_dict = node_data.get('neighbors', {}) if isinstance(node_data, dict) else {}
        if isinstance(neighbors_dict, str):
            try:
                neighbors_dict = json.loads(neighbors_dict)
            except:
                logger.error(f"Invalid neighbors str for {node}")
                return []
        
        # FIXED: Loop over items; use VALUE (data) as neighbor node, KEY (n) as direction (ignored)
        # Assume weight=1.0 (or parse if data had 'weight', but yours is str node)
        neighbors_list = []
        if isinstance(neighbors_dict, dict):
            for n, data in neighbors_dict.items():  # n='E', data='A2'
                actual_neighbor = data if isinstance(data, str) else n  # data is str node ID
                if actual_neighbor in self.blocked:
                    continue
                weight = 1.0  # Default; if data is dict {'node': 'A2', 'weight': 2}, adjust to data.get('weight', 1.0)
                neighbors_list.append((actual_neighbor, weight))
        logger.debug(f"Neighbors for {node}: {neighbors_list}")  # Now: [('A2', 1.0), ('B1', 1.0)]
        return neighbors_list

    def compute_path(self, start, end, node_loads, route_congestion, capacity_threshold=0.8, predicted_loads=None):
        if predicted_loads is None:
            predicted_loads = {}
        
        if start == end:
            return [start]
        
        nodes = list(self.flat_graph.keys()) if isinstance(self.flat_graph, dict) else []
        if not nodes or start not in nodes or end not in nodes:
            logger.warning(f"Invalid nodes/graph for path {start} -> {end}")
            return None
        
        distances = {node: inf for node in nodes}
        distances[start] = 0
        previous = {node: None for node in nodes}
        pq = [(0, start)]
        
        while pq:
            dist, current = heapq.heappop(pq)
            if dist > distances[current]:
                continue
            
            neighbors = self.get_neighbors(current)  # List of tuples
            for neighbor, base_weight in neighbors:
                route_key = f"{current.replace('->', '-')}-{neighbor.replace('->', '-')}"
                
                # Safe congestion (dict or fallback)
                cong_val = route_congestion.get(route_key, {'ratio': 0})
                congestion_ratio = cong_val.get('ratio', 0.0) if isinstance(cong_val, dict) else float(cong_val or 0)
                
                current_load = float(node_loads.get(neighbor, 0))
                predicted_load = float(predicted_loads.get(neighbor, 0))
                load_factor = (current_load + predicted_load) / capacity_threshold
                adjusted_weight = base_weight * (1 + congestion_ratio + load_factor)
                
                # Grid penalty (Manhattan; fixed indices)
                grid_penalty = 0
                sc = node_to_coords(current)
                nc = node_to_coords(neighbor)
                if sc and nc:
                    grid_penalty = abs(sc[0] - nc[0]) + abs(sc[1] - nc[1])
                
                alt = dist + adjusted_weight + grid_penalty
                if alt < distances[neighbor]:
                    distances[neighbor] = alt
                    previous[neighbor] = current
                    heapq.heappush(pq, (alt, neighbor))
        
        # Reconstruct path
        path = []
        current = end
        while current is not None:
            path.append(current)
            current = previous.get(current)
        path.reverse()
        
        if path and path[0] == start:
            logger.info(f"Path {start} -> {end}: {path} (total dist: {distances[end]:.2f})")
            return path
        else:
            logger.warning(f"No path {start} -> {end}; try greedy fallback")
            return self._greedy_fallback(start, end, node_loads, route_congestion, predicted_loads, capacity_threshold)
    
    def _greedy_fallback(self, start, end, node_loads, route_congestion, capacity_threshold=0.8, predicted_loads=None):
        path = [start]
        current = start
        max_steps = len(self.flat_graph) * 2
        steps = 0
        
        while current != end and steps < max_steps:
            neighbors = self.get_neighbors(current)
            if not neighbors:
                break
            
            # Score by adjusted weight
            def score(item):
                n, bw = item
                rk = f"{current.replace('->', '-')}-{n.replace('->', '-')}"
                cr = route_congestion.get(rk, {'ratio': 0}).get('ratio', 0) if isinstance(route_congestion.get(rk), dict) else float(route_congestion.get(rk) or 0)
                cl = float(node_loads.get(n, 0))
                pl = float(predicted_loads.get(n, 0))
                lf = (cl + pl) / capacity_threshold
                gp = 0
                sc = node_to_coords(current)
                nc = node_to_coords(n)
                if sc and nc:
                    gp = abs(sc[0] - nc[0]) + abs(sc[1] - nc[1])
                return bw * (1 + cr + lf) + gp
            
            next_item = min(neighbors, key=score)
            next_node, _ = next_item
            path.append(next_node)
            current = next_node
            steps += 1
        
        logger.info(f"Greedy path {start} -> {end}: {path}")
        return path if path[-1] == end else None

class TrafficAnalyzer:
    def __init__(self, db, mqtt_client, graph):  # mqtt_client is pre-connected AsyncMQTTClient
        self.db = db
        self.mqtt_client = mqtt_client  # Assume connected externally
        self.graph = graph
        self.node_loads = defaultdict(int)
        self.route_congestion = defaultdict(float)
        self.predicted_loads = defaultdict(int)
        self.planner = SmartRoutePlanner(graph)
        # Start connection check asynchronously
        asyncio.create_task(self._ensure_connected())

    async def _ensure_connected(self):
        """Ensure MQTT client is connected; retry if needed."""
        max_retries = 3
        retry = 0
        while retry < max_retries:
            try:
                if not self.mqtt_client._connected:  # FIXED: Use internal _connected
                    await self.mqtt_client.connect()
                    logger.info("MQTT client connected in analyzer")
                return
            except aiomqtt.MqttError as e:
                retry += 1
                logger.warning(f"MQTT connect failed (retry {retry}/{max_retries}): {e}")
                await asyncio.sleep(2 ** retry)  # Exponential backoff
        logger.error("Failed to connect MQTT after retries")

    async def analyze_metrics(self, triggered_by=""):
        """Restored core method: Analyze traffic from DB, update internal state, compute congestion/loads."""
        logger.info(f"Starting analysis triggered by: {triggered_by}")
        
        # Fetch graph and capacities from MongoDB (with fallback)
        graph_doc = await self.db.graph.find_one()
        route_caps_doc = await self.db.routeCapacities.find_one()
        
        # Use DB graph or fallback to parsed GRAPH_LIST
        if graph_doc:
            db_graph = graph_doc.get('nodes', {})
            graph = self.planner.flat_graph.copy()
            graph.update(db_graph)  # Merge/override
        else:
            graph = parse_graph(GRAPH_LIST)
        
        route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}
        
        # Fetch anomalies from sensorAlerts
        anomalies = [doc async for doc in self.db.sensorAlerts.find({'alert': True})]
        anomaly_penalties = {a['node']: 5 if a['severity'] == 'high' else 2 for a in anomalies}  # Penalty scores
        
        # Define blocked nodes (e.g., docks, non-route points) - adapt based on your graph
        blocked_nodes = ['C3', 'dock_A1', 'loading_zone']  # Example: block specific docks or areas
        
        # Fetch edges (now including posted ETA, journeyTime, finalNode for predictions)
        edges = [doc async for doc in self.db.edgeDevices.find()]  # Assuming 'edgeDevices' collection
        
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
        
        # Update internal state
        self.node_loads = node_loads
        self.route_congestion = route_congestion
        self.predicted_loads = predicted_loads
        
        # Optional: Generate suggestions and update edges (idle-only, like before)
        suggestions = []
        route_planner = SmartRoutePlanner(graph, blocked_nodes)
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
            
            # Compute intelligent path using congestion data, predictions, and blocking
            suggested_path = route_planner.compute_path(current_node, destination, node_loads, route_congestion, predicted_loads=predicted_loads)
            
            # Fallback to best single-hop if full path not computable
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
            
            # Prepare bulk update (optional; comment out if not needed during analysis)
            bulk_ops.append(UpdateOne(  # FIXED: Use UpdateOne directly
                {'id': edge['id']},
                {'$set': {'nextNode': best_node if best_node != "Null" else "Null", 
                          'eta': best_eta if best_eta != "N/A" else "N/A", 
                          'suggestedPath': suggested_path}}
            ))
            
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
        
        # Execute bulk updates if any (optional)
        if bulk_ops:
            try:
                result = await self.db.edgeDevices.bulk_write(bulk_ops)
                logger.debug(f"Bulk update: {result.modified_count} edges updated")
            except Exception as e:
                logger.error(f"Bulk update failed: {e}")
        
        # Store results in DB
        await self.db.trafficData.insert_one({
            'timestamp': datetime.utcnow(),
            'routeLoad': route_load,
            'nodeTraffic': node_loads,
            'routeCongestion': route_congestion,
            'nodeCongestion': node_congestion,
            'predictedLoads': predicted_loads,
            'suggestions': suggestions,
            'triggered_by': triggered_by,
            'anomaly_penalties': anomaly_penalties
        })
        
        logger.info(f"Analysis complete: {len(suggestions)} suggestions generated, triggered by {triggered_by}")
        return {'status': 'analyzed', 'suggestions': suggestions, 'node_loads': dict(node_loads)}

    def get_current_loads(self):
        return dict(self.node_loads)

    def get_route_congestion(self):
        return dict(self.route_congestion)

    def get_predicted_loads(self):
        return dict(self.predicted_loads)

    async def start_mqtt_listener(self):
        """Subscribe to path updates from port.js for dynamic triggers"""
        await self._ensure_connected()  # Connect if needed
        await self.mqtt_client.subscribe("harboursense/traffic/update/#")  # Wildcard for all edge updates
        
        async for message in self.mqtt_client.messages:  # No 'async with'—iterate directly
            topic = message.topic.decode()
            payload = json.loads(message.payload.decode())
            edge_id = topic.split('/')[-1]
            logger.info(f"Received path update from {edge_id}: {payload.get('remainingPath', [])}")
            
            # Trigger analysis on new path progress
            await self.analyze_metrics(triggered_by=f"Path update from {edge_id}")
            
            # Optional: Recompute and republish if congestion changed significantly
            if len(payload.get('remainingPath', [])) < 3:  # Threshold for reroute
                start = payload.get('currentLocation', 'A1')
                
                # DYNAMIC: Fetch destination from DB (set by manager during assignment)
                edge_doc = await self.db.edgeDevices.find_one({'id': edge_id})
                destination = edge_doc.get('destinationNode') if edge_doc else 'B4'  # Fallback to default
                logger.info(f"Fetched dynamic destination '{destination}' for {edge_id} from DB")
                
                node_loads = self.get_current_loads()
                route_congestion = self.get_route_congestion()
                predicted_loads = self.get_predicted_loads()
                new_path = self.planner.compute_path(start, destination, node_loads, route_congestion, predicted_loads=predicted_loads)
                if new_path:
                    await self.mqtt_client.publish(f"harboursense/traffic/{edge_id}", json.dumps({'path': new_path}))
                    logger.info(f"Rerouted and republished path to {destination} for {edge_id}")
                else:
                    logger.warning(f"Failed to compute new path from {start} to {destination}")
