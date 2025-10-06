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

    # FIXED: Safe float helper to prevent dict/float errors
    def safe_float(self, val, default=0.0):
        if isinstance(val, dict):
            logger.warning(f"Nested dict in loads/congestion: {val}; using default {default}")
            return default
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            logger.warning(f"Invalid float {val}; using {default}")
            return default

    def compute_path(self, start, end, node_loads, route_congestion, capacity_threshold=0.8, predicted_loads=None):
        if predicted_loads is None:
            predicted_loads = {}
        # FIXED: Scrub Null/None destinations (from idle edges)
        if end in ['Null', 'null', None, 'None']:
            logger.warning(f"Invalid end node {end} (likely idle default); fallback to 'C5'")
            end = 'C5'
        if start in ['Null', 'null', None, 'None']:
            logger.warning(f"Invalid start node {start}; fallback to 'A1'")
            start = 'A1'
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
                if isinstance(cong_val, dict):
                    congestion_ratio = cong_val.get('ratio', 0.0)
                else:
                    congestion_ratio = self.safe_float(cong_val, 0.0)
                # FIXED: Safe floats for loads (handles nested dicts)
                current_load = self.safe_float(node_loads.get(neighbor, 0), 0.0)
                predicted_load = self.safe_float(predicted_loads.get(neighbor, 0), 0.0)
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
            return self._greedy_fallback(start, end, node_loads, route_congestion, capacity_threshold, predicted_loads)

    def _greedy_fallback(self, start, end, node_loads, route_congestion, capacity_threshold=0.8, predicted_loads=None):
        if predicted_loads is None:
            predicted_loads = {}
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
                cr_val = route_congestion.get(rk, {'ratio': 0})
                if isinstance(cr_val, dict):
                    cr = cr_val.get('ratio', 0.0)
                else:
                    cr = self.safe_float(cr_val, 0.0)
                # FIXED: Safe floats
                cl = self.safe_float(node_loads.get(n, 0), 0.0)
                pl = self.safe_float(predicted_loads.get(n, 0), 0.0)
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
        return path if path[-1] == end else [start, end]  # Ultimate fallback

class TrafficAnalyzer:
    def __init__(self, db, mqtt_client=None, graph=None):  # mqtt_client is pre-connected AsyncMQTTClient
        self.db = db
        self.mqtt_client = mqtt_client  # Assume connected externally
        self.graph = graph
        self.node_loads = defaultdict(int)
        self.route_congestion = defaultdict(float)
        self.predicted_loads = defaultdict(int)
        self.planner = SmartRoutePlanner(graph or parse_graph(GRAPH_LIST))
        # Start connection check asynchronously
        if self.mqtt_client:
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
            # FIXED: Safe get + None scrub for all node fields (get returns None if key=None, so explicit check)
            current_loc_raw = edge.get('currentLocation')
            if current_loc_raw is None or current_loc_raw == 'Null':
                current_loc = 'na'  # Placeholder for idle
            else:
                current_loc = str(current_loc_raw).replace('->', '-')  # Standardize
            
            next_node_raw = edge.get('nextNode')
            if next_node_raw is None or next_node_raw == 'Null':
                next_node = 'Null'  # Placeholder for idle (no next)
            else:
                next_node = str(next_node_raw).replace('->', '-')  # Standardize
            
            if next_node == "Null":  # Handle placeholder as no next node
                continue
            route_key = f"{current_loc}-{next_node}"
            route_load[route_key] = route_load.get(route_key, 0) + 1
            node_loads[current_loc] = node_loads.get(current_loc, 0) + 1
            # Predict future loads based on ETA and finalNode with string checks
            if 'eta' in edge and edge.get('eta') != "N/A" and next_node != "Null":
                predicted_loads[next_node] = predicted_loads.get(next_node, 0) + 1
            if 'finalNode' in edge and 'journeyTime' in edge:
                final_node_raw = edge.get('finalNode')
                if final_node_raw is not None and final_node_raw not in ["None", "Null", "null"] and edge.get('journeyTime') != "N/A" and edge['journeyTime'] < 60:  # Near-term with string checks
                    if final_node_raw == 'Null':  # FIXED: Explicit scrub
                        final_node_raw = 'na'
                    final_node = str(final_node_raw).replace('->', '-')  # Standardize, safe
                    if final_node not in ["None", "Null"]:  # FIXED: Skip Null predictions
                        predicted_loads[final_node] = predicted_loads.get(final_node, 0) + 1
        # Dynamic congestion per route (ratio and level) - FIXED: Safe capacity (no / dict)
        route_congestion = {}
        for route_key, load in route_load.items():
            cap_val = route_capacities.get(route_key, 3)
            capacity = self.planner.safe_float(cap_val, 3.0)  # FIXED: Safe, prevents dict
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
            capacity = self.planner.safe_float(capacity, 5.0)  # FIXED: Safe
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
            # FIXED: Safe get + None scrub for current and destination
            current_node_raw = edge.get('currentLocation')
            if current_node_raw is None or current_node_raw == 'Null':
                current_node = 'na'
            else:
                current_node = str(current_node_raw).replace('->', '-')  # Standardize
            
            destination_raw = edge.get('finalNode')
            if destination_raw is None or destination_raw == 'Null' or destination_raw == 'None' or destination_raw == 'null':
                logger.debug(f"Skipping idle edge {edge.get('id')} with Null finalNode (no route needed)")
                continue  # FIXED: No path computation for undefined idle
            else:
                destination_val = str(destination_raw).replace('->', '-')  # Standardize, safe
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
                    neighbor = str(neighbor).replace('->', '-')  # FIXED: Safe str + standardize
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
                    best_node = str(best_neighbor).replace('->', '-')  # FIXED: Safe str + standardize
                    best_eta = best_dist / edge.get('speed', 10)
                else:
                    best_eta = min_eta if min_eta != inf else "N/A"
                suggested_path = [current_node, best_node] if best_node != "Null" else None
            else:
                best_node = suggested_path[1]
                best_eta = sum(1 for i in range(len(suggested_path)-1)) / edge.get('speed', 10)  # Simple unit weight ETA
            # Prepare bulk update (optional; comment out if not needed during analysis)
            next_node_val = best_node if best_node != "Null" else "Null"
            eta_val = best_eta if best_eta != "N/A" else "N/A"
            bulk_ops.append(UpdateOne(  # FIXED: Use UpdateOne directly
                {'id': edge['id']},
                {'$set': {'nextNode': next_node_val,
                        'eta': eta_val,
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
            # FIXED: Safe get + None scrub for all node fields (get returns None if key=None, so explicit check)
            current_loc_raw = edge.get('currentLocation')
            if current_loc_raw is None or current_loc_raw == 'Null':
                current_loc = 'na'  # Placeholder for idle
            else:
                current_loc = str(current_loc_raw).replace('->', '-')  # Standardize
            
            next_node_raw = edge.get('nextNode')
            if next_node_raw is None or next_node_raw == 'Null':
                next_node = 'Null'  # Placeholder for idle (no next)
            else:
                next_node = str(next_node_raw).replace('->', '-')  # Standardize
            
            if next_node == "Null":  # Handle placeholder as no next node
                continue
            route_key = f"{current_loc}-{next_node}"
            route_load[route_key] = route_load.get(route_key, 0) + 1
            node_loads[current_loc] = node_loads.get(current_loc, 0) + 1
            # Predict future loads based on ETA and finalNode with string checks
            if 'eta' in edge and edge.get('eta') != "N/A" and next_node != "Null":
                predicted_loads[next_node] = predicted_loads.get(next_node, 0) + 1
            if 'finalNode' in edge and 'journeyTime' in edge:
                final_node_raw = edge.get('finalNode')
                if final_node_raw is not None and final_node_raw not in ["None", "Null", "null"] and edge.get('journeyTime') != "N/A" and edge['journeyTime'] < 60:  # Near-term with string checks
                    if final_node_raw == 'Null':  # FIXED: Explicit scrub
                        final_node_raw = 'na'
                    final_node = str(final_node_raw).replace('->', '-')  # Standardize, safe
                    if final_node not in ["None", "Null"]:  # FIXED: Skip Null predictions
                        predicted_loads[final_node] = predicted_loads.get(final_node, 0) + 1
        # Dynamic congestion per route (ratio and level) - FIXED: Safe capacity (no / dict)
        route_congestion = {}
        for route_key, load in route_load.items():
            cap_val = route_capacities.get(route_key, 3)
            capacity = self.planner.safe_float(cap_val, 3.0)  # FIXED: Safe, prevents dict
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
            capacity = self.planner.safe_float(capacity, 5.0)  # FIXED: Safe
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
            # FIXED: Safe get + None scrub for current and destination
            current_node_raw = edge.get('currentLocation')
            if current_node_raw is None or current_node_raw == 'Null':
                current_node = 'na'
            else:
                current_node = str(current_node_raw).replace('->', '-')  # Standardize
            
            destination_raw = edge.get('finalNode')
            if destination_raw is None or destination_raw == 'Null' or destination_raw == 'None' or destination_raw == 'null':
                logger.debug(f"Skipping idle edge {edge.get('id')} with Null finalNode (no route needed)")
                continue  # FIXED: No path computation for undefined idle
            else:
                destination_val = str(destination_raw).replace('->', '-')  # Standardize, safe
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
                    neighbor = str(neighbor).replace('->', '-')  # FIXED: Safe str + standardize
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
                    best_node = str(best_neighbor).replace('->', '-')  # FIXED: Safe str + standardize
                    best_eta = best_dist / edge.get('speed', 10)
                else:
                    best_eta = min_eta if min_eta != inf else "N/A"
                suggested_path = [current_node, best_node] if best_node != "Null" else None
            else:
                best_node = suggested_path[1]
                best_eta = sum(1 for i in range(len(suggested_path)-1)) / edge.get('speed', 10)  # Simple unit weight ETA
            # Prepare bulk update (optional; comment out if not needed during analysis)
            next_node_val = best_node if best_node != "Null" else "Null"
            eta_val = best_eta if best_eta != "N/A" else "N/A"
            bulk_ops.append(UpdateOne(  # FIXED: Use UpdateOne directly
                {'id': edge['id']},
                {'$set': {'nextNode': next_node_val,
                        'eta': eta_val,
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

    def get_current_loads(self):
        return dict(self.node_loads)

    def get_route_congestion(self):
        return dict(self.route_congestion)

    def get_predicted_loads(self):
        return dict(self.predicted_loads)

    async def start_mqtt_listener(self):
        """Clean MQTT listener: Subscribe to path updates, trigger analysis/reroutes."""
        await self._ensure_connected()  # Ensure initial connection (your TLS/client setup)
        subscribe_topic = 'harboursense/traffic/update/#'  # Wildcard for all edge updates (e.g., /update/truck_1)
        await self.mqtt_client.subscribe(subscribe_topic)
        logger.info(f"MQTT listener started on {subscribe_topic}")

        async for message in self.mqtt_client.messages:
            try:
                # FIXED: Proper aiomqtt handling - topic as str, payload decode
                topic_str = str(message.topic)  # Topic is 'Topic' object → str (e.g., "harboursense/traffic/update/agv001")
                payload_bytes = message.payload  # Bytes
                payload_str = payload_bytes.decode('utf-8', errors='ignore')  # Handle any encoding issues
                payload = json.loads(payload_str)  # Parse JSON
                logger.debug(f"Analyzer MQTT: Topic={topic_str}, Payload sample={payload_str[:100]}...")

                # Extract edge_id from topic (e.g., /update/agv001 → agv001)
                topic_parts = topic_str.split('/')
                edge_id = topic_parts[-1] if len(topic_parts) > 0 else 'unknown'
                logger.info(f"Received path update from {edge_id}: remainingPath={payload.get('remainingPath', [])}")

                # Always trigger full analysis on any update (metrics, loads, etc.)
                await self.analyze_metrics(triggered_by=f"Path update from {edge_id}")

                # Reroute only if path short (e.g., <3 nodes, near end/congestion change)
                remaining_path = payload.get('remainingPath', [])
                if len(remaining_path) < 3:
                    start = payload.get('currentLocation', 'A1')
                    
                    # Fetch dynamic finalNode from DB (scrub Nulls/invalids)
                    edge_doc = await self.db.edgeDevices.find_one({'id': edge_id})
                    if edge_doc:
                        destination_raw = edge_doc.get('finalNode') or edge_doc.get('destinationNode', 'C5')
                        # FIXED: Scrub Null/None variants
                        if destination_raw in ['Null', None, 'null', 'None', 'undefined', '']:
                            destination = 'C5'
                            logger.debug(f"Scrubbed invalid destination for {edge_id}; fallback to {destination}")
                        else:
                            destination = str(destination_raw).strip().replace('->', '-')  # Clean up
                    else:
                        destination = 'C5'
                        logger.warning(f"No DB doc for {edge_id}; fallback destination {destination}")
                    
                    logger.debug(f"Reroute check for {edge_id}: start={start}, dest={destination}, current path len={len(remaining_path)}")

                    # Get current state for computation (consistent method names)
                    node_loads = self.get_current_loads()
                    route_congestion = self.get_route_congestion()  # FIXED: Singular (adjust if yours is plural)
                    predicted_loads = self.get_predicted_loads()
                    
                    # Compute new path
                    new_path = self.planner.compute_path(
                        start, destination, node_loads, route_congestion, predicted_loads=predicted_loads
                    )
                    
                    if new_path and new_path != remaining_path:
                        # Estimate ETA (add estimate_eta if missing: return (len(path)-1) / speed)
                        edge_doc_retry = await self.db.edgeDevices.find_one({'id': edge_id})  # Re-fetch for speed
                        edge_speed = edge_doc_retry.get('speed', 10) if edge_doc_retry else 10
                        eta = (len(new_path) - 1) / edge_speed if len(new_path) > 1 else 0  # Hops / speed
                        await self.mqtt_client.publish(
                            f"harboursense/traffic/update/{edge_id}",  # FIXED: Specific topic for republish
                            json.dumps({'suggestedPath': new_path, 'eta': eta})
                        )
                        logger.info(f"Rerouted {edge_id} from {start} to {destination}: {new_path} (ETA: {eta}s)")
                    else:
                        logger.debug(f"No improved path for {edge_id} (current: {remaining_path}; new: {new_path})")
                else:
                    logger.debug(f"Path sufficient for {edge_id} - len: {len(remaining_path)}")

            except UnicodeDecodeError as e:
                logger.error(f"Payload decode error on {topic_str}: {e}; raw bytes: {message.payload[:50]}...")
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error on {topic_str}: {e}; raw: {payload_str[:100]}...")
            except (AttributeError, IndexError) as e:
                logger.error(f"Topic parse error ({topic_str}): {e}; skipping message")
            except Exception as e:
                logger.error(f"Unexpected MQTT error on {topic_str}: {e}; continuing...")
            
            # FIXED: Reconnect only on disconnects (not every message) - let aiomqtt handle minor issues
            if not self.mqtt_client.is_connected():  # Assume you have this check; else add await self._ensure_connected() every 10 msgs
                logger.warning("MQTT disconnected; attempting reconnect...")
                await self._ensure_connected()
                await self.mqtt_client.subscribe(subscribe_topic)  # Re-subscribe post-reconnect

        logger.info("MQTT listener ended (e.g., on shutdown)")
