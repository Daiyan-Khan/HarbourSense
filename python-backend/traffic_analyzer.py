import logging
import sys
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
from mqtt_config import build_aiomqtt_params, get_mqtt_settings
from edge_view import load_merged_edges
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
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(console_handler)

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

NULL_NODE_SENTINELS = {None, "Null", "null", "None", "undefined", ""}


def is_missing_node(value):
    return value in NULL_NODE_SENTINELS


def normalize_node_id(value):
    """Normalize node IDs: strip hyphens, reject null sentinels."""
    if is_missing_node(value):
        return None
    return str(value).strip().replace("->", "-").replace("-", "")


def _neighbor_ids(planner_or_graph, node):
    """Return adjacent node IDs for a graph node."""
    if planner_or_graph is None:
        return []
    if hasattr(planner_or_graph, "get_neighbors"):
        return [normalize_node_id(neighbor) for neighbor, _ in planner_or_graph.get_neighbors(node)]
    node_data = planner_or_graph.get(node, {}) if isinstance(planner_or_graph, dict) else {}
    neighbors = node_data.get("neighbors", {}) if isinstance(node_data, dict) else {}
    ids = []
    for _, neighbor in neighbors.items():
        normalized = normalize_node_id(neighbor)
        if normalized:
            ids.append(normalized)
    return ids


def validate_path_adjacency(path, planner_or_graph):
    """Ensure each consecutive path pair is a graph neighbor; return path or None."""
    if not path or not isinstance(path, list):
        return None
    if len(path) < 2:
        return [normalize_node_id(path[0])] if normalize_node_id(path[0]) else None

    normalized_path = []
    for node in path:
        normalized = normalize_node_id(node)
        if not normalized:
            return None
        normalized_path.append(normalized)

    for idx in range(len(normalized_path) - 1):
        current = normalized_path[idx]
        nxt = normalized_path[idx + 1]
        if nxt not in _neighbor_ids(planner_or_graph, current):
            logger.warning(f"Non-adjacent hop in path: {current} -> {nxt}")
            return None
    return normalized_path


def trim_path_from_current(path, current_location):
    """Trim path so it starts at current_location when possible."""
    current = normalize_node_id(current_location)
    if not path or not current:
        return path
    normalized = [normalize_node_id(node) for node in path]
    if not normalized or normalized[0] == current:
        return path
    if current in normalized:
        start_idx = normalized.index(current)
        return path[start_idx:]
    return None

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

    def safe_float_recursive(self, val, default=0.0, context=""):
        """Recursive-safe float helper used by manager reroute checks."""
        if isinstance(val, dict):
            for key in ("value", "ratio", "load"):
                if key in val:
                    return self.safe_float_recursive(val[key], default, context)
            logger.warning(f"Nested dict in {context or 'loads'}: {val}; using default {default}")
            return default
        return self.safe_float(val, default)

    def compute_path(self, start, end, node_loads, route_congestion, capacity_threshold=0.8, predicted_loads=None):
        if predicted_loads is None:
            predicted_loads = {}
        # FIXED: Scrub Null/None destinations (from idle edges)
        if is_missing_node(end):
            logger.warning(f"Invalid end node {end} (likely idle default); fallback to 'C5'")
            end = 'C5'
        if is_missing_node(start):
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

    def _bfs_shortest_path(self, start, end):
        """Plain BFS without congestion weighting."""
        start = normalize_node_id(start)
        end = normalize_node_id(end)
        if not start or not end:
            return None
        if start == end:
            return [start]
        nodes = list(self.flat_graph.keys()) if isinstance(self.flat_graph, dict) else []
        if not nodes or start not in nodes or end not in nodes:
            return None

        previous = {node: None for node in nodes}
        queue = [start]
        visited = {start}
        while queue:
            current = queue.pop(0)
            if current == end:
                break
            for neighbor, _ in self.get_neighbors(current):
                neighbor = normalize_node_id(neighbor)
                if not neighbor or neighbor in visited:
                    continue
                visited.add(neighbor)
                previous[neighbor] = current
                queue.append(neighbor)

        if previous.get(end) is None and end != start:
            return None

        path = []
        current = end
        while current is not None:
            path.append(current)
            current = previous.get(current)
        path.reverse()
        return path if path and path[0] == start else None

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
        if path and path[-1] == end:
            validated = validate_path_adjacency(path, self)
            if validated:
                return validated

        bfs_path = self._bfs_shortest_path(start, end)
        if bfs_path:
            validated = validate_path_adjacency(bfs_path, self)
            if validated:
                logger.info(f"Unweighted BFS fallback {start} -> {end}: {validated}")
                return validated

        logger.warning(f"No valid adjacent path {start} -> {end}; skipping teleport fallback")
        return None

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

    # FIXED: _ensure_connected (around line 272) - Create client if None, no _connected assumption
    async def _ensure_connected(self):
        """Ensure MQTT client is connected; create if None."""
        if self.mqtt_client is None:
            mqtt_settings = get_mqtt_settings()
            self.mqtt_client = aiomqtt.Client(**build_aiomqtt_params("traffic_analyzer"))
            await self.mqtt_client.__aenter__()  # Connect explicitly
            logger.info(f"TrafficAnalyzer: Created MQTT client in {mqtt_settings.mode} mode")
        else:
            # Existing check (safe now, as None handled above)
            try:
                if not self.mqtt_client._connected:
                    await self.mqtt_client.__aenter__()  # Reconnect if needed
                    logger.debug("TrafficAnalyzer: Reconnected existing MQTT client")
            except AttributeError:
                # Fallback if no _connected (older aiomqtt)
                await self.mqtt_client.__aenter__()
        return self.mqtt_client

    async def analyze_metrics(self, triggered_by=""):
        """Analyze traffic from DB, update internal state, compute congestion/loads. Enhanced with sensor alerts for predictions."""
        logger.info(f"Starting analysis, triggered by {triggered_by}")

        # Fetch graph and capacities from MongoDB with fallback
        graph_doc = await self.db.graph.find_one()
        route_caps_doc = await self.db.routeCapacities.find_one()

        if graph_doc:
            db_graph = graph_doc.get('nodes', {})
            graph = self.planner.flat_graph.copy()
            graph.update(db_graph)  # Merge/override
        else:
            graph = parse_graph(GRAPH_LIST)

        route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}

        # NEW: Integrate sensor alerts for predicted load boosts (repairs/congestion)
        recent_alerts = await self.db.sensorAlerts.find({"resolved": False}).to_list(None)  # Unresolved anomalies
        alert_penalties = {}  # node: penalty (e.g., 20 for high vibration → virtual load)
        for alert in recent_alerts:
            node = alert.get("node", "")
            severity = alert.get("severity", "low")
            anomaly_type = alert.get("alert_type", "")
            if anomaly_type in ["vibration_spike", "occupancy_high"] and node:  # Core repair triggers
                penalty = 20 if severity == "high" else 10  # Boost predicted load (simulates repair tasks)
                alert_penalties[node] = max(alert_penalties.get(node, 0), penalty)
                logger.debug(f"Applied sensor penalty {penalty} to predicted load for {node} (alert: {anomaly_type}, severity: {severity})")

        # Define blocked nodes (e.g., docks, non-route points) - adapt based on your graph
        blocked_nodes = ["C3", "dockA1", "loadingzone"]  # Example: block specific docks or areas

        # Fetch edges (now including posted ETA, journeyTime, finalNode for predictions)
        edges = await load_merged_edges(self.db)

        route_load = {}
        node_loads = {}
        predicted_loads = defaultdict(int)  # Initialize with sensor penalties
        # FIXED: Optional wh filter (boost only if node is warehouse; add your wh list)
        warehouses = ['B4', 'D2', 'E5']  # Known wh nodes
        for node, penalty in alert_penalties.items():
            if node in graph:  # Congested road nodes affect routing as well as warehouses.
                predicted_loads[node] += penalty
                logger.debug(f"Wh-specific penalty {penalty} for {node}")

        # For congestion prediction
        for edge in edges:
            if not edge or not isinstance(edge, dict):  # Null/dict check
                logger.warning("Skipping invalid edge document")
                continue

            # Traffic load comes from devices moving along an active route.
            if edge.get('taskPhase') in ("idle", "completing"):
                continue
            if not edge.get("nextNode"):
                logger.debug(f"Skipping non-idle edge {edge.get('id')}: task={edge.get('task')}, phase={edge.get('taskPhase')}")
                continue

            current_loc_raw = edge.get('currentLocation')
            if is_missing_node(current_loc_raw):
                current_loc = "na"  # Placeholder for idle
            else:
                current_loc = str(current_loc_raw).replace("-", "")  # Standardize

            next_node_raw = edge.get('nextNode')
            if is_missing_node(next_node_raw):
                next_node = None
            else:
                next_node = str(next_node_raw).replace("-", "")  # Standardize

            if next_node is None:
                continue

            route_key = f"{current_loc}-{next_node}"
            route_load[route_key] = route_load.get(route_key, 0) + 1
            node_loads[current_loc] = node_loads.get(current_loc, 0) + 1

            # FIXED: Safe get/None scrub for all node fields (get returns None if key=None, so explicit check)
            if 'eta' in edge and edge.get('eta') != "NA" and next_node is not None:
                if next_node in warehouses:  # Wh-only pred inc (focus)
                    predicted_loads[next_node] += 1  # Predict arrival load

            if 'finalNode' in edge and 'journeyTime' in edge:
                final_node_raw = edge.get('finalNode')
                journey_time = self.planner.safe_float(edge.get('journeyTime'), inf)
                if not is_missing_node(final_node_raw) and journey_time < 60:  # Near-term with string checks
                    final_node = str(final_node_raw).replace("-", "")  # Standardize, safe
                    if not is_missing_node(final_node):  # FIXED: Skip Null predictions
                        if final_node in warehouses:  # Wh-only
                            predicted_loads[final_node] += 1  # Predict future loads based on ETA and finalNode with string checks

        # Dynamic congestion per route: ratio and level
        route_congestion = {}
        for route_key, load in route_load.items():
            cap_val = route_capacities.get(route_key, 3)
            capacity = self.planner.safe_float(cap_val, 3.0)  # FIXED: Safe, prevents dict
            ratio = load / capacity if capacity > 0 else 0
            level = "high" if ratio > 0.8 else "medium" if ratio > 0.5 else "low"
            route_congestion[route_key] = {"ratio": ratio, "level": level}  # FIXED: Safe capacity; no dict

        # Node congestion: dynamic, assuming node capacities in graph
        node_congestion = {}
        for node, load in node_loads.items():
            capacity_raw = graph.get(node, {}).get('capacity', 5)  # Default 5, fetch from graph if available
            capacity = self.planner.safe_float(capacity_raw, 5.0)  # FIXED: Safe
            ratio = load / capacity if capacity > 0 else 0
            level = "high" if ratio > 0.8 else "low"
            node_congestion[node] = {"ratio": ratio, "level": level}

        # Update internal state
        self.node_loads = node_loads
        self.route_congestion = route_congestion
        self.predicted_loads = predicted_loads  # Now includes sensor boosts (wh-focused)

        # Optional: Generate suggestions and update edges (idle-only, like before)
        suggestions = []
        route_planner = SmartRoutePlanner(graph, blocked_nodes)
        for edge in edges:
            if not edge or not isinstance(edge, dict) or edge.get('taskPhase') != "idle" or edge.get('task') == "idle":
                continue  # Idle-only

            current_node_raw = edge.get('currentLocation')
            if is_missing_node(current_node_raw):
                current_node = "na"
            else:
                current_node = str(current_node_raw).replace("-", "")  # Standardize

            destination_raw = edge.get('finalNode')
            if is_missing_node(destination_raw):
                logger.debug(f"Skipping idle edge {edge.get('id')} with Null finalNode (no route needed)")
                continue  # FIXED: No path computation for undefined idle

            destination_val = str(destination_raw).replace("-", "")  # Standardize, safe
            destination = destination_val
            neighbors = route_planner.get_neighbors(current_node)  # Filtered neighbors

            # FIXED: Safe get/None scrub for current and destination
            suggested_path = route_planner.compute_path(current_node, destination, node_loads, route_congestion, predicted_loads=predicted_loads)  # Compute intelligent path using congestion data, predictions, and blocking

            if not suggested_path or len(suggested_path) <= 2:
                best_node = None
                best_eta = "NA"
                min_eta = float('inf')
                # For finding the best...
                for neighbor, dist in neighbors:
                    neighbor = str(neighbor).replace("-", "")  # FIXED: Safe str standardize
                    route_key = f"{current_node}-{neighbor}"
                    congestion = route_congestion.get(route_key, {"ratio": 0, "level": "low"})
                    if congestion["level"] == "high":
                        continue  # Skip highly congested routes
                    speed = edge.get('speed', 10)
                    eta = dist / speed * (1 + congestion["ratio"])  # Adjust ETA for congestion
                    if eta < min_eta:
                        min_eta = eta
                        best_node = neighbor

                # Fallback to best single-hop if full path not computable
                if best_node is None and neighbors:
                    best_neighbor, best_dist = next(iter(neighbors))
                    best_node = str(best_neighbor).replace("-", "")  # FIXED: Safe str standardize
                    best_eta = best_dist / edge.get('speed', 10)
                elif min_eta != float('inf'):
                    best_eta = min_eta
                else:
                    best_eta = "NA"
                suggested_path = [current_node, best_node] if best_node is not None else None
            else:
                best_node = suggested_path[1]
                best_eta = sum(1 for i in range(len(suggested_path) - 1)) / edge.get('speed', 10)  # Simple unit weight ETA

            # Suggestions stored in trafficData only — do not write edgeDevices transit fields.
            route_key = f"{current_node}-{best_node}"
            suggestions.append({
                "edgeId": edge["id"],
                "currentLocation": current_node,
                "suggestedNextNode": best_node,
                "suggestedPath": suggested_path,
                "eta": best_eta,
                "congestionRatio": route_congestion.get(route_key, {"ratio": 0})["ratio"],
                "congestionLevel": route_congestion.get(route_key, {"level": "low"})["level"],
                "nodeCongestionAtNext": node_congestion.get(best_node, {"level": "low"})["level"]
            })  # Append suggestion with traffic insights

        # Store results in DB
        await self.db.trafficData.insert_one({
            "timestamp": datetime.utcnow(),
            "routeLoad": route_load,
            "nodeTraffic": node_loads,
            "routeCongestion": route_congestion,
            "nodeCongestion": node_congestion,
            "predictedLoads": dict(predicted_loads),  # Now sensor-enhanced (wh-focused)
            "suggestions": suggestions,
            "triggered_by": triggered_by,
            "anomaly_penalties": alert_penalties  # Log sensor impacts
        })

        logger.info(f"Analysis complete: {len(suggestions)} suggestions generated, triggered by {triggered_by}")

    def get_current_loads(self):
        return dict(self.node_loads)

    def get_route_congestion(self):
        return dict(self.route_congestion)

    def get_predicted_loads(self, node=None):
        """Return full dict of predicted loads, or scalar for specific node (wh-focused, sensor/ETA enhanced)."""
        loads = dict(self.predicted_loads)
        if node and node in loads:
            return loads.get(node, 0)
        return loads  # Full dict if no node

    async def start_mqtt_listener(self):
        """Metrics-only listener: traffic/update arrivals trigger analysis; reroutes handled by manager."""
        await self._ensure_connected()
        subscribe_topic = 'harboursense/traffic/update/#'
        await self.mqtt_client.subscribe(subscribe_topic)
        logger.info(f"TrafficAnalyzer metrics listener started on {subscribe_topic}")

        async for message in self.mqtt_client.messages:
            try:
                topic_str = str(message.topic)
                payload_str = message.payload.decode('utf-8', errors='ignore')
                payload = json.loads(payload_str)
                topic_parts = topic_str.split('/')
                edge_id = topic_parts[-1] if len(topic_parts) > 0 else 'unknown'
                logger.debug(
                    "Analyzer metrics: edge=%s status=%s remainingPath=%s",
                    edge_id,
                    payload.get('status'),
                    payload.get('remainingPath', []),
                )
                await self.analyze_metrics(triggered_by=f"Traffic event from {edge_id}")
            except UnicodeDecodeError as e:
                logger.error(f"Payload decode error on {topic_str}: {e}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error: {e}")
            except Exception as e:
                logger.error(f"Unexpected MQTT error: {e}")

        logger.info("MQTT metrics listener ended")
