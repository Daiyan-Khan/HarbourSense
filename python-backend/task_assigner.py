import json
import logging
import asyncio
from datetime import datetime
import heapq
from math import inf
import string

logging.basicConfig(level=logging.DEBUG)

# Helper from traffic_analyzer.py
def node_to_coords(node):
    if len(node) != 2: return None
    letter, number = node[0].upper(), node[1:]
    if letter not in string.ascii_uppercase or not number.isdigit(): return None
    y = ord(letter) - ord('A')
    x = int(number)
    return (y, x)

class SmartRoutePlanner:  # Merged from traffic_analyzer.py (superior for real-time traffic analysis)
    def __init__(self, graph, blocked_nodes=None):
        self.graph = graph
        self.blocked = set(blocked_nodes or [])

    def get_neighbors(self, node):
        if node in self.blocked: return []
        return [(k, v) for k, v in self.graph.get(node, {}).get('neighbors', {}).items() if k not in self.blocked]

    def compute_path(self, start, end, node_loads, route_congestion, predicted_loads, capacity_threshold=0.8):
        if not self.graph or start not in self.graph or end not in self.graph:  # Early check for empty/invalid graph/start/end
            logging.warning(f"Cannot compute path: Graph empty or missing start/end nodes ({start} to {end})")
            return None
        distances = {node: inf for node in self.graph}
        distances[start] = 0
        previous = {node: None for node in self.graph}
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
        return path if path and path[0] == start else None  # Return None if no path found

async def analyze_traffic(db):  # Merged/adapted from traffic_analyzer.py with fixes
    graph_doc = await db.graph.find_one()
    route_caps_doc = await db.routeCapacities.find_one()
    graph = graph_doc.get('nodes', {}) if graph_doc else {}
    route_capacities = route_caps_doc.get('capacities', {}) if route_caps_doc else {}
    
    # Fetch anomalies
    anomalies = [doc async for doc in db.sensorAlerts.find({'alert': True})]
    anomaly_penalties = {a['node']: 5 if a.get('severity') == 'high' else 2 for a in anomalies}
    
    # Example blocked nodes (customize based on your setup)
    blocked_nodes = ['C3', 'dockA1', 'loading_zone']
    
    # Fetch edges for loads/predictions with to_list for safety
    edges = await db.edgeDevices.find().to_list(None)
    route_load = {}
    node_loads = {}
    predicted_loads = {}
    for edge in edges:
        if not edge or not isinstance(edge, dict):  # Null/dict check
            logging.warning("Skipping invalid edge (None or non-dict)")
            continue
        # Idle-only check per workflow
        if edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            logging.info(f"Skipping non-idle edge {edge.get('id', 'unknown')} (phase: {edge.get('taskPhase')}, task: {edge.get('task')})")
            continue
        current_loc = edge.get('currentLocation', 'na').replace('-', '--')  # Safe get + default
        next_node_val = edge.get('nextNode', "Null")  # Use string placeholder
        if next_node_val == "Null":  # Handle "Null" as per string logic
            logging.info(f"Skipping edge {edge.get('id', 'unknown')} with 'Null' nextNode")
            continue
        next_node = next_node_val.replace('-', '--')  # Now safe
        route_key = f"{current_loc}-{next_node}"
        route_load[route_key] = route_load.get(route_key, 0) + 1
        node_loads[current_loc] = node_loads.get(current_loc, 0) + 1
        
        # Predict based on ETA/finalNode with checks
        if 'eta' in edge and next_node and edge.get('eta') != "N/A":
            predicted_loads[next_node] = predicted_loads.get(next_node, 0) + 1  # Arriving soon
        if all(k in edge for k in ['finalNode', 'journeyTime']):  # Key existence check
            if edge.get('finalNode') != "None" and edge.get('journeyTime') != "N/A" and edge['journeyTime'] < 60:  # Near-term with string checks
                final_node = edge.get('finalNode', 'na').replace('-', '--')  # Safe
                predicted_loads[final_node] = predicted_loads.get(final_node, 0) + 1
    
    # Compute congestion (unchanged, as inputs are now safe)
    route_congestion = {}
    for route_key, load in route_load.items():
        capacity = route_capacities.get(route_key, 3)  # Default 3
        ratio = load / capacity if capacity > 0 else 0
        level = 'low' if ratio < 0.5 else 'medium' if ratio < 0.8 else 'high'
        route_congestion[route_key] = {'ratio': ratio, 'level': level}
    
    node_congestion = {}
    for node, load in node_loads.items():
        capacity = graph.get(node, {}).get('capacity', 5)  # Default 5
        ratio = load / capacity if capacity > 0 else 0
        level = 'high' if ratio > 0.8 else 'low'
        node_congestion[node] = {'ratio': ratio, 'level': level}
    
    # Suggestions (with checks)
    suggestions = []
    route_planner = SmartRoutePlanner(graph, blocked_nodes)
    for edge in edges:
        if not edge or not isinstance(edge, dict) or edge.get('taskPhase') != 'idle' or edge.get('task') != 'idle':
            continue  # Idle-only
        current_node = edge.get('currentLocation', 'na').replace('-', '--')
        destination_val = edge.get('finalNode', "None")
        if destination_val == "None":
            destination = current_node  # Safe default
        else:
            destination = destination_val.replace('-', '--')
        suggested_path = route_planner.compute_path(current_node, destination, node_loads, route_congestion, predicted_loads)
        suggestions.append({'edgeId': edge.get('id', 'unknown'), 'suggestedPath': suggested_path})  # Simplified
    
    # Store results
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

class TaskAssigner:
    def __init__(self, db, mqtt_client, graph):
        self.db = db
        self.mqtt_client = mqtt_client
        self.smart_planner = SmartRoutePlanner(graph)  # Directly use superior SmartRoutePlanner (from traffic_analyzer)

    def distance(self, a, b):
        return abs(ord(a[0]) - ord(b[0])) + abs(int(a[1:]) - int(b[1:]))

    async def assign_task(self, edge_type, task_details):
        # Get latest traffic data (using superior analyzer)
        traffic_data = await analyze_traffic(self.db)
        
        # Find edges with to_list
        all_edges = await self.db.edgeDevices.find({'type': edge_type}).to_list(None)
        idle_edges = []
        for e in all_edges:
            if not e or not isinstance(e, dict):  # Null/dict check
                continue
            if e.get('task') == 'idle' and e.get('taskPhase') == 'idle':  # Idle-only per workflow
                idle_edges.append(e)
        
        if not idle_edges:
            logging.debug(f"No idle {edge_type}s available for task {task_details}")
            return

        # Select closest idle edge
        selected = min(idle_edges, key=lambda e: self.distance(e.get('currentLocation', 'na'), task_details.get('startNode', 'na')))

        # Intelligent destination selection (e.g., find suitable warehouse)
        suitable_warehouses = await self.db.graph.find({'type': 'warehouse', 'capacity': {'$gt': 0}}).to_list(None)  # Mock: Add node attributes to graph
        if not suitable_warehouses:
            logging.warning("No available warehouses for shipment")
            return
        task_details['destinationNode'] = min(suitable_warehouses, key=lambda w: self.distance(w.get('id', 'na'), task_details.get('startNode', 'na'))).get('id', 'na')  # Closest warehouse

        # NEW: Validation per your suggestion - Handle "Null" nextNode based on state
        if selected.get('nextNode') == "Null":
            if selected.get('taskPhase') == 'idle':
                # Idle: Set finalNode first, then compute path from currentLocation to destination
                logging.info(f"Edge {selected.get('id')} is idle with Null nextNode - Setting finalNode and computing path")
                await self.db.edgeDevices.update_one({'id': selected.get('id')}, {'$set': {'finalNode': task_details['destinationNode']}})
                path_start = selected.get('currentLocation', 'na')  # Use current for reassignment
            elif selected.get('taskPhase') in ['enroute_to_complete', 'executing']:  # Completing: Wait, don't assign
                logging.info(f"Edge {selected.get('id')} is completing task with Null nextNode - Skipping assignment until idle")
                return
            else:
                logging.warning(f"Unexpected state for edge {selected.get('id')} with Null nextNode - Skipping")
                return
        else:
            path_start = task_details.get('startNode', 'na')  # Default to startNode if nextNode not Null

        # Compute path using superior SmartRoutePlanner with traffic data
        path = self.smart_planner.compute_path(
            path_start, 
            task_details.get('destinationNode', 'na'), 
            traffic_data['node_loads'],
            traffic_data['route_congestion'],
            traffic_data['predicted_loads']
        )

        if not path:
            logging.warning(f"No valid path found for {selected.get('id')} from {path_start} to {task_details['destinationNode']} - Skipping assignment")
            return  # Graceful skip

        # Assign with phase, using string placeholders where appropriate
        update = {
            'task': task_details.get('task', 'unknown'),
            'path': path,
            'nextNode': path[1] if len(path) > 1 else "Null",
            'finalNode': task_details.get('destinationNode', 'na'),
            'taskPhase': 'enroute_to_start',  # New: Start phase
            'eta': "N/A",
            'taskCompletionTime': "N/A"
        }
        await self.db.edgeDevices.update_one({'id': selected.get('id')}, {'$set': update})
        self.mqtt_client.publish(f"harboursense/edge/{selected.get('id')}/task", json.dumps(update))
        logging.debug(f"Assigned task to {selected.get('id')} with phase 'enroute_to_start' to {task_details['destinationNode']} using traffic-aware path: {path}")

    async def handle_shipment(self, shipment):
        if not shipment or not isinstance(shipment, dict) or 'arrivalNode' not in shipment:  # Null/key check
            logging.error(f"Invalid shipment: missing 'arrivalNode' or not dict - cannot assign")
            return  # Or handle gracefully, e.g., set a default or skip

        task_details = {
            'task': 'transport shipment',
            'startNode': shipment.get('arrivalNode', 'na'),  # Fixed: Use 'arrivalNode' from port.js, safe
            'destinationNode': 'C3'  # Or compute intelligently as per previous suggestions
        }
        await self.assign_task('truck', task_details)  # Assuming truck for transport; adjust if needed
        await self.db.shipments.update_one({'id': shipment.get('id')}, {'$set': {'status': 'assigned'}})

    async def monitor_and_assign(self):
        while True:
            # Check for waiting shipments and assign
            shipments = await self.db.shipments.find({'status': 'waiting'}).to_list(None)
            for s in shipments:
                await self.handle_shipment(s)
            await asyncio.sleep(5)  # Poll interval

    def suggest_next_node(self, traffic_data, current, dest):
        # Basic implementation: Get possible next nodes from graph (assuming self.smart_planner.graph has 'nodes')
        possible = list(self.smart_planner.get_neighbors(current))
        if not possible:
            return 'na'
        # Select best based on route load from traffic_data
        return min(possible, key=lambda n: traffic_data.get('route_load', {}).get(f"{current}-{n[0]}", float('inf')))[0] or 'na'

    async def complete_task(self, device_id):  # With checks
        device = await self.db.edgeDevices.find_one({'id': device_id})
        if not device or not isinstance(device, dict):
            logging.warning(f"Cannot complete task for invalid device {device_id}")
            return
        await self.db.edgeDevices.update_one(
            {'id': device_id},
            {'$set': {'nextNode': "Null", 'finalNode': "Null", 'task': 'idle', 'taskPhase': 'idle', 'eta': "N/A", 'taskCompletionTime': "N/A", 'journeyTime': "N/A"}}
        )
        logging.info(f"Reset {device_id} to idle with string placeholders - ready for reassignment")
        # Re-trigger assignment if shipments pending
        await self.monitor_and_assign()  # Or specific call
