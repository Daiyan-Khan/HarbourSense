import asyncio
import logging
import json
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from math import inf  # For distance inf
import time


logger = logging.getLogger("TaskAssigner")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)



# Fallback GRAPH_LIST (minimal; DB populated, so rarely used)
GRAPH_LIST = [
    {"id": "A1", "neighbors": {"E":"A2","S":"B1"}, "type":"dock", "capacity":8, "currentOccupancy":0},
    {"id": "B4", "neighbors": {"N":"A4","S":"C4","E":"B5","W":"B3"}, "type":"warehouse", "capacity":35, "currentOccupancy":0},
    {"id": "D2", "neighbors": {"N":"C2","S":"E2","E":"D3","W":"D1"}, "type":"warehouse", "capacity":25, "currentOccupancy":0},
    {"id": "E5", "neighbors": {"N":"D5","W":"E4"}, "type":"warehouse", "capacity":35, "currentOccupancy":0},
    # Expand with full 25 from graph.json if needed; but DB handles
]



def parse_graph(graph_data):
    """Parse graph from Manager/DB/fallback (list, dict, or single doc). Handles flat 25-doc DB."""
    graph = {}
    if isinstance(graph_data, list):
        # Flat list (DB to_list() or GRAPH_LIST)
        for node in graph_data:
            node_id = node.get('id')
            if node_id:
                graph[node_id] = {
                    'neighbors': node.get('neighbors', {}),
                    'type': node.get('type', 'route_point'),
                    'capacity': node.get('capacity', 5),
                    'currentOccupancy': node.get('currentOccupancy', 0)  # Default 0 for warehouses
                }
    elif isinstance(graph_data, dict):
        # Single doc: {'nodes': [...]} or flat {id: {...}}
        nodes_list = graph_data.get('nodes', list(graph_data.values()))  # Flatten 'nodes' or values
        if isinstance(nodes_list, list):
            return parse_graph(nodes_list)  # Recurse for list
        else:
            # Dict {id: node}
            graph = graph_data
            # Ensure occupancy for warehouses
            for node_id, node_data in graph.items():
                if node_data.get('type') == 'warehouse' and 'currentOccupancy' not in node_data:
                    graph[node_id]['currentOccupancy'] = 0
    return graph



class TaskAssigner:
    def __init__(self, db, mqtt_client, analyzer, graph=None):  # FIXED: Accepts graph=None
        self.db = db
        self.mqtt_client = mqtt_client
        self.analyzer = analyzer
        self.graph = parse_graph(graph) if graph is not None else {}  # Use provided (Manager's full 25)
        if self.graph:
            logger.info(f"Using provided graph with {len(self.graph)} nodes")
            # Verify key nodes/warehouses (B4/D2/E5 from DB)
            warehouses = [n for n, g in self.graph.items() if g.get('type') == 'warehouse']
            logger.debug(f"Available warehouses: {warehouses}")
            # Idempotent occupancy ensure (Manager adds; safe here too)
            for node_id in self.graph:
                if self.graph[node_id].get('type') == 'warehouse' and 'currentOccupancy' not in self.graph[node_id]:
                    self.graph[node_id]['currentOccupancy'] = 0
                    logger.debug(f"Ensured occupancy=0 for warehouse {node_id}")
        else:
            # Fallback async load (e.g., if direct init without Manager)
            asyncio.create_task(self._load_graph())



    async def _load_graph(self):
        """Fallback: Load full graph from DB (enhanced for 25 docs). Skips if already loaded."""
        if self.graph:
            logger.debug("Graph already loaded; skipping fallback")
            return
        try:
            raw_nodes = await self.db.graph.find().to_list(None)  # FIXED: to_list for full 25 (not find_one)
            logger.debug(f"Fallback raw graph docs count: {len(raw_nodes)}; sample: {json.dumps(raw_nodes[:1], default=str) if raw_nodes else 'Empty'}")
            if raw_nodes:
                # Handle flat list (your DB) or single {'nodes': [...]}
                if len(raw_nodes) == 1 and 'nodes' in raw_nodes[0]:
                    self.graph = parse_graph(raw_nodes[0]['nodes'])
                else:
                    self.graph = parse_graph(raw_nodes)  # Flat 25 docs
                logger.info(f"Fallback loaded graph with {len(self.graph)} nodes from DB")
                node_types = {n: g['type'] for n, g in self.graph.items() if 'type' in g}
                logger.debug(f"Node types (fallback): {dict(list(node_types.items())[:5])}")
                warehouses = [n for n, g in self.graph.items() if g.get('type') == 'warehouse']
                logger.debug(f"Fallback warehouses: {warehouses}")
            else:
                self.graph = parse_graph(GRAPH_LIST)
                logger.warning(f"DB graph empty; using GRAPH_LIST with {len(self.graph)} nodes")
        except Exception as e:
            logger.error(f"Fallback graph load failed: {e}; using GRAPH_LIST")
            self.graph = parse_graph(GRAPH_LIST)



    def _is_dock_or_berth(self, node):
        """Check if node is dock/berth for offload."""
        if node in self.graph:
            node_type = self.graph[node].get('type', '')
            return node_type in ['dock', 'berth']
        return node.startswith('A')  # Fallback: A-row docks

    def _manhattan_distance(self, node1, node2):
        """Compute Manhattan distance between two nodes using their x/y coords from self.graph. Returns float (0 if invalid/missing)."""
        if not node1 or not node2:
            logger.warning(f"Invalid nodes for distance: {node1} to {node2}; return 0")
            return 0.0
        
        n1_data = self.graph.get(node1, {})
        n2_data = self.graph.get(node2, {})
        x1 = n1_data.get('x', 0)
        y1 = n1_data.get('y', 0)
        x2 = n2_data.get('x', 0)
        y2 = n2_data.get('y', 0)
        
        # Safe float (handles int/str/None from DB/graph.json)
        try:
            x1 = float(x1) if x1 is not None else 0.0
            y1 = float(y1) if y1 is not None else 0.0
            x2 = float(x2) if x2 is not None else 0.0
            y2 = float(y2) if y2 is not None else 0.0
        except (ValueError, TypeError) as e:
            logger.warning(f"Coord parse error for {node1}-{node2}: {e}; default to 0")
            return 0.0
        
        dist = abs(x1 - x2) + abs(y1 - y2)
        logger.debug(f"Manhattan dist {node1} ({x1},{y1}) to {node2} ({x2},{y2}) = {dist}")
        return dist

    def _nearest_warehouse(self, from_node):
        """Find nearest warehouse to from_node (dynamic for multi-warehouses)."""
        warehouses = [n for n, g in self.graph.items() if g.get('type') == 'warehouse']  # B4, D2, E5
        if not warehouses:
            return 'B4'  # Default
        min_dist = inf
        nearest = 'B4'
        for wh in warehouses:
            dist = self._distance(from_node, wh)
            if dist < min_dist:
                min_dist = dist
                nearest = wh
        return nearest



    def _distance(self, loc1, loc2):
        """Manhattan distance (grid-aware)."""
        if len(loc1) < 2 or len(loc2) < 2:
            return inf
        y1, x1 = ord(loc1[0].upper()) - ord('A'), int(loc1[1:])
        y2, x2 = ord(loc2[0].upper()) - ord('A'), int(loc2[1:])
        return abs(y1 - y2) + abs(x1 - x2)



    async def assign_task(self, device_type, task_details, edge_id=None):
        """Assign specific idle edge to task; dynamic from DB, closest to start."""
        shipment_id = task_details.get('shipmentId')
        shipment = await self.db.shipments.find_one({'id': shipment_id}) if shipment_id else None
        current_node = shipment.get('currentNode', task_details.get('startNode', 'A1')) if shipment else task_details.get('startNode', 'A1')



        if not edge_id:
            # Dynamic: Find closest idle of type to current_node/start
            edges = await self.db.edgeDevices.find({
                'taskPhase': 'idle',
                'type': device_type
            }).to_list(None)
            if not edges:
                logger.warning(f"No available {device_type} for task at {current_node}")
                return None
            # Sort by distance to start_node
            start_node = task_details.get('startNode', current_node)
            edges.sort(key=lambda e: self._distance(e.get('currentLocation', 'A1'), start_node))
            edge = edges[0]
            edge_id = edge['id']
            logger.debug(f"Selected closest {device_type} {edge_id} (dist: {self._distance(edge.get('currentLocation', 'A1'), start_node)})")
        else:
            # Verify idle + type
            edge = await self.db.edgeDevices.find_one({'id': edge_id})
            if not edge or edge['taskPhase'] != 'idle' or edge['type'] != device_type:
                logger.warning(f"Edge {edge_id} not idle or wrong type ({device_type}); skipping")
                return None



        # Dynamic start/final based on phase/node (fallback; overridden by required_place in path)
        phase = task_details.get('phase', 'unknown')
        start_node = task_details.get('startNode', current_node)
        if phase == 'offload':
            final_node = start_node  # Crane stays at dock/berth
        elif phase == 'transport':
            final_node = shipment.get('destination', self._nearest_warehouse(start_node)) if shipment else self._nearest_warehouse(start_node)  # To assigned warehouse
        elif phase == 'store':
            final_node = shipment.get('destination', 'C5') if shipment else 'C5'  # To storage
            start_node = task_details.get('pickupNode', final_node)  # From warehouse
        elif phase == 'delivery':
            # NEW: For delivery, start from warehouse, final='E5' (exit)
            final_node = 'E5'  # Exit gate
            start_node = shipment.get('destination', self._nearest_warehouse(current_node)) if shipment else self._nearest_warehouse(current_node)  # Pickup from warehouse
        else:
            final_node = task_details.get('finalNode', 'B4')



        # FIXED: Extended path for "go to place" (logical start from device_loc; en_route_start if needed)
        if 'path' not in task_details or not task_details['path']:
            node_loads = self.analyzer.get_current_loads() if self.analyzer else {}
            route_congestion = self.analyzer.get_route_congestion() if self.analyzer else {}
            predicted_loads = self.analyzer.get_predicted_loads() if self.analyzer else {}
            # Safe dicts
            for d in [node_loads, route_congestion, predicted_loads]:
                if not isinstance(d, dict):
                    logger.warning(f"{d.__class__.__name__} not dict; defaulting")
                    d = {}
            
            # Logical start: From device's actual location
            device_loc = edge.get('currentLocation', start_node)  # e.g., C5 if idle there
            required_place = task_details.get('requiredPlace', start_node)  # Dock/warehouse (set in _assign_stage_device)
            final_node = task_details.get('finalNode', 'B4')
            
            # Path: device_loc → required_place (go-to first) → final_node (if different)
            if device_loc == required_place and required_place == final_node:
                # Already at place + stationary task
                path = [device_loc]
                logger.debug(f"{edge_id} already at {required_place} (stationary {phase}); path=[{device_loc}]")
            elif device_loc == required_place:
                # At place: Direct required_place → final
                path = self.analyzer.planner.compute_path(
                    required_place, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                ) or [required_place, final_node]
                logger.debug(f"{edge_id} at {required_place}; direct to {final_node}: {path}")
            else:
                # En_route to required_place first, then extend
                path_to_place = self.analyzer.planner.compute_path(
                    device_loc, required_place, node_loads, route_congestion, predicted_loads=predicted_loads
                ) or [device_loc, required_place]
                if required_place == final_node:
                    # Stationary at place (e.g., offload/store)
                    path = path_to_place
                    logger.debug(f"{edge_id} go to {required_place} only (stationary {phase}): {path}")
                else:
                    # Extend: to place → final (e.g., transport)
                    path_from_place = self.analyzer.planner.compute_path(
                        required_place, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                    ) or [required_place, final_node]
                    path = path_to_place[:-1] + path_from_place  # Merge, avoid duplicate required_place
                    logger.debug(f"{edge_id} extended: {device_loc} → {required_place} → {final_node}: {path}")
            
            # Validate: Ensure starts with device_loc; fallback Manhattan
            try:
                if len(path) < 1 or path[0] != device_loc:
                    path = [device_loc] + [required_place, final_node]
                logger.warning(f"Path validation failed for {edge_id}; fallback: {path}")
            except Exception as e:
                logger.error(f"Path error for {edge_id} {device_loc}->{final_node}: {e}; fallback")
                path = [device_loc, required_place, final_node]
        else:
            path = task_details['path']



        # For AGVs/berths: If berth and device_type='agv', specialize (future: add speed/paths)
        if self._is_dock_or_berth(final_node) and device_type == 'agv':
            logger.info(f"AGV specialized for berth {final_node}")



        full_details = {
            **task_details,
            'shipmentId': shipment_id,
            'task': phase,
            'startNode': start_node,
            'finalNode': final_node,
            'path': path
        }



        # Update edge (en_route_start if needs movement to start/place; assigned if already there)
        next_node = path[1] if len(path) > 1 else final_node
        eta = 0 if device_loc == required_place else len(path) / edge.get('speed', 10)  # 0 if no go-to needed
        updates = {
            'taskPhase': 'en_route_start' if device_loc != required_place else 'assigned',  # FIXED: En_route for go-to; assigned if at place
            'finalNode': final_node,
            'path': path,
            'nextNode': next_node,
            'path' : path,
            'eta': eta,
            'task': full_details,
            'updatedAt': datetime.now()
        }
        await self.db.edgeDevices.update_one({'id': edge_id}, {'$set': updates})



        # Update shipment assignedEdges
        assigned_entry = f"{edge_id}:{phase}"
        await self.db.shipments.update_one(
            {'id': shipment_id},
            {'$push': {'assignedEdges': assigned_entry}}
        )



        # Publish
        await self.mqtt_client.publish(
            f"harboursense/edge/{edge_id}/task",
            json.dumps(full_details)
        )



        logger.info(f"Dynamic assign {device_type} {edge_id} ({phase}) for {shipment_id}: {device_loc}->{final_node} (via {required_place}), path={path}")
        return edge_id

    async def _assign_stage_device(self, shipment_id, phase, details):
        """Assign nearest idle device for phase; compute path with loads/congestion if mobile. FIXED: Pass node_loads/route_congestion to compute_path; offload=crane (no path); try/except per candidate."""
        logger.info(f"Assigning {phase} for {shipment_id}: {details}")
        
        # Device type mapping (distinctions: tempo vs delivery trucks; robot/forklift stages)
        device_type_map = {
            'offload': 'crane',      # Stationary at dock (A1/A2); no path
            'transport': 'truck_tempo',  # Dock → wh
            'delivery': 'truck_delivery',  # Wh → E5 exit
            'store_move': 'robot',   # Wh internal move
            'store_load': 'forklift' # Wh place + occ inc
        }
        device_type = device_type_map.get(phase, 'agv')  # Fallback AGV for unknown
        
        required_place = details.get('destNode') or details.get('startNode') or details.get('pickupNode') or 'A1'
        start_node = details.get('startNode') or required_place
        final_node = details.get('finalNode')
        
        # Get current/traffic data (safe dicts; empty if analyzer None)
        node_loads = self.analyzer.get_current_loads() if self.analyzer else {}
        route_congestion = self.analyzer.get_route_congestion() if self.analyzer else {}
        predicted_loads = self.analyzer.get_predicted_loads() if self.analyzer else {}
        
        candidates = []
        try:
            # Scan idle devices of type (add location filter if needed, e.g., near wh for store)
            query = {'taskPhase': 'idle', 'type': device_type}
            if phase == 'offload': query['currentLocation'] = {'$in': ['A1', 'A2', 'A3']}  # Dock cranes only
            elif phase == 'delivery': query['currentLocation'] = {'$in': ['B4', 'D2', 'E5']}  # Wh trucks
            elif 'store' in phase: query['currentLocation'] = {'$in': ['B4', 'D2', 'E5']}  # Robots/forklifts at wh
            
            idle_devices = await self.db.edgeDevices.find(query).to_list(None)
            logger.debug(f"Found {len(idle_devices)} idle {device_type} for {phase} near {required_place}")
            
            for device in idle_devices:
                try:
                    device_id = device['id']
                    current_loc = device.get('currentLocation', start_node)
                    dist = self._manhattan_distance(current_loc, required_place)
                    
                    # Path if mobile + final_node (crane offload: no path, stationary)
                    path = None
                    if final_node and device_type != 'crane':  # Skip path for stationary offload
                        path = SmartRoutePlanner.compute_path(
                            start_node, final_node, 
                            node_loads, route_congestion, 
                            predicted_loads=predicted_loads  # Optional
                        )
                        if not path or len(path) < 2:
                            logger.warning(f"Invalid path for {device_id} {start_node}→{final_node}; skip candidate")
                            continue
                        dist += sum(self._edge_weight(p) for p in path[1:])  # Optional: Path cost add
                    else:
                        path = [start_node, final_node] if final_node else [required_place]  # Simple for stationary
                    
                    candidates.append((device_id, dist, path, device))
                    logger.debug(f"Candidate {device_id} at {current_loc}: dist={dist}, path={path[:3]}...")
                    
                except Exception as e:
                    logger.warning(f"Path/assign fail for candidate {device.get('id', 'unknown')}: {e}; next")
                    continue
            
            if candidates:
                # Nearest (min dist)
                nearest_id, _, best_path, nearest_device = min(candidates, key=lambda x: x[1])
                logger.info(f"Assigned nearest {device_type} {nearest_id} for {shipment_id} {phase} (dist={candidates[0][1]:.1f})")
                
                # Update edge (set path, task, shipment)
                task = {
                    'shipmentId': shipment_id,
                    'phase': phase,
                    'path': best_path,
                    'startNode': start_node,
                    'finalNode': final_node,
                    'subPhase': details.get('subPhase'),
                    'requiredPlace': required_place,
                    'assignedAt': datetime.now()
                }
                update_data = {
                    '$set': {
                        'taskPhase': 'assigned',
                        'shipmentId': shipment_id,
                        'assignedShipment': shipment_id,
                        'task': task,
                        'path': best_path,
                        'currentLocation': start_node,  # Sync start
                        'updatedAt': datetime.now()
                    }
                }
                await self.db.edgeDevices.update_one({'id': nearest_id}, update_data)
                
                # Publish to MQTT for sim/UI (edge start)
                if self.mqtt_client:
                    await self.mqtt_client.publish(
                        f"harboursense/edge/assign/{nearest_id}",
                        json.dumps({'device': nearest_id, 'task': task, 'shipment': shipment_id})
                    )
                
                # Add to shipment edges
                edge_str = f"{nearest_id}:{phase}"
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$push': {'assignedEdges': edge_str}, '$set': {'updatedAt': datetime.now()}}
                )
                return nearest_id
            
            else:
                logger.warning(f"No valid {device_type} candidates for {shipment_id} {phase} ({len(idle_devices)} idle checked); queue")
                queued_key = f"{phase}Queued"
                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {queued_key: True, 'updatedAt': datetime.now()}})
                return None
                
        except Exception as e:
            logger.error(f"Unexpected error in _assign_stage_device for {shipment_id} {phase}: {e}")
            return None

    def _edge_weight(self, edge_node):
        """Optional: Weight for path cost (e.g., type-based slowdown); default 1.0."""
        node = self.graph.get(edge_node, {})
        return node.get('weight', 1.0) or 1.0

    def _next_phase(self, current_phase):
        """Helper: Chain phases (offload → transport, transport → store_move, etc.)."""
        phase_chain = {
            'offload': 'transport',
            'transport': 'store_move',
            'store_move': 'store_load',
            'store_load': 'delivery',
            'delivery': 'completed'
        }
        return phase_chain.get(current_phase, 'idle')

    # Ensure _a_star_fallback and _calculate_eta exist (from prior; add if missing)
    def _a_star_fallback(self, start, goal):
        """BFS fallback."""
        if start == goal:
            return [start]
        from collections import deque
        queue = deque([([start], start)])
        visited = set([start])
        while queue:
            path, current = queue.popleft()
            if current == goal:
                return path
            for neigh in self.graph.get(current, {}).get('neighbors', {}).values():
                if neigh not in visited:
                    visited.add(neigh)
                    queue.append((path + [neigh], neigh))
        return None

    def _calculate_eta(self, edge, path, phase):
        """ETA: dist * (1/speed) + phase_time."""
        if len(path) == 1:
            return 5 if phase in ['offload', 'store_load'] else 2  # Stationary action time
        dist = len(path) - 1
        speed = edge.get('speed', 10)
        return (dist / speed) * 60 + (5 if phase in ['offload', 'delivery'] else 0)  # s


    

    async def handle_completion(db, task_assigner, device_id, task_payload, mqtt_client=None):
        logger.info(f"Handling completion for {device_id}; raw payload: {task_payload}")
        try:
            # FIXED: Handle str/bytes payload (MQTT common); loads to dict
            if isinstance(task_payload, (str, bytes)):
                if isinstance(task_payload, bytes):
                    task_payload = task_payload.decode('utf-8', errors='ignore')
                try:
                    task_payload = json.loads(task_payload)
                    logger.debug(f"Parsed completion payload: {json.dumps(task_payload, default=str)}")
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode fail for {device_id}: {e}; treat as simple completion (no task details)")
                    task_payload = {'deviceId': device_id, 'phase': 'unknown', 'shipmentId': None}  # Minimal dict
            elif not isinstance(task_payload, dict):
                logger.error(f"Invalid payload type for {device_id}: {type(task_payload)}; skip")
                return
            
            # Now safe .get()
            shipment_id = task_payload.get('shipmentId')
            phase = task_payload.get('phase', 'unknown')
            
            # Call TaskAssigner (updates edge/status)
            await task_assigner.handle_completion(device_id, task_payload)
            
            # Fallback updates (if TaskAssigner misses)
            if shipment_id:
                new_status = {'offload': 'offloaded', 'transport': 'transported', 'store_move': 'storing', 'store_load': 'stored', 'delivery': 'delivered'}.get(phase, 'completed')
                await db.edgeDevices.update_one({'id': device_id}, {'$set': {'shipmentId': None, 'assignedShipment': None, 'taskPhase': 'idle', 'task': None}})
                await db.shipments.update_one({'id': shipment_id}, {'$set': {'status': new_status, 'updatedAt': datetime.now()}})
                if phase == 'store_load':  # Forklift end: occ already +1 in monitor
                    await db.shipments.update_one({'id': shipment_id}, {'$set': {'storageCompleteAt': datetime.now(), 'deliveryStatus': 'pending'}})
                logger.info(f"Completed {phase} for {shipment_id} → {new_status}; edge {device_id} idle")
                
                # Publish
                if mqtt_client:
                    await mqtt_client.publish(f"harboursense/edge/completion/{device_id}", json.dumps({'status': 'completed', 'shipment': shipment_id, 'phase': phase}))
            
            # Re-analyze post-complete
            if task_assigner.analyzer:
                await task_assigner.analyzer.analyze_metrics(f"completion_{device_id}")
                
        except Exception as e:
            logger.error(f"Error in handle_completion for {device_id}: {e}")
            # Minimal fallback: Reset edge to idle
            await db.edgeDevices.update_one({'id': device_id}, {'$set': {'taskPhase': 'idle', 'shipmentId': None, 'assignedShipment': None, 'task': None}})

    # NEW: Helper (add after handle_completion)
    async def _await_and_assign_forklift(self, shipment_id, warehouse):
        """Wait for robot store_move complete, then assign store_load (forklift)."""
        await asyncio.sleep(15)  # Estimate robot time; adjustable
        shipment = await self.db.shipments.find_one({'id': shipment_id})
        if not shipment or shipment.get('nextPhase') != 'store_load':
            return  # Already progressed or canceled
        
        robot_complete = await self.db.edgeDevices.find_one({
            'assignedShipment': shipment_id, 
            'type': 'robot', 
            'taskPhase': 'idle'
        })
        if robot_complete:
            logger.info(f"Robot complete for {shipment_id}; assigning store_load (forklift)")
            load_details = {'shipmentId': shipment_id, 'pickupNode': warehouse, 'subPhase': 'load'}
            await self._assign_stage_device(shipment_id, 'store_load', load_details)
            # Forklift completion will trigger delivery in handle_completion above
        else:
            logger.debug(f"Robot still active for {shipment_id}; retry in 5s")
            await asyncio.sleep(5)
            await self._await_and_assign_forklift(shipment_id, warehouse)  # Retry with backoff

    async def _delay_truck_return(self, truck_id, delay_secs=12):
        """NEW: Helper for truck return delay (simulate 12s return trip after delivery)."""
        await asyncio.sleep(delay_secs)
        await self.db.edgeDevices.update_one(
            {'id': truck_id},
            {'$set': {'taskPhase': 'idle', 'path': [], 'currentLocation': 'E5'}}  # Back at exit or origin
        )
        logger.info(f"Truck {truck_id} returned idle after {delay_secs}s")



    async def monitor_and_assign(self):
        """Dynamic loop: Scan shipments by status/node, assign phases. CLEANED: TaskAssigner context (self.graph/self.analyzer); truck distinction + robot/forklift pairing; no dups with handler."""
        while True:
            try:
                logger.info("Monitor: Scanning shipments...")
                shipments = await self.db.shipments.find().sort('updatedAt', -1).to_list(None)
                
                # Prioritize scans (no offloaded/arrived/transported/stored dups – handler syncs, monitor assigns only unassigned)
                pending_offloaded = [s for s in shipments if s.get('status') == 'offloaded' and not any('transport' in str(e) for e in s.get('assignedEdges', []))]
                pending_arrived = [s for s in shipments if s.get('status') in ['arrived', 'waiting'] and not any('offload' in str(e) for e in s.get('assignedEdges', []))]
                pending_transported = [s for s in shipments if s.get('status') == 'transported' and not any('store' in str(e) for e in s.get('assignedEdges', []))]
                pending_stored = [s for s in shipments if s.get('status') == 'stored' and not any('delivery' in str(e) for e in s.get('assignedEdges', []))]
                
                logger.debug(f"Monitor counts: offloaded={len(pending_offloaded)}, arrived={len(pending_arrived)}, transported={len(pending_transported)}, stored={len(pending_stored)}")

                # Safe wh load (TaskAssigner context: self.graph/self.analyzer)
                def safe_warehouse_load(wh_id):
                    wh_data = self.graph.get(wh_id, {})
                    occ = wh_data.get('currentOccupancy', 0) or 0
                    # FIXED: Plural method for full dict, then .get(wh_id, 0); safe if analyzer None or non-dict
                    if self.analyzer:
                        try:
                            pred_loads = self.analyzer.get_predicted_loads()
                            pred = int(pred_loads.get(wh_id, 0)) if isinstance(pred_loads, dict) else 0
                        except (AttributeError, TypeError):
                            pred = 0  # Fallback on method errors (e.g., during init)
                    else:
                        pred = 0
                    total = int(occ) + pred
                    cap = wh_data.get('capacity', 35) or 35  # Wh-specific: B4=3, D2=2, E5=35
                    load_pct = total / cap if cap > 0 else 0.0
                    logger.debug(f"Wh {wh_id} load: occ={occ}, pred={pred}, total={total}/{cap} (pct={load_pct:.2f})")
                    return total, cap, load_pct

                # Transports (truck_tempo for offloaded) – queue high wh
                for shipment in pending_offloaded:
                    try:
                        shipment_id = shipment['id']
                        current_node = shipment.get('currentNode', 'A1') or 'A1'
                        if self._is_dock_or_berth(current_node):  # Class helper
                            warehouse = shipment.get('destination') or self._nearest_warehouse(current_node)
                            wh_total, wh_cap, wh_pct = safe_warehouse_load(warehouse)
                            if wh_pct < 0.8:
                                logger.info(f"Transport (truck_tempo) for {shipment_id}: {current_node} → {warehouse} ({wh_total}/{wh_cap} = {wh_pct:.1%})")
                                details = {'shipmentId': shipment_id, 'startNode': current_node, 'finalNode': warehouse}
                                assigned = await self._assign_stage_device(shipment_id, 'transport', details)  # → truck_tempo
                                if assigned:
                                    await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'transported', 'updatedAt': datetime.now()}})
                            else:
                                logger.debug(f"High wh load {wh_pct:.1%}; queue transport {shipment_id}")
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'transportQueued': True}})
                    except Exception as e:
                        logger.warning(f"Transport fail {shipment_id}: {e}")
                    await asyncio.sleep(0.1)

                # Offloads (crane for arrived)
                for shipment in pending_arrived:
                    try:
                        shipment_id = shipment['id']
                        current_node = shipment.get('currentNode', 'A1') or 'A1'
                        if self._is_dock_or_berth(current_node):
                            logger.info(f"Offload (crane) for {shipment_id} at {current_node}")
                            details = {'shipmentId': shipment_id, 'destNode': current_node}
                            assigned = await self._assign_stage_device(shipment_id, 'offload', details)
                            if assigned:
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'offloaded', 'updatedAt': datetime.now()}})
                    except Exception as e:
                        logger.warning(f"Offload fail {shipment_id}: {e}")
                    await asyncio.sleep(0.1)

                # Stores (robot_move + forklift_load for transported) – pair, queue full, priority queued
                for shipment in pending_transported:
                    try:
                        shipment_id = shipment['id']
                        current_node = shipment.get('currentNode', 'B4') or 'B4'  # Post-transport at wh
                        warehouse = shipment.get('destination') or self._nearest_warehouse(current_node)
                        if warehouse == current_node and self._is_warehouse(warehouse):  # Confirm
                            wh_total, wh_cap, wh_pct = safe_warehouse_load(warehouse)
                            if wh_total >= wh_cap:
                                logger.warning(f"Full wh {warehouse} ({wh_pct:.1%}); queue store {shipment_id}")
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'storeQueued': True}})
                                continue
                            
                            queued = shipment.get('storeQueued', False)
                            if queued: logger.info(f"Priority store for queued {shipment_id}")
                            
                            assigned_move = any('store_move' in str(e) for e in shipment.get('assignedEdges', []))
                            assigned_load = any('store_load' in str(e) for e in shipment.get('assignedEdges', []))
                            
                            if not assigned_move:
                                logger.info(f"Store_move (robot) for {shipment_id} at {warehouse}")
                                details = {'shipmentId': shipment_id, 'pickupNode': warehouse, 'finalNode': warehouse, 'subPhase': 'move'}
                                assigned = await self._assign_stage_device(shipment_id, 'store_move', details)  # Robot haul
                                if assigned:
                                    await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'storing', 'updatedAt': datetime.now()}})
                            elif assigned_move and not assigned_load:
                                robot_complete = await self.db.edgeDevices.find_one({'assignedShipment': shipment_id, 'type': 'robot', 'taskPhase': 'idle'})
                                if robot_complete:
                                    logger.info(f"Store_load (forklift) pair for {shipment_id} at {warehouse}")
                                    details = {'shipmentId': shipment_id, 'pickupNode': warehouse, 'finalNode': warehouse, 'subPhase': 'load'}
                                    assigned = await self._assign_stage_device(shipment_id, 'store_load', details)  # Forklift place
                                    if assigned:
                                        # Occ +1 + stored
                                        await self.db.graph.update_one({'id': warehouse}, {'$inc': {'currentOccupancy': 1}})
                                        await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'stored', 'storeQueued': False, 'storageCompleteAt': datetime.now(), 'updatedAt': datetime.now()}})
                                        logger.debug(f"Store pair done; occ +1 at {warehouse}")
                                else:
                                    logger.debug(f"Wait robot complete for {shipment_id}")
                            else:
                                # Fallback complete
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'stored', 'storeQueued': False, 'updatedAt': datetime.now()}})
                    except Exception as e:
                        logger.warning(f"Store fail {shipment_id}: {e}")
                    await asyncio.sleep(0.1)

                # Deliveries (truck_delivery for stored)
                for shipment in pending_stored:
                    try:
                        shipment_id = shipment['id']
                        warehouse = shipment.get('destination') or 'B4'
                        if self._is_warehouse(warehouse):
                            logger.info(f"Delivery (truck_delivery) for {shipment_id}: {warehouse} → E5")
                            details = {'shipmentId': shipment_id, 'startNode': warehouse, 'finalNode': 'E5', 'pickupNode': warehouse}
                            assigned = await self._assign_stage_device(shipment_id, 'delivery', details)  # → truck_delivery
                            if assigned:
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'delivered', 'deliveryCompleteAt': datetime.now(), 'updatedAt': datetime.now()}})
                    except Exception as e:
                        logger.warning(f"Delivery fail {shipment_id}: {e}")
                    await asyncio.sleep(0.1)

                logger.info(f"Monitor cycle: {len(pending_offloaded)+len(pending_arrived)+len(pending_transported)+len(pending_stored)} assigns")

                # Completions (poll + handle; overlaps MQTT but safe – idempotent)
                completing = await self.db.edgeDevices.find({'taskPhase': 'completing'}).to_list(None)
                for edge in completing:
                    try:
                        await self.handle_completion(edge['id'], edge.get('task', {}))  # Updates loc/status
                    except Exception as e:
                        logger.warning(f"Completion fail {edge.get('id')}: {e}")

                await asyncio.sleep(1)  # Fast poll
            
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(5)

    # Add if missing: Warehouse check helper (in task_assigner.py or here)
    def _is_warehouse(self, node):
        """Check if node is warehouse (B4/D2/E5)."""
        warehouses = ['B4', 'D2', 'E5']
        return node in warehouses


    async def _select_warehouse(self, current_node, shipment_id):
        """... (existing)"""
        if not self.graph:
            return 'C5'  # Fallback (no C5 in test-graph? Use B4)
        
        warehouses = {nid: ndata for nid, ndata in self.graph.items() if ndata.get('type') == 'warehouse'}  # B4=3, D2=2, E5=35
        if not warehouses:
            return 'B4'  # Primary
        
        # FIXED: Dynamic max_cap from graph (E5=35; avoids hardcode if changes)
        max_cap = max(wh_data.get('capacity', 35) for wh_data in warehouses.values()) or 35
        
        predicted_loads = self.analyzer.get_predicted_loads() if self.analyzer else {}  # Dict or None → {}
        if not isinstance(predicted_loads, dict):
            predicted_loads = {}
            logger.warning("Predicted loads not dict; default empty")
        
        best_warehouse = None
        best_score = float('inf')
        
        for wh_id, wh_data in warehouses.items():
            # Coords/dist (existing)
            if current_node not in self.graph:
                dist = float('inf')
            else:
                c_row, c_col = self._node_to_coords(current_node)
                w_row, w_col = self._node_to_coords(wh_id)
                dist = abs(c_row - w_row) + abs(c_col - w_col)
            
            # FIXED: Safe cap/load (all have cap, but guard occ/pred)
            cap = wh_data.get('capacity')  # e.g., 3 for B4
            if cap is None:
                cap = 35
                logger.debug(f"None cap fallback for {wh_id}")
            
            occ = wh_data.get('currentOccupancy', 0)
            safe_occ = int(occ) if occ is not None else 0  # None → 0
            
            pred_load = predicted_loads.get(wh_id, 0)
            if pred_load is None:
                pred_load = 0
                logger.debug(f"None pred_load fallback for {wh_id}")
            pred_load = int(pred_load) if isinstance(pred_load, (int, float)) else 0
            
            total_load = safe_occ + pred_load  # Now int
            load_score = total_load / cap if cap > 0 else float('inf')  # Safe / int
            
            cap_score = cap / max_cap  # Normalize to graph max (e.g., 3/35=0.09 for B4)
            
            score = 0.6 * dist + 0.2 * (1 - cap_score) + 0.2 * load_score
            logger.debug(f"{wh_id}: dist={dist}, cap_score={cap_score:.2f} (cap={cap}/{max_cap}), load_score={load_score:.2f} (total={total_load}), score={score:.2f}")
            
            if score < best_score:
                best_score = score
                best_warehouse = wh_id
        
        if best_warehouse:
            logger.info(f"Selected {best_warehouse} for {shipment_id} at {current_node} (score {best_score:.2f}; favors low dist/load, high cap)")
            return best_warehouse
        return 'B4'  # Fallback primary warehouse


    def _node_to_coords(self, node_id):
        """Helper: Parse node to (row, col) for Manhattan dist (A1=1,1; B4=2,4)."""
        row = ord(node_id[0].upper()) - ord('A') + 1  # A=1, B=2, etc.
        col = int(node_id[1:]) if node_id[1:].isdigit() else 1  # A1 col=1
        return row, col


    # NEW: Maintenance task assignment (integrated with analyzer.planner; no self.planner)
    async def assign_maintenance_task(self, node, db, mqtt_client):
        """Assign idle robot/truck for repair/inspection on anomalous node."""
        logger.info(f"Assigning maintenance for anomaly at {node}")
        idle_edges = await db.edgeDevices.find({"type": {"$in": ["robot"]}, "taskPhase": "idle"}).to_list(None)
        if not idle_edges:
            logger.warning(f"No idle edges for maintenance at {node}")
            return


        edge = idle_edges[0]  # Pick first idle
        maint_task = {
            "shipmentId": f"repair_{node}_{int(time.time())}",  # Temp shipment ID
            "phase": "maintenance",
            "startNode": edge["currentLocation"],
            "finalNode": node,
            "requiredPlace": node,
            "task": "inspect_repair",
            "path": self.analyzer.planner.find_shortest_path(edge["currentLocation"], node) if self.analyzer.planner else [edge["currentLocation"], node]  # Use analyzer.planner; fallback
        }
        await db.edgeDevices.update_one({"id": edge["id"]}, {"$set": {"taskPhase": "en_route_start", "task": maint_task}})
        await mqtt_client.publish(f"harboursense/edge/{edge['id']}/task", json.dumps(maint_task))
        logger.info(f"Assigned {edge['id']} for repair at {node}")
