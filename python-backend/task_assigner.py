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



        # FIXED: Extended path for "go to place" (logical start from device_loc; enroute_start if needed)
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
                # Enroute to required_place first, then extend
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



        # Update edge (enroute_start if needs movement to start/place; assigned if already there)
        next_node = path[1] if len(path) > 1 else final_node
        eta = 0 if device_loc == required_place else len(path) / edge.get('speed', 10)  # 0 if no go-to needed
        updates = {
            'taskPhase': 'enroute_start' if device_loc != required_place else 'assigned',  # FIXED: Enroute for go-to; assigned if at place
            'finalNode': final_node,
            'path': path,
            'nextNode': next_node,
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



    async def _assign_stage_device(self, shipment_id, phase, task_details, edge_id=None):
        """Core: Assign by phase, dynamic nodes/types. FIXED: required_place for go-to; use destination."""
        shipment = await self.db.shipments.find_one({'id': shipment_id})
        if not shipment:
            logger.warning(f"Shipment {shipment_id} not found")
            return None



        assigned_edges = shipment.get('assignedEdges', [])
        if any(phase in entry for entry in assigned_edges):
            logger.debug(f"{shipment_id} already {phase}; skip")
            return None



        # FIXED: Dynamic device_type (crane for dock/berth; agv otherwise for offload)
        device_type_map = {
            'offload': 'crane' if self._is_dock_or_berth(shipment.get('currentNode', 'A1')) else 'agv',  # Crane for docks/berths
            'transport': 'truck',
            'store': 'robot',
            'delivery': 'truck',  # NEW: Truck for delivery
            'berth_unload': 'agv'  # Future phase
        }
        device_type = device_type_map.get(phase, 'truck')
        task_details['phase'] = phase



        # Dynamic task_details (start from current, final per phase)
        current_node = shipment.get('currentNode', 'A1')
        task_details['startNode'] = current_node
        destination = shipment.get('destination', self._nearest_warehouse(current_node))  # From Manager's assign



        # FIXED: Define required_place (dock for offload/transport; warehouse for store)
        if phase == 'offload':
            required_place = shipment.get('arrivalNode', current_node)  # Dock (A1) for arrived
            task_details['finalNode'] = required_place  # Stationary offload
        elif phase == 'transport':
            required_place = current_node  # Dock (A1) post-offload for pickup
            task_details['finalNode'] = destination  # Assigned warehouse (B4)
        elif phase == 'store':
            required_place = destination  # Warehouse (B4) for storage
            task_details['pickupNode'] = required_place  # From warehouse
            task_details['finalNode'] = required_place  # Stationary store (extend to sub if needed)
        elif phase == 'delivery':
            # NEW: Pickup from warehouse, deliver to E5
            required_place = destination  # Warehouse for pickup
            task_details['finalNode'] = 'E5'  # Exit gate
            task_details['pickupNode'] = required_place  # From warehouse
        else:
            required_place = current_node
            task_details['finalNode'] = destination or 'B4'



        # Add to task_details for path computation
        task_details['requiredPlace'] = required_place
        logger.debug(f"{phase} for {shipment_id}: required_place={required_place}, final={task_details['finalNode']}, dest={destination}")



        assigned = await self.assign_task(device_type, task_details, edge_id)
        if assigned:
            await self.db.edgeDevices.update_one(
                {'id': assigned},
                {'$set': {'assignedShipment': shipment_id}}
            )
            logger.info(f"Assigned {assigned} ({device_type}) for {phase} on {shipment_id} at {current_node} (go to {required_place})")
        return assigned



    async def handle_completion(self, device_id, task_payload):
        """Reset on complete; update shipment status based on phase/node."""
        edge = await self.db.edgeDevices.find_one({'id': device_id})
        if not edge:
            logger.warning(f"No edge found for {device_id}; skipping completion")
            return



        # Extract key fields from task_payload to avoid recursion
        shipment_id = task_payload.get('shipmentId') or edge.get('assignedShipment')
        phase = task_payload.get('phase', 'unknown')
        current_node = edge.get('currentLocation', 'A1')
        new_status = {'offload': 'offloaded', 'transport': 'transported', 'store': 'stored', 'delivery': 'completed'}.get(phase, 'completed')



        # Reset edge fully, including shipment links
        await self.db.edgeDevices.update_one(
            {'id': device_id},
            {'$set': {
                'taskPhase': 'idle' if phase != 'delivery' else 'returning',  # NEW: 'returning' for delivery trucks
                'task': 'idle',
                'path': [],
                'finalNode': None,
                'nextNode': None,
                'shipmentId': None,
                'assignedShipment': None,
                'updatedAt': datetime.now()
            }}
        )
        logger.info(f"Reset edge {device_id} to {'idle' if phase != 'delivery' else 'returning'} at {current_node}")



        # Update shipment if linked
        if shipment_id:
            update_data = {
                'status': new_status,
                'currentNode': current_node,
                'updatedAt': datetime.now()
            }
            if phase == 'delivery':
                # NEW: Mark fully completed with exit timestamp
                update_data.update({
                    'deliveryStatus': 'delivered',
                    'exitTime': datetime.now(),
                    'deliveryEndAt': datetime.now()
                })
            await self.db.shipments.update_one(
                {'id': shipment_id},
                {'$set': update_data}
            )
            logger.info(f"Completed {phase} for {shipment_id} at {current_node}; status → {new_status}")



        # FIXED: Flat payload to prevent recursion/nesting
        completion_payload = {
            'deviceId': device_id,
            'status': 'completed',
            'shipmentId': shipment_id,
            'phase': phase,
            'endLocation': current_node,
            'completedAt': datetime.now().isoformat()
            # REMOVED: 'task': task_payload  # Avoids potential circular reference or deep nesting
        }
        
        # FIXED: Add source flag to break potential MQTT loops (manager can check != 'internal')
        completion_payload['source'] = 'task_assigner'
        
        # Publish to a dedicated internal topic to avoid manager re-handling completions
        await self.mqtt_client.publish(
            f"harboursense/internal/completion/{device_id}",
            json.dumps(completion_payload)
        )
        logger.debug(f"Published flat completion for {device_id}: {completion_payload}")



        # Optional: Trigger analyzer if needed
        await self.analyzer.analyze_metrics(triggered_by=f"Completion {device_id} ({phase})")



        # NEW: For delivery, delay truck return to idle
        if phase == 'delivery':
            asyncio.create_task(self._delay_truck_return(device_id))



        logger.info(f"Completion handled for {device_id} ({phase}) at {current_node}")



    async def _delay_truck_return(self, truck_id, delay_secs=12):
        """NEW: Helper for truck return delay (simulate 12s return trip after delivery)."""
        await asyncio.sleep(delay_secs)
        await self.db.edgeDevices.update_one(
            {'id': truck_id},
            {'$set': {'taskPhase': 'idle', 'path': [], 'currentLocation': 'E5'}}  # Back at exit or origin
        )
        logger.info(f"Truck {truck_id} returned idle after {delay_secs}s")



    async def monitor_and_assign(self):
        """Dynamic loop: Scan shipments by status/node, assign phases."""
        while True:
            try:
                logger.info("Monitor: Scanning shipments...")
                shipments = await self.db.shipments.find().sort('updatedAt', -1).to_list(None)
                
                # Prioritize pending: offloaded first, then arrived, then transported
                pending_offloaded = [s for s in shipments if s.get('status') == 'offloaded' and not any('transport' in str(e) for e in s.get('assignedEdges', []))]
                pending_arrived = [s for s in shipments if s.get('status') in ['arrived', 'waiting'] and not any('offload' in str(e) for e in s.get('assignedEdges', []))]
                pending_transported = [s for s in shipments if s.get('status') == 'transported' and not any('store' in str(e) for e in s.get('assignedEdges', []))]
                
                # NEW: Delivery scan: Post-storage assignments (>30s elapsed, pending)
                pending_delivery = [s for s in shipments if (
                    s.get('status') == 'stored' and
                    s.get('deliveryStatus') == 'pending' and
                    s.get('storageCompleteAt') and
                    s.get('storageCompleteAt') < datetime.now() - timedelta(seconds=30)
                )]
                logger.debug(f"Delivery scan: {len(pending_delivery)} stored shipments ready for truck assignment")
                
                # Assign pending transports (trucks for offloaded)
                for shipment in pending_offloaded:
                    shipment_id = shipment['id']
                    current_node = shipment.get('currentNode', 'A1')
                    if self._is_dock_or_berth(current_node):
                        logger.info(f"Transport assign for {shipment_id} from {current_node} to warehouse")
                        transport_details = {'shipmentId': shipment_id}
                        await self._assign_stage_device(shipment_id, 'transport', transport_details)
                    await asyncio.sleep(0.1)  # Yield for concurrency
                
                # Offloads (cranes)
                for shipment in pending_arrived:
                    shipment_id = shipment['id']
                    current_node = shipment.get('currentNode', 'A1')
                    if self._is_dock_or_berth(current_node):
                        logger.info(f"Offload assign for {shipment_id} at {current_node} (dock/berth)")
                        offload_details = {'shipmentId': shipment_id, 'destNode': current_node}
                        await self._assign_stage_device(shipment_id, 'offload', offload_details)
                    await asyncio.sleep(0.1)
                
                # Stores (robots/forklifts during transport)
                for shipment in pending_transported:
                    shipment_id = shipment['id']
                    warehouse = shipment.get('destination', self._nearest_warehouse(shipment.get('currentNode', 'A1')))
                    if warehouse == shipment.get('currentNode', 'A1'):  # Already at warehouse
                        logger.info(f"Store assign for {shipment_id} from {warehouse} to {shipment.get('destination', 'C5')}")
                        store_details = {'shipmentId': shipment_id, 'pickupNode': warehouse}
                        await self._assign_stage_device(shipment_id, 'store', store_details)
                    await asyncio.sleep(0.1)
                
                # NEW: Delivery assignments (trucks post-storage)
                for shipment in pending_delivery:
                    shipment_id = shipment['id']
                    warehouse = shipment.get('destination') or shipment.get('warehouseAssigned', 'B4')  # From earlier assignment
                    if not warehouse:
                        logger.warning(f"No warehouse for {shipment_id}; skipping delivery")
                        continue
                    
                    # Find idle truck
                    idle_trucks = await self.db.edgeDevices.find({
                        'type': 'truck',  # Assume trucks tagged as such
                        'taskPhase': 'idle',
                        'shipmentId': None
                    }).sort('id', 1).to_list(1)  # Nearest/first idle
                    
                    if not idle_trucks:
                        await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'deliveryStatus': 'queued'}})
                        logger.warning(f"No idle truck for {shipment_id}; queued")
                        continue
                    
                    truck = idle_trucks[0]
                    truck_id = truck['id']
                    
                    # Compute path: warehouse → E5 (exit) - but assign_task handles via details
                    delivery_details = {
                        'shipmentId': shipment_id,
                        'pickupNode': warehouse
                    }
                    assigned = await self._assign_stage_device(shipment_id, 'delivery', delivery_details, truck_id)
                    if assigned:
                        # Update shipment
                        await self.db.shipments.update_one(
                            {'id': shipment_id},
                            {'$set': {
                                'deliveryStatus': 'assigned',
                                'assignedTruck': truck_id,
                                'updatedAt': datetime.now()
                            }}
                        )
                        logger.info(f"Assigned truck {truck_id} for delivery of {shipment_id} from {warehouse} to E5")
                        # Trigger analyzer for traffic impact
                        await self.analyzer.analyze_metrics(f"delivery_{shipment_id}")
                        
                        # Publish to port.js
                        await self.mqtt_client.publish(
                            f"harboursense/shipments/{shipment_id}/delivery",
                            json.dumps({'truck': truck_id, 'status': 'delivering'})
                        )
                    await asyncio.sleep(0.1)
                
                logger.debug(f"Monitor cycle: Assigned {len(pending_offloaded) + len(pending_arrived) + len(pending_transported) + len(pending_delivery)} tasks")


                # Handle completing edges
                completing = await self.db.edgeDevices.find({'taskPhase': 'completing'}).to_list(None)
                for edge in completing:
                    await self.handle_completion(edge['id'], edge.get('task', {}))



                await asyncio.sleep(3)  # Faster for pendings
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(10)



    async def _select_warehouse(self, current_node, shipment_id):
        """Dynamic warehouse selection using graph (nearest by dist/cap/load).
        Returns best warehouse ID (e.g., 'B4') or 'C5' fallback."""
        if not self.graph:
            logger.warning(f"No graph for warehouse selection (shipment {shipment_id}); fallback 'C5'")
            return 'C5'
        
        # Filter warehouses from graph (types: B4/D2/E5)
        warehouses = {nid: ndata for nid, ndata in self.graph.items() if ndata.get('type') == 'warehouse'}
        if not warehouses:
            logger.warning(f"No warehouses in graph for {shipment_id}; fallback 'C5'")
            return 'C5'
        
        logger.debug(f"Selecting warehouse for {shipment_id} at {current_node}; available: {list(warehouses.keys())}")
        
        # Get predicted loads from analyzer (for dynamic load avoidance)
        predicted_loads = self.analyzer.get_predicted_loads()
        
        best_warehouse = None
        best_score = float('inf')  # Lower score better
        
        for wh_id, wh_data in warehouses.items():
            # Distance: Manhattan from current_node (parse grid: A1=1,1; B4=2,4)
            if current_node not in self.graph:
                dist = float('inf')
            else:
                c_row, c_col = self._node_to_coords(current_node)
                w_row, w_col = self._node_to_coords(wh_id)
                dist = abs(c_row - w_row) + abs(c_col - w_col)
            
            # Capacity score (higher better, normalize 0-1)
            cap = wh_data.get('capacity', 0)
            cap_score = cap / 35.0  # Max cap=35 (B4/E5)
            
            # Load score (lower better: current + predicted; avoid >80% full)
            current_load = wh_data.get('currentOccupancy', 0)
            pred_load = predicted_loads.get(wh_id, 0)
            total_load = current_load + pred_load
            load_score = total_load / cap if cap > 0 else float('inf')  # % full
            
            # Combined score: 0.6*dist + 0.2*(1-cap_score) + 0.2*load_score (weighted)
            score = 0.6 * dist + 0.2 * (1 - cap_score) + 0.2 * load_score
            
            logger.debug(f"Warehouse {wh_id}: dist={dist}, cap_score={cap_score:.2f}, load_score={load_score:.2f}, total_score={score:.2f}")
            
            if score < best_score:
                best_score = score
                best_warehouse = wh_id
        
        if best_warehouse:
            reason = f"nearest/low-load (score {best_score:.2f})"
            logger.info(f"Selected warehouse {best_warehouse} for {shipment_id} at {current_node} ({reason})")
            return best_warehouse
        else:
            logger.warning(f"No valid warehouse score for {shipment_id}; fallback 'C5'")
            return 'C5'



    def _node_to_coords(self, node_id):
        """Helper: Parse node to (row, col) for Manhattan dist (A1=1,1; B4=2,4)."""
        row = ord(node_id[0].upper()) - ord('A') + 1  # A=1, B=2, etc.
        col = int(node_id[1:]) if node_id[1:].isdigit() else 1  # A1 col=1
        return row, col


    # NEW: Maintenance task assignment (integrated with analyzer.planner; no self.planner)
    async def assign_maintenance_task(self, node, db, mqtt_client):
        """Assign idle robot/truck for repair/inspection on anomalous node."""
        logger.info(f"Assigning maintenance for anomaly at {node}")
        idle_edges = await db.edgeDevices.find({"type": {"$in": ["robot", "truck"]}, "taskPhase": "idle"}).to_list(None)
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
