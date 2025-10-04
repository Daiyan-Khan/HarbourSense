import json
import logging
import asyncio
from datetime import datetime
import heapq
from math import inf
import string
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SHIPMENT_STAGES for lifecycle (as you wanted)
SHIPMENT_STAGES = {
    'arrived': {'description': 'Shipment at dock', 'next_stage': 'unloading', 'est_duration': 0},  # No device needed
    'unloading': {'description': 'Crane offload at dock', 'next_stage': 'processing', 'device_types': ['crane'], 'est_duration': 30},  # Seconds for crane task
    'processing': {'description': 'AGV moves to staging/unload prep', 'next_stage': 'transporting', 'device_types': ['agv'], 'est_duration': 20},  # AGV staging
    'transporting': {'description': 'Truck transport to warehouse', 'next_stage': 'unloading_truck', 'device_types': ['truck'], 'est_duration': 60},  # Includes travel
    'unloading_truck': {'description': 'Forklift/AGV unloads truck', 'next_stage': 'storing', 'device_types': ['agv'], 'est_duration': 40},  # Forklift role
    'storing': {'description': 'Robots place in warehouse', 'next_stage': 'stored', 'device_types': ['robot'], 'est_duration': 25},
    'stored': {'description': 'Shipment stored', 'next_stage': None, 'est_duration': 0}
}


def node_to_coords(node):
    if len(node) != 2: return None
    letter, number = node[0].upper(), node[1:]
    if letter not in string.ascii_uppercase or not number.isdigit(): return None
    y = ord(letter) - ord('A')
    x = int(number)
    return (y, x)

    
class TaskAssigner:
    def __init__(self, db, mqtt_client, analyzer):
        self.db = db
        self.mqtt_client = mqtt_client
        self.analyzer = analyzer
        self.logger = logger

    async def _ensure_mqtt_connected(self):
        """Basic connectivity check - assumes client is managed by manager.py"""
        try:
            # For aiomqtt, client connection is managed by context manager in manager.py
            # We don't need to manually connect here
            return True
        except Exception as e:
            logger.warning(f"MQTT connectivity issue: {e}")
            return False

    async def _assign_stage_device(self, shipment_id, stage, task_details):
        """Assign device for specific lifecycle stage"""
        if stage not in SHIPMENT_STAGES or SHIPMENT_STAGES[stage]['next_stage'] is None:
            logger.warning(f"Invalid or final stage {stage} for shipment {shipment_id}")
            return None

        stage_info = SHIPMENT_STAGES[stage]
        device_types = stage_info.get('device_types', ['truck'])

        # Determine destination based on stage
        destination_node = task_details.get('destinationNode')
        if not destination_node:
            if stage == 'unloading':
                destination_node = 'B2'  # Processing area
            elif stage == 'processing':
                destination_node = 'B3'  # Pre-warehouse staging
            elif stage == 'transporting':
                # Pick warehouse from graph
                graph_nodes = self.analyzer.graph
                warehouses = [node_id for node_id, node in graph_nodes.items() if node.get('type') == 'warehouse']
                destination_node = warehouses[0] if warehouses else 'B4'
            elif stage == 'loading':
                destination_node = task_details.get('warehouseNode', 'C3')
            logger.info(f"Dynamic destination for stage {stage}: {destination_node}")

        # Try to assign from preferred device types
        assigned_id = None
        for device_type in device_types:
            device = await self.db.edgeDevices.find_one({
                "type": device_type,
                "task": "idle",
                "taskPhase": "idle"
            })
            if device:
                assigned_id = device['id']
                break

        if not assigned_id:
            logger.warning(f"No idle devices available for stage {stage} (tried {device_types})")
            return None

        # Get traffic metrics for intelligent pathing
        await self.analyzer.analyze_metrics(f"Assignment for {assigned_id} in stage {stage}")
        node_loads = self.analyzer.get_current_loads()
        route_congestion = self.analyzer.get_route_congestion()
        predicted_loads = self.analyzer.get_predicted_loads()

        start_node = task_details.get('startNode', 'A1')
        path = self.analyzer.planner.compute_path(
            start_node, destination_node,
            node_loads=node_loads,
            route_congestion=route_congestion,
            predicted_loads=predicted_loads
        )
        
        if not path or len(path) < 2:
            logger.warning(f"Invalid path computed for {assigned_id} in stage {stage}: {path}")
            return None

        # Update device in DB
        task_name = f"{stage}_shipment_{shipment_id}"
        await self.db.edgeDevices.update_one(
            {"id": assigned_id},
            {"$set": {
                "task": task_name,
                "taskPhase": "en_route_start",
                "startNode": start_node,
                "finalNode": destination_node,
                "path": path,
                "currentLocation": start_node,
                "shipmentId": shipment_id
            }}
        )

        # Update shipment with stage progress
        await self.db.shipments.update_one(
            {"id": shipment_id},
            {"$set": {
                "status": stage,
                "assignedDevice": assigned_id,
                "assignedStage": stage,
                "assignedAt": datetime.now(),
                "currentStageDestination": destination_node
            }}
        )

        # Publish task via MQTT
        task_payload = {
            "shipment_id": shipment_id,
            "task": task_name,
            "stage": stage,
            "startNode": start_node,
            "finalNode": destination_node,
            "path": path
        }
        
        if await self._ensure_mqtt_connected():
            try:
                await self.mqtt_client.publish(f"harboursense/edge/{assigned_id}/task", json.dumps(task_payload))
                logger.info(f"Published task for stage {stage} to {assigned_id}")
            except Exception as e:
                logger.error(f"Failed to publish task for {assigned_id}: {e}")
        else:
            logger.error(f"Cannot publish task for {assigned_id}: MQTT connection failed")

        # Optional: Publish to traffic for monitoring
        if len(path) > 1:
            try:
                await self.mqtt_client.publish(f"harboursense/traffic/{assigned_id}", json.dumps({'path': path, 'stage': stage}))
            except Exception as e:
                logger.warning(f"Failed to publish traffic update for {assigned_id}: {e}")

        logger.info(f"Assigned {device_type} {assigned_id} for shipment {shipment_id} stage {stage}: {start_node} -> {destination_node}")
        return assigned_id

    async def assign_task(self, device_type, task_details):
        """Legacy single-stage assign (for backward compatibility)"""
        task_details['shipment_id'] = task_details.get('shipment_id', 'unknown')
        return await self._assign_stage_device(task_details['shipment_id'], 'transporting', task_details)

    async def calculate_chain_eta(self, shipment_id, start_stage, device_types_chain, shipment_location):
        """Estimate total time for stage chain (travel + task). Returns dict of ETAs per stage."""
        total_eta = 0
        etas = {}
        shipment = await self.db.shipments.find_one({"id": shipment_id})
        current_loc = shipment.get('currentLocation', shipment_location)  # Dock aware
        
        for stage in [start_stage] + [SHIPMENT_STAGES[s]['next_stage'] for s in [start_stage] if SHIPMENT_STAGES[s]['next_stage']]:
            if stage is None: break
            stage_info = SHIPMENT_STAGES[stage]
            dest_node = self._get_stage_destination(stage, shipment)  # Helper to get dynamic dest
            
            # Pick sample device for speed (query idle one)
            sample_device = await self.db.edgeDevices.find_one({
                "type": device_types_chain.get(stage, stage_info['device_types'][0]),
                "taskPhase": "idle"
            })
            speed = sample_device.get('speed', 5) if sample_device else 5  # Default units/sec
            
            # Compute path distance (nodes * avg edge weight)
            node_loads = self.analyzer.get_current_loads()
            path = self.analyzer.planner.compute_path(current_loc, dest_node, node_loads, 
                                                    self.analyzer.get_route_congestion(), 
                                                    self.analyzer.get_predicted_loads())
            dist = len(path) - 1 if path else 10  # Fallback distance
            travel_time = (dist / speed) * 60  # Convert to seconds
            
            task_time = stage_info['est_duration']
            stage_eta = travel_time + task_time
            etas[stage] = stage_eta
            total_eta += stage_eta
            current_loc = dest_node  # Update for next stage
        
        # Get next shipment ETA (from db or MQTT forecast)
        next_shipment = await self.db.shipments.find_one({"status": "arrived"}, sort=[("createdAt", 1)])
        next_eta = (next_shipment.get('predictedArrival', datetime.now()) - datetime.now()).total_seconds() if next_shipment else float('inf')
        
        return {'total_chain': total_eta, 'per_stage': etas, 'next_shipment_gap': next_eta - total_eta}  # Positive gap = room to pre-position


    async def monitor_and_assign(self):
        """Enhanced monitor with predictive chaining"""
        while True:
            try:
                shipments = await self.db.shipments.find().sort("createdAt", 1).to_list(None)
                for shipment in shipments:
                    shipment_id = shipment.get('id')
                    current_status = shipment.get('status', 'arrived')
                    
                    if current_status == 'stored':
                        continue
                    
                    next_stage = SHIPMENT_STAGES.get(current_status, {}).get('next_stage')
                    if next_stage is None:
                        continue
                    
                    # Existing assignment logic...
                    task_details = {
                        'shipment_id': shipment_id,
                        'startNode': shipment.get('currentLocation', 'A1'),
                        'destinationNode': shipment.get('currentStageDestination')
                    }
                    
                    # NEW: Predictive check if this is unloading (crane trigger)
                    if next_stage == 'unloading' and current_status == 'arrived':
                        # Calculate chain ETA from unloading onward
                        chain_types = {
                            'unloading': ['crane'],
                            'processing': ['agv'],
                            'transporting': ['truck'],
                            'unloading_truck': ['agv'],  # Forklift
                            'storing': ['robot']
                        }
                        eta_data = await self.calculate_chain_eta(shipment_id, 'unloading', chain_types, shipment.get('arrivalNode', 'A1'))
                        
                        if eta_data['next_shipment_gap'] > 30:  # 30s buffer for pre-positioning
                            logger.info(f"Pre-positioning for {shipment_id}: gap {eta_data['next_shipment_gap']}s > chain {eta_data['total_chain']}s")
                            
                            # Assign crane first (as priority)
                            crane_id = await self._assign_stage_device(shipment_id, 'unloading', task_details)
                            if crane_id:
                                # Pre-assign truck to dock (wait for crane completion via MQTT)
                                truck_details = task_details.copy()
                                truck_details['startNode'] = 'A1'  # Dock
                                truck_details['destinationNode'] = 'D5'  # Warehouse
                                truck_details['prePosition'] = True  # Flag for port.js to idle-wait
                                await self._assign_stage_device(shipment_id, 'transporting', truck_details)
                                
                                # Pre-position AGV (forklift) at warehouse unload zone
                                agv_details = task_details.copy()
                                agv_details['startNode'] = agv_details.get('currentLocation', 'B3')
                                agv_details['destinationNode'] = 'D4'  # Pre-move to unload zone
                                await self._assign_stage_device(shipment_id, 'unloading_truck', agv_details)
                                
                                # Pre-position robots at storage
                                robot_details = task_details.copy()
                                robot_details['startNode'] = robot_details.get('currentLocation', 'C3')
                                robot_details['destinationNode'] = 'D5'
                                await self._assign_stage_device(shipment_id, 'storing', robot_details)
                                
                                # MQTT notify chain (for port.js to sequence on crane arrival)
                                await self.mqtt_client.publish(
                                    f"harboursense/shipment/{shipment_id}/chain_prepped",
                                    json.dumps({"shipment_id": shipment_id, "prepped_stages": ["transporting", "unloading_truck", "storing"]})
                                )
                        else:
                            # No time: Assign sequentially as before
                            assigned_id = await self._assign_stage_device(shipment_id, next_stage, task_details)
                    
                    else:
                        # Non-unloading: Standard assign
                        active_in_stage = await self.db.edgeDevices.count_documents({
                            'task': {'$regex': f"{next_stage}_shipment_"},
                            'taskPhase': {'$ne': 'idle'}
                        })
                        if active_in_stage < 3:  # Concurrency limit
                            assigned_id = await self._assign_stage_device(shipment_id, next_stage, task_details)
                            if assigned_id:
                                await self.analyzer.analyze_metrics(f"Assignment in {next_stage} for {shipment_id}")
                
                await asyncio.sleep(10)
            
            except Exception as e:
                logger.error(f"Error in monitor_and_assign: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

            """Monitor shipments by stage and assign intelligently"""
            while True:
                try:
                    # Fetch all shipments, sort by creation time
                    shipments = await self.db.shipments.find().sort("createdAt", 1).to_list(None)
                    processed = 0
                    
                    for shipment in shipments:
                        shipment_id = shipment.get('id')
                        current_status = shipment.get('status', 'arrived')
                        
                        if current_status == 'stored':
                            continue  # Completed

                        # Determine next stage
                        next_stage = SHIPMENT_STAGES.get(current_status, {}).get('next_stage')
                        if next_stage is None:
                            continue

                        # Stage-specific task details
                        task_details = {
                            'shipment_id': shipment_id,
                            'startNode': shipment.get('currentLocation', shipment.get('arrivalNode', 'A1')),
                            'destinationNode': shipment.get('currentStageDestination')  # From previous assignment
                        }

                        # Limit concurrent assignments
                        active_in_stage = await self.db.edgeDevices.count_documents({
                            'task': {'$regex': f"{next_stage}_shipment_"},
                            'taskPhase': {'$ne': 'idle'}
                        })
                        max_per_stage = 3
                        if active_in_stage >= max_per_stage:
                            logger.info(f"Max active ({max_per_stage}) for stage {next_stage}; skipping shipment {shipment_id}")
                            continue

                        assigned_id = await self._assign_stage_device(shipment_id, next_stage, task_details)
                        if assigned_id:
                            processed += 1
                            # Trigger analyzer update
                            await self.analyzer.analyze_metrics(f"New assignment in {next_stage} for {shipment_id}")

                    if processed == 0:
                        logger.debug("No new assignments needed")
                        
                    await asyncio.sleep(10)  # Poll interval
                    
                except Exception as e:
                    logger.error(f"Error in monitor_and_assign: {e}")
                    logger.error(f"Full traceback:\n{traceback.format_exc()}")
                    await asyncio.sleep(5)

    async def handle_completion(self, device_id):
        """Handle task completion and advance shipment stage"""
        try:
            device = await self.db.edgeDevices.find_one({"id": device_id})
            if not device or device.get('task') == 'idle':
                return

            shipment_id = device.get('shipmentId')
            if not shipment_id:
                logger.warning(f"No shipment linked to completing device {device_id}")
                return

            shipment = await self.db.shipments.find_one({"id": shipment_id})
            if not shipment:
                return

            current_stage = shipment.get('assignedStage', 'arrived')
            next_stage = SHIPMENT_STAGES.get(current_stage, {}).get('next_stage')

            # Update shipment location and status
            new_location = device.get('currentLocation', shipment.get('currentStageDestination'))
            await self.db.shipments.update_one(
                {"id": shipment_id},
                {"$set": {
                    "status": next_stage if next_stage else 'stored',
                    "currentLocation": new_location,
                    "stageCompletedAt": datetime.now(),
                    "completedDevice": device_id
                }}
            )

            # Reset device to idle
            await self.db.edgeDevices.update_one(
                {"id": device_id},
                {"$set": {
                    "task": "idle",
                    "taskPhase": "idle",
                    "path": [],
                    "shipmentId": None,
                    "startNode": "Null",
                    "finalNode": "Null"
                }}
            )

            logger.info(f"Completed stage {current_stage} for shipment {shipment_id} by {device_id}; advanced to {next_stage or 'stored'}")

            # If final stage, notify
            if next_stage is None:
                await self.mqtt_client.publish(
                    f"harboursense/shipment/{shipment_id}/completed",
                    json.dumps({"shipment_id": shipment_id, "status": "stored", "location": new_location})
                )

        except Exception as e:
            logger.error(f"Error handling completion for {device_id}: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
    async def _get_stage_destination(self, stage, shipment):
        """
        Dynamically determine destination node based on stage, shipment data, and graph.
        Uses analyzer.graph for node types and proximity.
        """
        if not shipment:
            logger.warning(f"No shipment provided for stage {stage}; using default staging")
            return list(self.analyzer.graph.keys())[0] if self.analyzer.graph else 'B1'
        
        current_loc = shipment.get('currentLocation', shipment.get('arrivalNode'))
        arrival_node = shipment.get('arrivalNode')  # Dock from arrival
        target_warehouse = shipment.get('targetWarehouse')  # If pre-assigned in manager.py
        
        # Helper to get nearest node of type to a location
        def nearest_node_of_type(node_type, to_loc):
            candidates = [node for node, data in self.analyzer.graph.items() 
                        if data.get('type') == node_type]
            if not candidates:
                logger.warning(f"No {node_type} nodes in graph")
                return None
            
            # Use Manhattan distance via coords (from your node_to_coords)
            to_coords = node_to_coords(to_loc)
            if not to_coords:
                # Fallback: alphabetical/node ID proximity (simple for grid)
                return min(candidates, key=lambda n: abs(ord(n[0]) - ord(to_loc[0])) + abs(int(n[1:]) - int(to_loc[1:])))
            
            def node_dist(n):
                n_coords = node_to_coords(n)
                return abs(to_coords[0] - n_coords[0]) + abs(to_coords[1] - n_coords[1]) if n_coords else float('inf')
            
            nearest = min(candidates, key=node_dist)
            logger.debug(f"Nearest {node_type} to {to_loc}: {nearest} (dist: {node_dist(nearest)})")
            return nearest
        
        # Stage-specific dynamic logic
        if stage == 'unloading':
            if not arrival_node:
                logger.warning(f"No arrivalNode in shipment {shipment.get('id')}; query nearest dock")
                arrival_node = nearest_node_of_type('dock', current_loc or 'A1')
            return arrival_node  # Shipment's actual dock
        
        elif stage == 'processing':
            # Staging area near dock
            staging = nearest_node_of_type('staging', arrival_node or current_loc)
            return staging or 'B3'  # Graph fallback
        
        elif stage == 'transporting':
            if target_warehouse:
                return target_warehouse  # Use pre-assigned from manager/shipment
            warehouse = nearest_node_of_type('warehouse', arrival_node or current_loc)
            if warehouse:
                shipment['targetWarehouse'] = warehouse  # Cache for future stages
                await self.db.shipments.update_one({"id": shipment.get('id')}, {"$set": {"targetWarehouse": warehouse}})
            return warehouse or list(self.analyzer.graph.keys())[-1]  # Last node as fallback (likely warehouse)
        
        elif stage == 'unloading_truck':
            warehouse = shipment.get('targetWarehouse') or nearest_node_of_type('warehouse', current_loc)
            if warehouse:
                # Find unload zone near warehouse (type='unload' or adjacent staging)
                unload_zone = nearest_node_of_type('unload_zone', warehouse) or nearest_node_of_type('staging', warehouse)
                return unload_zone or f"{warehouse[:-1]}4"  # e.g., if D5 -> D4 (simple offset; tune per graph)
            return 'D4'  # Rare fallback
        
        elif stage == 'storing':
            return shipment.get('targetWarehouse') or nearest_node_of_type('warehouse', current_loc)
        
        elif stage == 'loading':  # If needed for outbound
            return shipment.get('currentStageDestination') or nearest_node_of_type('loading_zone', current_loc)
        
        else:
            # Default: Use shipment's last destination or nearest staging
            return shipment.get('currentStageDestination') or nearest_node_of_type('staging', current_loc) or 'B3'

