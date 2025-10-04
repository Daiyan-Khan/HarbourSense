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
    'arrived': {'description': 'Shipment at dock', 'next_stage': 'unloading'},
    'unloading': {'description': 'Unloading at dock', 'next_stage': 'processing', 'device_types': ['crane', 'robot']},
    'processing': {'description': 'Internal processing/moving', 'next_stage': 'transporting', 'device_types': ['agv', 'conveyor']},
    'transporting': {'description': 'Transport to warehouse', 'next_stage': 'loading', 'device_types': ['truck']},
    'loading': {'description': 'Loading at warehouse', 'next_stage': 'stored', 'device_types': ['robot', 'crane']},
    'stored': {'description': 'Shipment stored', 'next_stage': None}
}

def node_to_coords(node):
    if len(node) != 2: return None
    letter, number = node[0].upper(), node[1:]
    if letter not in string.ascii_uppercase or not number.isdigit(): return None
    y = ord(letter) - ord('A')
    x = int(number)
    return (y, x)

class SmartRoutePlanner:
    def __init__(self, graph, blocked_nodes=None):
        self.graph = graph
        self.blocked = set(blocked_nodes or [])

    def get_neighbors(self, node):
        if node in self.blocked: return []
        return [(k, v) for k, v in self.graph.get(node, {}).get('neighbors', {}).items() if k not in self.blocked]

    def compute_path(self, start, end, node_loads, route_congestion, predicted_loads, capacity_threshold=0.8):
        if not self.graph or start not in self.graph or end not in self.graph:
            logger.warning(f"Cannot compute path: missing start/end ({start} to {end})")
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
        return path if path and path[0] == start else None

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

    async def monitor_and_assign(self):
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
