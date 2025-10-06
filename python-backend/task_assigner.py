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


# SHIPMENT_STAGES for lifecycle
SHIPMENT_STAGES = {
    'arrived': {'description': 'Shipment at dock', 'next_stage': 'unloading', 'est_duration': 0},
    'unloading': {'description': 'Crane offload at dock', 'next_stage': 'processing', 'device_types': ['crane'], 'est_duration': 30},
    'processing': {'description': 'AGV moves to staging/unload prep', 'next_stage': 'transporting', 'device_types': ['agv'], 'est_duration': 20},
    'transporting': {'description': 'Truck transport to warehouse', 'next_stage': 'unloading_truck', 'device_types': ['truck'], 'est_duration': 60},
    'unloading_truck': {'description': 'Forklift/AGV unloads truck', 'next_stage': 'storing', 'device_types': ['agv'], 'est_duration': 40},
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
    # Class constants for dock/warehouse
    DOCK_NODE = 'A1'
    WAREHOUSE_NODES = ['C5', 'D5', 'E4']  # From graph; make dynamic via config or shipment.destination

    def __init__(self, db, mqtt_client, analyzer):
        self.db = db
        self.mqtt_client = mqtt_client
        self.analyzer = analyzer
        self.logger = logger

    async def find_available_edge(self, edge_coll, device_types):
        """Find idle edge of specific type(s) (crane/forklift/etc.). Updated to handle list of types."""
        if isinstance(device_types, str):
            device_types = [device_types]  # Ensure list
        for device_type in device_types:
            available = await edge_coll.find_one({
                'type': device_type,
                'taskPhase': 'idle',
                'task': 'idle'
            }, sort=[('lastUpdated', -1)])
            if available:
                return available
        return None

    async def _ensure_mqtt_connected(self):
        """Basic connectivity check."""
        try:
            return True  # Assumes managed by manager.py
        except Exception as e:
            self.logger.warning(f"MQTT connectivity issue: {e}")
            return False

    async def _assign_stage_device(self, shipment_id, stage, task_details, device=None):
        """Assign device for specific lifecycle stage. Updated to accept optional device."""
        if stage not in SHIPMENT_STAGES or SHIPMENT_STAGES[stage]['next_stage'] is None:
            self.logger.warning(f"Invalid or final stage {stage} for shipment {shipment_id}")
            return None

        stage_info = SHIPMENT_STAGES[stage]
        device_types = stage_info.get('device_types', ['truck'])

        # Determine destination based on stage
        destination_node = task_details.get('finalNode')  # Use from task_details for arrive tasks
        if not destination_node:
            if stage == 'unloading':
                destination_node = 'B2'  # Processing area
            elif stage == 'processing':
                destination_node = 'B3'  # Pre-warehouse staging
            elif stage == 'transporting':
                graph_nodes = self.analyzer.graph
                warehouses = [node_id for node_id, node in graph_nodes.items() if node.get('type') == 'warehouse']
                destination_node = warehouses[0] if warehouses else 'B4'
            elif stage == 'loading':
                destination_node = task_details.get('warehouseNode', 'C3')
            self.logger.info(f"Dynamic destination for stage {stage}: {destination_node}")

        # Use provided device or find one
        if device:
            assigned_id = device['id']
        else:
            # Try to assign from preferred device types
            assigned_id = None
            for device_type in device_types:
                candidate = await self.db.edgeDevices.find_one({
                    "type": device_type,
                    "task": "idle",
                    "taskPhase": "idle"
                })
                if candidate:
                    assigned_id = candidate['id']
                    break

            if not assigned_id:
                self.logger.warning(f"No idle devices available for stage {stage} (tried {device_types})")
                return None

        # Get traffic metrics for intelligent pathing
        await self.analyzer.analyze_metrics(f"Assignment for {assigned_id} in stage {stage}")
        node_loads = self.analyzer.get_current_loads()
        route_congestion = self.analyzer.get_route_congestion()
        predicted_loads = self.analyzer.get_predicted_loads()

        start_node = task_details.get('startNode', 'A1')
        # Use provided path or compute new one
        path = task_details.get('path')
        if not path or len(path) < 2:
            path = self.analyzer.planner.compute_path(
                start_node, destination_node,
                node_loads=node_loads,
                route_congestion=route_congestion,
                predicted_loads=predicted_loads
            )

        if not path or len(path) < 2:
            self.logger.warning(f"Invalid path computed for {assigned_id} in stage {stage}: {path}")
            return None

        # Update device in DB
        task_name = task_details.get('task', f"{stage}_shipment_{shipment_id}")
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
                "currentNode": start_node,  # Track current
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
                self.logger.info(f"Published task for stage {stage} to {assigned_id}")
            except Exception as e:
                self.logger.error(f"Failed to publish task for {assigned_id}: {e}")
        else:
            self.logger.error(f"Cannot publish task for {assigned_id}: MQTT connection failed")

        # Optional: Publish to traffic for monitoring
        if len(path) > 1:
            try:
                await self.mqtt_client.publish(f"harboursense/traffic/{assigned_id}", json.dumps({'path': path, 'stage': stage}))
            except Exception as e:
                self.logger.warning(f"Failed to publish traffic update for {assigned_id}: {e}")

        self.logger.info(f"Assigned {device_types[0]} {assigned_id} for shipment {shipment_id} stage {stage}: {start_node} -> {destination_node}")
        return assigned_id

    async def assign_task(self, device_type, task_details):
        """Legacy single-stage assign (for backward compatibility)."""
        task_details['shipment_id'] = task_details.get('shipment_id', 'unknown')
        return await self._assign_stage_device(task_details['shipment_id'], 'transporting', task_details)

    async def calculate_chain_eta(self, shipment_id, start_stage, device_types_chain, shipment_location):
        """Estimate total time for stage chain (travel + task)."""
        total_eta = 0
        etas = {}
        shipment = await self.db.shipments.find_one({"id": shipment_id})
        current_loc = shipment.get('currentNode', shipment_location)

        stages_to_process = [start_stage]
        next_s = start_stage
        while next_s and SHIPMENT_STAGES[next_s]['next_stage']:
            next_s = SHIPMENT_STAGES[next_s]['next_stage']
            stages_to_process.append(next_s)
            if next_s == 'stored': break

        for stage in stages_to_process:
            if stage == 'stored': break
            stage_info = SHIPMENT_STAGES[stage]
            dest_node = await self._get_stage_destination(stage, shipment)

            # Pick sample device for speed
            sample_device = await self.db.edgeDevices.find_one({
                "type": device_types_chain.get(stage, stage_info['device_types'][0]),
                "taskPhase": "idle"
            })
            speed = sample_device.get('speed', 5) if sample_device else 5

            # Compute path distance
            node_loads = self.analyzer.get_current_loads()
            path = self.analyzer.planner.compute_path(current_loc, dest_node, node_loads,
                                                      self.analyzer.get_route_congestion(),
                                                      self.analyzer.get_predicted_loads())
            dist = len(path) - 1 if path else 10
            travel_time = (dist / speed) * 60  # Seconds

            task_time = stage_info['est_duration']
            stage_eta = travel_time + task_time
            etas[stage] = stage_eta
            total_eta += stage_eta
            current_loc = dest_node

        # Next shipment ETA
        next_shipment = await self.db.shipments.find_one({"status": "arrived"}, sort=[("createdAt", 1)])
        next_eta = (next_shipment.get('predictedArrival', datetime.now()) - datetime.now()).total_seconds() if next_shipment else float('inf')

        return {'total_chain': total_eta, 'per_stage': etas, 'next_shipment_gap': next_eta - total_eta}

    async def monitor_and_assign(self):
        """Monitor shipments and assign tasks dynamically, including arrive_to_dock/warehouse."""
        while True:
            shipments = await self.db.shipments.find({'status': {'$in': list(SHIPMENT_STAGES.keys())}}).to_list(None)
            for shipment in shipments:
                shipment_id = shipment['id']
                current_status = shipment['status']
                current_node = shipment.get('currentNode', shipment.get('arrivalNode', self.DOCK_NODE))

                # NEW: Check for dock arrival need (before offloading/transport)
                if current_status in ['waiting', 'arrived'] and shipment.get('needsOffloading', True):
                    if current_node != self.DOCK_NODE:
                        # Find available device (now handles list)
                        available_device = await self.find_available_edge(self.db.edgeDevices, ['truck', 'agv'])
                        if available_device:
                            target_node = self.DOCK_NODE
                            # Compute path using analyzer.planner
                            node_loads = self.analyzer.get_current_loads()
                            path = self.analyzer.planner.compute_path(
                                current_node, target_node,
                                node_loads=node_loads,
                                route_congestion=self.analyzer.get_route_congestion(),
                                predicted_loads=self.analyzer.get_predicted_loads()
                            )
                            if path and len(path) >= 2:
                                task_details = {
                                    'task': f"arrive_to_dock for {shipment_id}",
                                    'shipment_id': shipment_id,
                                    'path': path,
                                    'startNode': current_node,
                                    'finalNode': target_node
                                }
                                await self._assign_stage_device(shipment_id, 'arrived', task_details, device=available_device)
                                self.logger.info(f"Assigned arrive_to_dock for {shipment_id} from {current_node} to {self.DOCK_NODE}")
                                continue  # Skip to next shipment

                # NEW: Check for warehouse arrival (after offload, for storing/processing)
                elif current_status in ['offloaded', 'transporting'] and shipment.get('destination') in self.WAREHOUSE_NODES:
                    warehouse_node = shipment['destination']  # Dynamic from shipment
                    if current_node != warehouse_node:
                        # Get dynamic types
                        stage_types = self.get_stage_device_types('transporting')  # e.g., ['robot', 'agv']
                        available_device = await self.find_available_edge(self.db.edgeDevices, stage_types)
                        if available_device:
                            node_loads = self.analyzer.get_current_loads()
                            path = self.analyzer.planner.compute_path(
                                current_node, warehouse_node,
                                node_loads=node_loads,
                                route_congestion=self.analyzer.get_route_congestion(),
                                predicted_loads=self.analyzer.get_predicted_loads()
                            )
                            if path and len(path) >= 2:
                                task_details = {
                                    'task': f"arrive_to_warehouse for {shipment_id}",
                                    'shipment_id': shipment_id,
                                    'path': path,
                                    'startNode': current_node,
                                    'finalNode': warehouse_node
                                }
                                await self._assign_stage_device(shipment_id, 'transporting', task_details, device=available_device)
                                self.logger.info(f"Assigned arrive_to_warehouse for {shipment_id} from {current_node} to {warehouse_node}")
                                continue

                # Existing stage logic (full paths for other cases)
                # Add your original stage-specific assignments here if needed, e.g.:
                # if current_status == 'unloading':
                #     await self._assign_stage_device(shipment_id, 'unloading', {'shipment_id': shipment_id})
                # etc.

            await asyncio.sleep(10)  # Poll interval

    def get_stage_device_types(self, stage):
        """Helper to get dynamic types per stage."""
        stage_config = {
            'unloading': ['crane'],
            'processing': ['conveyor', 'robot'],
            'transporting': ['truck', 'agv', 'robot'],  # Dynamic for warehouse needs
            # Add more as needed
        }
        return stage_config.get(stage, ['truck', 'agv'])  # Fallback

    async def handle_completion(self, device_id, task):
        """Handle task completion and advance shipment stage. Enhanced for arrival tasks."""
        try:
            device = await self.db.edgeDevices.find_one({"id": device_id})
            if not device or device.get('task') == 'idle':
                return

            shipment_id = device.get('shipmentId')
            if not shipment_id:
                self.logger.warning(f"No shipment linked to completing device {device_id}")
                return

            shipment = await self.db.shipments.find_one({"id": shipment_id})
            if not shipment:
                return

            # Check for special arrival tasks first
            task_name = device.get('task', '')
            if 'arrive_to_dock' in task_name:
                # Advance to unloading
                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'unloading', 'currentNode': self.DOCK_NODE}})
                self.logger.info(f"Shipment {shipment_id} arrived at dock, advancing to unloading")
                # Trigger crane assignment in next monitor cycle
            elif 'arrive_to_warehouse' in task_name:
                # Advance to storing/processing
                dest = shipment.get('destination', self.WAREHOUSE_NODES[0])
                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'storing', 'currentNode': dest}})
                self.logger.info(f"Shipment {shipment_id} arrived at warehouse, advancing to storing")
            else:
                # Existing logic for standard stages
                current_stage = shipment.get('assignedStage', 'arrived')
                next_stage = SHIPMENT_STAGES.get(current_stage, {}).get('next_stage')

                # Update shipment location and status
                new_location = device.get('currentLocation', shipment.get('currentStageDestination'))
                await self.db.shipments.update_one(
                    {"id": shipment_id},
                    {"$set": {
                        "status": next_stage if next_stage else 'stored',
                        "currentNode": new_location,
                        "stageCompletedAt": datetime.now(),
                        "completedDevice": device_id
                    }}
                )

                # If final stage, notify
                if next_stage is None:
                    await self.mqtt_client.publish(
                        f"harboursense/shipment/{shipment_id}/completed",
                        json.dumps({"shipment_id": shipment_id, "status": "stored", "location": new_location})
                    )

            # Reset device to idle (always)
            await self.db.edgeDevices.update_one(
                {"id": device_id},
                {"$set": {
                    "task": "idle",
                    "taskPhase": "idle",
                    "path": [],
                    "shipmentId": None,
                    "startNode": None,
                    "finalNode": None
                }}
            )

            self.logger.info(f"Completed task {task_name} for shipment {shipment_id} by {device_id}")

        except Exception as e:
            self.logger.error(f"Error handling completion for {device_id}: {e}\n{traceback.format_exc()}")

    async def _get_stage_destination(self, stage, shipment):
        """Dynamically determine destination node based on stage, shipment data, and graph."""
        if not shipment:
            self.logger.warning(f"No shipment provided for stage {stage}; using default staging")
            return list(self.analyzer.graph.keys())[0] if self.analyzer.graph else 'B1'

        current_loc = shipment.get('currentNode', shipment.get('arrivalNode'))
        arrival_node = shipment.get('arrivalNode')
        target_warehouse = shipment.get('targetWarehouse')

        # Helper to get nearest node of type to a location
        def nearest_node_of_type(node_type, to_loc):
            candidates = [node for node, data in self.analyzer.graph.items() if data.get('type') == node_type]
            if not candidates:
                self.logger.warning(f"No {node_type} nodes in graph")
                return None

            to_coords = node_to_coords(to_loc)
            if not to_coords:
                # Fallback: alphabetical proximity
                return min(candidates, key=lambda n: abs(ord(n[0]) - ord(to_loc[0])) + abs(int(n[1:]) - int(to_loc[1:])))

            def node_dist(n):
                n_coords = node_to_coords(n)
                return abs(to_coords[0] - n_coords[0]) + abs(to_coords[1] - n_coords[1]) if n_coords else float('inf')

            nearest = min(candidates, key=node_dist)
            self.logger.debug(f"Nearest {node_type} to {to_loc}: {nearest} (dist: {node_dist(nearest)})")
            return nearest

        # Stage-specific logic
        if stage == 'unloading':
            if not arrival_node:
                arrival_node = nearest_node_of_type('dock', current_loc or 'A1')
            return arrival_node

        elif stage == 'processing':
            staging = nearest_node_of_type('staging', arrival_node or current_loc)
            return staging or 'B3'

        elif stage == 'transporting':
            if target_warehouse:
                return target_warehouse
            warehouse = nearest_node_of_type('warehouse', arrival_node or current_loc)
            if warehouse:
                shipment['targetWarehouse'] = warehouse  # Temp cache
                await self.db.shipments.update_one({"id": shipment.get('id')}, {"$set": {"targetWarehouse": warehouse}})
            return warehouse or list(self.analyzer.graph.keys())[-1]

        elif stage == 'unloading_truck':
            warehouse = shipment.get('targetWarehouse') or nearest_node_of_type('warehouse', current_loc)
            if warehouse:
                unload_zone = nearest_node_of_type('unload_zone', warehouse) or nearest_node_of_type('staging', warehouse)
                return unload_zone or f"{warehouse[:-1]}4"  # e.g., D5 -> D4
            return 'D4'

        elif stage == 'storing':
            return shipment.get('targetWarehouse') or nearest_node_of_type('warehouse', current_loc)

        elif stage == 'loading':  # If needed
            return shipment.get('currentStageDestination') or nearest_node_of_type('loading_zone', current_loc)

        else:
            return shipment.get('currentStageDestination') or nearest_node_of_type('staging', current_loc) or 'B3'
