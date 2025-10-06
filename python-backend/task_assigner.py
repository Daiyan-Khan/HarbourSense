import asyncio
import logging
import json
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger("TaskAssigner")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)

class TaskAssigner:
    def __init__(self, db, mqtt_client, analyzer):
        self.db = db
        self.mqtt_client = mqtt_client
        self.analyzer = analyzer
        # No hardcoded available_devices—query DB dynamically for idle

    async def assign_task(self, device_type, task_details, edge_id=None):
        """Assign specific idle edge to task; dynamic from DB."""
        if not edge_id:
            # Dynamic: Find first idle of type
            edges = await self.db.edgeDevices.find({
                'taskPhase': 'idle',
                'type': device_type
            }).to_list(None)
            if not edges:
                logger.warning(f"No available {device_type} for task")
                return None
            edge = edges[0]  # Or min by distance to start_node
            edge_id = edge['id']
        else:
            # Verify idle
            edge = await self.db.edgeDevices.find_one({'id': edge_id})
            if not edge or edge['taskPhase'] != 'idle' or edge['type'] != device_type:
                logger.warning(f"Edge {edge_id} not idle or wrong type ({device_type}); skipping")
                return None

        start_node = task_details.get('startNode', 'A1')
        final_node = task_details.get('finalNode', 'B4')

        # Compute path if not provided (safe kwargs)
        if 'path' not in task_details or not task_details['path']:
            node_loads = self.analyzer.get_current_loads()
            route_congestion = self.analyzer.get_route_congestion()
            predicted_loads = self.analyzer.get_predicted_loads()
            # Type check (warn but no flatten—planner handles nested)
            if not isinstance(node_loads, dict):
                logger.warning(f"node_loads not dict: {type(node_loads)}; defaulting to {{}}")
                node_loads = {}
            if not isinstance(route_congestion, dict):
                logger.warning(f"route_congestion not dict: {type(route_congestion)}; defaulting to {{}}")
                route_congestion = {}
            if not isinstance(predicted_loads, dict):
                logger.warning(f"predicted_loads not dict: {type(predicted_loads)}; defaulting to {{}}")
                predicted_loads = {}
            try:
                path = self.analyzer.planner.compute_path(
                    start_node, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                ) or [start_node, final_node]
            except TypeError as e:
                logger.error(f"Path computation error for {edge_id}: {e}; using fallback [{start_node}, {final_node}]")
                path = [start_node, final_node]
        else:
            path = task_details['path']

        full_details = {
            **task_details,
            'shipmentId': task_details.get('shipmentId'),
            'task': task_details.get('phase', 'unknown'),  # e.g., 'offload'
            'startNode': start_node,
            'finalNode': final_node,
            'path': path
        }

        # Update edge to enroute_start
        updates = {
            'taskPhase': 'enroute_start',
            'finalNode': final_node,
            'path': path,
            'nextNode': path[1] if len(path) > 1 else final_node,
            'eta': len(path) / edge.get('speed', 10),  # Simple
            'task': full_details,  # Full incl. phase/shipmentId
            'updatedAt': datetime.now()
        }
        await self.db.edgeDevices.update_one({'id': edge_id}, {'$set': updates})

        # Update shipment: Push to assignedEdges as f"{edge}:{phase}"
        phase = task_details.get('phase', 'unknown')
        assigned_entry = f"{edge_id}:{phase}"
        await self.db.shipments.update_one(
            {'id': full_details.get('shipmentId')},
            {
                '$push': {'assignedEdges': assigned_entry},
                '$set': {'needsOffloading': False}  # Disable everywhere
            }
        )

        # Publish task MQTT
        await self.mqtt_client.publish(
            f"harboursense/edge/{edge_id}/task",
            json.dumps(full_details)  # Full payload for port.js
        )

        logger.info(f"Assigned {device_type} {edge_id} to {phase} for shipment {full_details.get('shipmentId')}: path={path}")
        return edge_id

    async def _assign_stage_device(self, shipment_id, phase, task_details, edge_id=None, all_edges=None):
        """Core assign: For given phase (offload/transport/store), find/verify edge, call assign_task."""
        shipment = await self.db.shipments.find_one({'id': shipment_id})
        if not shipment:
            logger.warning(f"Shipment {shipment_id} not found")
            return None

        # Skip if already assigned for phase (check assignedEdges)
        assigned_edges = shipment.get('assignedEdges', [])
        if any(phase in entry for entry in assigned_edges):
            logger.debug(f"Shipment {shipment_id} already assigned for {phase}; skipping")
            return None

        # Map phase to device_type
        device_type_map = {
            'offload': 'crane',
            'transport': 'truck',
            'store': 'robot'
        }
        device_type = device_type_map.get(phase, 'truck')

        # Ensure task_details has phase
        task_details['phase'] = phase

        # Assign (dynamic or specified edge_id)
        assigned = await self.assign_task(device_type, task_details, edge_id)
        if assigned:
            # Set assignedShipment on edge
            await self.db.edgeDevices.update_one(
                {'id': assigned},
                {'$set': {'assignedShipment': shipment_id}}
            )
            logger.info(f"Successfully assigned {assigned} ({device_type}) for {phase} on {shipment_id}")
        else:
            logger.warning(f"Failed to assign {device_type} for {shipment_id} in {phase}")
        return assigned

    async def handle_completion(self, device_id, task):
        """Handle edge completion: Reset idle, publish, infer shipment status update."""
        edge = await self.db.edgeDevices.find_one({'id': device_id})
        if not edge:
            logger.warning(f"Edge {device_id} not found for completion")
            return

        # Reset edge
        await self.db.edgeDevices.update_one(
            {'id': device_id},
            {'$set': {
                'taskPhase': 'idle',
                'task': 'idle',
                'path': [],
                'finalNode': None,
                'nextNode': None,
                'updatedAt': datetime.now()
            }}
        )

        shipment_id = task.get('shipmentId') or edge.get('assignedShipment')
        phase = task.get('phase', 'unknown')
        new_status = {'offload': 'offloaded', 'transport': 'transported', 'store': 'stored'}.get(phase, 'completed')

        # Update shipment status if shipmentId present (triggers next in monitor)
        if shipment_id:
            await self.db.shipments.update_one(
                {'id': shipment_id},
                {'$set': {'status': new_status, 'currentNode': edge.get('currentLocation', 'A1')}}
            )
            logger.info(f"Completed {phase} for {shipment_id}; status → {new_status}")

        # Publish per-ID completion (for manager/port.js)
        completion_payload = {
            'deviceId': device_id,
            'task': task,
            'status': 'completed',
            'shipmentId': shipment_id,
            'phase': phase,
            'endLocation': edge.get('currentLocation', 'A1')
        }
        await self.mqtt_client.publish(
            f"harboursense/edge/completion/{device_id}",
            json.dumps(completion_payload)
        )

        logger.info(f"Handled completion for {device_id} ({phase}) on shipment {shipment_id}")

    def _get_device_type(self, device_id):
        """Infer type from DB (dynamic)."""
        edge = self.db.edgeDevices.find_one({'id': device_id})  # Async? Wrap if needed
        return edge.get('type') if edge else None

    def _distance(self, loc1, loc2):
        """Manhattan distance."""
        if len(loc1) < 2 or len(loc2) < 2:
            return 0
        return abs(ord(loc1[0]) - ord(loc2[0])) + abs(int(loc1[1:]) - int(loc2[1:]))

    async def monitor_and_assign(self):
        """Status-driven loop: Scan shipments, assign by phase."""
        while True:
            try:
                logger.info("Monitor loop: Scanning shipments...")
                shipments = await self.db.shipments.find().sort('createdAt', 1).to_list(None)
                logger.debug(f"Scanned {len(shipments)} shipments")

                for shipment in shipments:
                    shipment_id = shipment['id']
                    status = shipment.get('status', 'unknown')
                    current_node = shipment.get('currentNode', 'A1')
                    assigned = shipment.get('assignedEdges', [])
                    logger.debug(f"Checking {shipment_id}: status={status}, current={current_node}, assigned={len(assigned)} entries")

                    # Arrived/Waiting → Crane offload + preemptive truck to A1
                    if status in ['arrived', 'waiting'] and len([e for e in assigned if 'offload' in e]) == 0:
                        logger.info(f"Assigning crane for offload on {shipment_id} at {current_node}")
                        offload_details = {'shipmentId': shipment_id, 'destNode': current_node}  # A1
                        await self._assign_stage_device(shipment_id, 'offload', offload_details)

                        # Pre-assign truck to A1
                        transport_details = {'shipmentId': shipment_id, 'destNode': 'A1', 'preemptive': True}
                        logger.info(f"Pre-assigning truck to A1 for {shipment_id}")
                        await self._assign_stage_device(shipment_id, 'transport', transport_details)

                    # Offloaded → Ensure truck transport to B4 (if pre not done)
                    elif status == 'offloaded' and len([e for e in assigned if 'transport' in e]) == 0:
                        logger.info(f"Assigning truck for transport on {shipment_id} from {current_node} to B4")
                        transport_details = {'shipmentId': shipment_id, 'destNode': 'B4', 'pickupNode': current_node}
                        await self._assign_stage_device(shipment_id, 'transport', transport_details)

                    # Transported → Robot store from B4 to C5
                    elif status == 'transported' and len([e for e in assigned if 'store' in e]) == 0:
                        logger.info(f"Assigning robot for store on {shipment_id} from B4 to C5")
                        store_details = {'shipmentId': shipment_id, 'destNode': shipment.get('destination', 'C5'), 'pickupNode': 'B4'}
                        await self._assign_stage_device(shipment_id, 'store', store_details)

                # Optional: Check DB for completing edges → handle_completion
                completing = await self.db.edgeDevices.find({'taskPhase': 'completing'}).to_list(None)
                for edge in completing:
                    await self.handle_completion(edge['id'], edge.get('task', {}))

                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(10)
