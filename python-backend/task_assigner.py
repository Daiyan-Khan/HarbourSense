import logging
import asyncio
from traffic_analyzer import SmartRoutePlanner  # Reuse planner

logger = logging.getLogger("TaskAssigner")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("task_log.txt", mode="a")
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

class TaskAssigner:
    def __init__(self, db, mqtt_client, graph):
        self.db = db
        self.mqtt_client = mqtt_client
        self.graph = graph
        self.smart_planner = SmartRoutePlanner(self.graph)

    async def assign_task(self, device_type, task_details):
        edges = await self.db.edgeDevices.find({'type': device_type, 'task': 'idle', 'taskPhase': 'idle'}).to_list(None)
        if not edges:
            return False

        # Select closest (simplified - use real distance if available)
        closest_edge = edges[0]  # Or implement distance calc
        current_loc = closest_edge.get('currentLocation', 'A1')  # Default if Null

        path = self.smart_planner.compute_path(
            current_loc, task_details['destinationNode'],
            {}, {}, {},  # Fetch real loads if needed
        )
        if not path:
            logger.warning(f"No path for {closest_edge['id']} from {current_loc} to {task_details['destinationNode']}")
            return False

        next_node = path[1] if len(path) > 1 else task_details['destinationNode']  # Handle short paths

        update = {
            'task': task_details['task'],
            'taskPhase': 'assigned',
            'nextNode': next_node,
            'finalNode': task_details['destinationNode'],
            'eta': 'N/A'  # Calculate if needed
        }
        await self.db.edgeDevices.update_one({'_id': closest_edge['_id']}, {'$set': update})
        logger.info(f"Assigned task to {closest_edge['id']} with path {path}")
        return True

    async def monitor_and_assign(self):
        while True:
            shipments = await self.db.shipments.find({'status': 'waiting'}).to_list(None)
            for s in shipments:
                task_details = {
                    'task': 'transport',
                    'destinationNode': s.get('destinationNode', 'B4')
                }
                await self.assign_task('truck', task_details)  # Example type
            await asyncio.sleep(5)
