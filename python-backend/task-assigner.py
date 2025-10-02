import json
import paho.mqtt.client as mqtt
from route_planner import RoutePlanner  # Import from route_planners.py

class TaskAssigner:
    def __init__(self, db, mqtt_client, graph):
        self.db = db
        self.mqtt_client = mqtt_client
        self.route_planner = RoutePlanner(graph)

    def distance(self, a, b):
        return abs(ord(a[0]) - ord(b[0])) + abs(int(a[1:]) - int(b[1:]))

    async def assign_task(self, edge_type, task_details):
        # General assignment logic (unchanged, but now used for anomalies too)
        idle_edges = list(self.db['edges'].find({'type': edge_type, 'state': 'idle'}))
        if not idle_edges:
            return
        selected = min(idle_edges, key=lambda e: self.distance(e['currentLocation'], task_details['startNode']))
        node_loads = {}  # Compute loads
        edges = list(self.db['edges'].find({}))
        for e in edges:
            loc = e.get('currentLocation')
            if loc:
                node_loads[loc] = node_loads.get(loc, 0) + 1
        path = self.route_planner.compute_path(task_details['startNode'], task_details['destinationNode'], node_loads)
        if path:
            # Assign dynamic task time
            task_time = 15 if task_details['task'] == 'transport' else 30  # Example: longer for repairs
            if node_loads.get(task_details['destinationNode'], 0) > 3:
                task_time += 10

            task = {**task_details, 'path': path, 'nextNode': path[1] if len(path) > 1 else None, 'state': 'enroute', 'taskCompletionTime': task_time}
            self.db['edges'].update_one({'id': selected['id']}, {'$set': task})
            self.mqtt_client.publish(f"harboursense/edge/{selected['id']}/task", json.dumps(task))

    async def handle_shipment(self, shipment):
        # Unchanged
        task_details = {'task': 'transport', 'startNode': shipment['arrivalNode'], 'destinationNode': 'warehouse_E2'}
        await self.assign_task('truck', task_details)

    async def handle_repair(self):
        # Enhanced: Now queries sensorAlerts for anomalies and assigns tasks
        alerts = list(self.db['sensorAlerts'].find({'alert': True}))
        for alert in alerts:
            task_type = 'repair' if alert['type'] == 'repair_needed' else 'inspect'  # Based on anomaly type
            priority = 'high' if alert.get('severity') == 'high' else 'low'
            task_details = {'task': task_type, 'startNode': alert['node'], 'destinationNode': alert['node'], 'priority': priority}
            edge_type = 'robot' if priority == 'low' else 'personnel'
            await self.assign_task(edge_type, task_details)
            self.db['sensorAlerts'].update_one({'_id': alert['_id']}, {'$set': {'alert': False}})  # Mark as handled

    async def monitor_and_assign(self):
        while True:
            shipments = list(self.db['shipments'].find({'status': 'waiting'}))
            for s in shipments:
                await self.handle_shipment(s)
            await self.handle_repair()  # Now includes anomaly handling
            await asyncio.sleep(5)  # Adjust
