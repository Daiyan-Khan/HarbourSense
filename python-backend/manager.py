import asyncio
import json
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from math import inf
from task_assigner import TaskAssigner
from route_planner import RoutePlanner

uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net'
client = MongoClient(uri)
db = client['port']

mqtt_client = mqtt.Client(client_id="manager")
# mqtt_client.tls_set(...)  # Adapt
mqtt_client.connect('a1dghi6and062t-ats.iot.us-east-1.amazonaws.com', 8883, 60)
mqtt_client.loop_start()

graph = db['graph'].find_one()
task_assigner = TaskAssigner(db, mqtt_client, graph)
route_planner = RoutePlanner(graph)
async def monitor_anomalies():
    # Change stream for real-time anomaly detection
    pipeline = [{'$match': {'operationType': 'insert', 'fullDocument.alert': True}}]
    async with db.sensorAlerts.watch(pipeline) as stream:
        async for change in stream:
            anomaly = change['fullDocument']
            print(f"New anomaly detected at {anomaly['node']}")
            # Trigger task assignment
            await task_assigner.handle_repair()  # Calls enhanced method
            # Trigger traffic analysis for route updates
            # Call your /analyze_traffic endpoint or function here
async def sensor_analyzer():
    # Your old sensor analyzer logic (e.g., process alerts, traffic)
    while True:
        alerts = list(db['sensorAlerts'].find({'alert': True}))
        traffic_doc = db['trafficData'].find_one(sort=[('timestamp', -1)])
        # Analyze and update loads/congestion (store in DB for route_planner)
        node_loads = {}  # Compute and save
        await asyncio.sleep(10)

async def on_edge_arrival(edge_id, arrival_node):
    await route_planner.replan_if_needed(edge_id, db)
    # Trigger assignment if needed
    shipments = list(db['shipments'].find({'arrivalNode': arrival_node, 'status': 'arrived'}))
    for s in shipments:
        await task_assigner.handle_shipment(s)

def on_mqtt_message(client, userdata, msg):
    if msg.topic.startswith('harboursense/edge/') and '/update' in msg.topic:
        data = json.loads(msg.payload)
        asyncio.run(on_edge_arrival(data['id'], data['currentLocation']))

mqtt_client.on_message = on_mqtt_message
mqtt_client.subscribe('harboursense/edge/+/update')

async def monitor_changes():
    pipeline = [{'$match': {'operationType': 'update'}}]
    async for change in db['edges'].watch(pipeline):
        edge = change['fullDocument']
        if edge['state'] == 'completed':
            # Trigger next in cycle via task_assigner
            await task_assigner.handle_shipment({'arrivalNode': edge['destinationNode']})  # Example chaining

async def main():
    await asyncio.gather(
        sensor_analyzer(),
        task_assigner.monitor_and_assign(),
        monitor_changes()
    )

if __name__ == "__main__":
    asyncio.run(main())
