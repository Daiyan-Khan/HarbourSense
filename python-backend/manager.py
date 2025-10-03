import sys
import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import logging
import json
from motor.motor_asyncio import AsyncIOMotorClient
from task_assigner import TaskAssigner  # Import your fixed task_assigner.py
from traffic_analyzer import analyze_traffic  # Import fixed analyze_traffic
import aiomqtt  # Updated import (install via: pip install aiomqtt)
import ssl  # For TLS setup

# Logging setup with UTF-8 encoding to handle emojis
logger = logging.getLogger("HarbourSenseManager")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.pool").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
logging.getLogger("pymongo.server_selection").setLevel(logging.WARNING)
logging.getLogger("pymongo.cursor").setLevel(logging.WARNING)
logging.getLogger("pymongo.command").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("manager_log.txt", mode="a", encoding="utf-8")  # Add encoding
console_handler = logging.StreamHandler(stream=sys.stdout)  # Explicitly set to stdout
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# In-memory tracking for hop-by-hop paths
device_paths = {}  # e.g., {'truck001': {'path': ['A1', 'B1', 'B4'], 'current_index': 0, 'final': 'B4'}}

class ShipmentManager:
    def __init__(self, db, task_assigner):
        self.db = db
        self.task_assigner = task_assigner

    async def process_shipment_steps(self, shipment):
        if shipment['status'] == 'waiting':
            task_details = {
                'task': 'transport',
                'startNode': shipment.get('arrivalNode', 'A1'),
                'destinationNode': shipment.get('destinationNode', 'B4')
            }
            assigned = await self.task_assigner.assign_task('truck', task_details)
            if assigned:
                await self.db.shipments.update_one({'_id': shipment['_id']}, {'$set': {'status': 'processing'}})
                logger.info(f"Processed shipment {shipment.get('id')}")

async def shipment_watcher(db, shipment_manager):
    while True:
        waiting_shipments = await db.shipments.find({"status": {"$in": ["waiting", "processing"]}}).to_list(None)
        for shipment in waiting_shipments:
            await shipment_manager.process_shipment_steps(shipment)
        await asyncio.sleep(5)

async def periodic_traffic_analysis(db):
    while True:
        await analyze_traffic(db)  # Now accepts db
        await asyncio.sleep(30)

# Placeholder for reroute check (customize based on your traffic data)
def needs_reroute(current_node, remaining_path, traffic_data):
    # Example: Check if any node in remaining_path has high congestion
    for node in remaining_path:
        if traffic_data.get(node, {}).get('congestion_level', 0) > 0.7:  # Threshold
            return True
    return False

# Async MQTT handler task
async def mqtt_handler(db, mqtt_client):
    topic_filter = "harboursense/edge/+/update"
    await mqtt_client.subscribe(topic_filter)
    logger.info(f"Manager subscribed to MQTT topic filter: {topic_filter}")  # Removed emoji

    async for message in mqtt_client.messages:
        topic = str(message.topic)  # Convert Topic object to string
        try:
            payload = message.payload.decode()
            logger.debug(f"MQTT message received on topic={topic}, raw={payload}")  # Removed emoji
        except Exception as e:
            logger.error(f"Error decoding MQTT payload: {e}, raw={message.payload}")
            continue

        edge_id = None  # Initialize to avoid UnboundLocalError
        try:
            data = json.loads(payload)
            edge_id = topic.split('/')[2]  # Extract from harboursense/edge/<id>/update
            logger.debug(f"Received payload for {edge_id}: {json.dumps(data)}")

            if data.get('status') == 'arrived':
                logger.info(f"Arrival detected for {edge_id} at {data['currentLocation']}")
                if edge_id in device_paths:
                    path_info = device_paths[edge_id]
                    current_index = path_info['current_index']
                    full_path = path_info['path']
                    logger.debug(f"Current state for {edge_id}: index={current_index}, path={full_path}, final={path_info['final']}")

                    if current_index < len(full_path) - 1:  # Not at final
                        next_index = current_index + 1
                        next_node = full_path[next_index]

                        # Optional: Recompute if traffic warrants it
                        traffic_data = await analyze_traffic(db)  # Quick analysis (now async)
                        logger.debug(f"Traffic data: {json.dumps(traffic_data)}")
                        if needs_reroute(data['currentLocation'], full_path[next_index:], traffic_data):
                            new_path = compute_path(data['currentLocation'], path_info['final'], traffic_data)  # Assume compute_path exists in task_assigner
                            full_path = new_path
                            next_node = full_path[1] if len(full_path) > 1 else 'Null'
                            device_paths[edge_id]['path'] = full_path
                            next_index = 0  # Reset index
                            logger.info(f"Rerouted path for {edge_id}: {full_path}")

                        # Send next hop with explicit status reset
                        update_msg = {
                            'task': 'transport',  # Or current task
                            'nextNode': next_node,
                            'finalNode': full_path[-1],
                            'taskPhase': 'enroute_to_complete',  # Adjust phase as needed
                            'status': 'enroute'  # Explicitly reset status to 'enroute'
                        }
                        await mqtt_client.publish(f"harboursense/edge/{edge_id}/task", json.dumps(update_msg))
                        device_paths[edge_id]['current_index'] = next_index
                        logger.info(f"Sent next hop to {edge_id}: nextNode={next_node}, finalNode={full_path[-1]}, taskPhase='enroute_to_complete', status='enroute'")
                    else:
                        logger.info(f"{edge_id} at final node; no further hops. Current phase: {data.get('taskPhase')}, status: {data.get('status')}")
                else:
                    logger.warning(f"No path info found for {edge_id} - possible assignment issue")
        except Exception as e:
            logger.error(f"Error processing MQTT message for {edge_id or 'unknown'}: {e} - Payload: {payload}")

async def setup():
    uri = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
    client = AsyncIOMotorClient(uri)
    db = client["port"]
    logger.info("Connected to MongoDB")

    # TLS setup for AWS IoT - Corrected to SERVER_AUTH for client verifying server
    tls_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    tls_context.load_verify_locations(cafile="../certs/AmazonRootCA1.pem")
    tls_context.load_cert_chain(certfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt",
                               keyfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key")

    async with aiomqtt.Client(
        hostname="a1dghi6and062t-ats.iot.us-east-1.amazonaws.com",
        port=8883,
        tls_context=tls_context,
        identifier="manager"
    ) as mqtt_client:
        logger.info("Async MQTT client initialized and connected")

        # Load graph with conversion
        from traffic_analyzer import convert_bson_numbers  # Reuse from traffic_analyzer
        raw_nodes = await db.graph.find().to_list(None)
        if not raw_nodes:
            logger.error("Graph missing or empty in DB")
            return
        processed_nodes = convert_bson_numbers(raw_nodes)
        graph = {}
        for node in processed_nodes:
            node_id = node.get('id')
            if node_id:
                graph[node_id] = node
            else:
                logger.warning(f"Skipping invalid node without 'id': {node}")
        logger.info(f"Loaded graph with {len(graph)} nodes")

        task_assigner = TaskAssigner(db, mqtt_client, graph)  # Pass async mqtt_client
        shipment_manager = ShipmentManager(db, task_assigner)

        # Create tasks
        asyncio.create_task(shipment_watcher(db, shipment_manager))
        asyncio.create_task(periodic_traffic_analysis(db))
        asyncio.create_task(task_assigner.monitor_and_assign())  # Assuming this method exists in TaskAssigner
        asyncio.create_task(mqtt_handler(db, mqtt_client))  # Async MQTT handler

        logger.info("Setup complete, monitoring...")

        # Keep running
        await asyncio.Event().wait()  # Or use asyncio.gather if you have tasks to await

# Update assign_task in task_assigner.py (add this if not present)
# async def assign_task(self, device_type, task_details):
#     # Compute full path using your route_planner or similar
#     path = self.compute_path(task_details['startNode'], task_details['destinationNode'])  # Assume this exists
#     selected_device = # Your device selection logic
#     device_paths[selected_device['id']] = {'path': path, 'current_index': 0, 'final': task_details['destinationNode']}
#     # Send first hop
#     first_hop = path[1] if len(path) > 1 else 'Null'
#     payload = {
#         'task': task_details['task'],
#         'nextNode': first_hop,
#         'finalNode': path[-1],
#         'taskPhase': 'enroute_to_start'
#     }
#     await self.mqtt_client.publish(f"harboursense/edge/{selected_device['id']}/task", json.dumps(payload))
#     return True

if __name__ == "__main__":
    asyncio.run(setup())
