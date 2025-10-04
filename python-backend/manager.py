import sys
import asyncio
from datetime import datetime  # ADD: For ISO datetime parsing in shipment handler

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import logging
import json
from motor.motor_asyncio import AsyncIOMotorClient
from task_assigner import TaskAssigner  # Import your fixed task_assigner.py
from traffic_analyzer import TrafficAnalyzer, SmartRoutePlanner  # Updated import for class-based analyzer
import aiomqtt  # Updated import (install via: pip install aiomqtt)
import ssl  # For TLS setup
from enum import Enum  # Add this import to fix the NameError

# Logging setup with UTF-8 encoding to handle emojis
logger = logging.getLogger("HarbourSenseManager")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.pool").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
logging.getLogger("pymongo.server_selection").setLevel(logging.WARNING)
logging.getLogger("pymongo.cursor").setLevel(logging.WARNING)
logging.getLogger("pymongo.command").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)

class TaskPhase(Enum):
    IDLE = 'idle'
    ENROUTE_START = 'en_route_start'
    ASSIGNED = 'assigned'
    COMPLETING = 'completing'

file_handler = logging.FileHandler("manager_log.txt", mode="a", encoding="utf-8")  # Add encoding
console_handler = logging.StreamHandler(stream=sys.stdout)  # Explicitly set to stdout
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

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
        await asyncio.sleep(5)  # Debounce to reduce rapid assignments

# Placeholder for reroute check (uses analyzer metrics)
def needs_reroute(current_node, remaining_path, analyzer):
    # Check if any node in remaining_path has high predicted load (threshold for efficiency)
    predicted_loads = analyzer.get_predicted_loads()
    for node in remaining_path:
        if predicted_loads.get(node, 0) > 2:  # Low threshold since port is free; adjust as needed
            return True
    return False

# Async function to check and publish reroute if needed (uses analyzer)
async def handle_traffic_update(db, mqtt_client, edge_id, analyzer):
    edge = await db.edgeDevices.find_one({'id': edge_id})
    if not edge or not edge.get('path'):
        return

    # Trigger dynamic analysis
    await analyzer.analyze_metrics(triggered_by=f"Reroute check for {edge_id}")
    
    remaining_path = edge['path'][1:] if len(edge['path']) > 1 else []  # Skip current
    if needs_reroute(edge['currentLocation'], remaining_path, analyzer):
        # Compute new path with analyzer metrics
        start = edge['currentLocation']
        destination = edge.get('destinationNode', edge.get('finalNode', 'B4'))  # Dynamic from DB
        node_loads = analyzer.get_current_loads()
        route_congestion = analyzer.get_route_congestion()
        predicted_loads = analyzer.get_predicted_loads()
        
        new_path = analyzer.planner.compute_path(start, destination, node_loads, route_congestion, predicted_loads)
        if new_path:
            # Publish to traffic MQTT only if reroute needed (efficient for free port)
            update_msg = {'path': new_path, 'finalNode': destination}
            await mqtt_client.publish(f"harboursense/traffic/{edge_id}", json.dumps(update_msg))
            logger.info(f"Pushed reroute for {edge_id} due to detected load: {new_path}")
        else:
            logger.warning(f"Failed to compute reroute for {edge_id}")
    else:
        logger.debug(f"No reroute needed for {edge_id} (port free, low congestion)")

# ADD: New handler for shipment creation events (after handle_traffic_update)
async def handle_new_shipment(db, task_assigner, analyzer, message):
    try:
        topic = str(message.topic)
        if 'harboursense/shipment/new' in topic:
            payload = json.loads(message.payload.decode())
            shipment_id = payload['id']
            arrival_node = payload['arrivalNode']
            status = payload['status']  # e.g., 'waiting' or 'arrived'
            current_location = payload.get('currentLocation', arrival_node)
            created_at = payload['createdAt']

            # Sync to DB if not exists (idempotent)
            existing = await db.shipments.find_one({'id': shipment_id})
            if not existing:
                await db.shipments.insert_one({
                    'id': shipment_id,
                    'arrivalNode': arrival_node,
                    'status': status,
                    'currentLocation': current_location,
                    'createdAt': datetime.fromisoformat(created_at.replace('Z', '+00:00')) if created_at else datetime.now()  # Handle ISO with timezone
                })
                logger.info(f"New shipment {shipment_id} synced from MQTT to DB at {arrival_node}")

            # Trigger assignment: Start with 'unloading' stage (crane at dock)
            # Fallback destination if analyzer.get_stage_destination not available
            destination_node = 'B2'  # Default processing area
            try:
                destination_node = analyzer.get_stage_destination('unloading', {'id': shipment_id, 'arrivalNode': arrival_node})
            except AttributeError:
                logger.debug("Analyzer.get_stage_destination unavailable; using default 'B2'")
            
            task_details = {
                'shipmentId': shipment_id,
                'startNode': arrival_node,
                'destinationNode': destination_node
            }
            crane_id = await task_assigner.assign_stage_device(shipment_id, 'unloading', task_details)
            if crane_id:
                logger.info(f"MQTT-triggered unloading assignment for {shipment_id}: crane {crane_id}")
                # Optional: Trigger predictive chaining if gap allows (uncomment if calculate_chain_etas implemented)
                # etas = await task_assigner.calculate_chain_etas(shipment_id, 'unloading', {'unloading': ['crane'], 'processing': ['agv']}, arrival_node)
                # if etas.get('nextShipmentGap', 0) > 30:  # 30s buffer
                #     logger.info(f"Pre-positioning chain for {shipment_id} (gap: {etas['nextShipmentGap']}s)")
            else:
                logger.warning(f"No crane available for new shipment {shipment_id} via MQTT")
    except Exception as e:
        logger.error(f"Error handling new shipment MQTT: {e}")

# REPLACE: Updated MQTT handler task (integrates analyzer and new shipment handling)
async def mqtt_handler(db, mqtt_client, analyzer, task_assigner):  # ADD: task_assigner param
    # Subscribe to edge updates (existing)
    topic_filter = "harboursense/edge/+/update"
    await mqtt_client.subscribe(topic_filter)
    logger.info(f"Manager subscribed to MQTT topic filter: {topic_filter}")

    # ADD: Subscribe to new shipments
    await mqtt_client.subscribe('harboursense/shipment/new')
    logger.info("Manager subscribed to shipment/new for real-time arrivals")

    async for message in mqtt_client.messages:
        topic = str(message.topic)  # Convert Topic object to string
        try:
            payload = message.payload.decode()
            logger.debug(f"MQTT message received on topic={topic}, raw={payload}")
        except Exception as e:
            logger.error(f"Error decoding MQTT payload: {e}, raw={message.payload}")
            continue

        # ADD: Handle new shipment FIRST (before edge processing)
        if 'harboursense/shipment/new' in topic:
            await handle_new_shipment(db, task_assigner, analyzer, message)
            continue  # Skip to next message (shipment-specific)

        # Existing edge processing (unchanged)
        edge_id = None
        try:
            data = json.loads(payload)
            edge_id = topic.split('/')[2]  # Extract from harboursense/edge/<id>/update
            logger.debug(f"Received payload for {edge_id}: {json.dumps(data)}")
            edge = await db.edgeDevices.find_one({'id': edge_id})
            if not edge:
                continue  # Changed from return to continue for loop safety

            current = data.get('currentLocation')
            final = data.get('finalNode')
            phase = data.get('taskPhase')

            # Check for reroute proactively on each progress update (using analyzer)
            await handle_traffic_update(db, mqtt_client, edge_id, analyzer)

            # Handle completion (no nextNode sends; edge handles autonomously)
            if phase == 'assigned' and current == final:
                # Device should transition to completing; server handles shipment
                shipment = await db.shipments.find_one({'assignedDevice': edge_id, 'status': 'processing'})
                if shipment:
                    await db.shipments.update_one({'_id': shipment['_id']}, {'$set': {'status': 'completed'}})
                    logger.info(f"Shipment {shipment['id']} completed by {edge_id}")
                logger.info(f"Arrival detected for {edge_id} at {current}")

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

        # Load graph with conversion (reuse from traffic_analyzer)
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

        # NEW: Instantiate TrafficAnalyzer for dynamic use
        analyzer = TrafficAnalyzer(db, mqtt_client, graph)
        
        # Start analyzer's dynamic MQTT listener (event-driven on path updates)
        asyncio.create_task(analyzer.start_mqtt_listener())
        logger.info("TrafficAnalyzer listener started (dynamic)")
        

        task_assigner = TaskAssigner(db, mqtt_client, analyzer)  # Pass analyzer for metrics
        shipment_manager = ShipmentManager(db, task_assigner)

        # Initial analysis to populate metrics (since traffic MQTT empty/port free)
        await analyzer.analyze_metrics("startup")
        logger.info("Initial traffic analysis complete")

        # Create tasks
        asyncio.create_task(shipment_watcher(db, shipment_manager))
        # Removed periodic_traffic_analysis—now dynamic via analyzer
        asyncio.create_task(task_assigner.monitor_and_assign())  # Assuming this method exists in TaskAssigner
        asyncio.create_task(mqtt_handler(db, mqtt_client, analyzer, task_assigner))  # ADD: Pass task_assigner

        logger.info("Setup complete, monitoring...")

        # Keep running
        await asyncio.Event().wait()  # Or use asyncio.gather if you have tasks to await
        
if __name__ == "__main__":
    asyncio.run(setup())
