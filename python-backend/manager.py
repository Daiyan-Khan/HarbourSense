import sys
import asyncio
from datetime import datetime

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import logging
import json
from motor.motor_asyncio import AsyncIOMotorClient
from task_assigner import TaskAssigner
from traffic_analyzer import TrafficAnalyzer, SmartRoutePlanner
import aiomqtt
import ssl
from enum import Enum

# ----------------------- Logging Setup -----------------------
logger = logging.getLogger("HarbourSenseManager")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("manager_log.txt", mode="a", encoding="utf-8")
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ----------------------- TaskPhase Enum -----------------------
class TaskPhase(Enum):
    IDLE = 'idle'
    ENROUTE_START = 'en_route_start'
    ASSIGNED = 'assigned'
    COMPLETING = 'completing'

# ----------------------- ShipmentManager -----------------------
class ShipmentManager:
    def __init__(self, db, task_assigner):
        self.db = db
        self.task_assigner = task_assigner

    async def process_shipment_steps(self, shipment):
        logger.debug(f"Processing shipment: {shipment.get('id')} with status: {shipment.get('status')}")
        if shipment['status'] == 'waiting':
            task_details = {
                'task': 'transport',
                'startNode': shipment.get('arrivalNode', 'A1'),
                'destinationNode': shipment.get('destinationNode', 'B4')
            }
            assigned = await self.task_assigner.assign_task('truck', task_details)
            logger.debug(f"Task assignment attempt for shipment {shipment.get('id')}: {assigned}")
            if assigned:
                await self.db.shipments.update_one({'_id': shipment['_id']}, {'$set': {'status': 'processing'}})
                logger.info(f"Processed shipment {shipment.get('id')} and set status to 'processing'")

# ----------------------- Shipment Watcher -----------------------
async def shipment_watcher(db, shipment_manager):
    while True:
        waiting_shipments = await db.shipments.find({"status": {"$in": ["waiting", "processing"]}}).to_list(None)
        logger.debug(f"Found {len(waiting_shipments)} shipments to process")
        for shipment in waiting_shipments:
            await shipment_manager.process_shipment_steps(shipment)
        await asyncio.sleep(5)

# ----------------------- Reroute Helper -----------------------
def needs_reroute(current_node, remaining_path, analyzer):
    predicted_loads = analyzer.get_predicted_loads()
    logger.debug(f"Checking reroute for current node {current_node}, remaining path {remaining_path}")
    for node in remaining_path:
        if predicted_loads.get(node, 0) > 2:
            logger.info(f"Reroute needed: Node {node} has predicted load {predicted_loads[node]}")
            return True
    return False

# ----------------------- Traffic Update Handler -----------------------
async def handle_traffic_update(db, mqtt_client, edge_id, analyzer):
    edge = await db.edgeDevices.find_one({'id': edge_id})
    if not edge or not edge.get('path'):
        logger.debug(f"No path found for edge {edge_id}, skipping reroute check")
        return

    await analyzer.analyze_metrics(triggered_by=f"Reroute check for {edge_id}")
    remaining_path = edge['path'][1:] if len(edge['path']) > 1 else []

    if needs_reroute(edge['currentLocation'], remaining_path, analyzer):
        start = edge['currentLocation']
        destination = edge.get('destinationNode', edge.get('finalNode', 'B4'))
        node_loads = analyzer.get_current_loads()
        route_congestion = analyzer.get_route_congestion()
        predicted_loads = analyzer.get_predicted_loads()

        logger.debug(f"Computing new path for edge {edge_id}. Start: {start}, Destination: {destination}")
        new_path = analyzer.planner.compute_path(start, destination, node_loads, route_congestion, predicted_loads)
        if new_path:
            update_msg = {'path': new_path, 'finalNode': destination}
            await mqtt_client.publish(f"harboursense/traffic/{edge_id}", json.dumps(update_msg))
            logger.info(f"Pushed reroute for {edge_id} due to detected load. New path: {new_path}")
        else:
            logger.warning(f"Failed to compute reroute for {edge_id}")
    else:
        logger.debug(f"No reroute needed for {edge_id}")

# ----------------------- New Shipment MQTT Handler -----------------------
async def handle_new_shipment(db, task_assigner, analyzer, message):
    try:
        topic = str(message.topic)
        if 'harboursense/shipment/new' in topic:
            payload = json.loads(message.payload.decode())
            logger.debug(f"Received new shipment payload: {json.dumps(payload, indent=2)}")
            shipment_id = payload['id']
            arrival_node = payload['arrivalNode']
            status = payload['status']
            current_location = payload.get('currentLocation', arrival_node)
            created_at = payload['createdAt']

            existing = await db.shipments.find_one({'id': shipment_id})
            if not existing:
                await db.shipments.insert_one({
                    'id': shipment_id,
                    'arrivalNode': arrival_node,
                    'status': status,
                    'currentLocation': current_location,
                    'createdAt': datetime.fromisoformat(created_at.replace('Z', '+00:00')) if created_at else datetime.now()
                })
                logger.info(f"New shipment {shipment_id} synced from MQTT to DB at {arrival_node}")

            destination_node = 'B2'
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
            else:
                logger.warning(f"No crane available for new shipment {shipment_id} via MQTT")
    except Exception as e:
        logger.error(f"Error handling new shipment MQTT: {e}")

# ----------------------- MQTT Handler -----------------------
async def mqtt_handler(db, mqtt_client, analyzer, task_assigner):
    await mqtt_client.subscribe("harboursense/edge/+/update")
    logger.info("Subscribed to edge updates")
    await mqtt_client.subscribe('harboursense/shipment/new')
    logger.info("Subscribed to shipment/new topic")

    async for message in mqtt_client.messages:
        topic = str(message.topic)
        try:
            payload = message.payload.decode()
            logger.debug(f"MQTT message received on topic={topic}, raw={payload}")
        except Exception as e:
            logger.error(f"Error decoding MQTT payload: {e}, raw={message.payload}")
            continue

        if 'harboursense/shipment/new' in topic:
            await handle_new_shipment(db, task_assigner, analyzer, message)
            continue

        edge_id = None
        try:
            data = json.loads(payload)
            edge_id = topic.split('/')[2]
            logger.debug(f"Processing edge update for {edge_id}: {json.dumps(data, indent=2)}")
            edge = await db.edgeDevices.find_one({'id': edge_id})
            if not edge:
                continue

            current = data.get('currentLocation')
            final = data.get('finalNode')
            phase = data.get('taskPhase')

            await handle_traffic_update(db, mqtt_client, edge_id, analyzer)

            if phase == 'assigned' and current == final:
                shipment = await db.shipments.find_one({'assignedDevice': edge_id, 'status': 'processing'})
                if shipment:
                    await db.shipments.update_one({'_id': shipment['_id']}, {'$set': {'status': 'completed'}})
                    logger.info(f"Shipment {shipment['id']} completed by {edge_id}")
                logger.info(f"Edge {edge_id} arrived at final node {current}")

        except Exception as e:
            logger.error(f"Error processing MQTT message for {edge_id or 'unknown'}: {e} - Payload: {payload}")

# ----------------------- Setup -----------------------
async def setup():
    uri = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
    client = AsyncIOMotorClient(uri)
    db = client["port"]
    logger.info("Connected to MongoDB")

    tls_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    tls_context.load_verify_locations(cafile="../certs/AmazonRootCA1.pem")
    tls_context.load_cert_chain(
        certfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt",
        keyfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key"
    )

    async with aiomqtt.Client(
        hostname="a1dghi6and062t-ats.iot.us-east-1.amazonaws.com",
        port=8883,
        tls_context=tls_context,
        identifier="manager"
    ) as mqtt_client:
        logger.info("Connected to MQTT broker")

        from traffic_analyzer import convert_bson_numbers
        raw_nodes = await db.graph.find().to_list(None)
        if not raw_nodes:
            logger.error("Graph missing or empty in DB")
            return

        processed_nodes = convert_bson_numbers(raw_nodes)
        graph = {node['id']: node for node in processed_nodes if 'id' in node}
        logger.info(f"Loaded graph with {len(graph)} nodes")

        analyzer = TrafficAnalyzer(db, mqtt_client, graph)
        asyncio.create_task(analyzer.start_mqtt_listener())
        logger.info("TrafficAnalyzer listener started")

        task_assigner = TaskAssigner(db, mqtt_client, analyzer)
        shipment_manager = ShipmentManager(db, task_assigner)

        await analyzer.analyze_metrics("startup")
        logger.info("Initial traffic analysis complete")

        asyncio.create_task(shipment_watcher(db, shipment_manager))
        asyncio.create_task(task_assigner.monitor_and_assign())
        asyncio.create_task(mqtt_handler(db, mqtt_client, analyzer, task_assigner))

        logger.info("Setup complete. Manager running.")
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(setup())
