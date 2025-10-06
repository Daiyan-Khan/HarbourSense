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

# ----------------------- ShipmentManager -----------------------
class ShipmentManager:
    def __init__(self, db, task_assigner):
        self.db = db
        self.task_assigner = task_assigner

    async def process_shipment_steps(self, shipment):
        logger.debug(f"Processing shipment: {shipment.get('id')} with status: {shipment.get('status')}")
        shipment_id = shipment['id']
        status = shipment['status']
        
        # ADDED: Log full shipment doc for invalid tracing
        logger.debug(f"Full shipment doc for {shipment_id}: {json.dumps(shipment, default=str, indent=2)}")
        current_node = shipment.get('currentNode', 'A1')
        dest = shipment.get('destination', 'C5')
        logger.debug(f"Shipment {shipment_id} nodes: current={current_node}, dest={dest} (Null check: current=='Null'={current_node=='Null'}, dest=='Null'={dest=='Null'})")
        
        if dest == 'Null':
            logger.warning(f"Invalid Null dest in shipment {shipment_id}; fixing to 'C5'")
            dest = 'C5'
            await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'destination': 'C5'}})
        
        # Handle phase-specific assignments
        if status in ['waiting', 'docked']:  # Initial → treat as docked
            # Assign crane for offload
            task_details = {
                'task': 'offload',
                'startNode': current_node,
                'finalNode': current_node  # Short path for offload
            }
            logger.debug(f"Attempting crane assignment for {shipment_id} docked: details={task_details}")
            crane_assigned = await self.task_assigner._assign_stage_device(shipment_id, 'docked', task_details)
            if crane_assigned:
                # Preemptive: Prep truck to staging (e.g., A3 near dock)
                truck_details = {
                    'task': 'prep_transport',
                    'startNode': current_node,
                    'finalNode': 'A3',  # Dynamic staging
                    'path': []  # Computed in assign
                }
                logger.debug(f"Preemptively assigning truck for {shipment_id}: details={truck_details}")
                await self.task_assigner._assign_stage_device(shipment_id, 'offloaded', truck_details, device_type='truck')
                # Preemptive: Prep robot at B4 (dest)
                b4 = 'B4'  # Intermediate before warehouse
                robot_details = {
                    'task': 'prep_storing',
                    'startNode': 'B3',  # Current robot loc
                    'finalNode': b4,
                    'path': []
                }
                logger.debug(f"Preemptively assigning robot for {shipment_id}: details={robot_details}")
                await self.task_assigner._assign_stage_device(shipment_id, 'transported', robot_details, device_type='robot')
                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'offloading'}})  # Advance to active
                logger.info(f"Assigned crane {crane_assigned} for offload on {shipment_id}; prepped truck/robot")
            else:
                logger.warning(f"No crane available for {shipment_id} offload")
        elif status == 'offloaded':
            # Assign truck for transport to B4/dest
            task_details = {
                'task': 'transport',
                'startNode': current_node,
                'finalNode': 'B4'  # Or dynamic intermediate to dest
            }
            logger.debug(f"Attempting truck assignment for {shipment_id} offloaded: details={task_details}")
            truck_assigned = await self.task_assigner._assign_stage_device(shipment_id, 'offloaded', task_details)
            if truck_assigned:
                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'transporting'}})
                logger.info(f"Assigned truck {truck_assigned} for transport on {shipment_id}")
            else:
                logger.warning(f"No truck available for {shipment_id} transport")
        elif status == 'transported':
            # Assign robot for storing at B4/C5
            task_details = {
                'task': 'store',
                'startNode': current_node,
                'finalNode': dest,
                'path': []  # Short/local
            }
            logger.debug(f"Attempting robot assignment for {shipment_id} transported: details={task_details}")
            robot_assigned = await self.task_assigner._assign_stage_device(shipment_id, 'transported', task_details)
            if robot_assigned:
                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'status': 'storing'}})
                logger.info(f"Assigned robot {robot_assigned} for storing on {shipment_id} at {dest}")
            else:
                logger.warning(f"No robot available for {shipment_id} storing")

# ----------------------- Reroute Helper -----------------------
def needs_reroute(current_node, remaining_path, analyzer):
    predicted_loads = analyzer.get_predicted_loads()
    logger.debug(f"Checking reroute for current node {current_node}, remaining path {remaining_path}")
    # ADDED: Log predicted loads sample for invalid tracing
    logger.debug(f"Predicted loads sample: {dict(list(predicted_loads.items())[:5])} (type: {type(list(predicted_loads.values())[0]) if predicted_loads else 'empty'})")
    for node in remaining_path:
        load = predicted_loads.get(node, 0)
        logger.debug(f"Node {node} predicted load: {load} (type: {type(load)})")
        if load > 2:
            logger.info(f"Reroute needed: Node {node} has predicted load {load}")
            return True
    return False

# ----------------------- Traffic Update Handler -----------------------
async def handle_traffic_update(db, mqtt_client, edge_id, analyzer):
    edge = await db.edgeDevices.find_one({'id': edge_id})
    # ADDED: Log full edge doc for invalid tracing
    logger.debug(f"Full edge doc for {edge_id}: {json.dumps(edge, default=str, indent=2) if edge else 'None'}")
    if not edge or not edge.get('path'):
        logger.debug(f"No path found for edge {edge_id}, skipping reroute check")
        return

    path = edge.get('path', [])
    dest = edge.get('destinationNode', edge.get('finalNode', 'B4'))
    logger.debug(f"Edge {edge_id} path: {path} (type: {type(path)}), dest: {dest} (Null check: {dest=='Null'})")
    
    await analyzer.analyze_metrics(triggered_by=f"Reroute check for {edge_id}")
    remaining_path = path[1:] if len(path) > 1 else []
    
    if needs_reroute(edge['currentLocation'], remaining_path, analyzer):
        start = edge['currentLocation']
        node_loads = analyzer.get_current_loads()
        route_congestion = analyzer.get_route_congestion()
        predicted_loads = analyzer.get_predicted_loads()
        # ADDED: Log inputs for division/Null tracing
        logger.debug(f"Computing new path for {edge_id}. Start: {start}, Dest: {dest}, node_loads sample: {dict(list(node_loads.items())[:3])}, congestion sample: {dict(list(route_congestion.items())[:3])}, predicted sample: {dict(list(predicted_loads.items())[:3])}")

        new_path = analyzer.planner.compute_path(start, dest, node_loads, route_congestion, predicted_loads=predicted_loads)
        if new_path:
            update_msg = {'path': new_path, 'finalNode': dest}
            await mqtt_client.publish(f"harboursense/traffic/{edge_id}", json.dumps(update_msg))
            logger.info(f"Pushed reroute for {edge_id} due to detected load. New path: {new_path}")
        else:
            logger.warning(f"Failed to compute reroute for {edge_id} (invalid nodes? check logs above)")
    else:
        logger.debug(f"No reroute needed for {edge_id}")

# ----------------------- Shipment MQTT Handler (Updated for Specific IDs) -----------------------
async def handle_shipment_update(db, task_assigner, analyzer, message):
    try:
        topic = str(message.topic)
        # ADDED: Log raw message for tracing
        raw_payload = message.payload.decode('utf-8', errors='ignore')
        logger.debug(f"Raw MQTT shipment message: topic={topic}, payload={raw_payload}")
        
        if 'harboursense/shipments/' in topic:
            try:
                payload = json.loads(raw_payload)
                # ADDED: Log parsed payload
                logger.debug(f"Parsed shipment payload: {json.dumps(payload, indent=2)} (keys: {list(payload.keys())})")
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for shipment topic {topic}: {e}; raw={raw_payload}")
                return
            
            shipment_id = topic.split('/')[-1]  # Extract ID from topic (e.g., /shipment_1)
            logger.debug(f"Received shipment update for {shipment_id}: {json.dumps(payload, indent=2)}")
            
            # Check for invalid/Null in payload
            status = payload.get('status', 'waiting')
            current_node = payload.get('currentNode', 'A1')
            dest = payload.get('destination', 'C5')
            logger.debug(f"Payload extracted - status: {status}, current: {current_node} (Null? {current_node=='Null'}), dest: {dest} (Null? {dest=='Null'})")
            if dest == 'Null':
                logger.warning(f"Null dest in payload for {shipment_id}; fixing to 'C5'")
                payload['destination'] = 'C5'
                dest = 'C5'
            
            # Upsert/update in DB
            created_at = payload.get('createdAt')
            try:
                created_at_parsed = datetime.fromisoformat(created_at.replace('Z', '+00:00')) if created_at else datetime.now()
            except ValueError as e:
                logger.warning(f"Invalid createdAt '{created_at}' for {shipment_id}: {e}; using now")
                created_at_parsed = datetime.now()
            
            update_data = {
                'id': shipment_id,
                'status': status,
                'currentNode': current_node,
                'needsOffloading': payload.get('needsOffloading', False),
                'destination': dest,
                'assignedEdges': payload.get('assignedEdges', []),
                'createdAt': created_at_parsed,
                'updatedAt': datetime.now()
            }
            # ADDED: Log update_data before DB write
            logger.debug(f"Updating DB for {shipment_id} with: {json.dumps(update_data, default=str, indent=2)}")
            
            await db.shipments.update_one({'id': shipment_id}, {'$set': update_data}, upsert=True)
            logger.info(f"Synced shipment {shipment_id} update: status={status} at {current_node}")

            # Trigger assignment based on new status (e.g., 'docked' from port.js init)
            if status in ['waiting', 'docked']:  # Initial → assign offload
                task_details = {'startNode': current_node, 'finalNode': current_node}
                logger.debug(f"Triggering docked assignment for {shipment_id}: {task_details}")
                await task_assigner._assign_stage_device(shipment_id, 'docked', task_details)
            elif status == 'offloaded':
                # Trigger transport
                task_details = {'startNode': current_node, 'finalNode': 'B4'}  # Intermediate to dest
                logger.debug(f"Triggering offloaded assignment for {shipment_id}: {task_details}")
                await task_assigner._assign_stage_device(shipment_id, 'offloaded', task_details)
            elif status == 'transported':
                # Trigger storing
                task_details = {'startNode': current_node, 'finalNode': dest}
                logger.debug(f"Triggering transported assignment for {shipment_id}: {task_details}")
                await task_assigner._assign_stage_device(shipment_id, 'transported', task_details)
        elif topic.startswith('harboursense/edge/completion/'):
            edge_id = topic.split('/')[-1]
            payload = json.loads(message.payload)
            shipment_id = payload.get('shipmentId')
            phase = payload.get('phase')
            if shipment_id:
                new_status = {'offload': 'offloaded', 'transport': 'transported', 'store': 'stored'}.get(phase, status)
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {'status': new_status, 'currentNode': payload.get('currentLocation', 'A1')}}
                )
                logger.info(f"Completed {phase} for {shipment_id} by {edge_id}; status → {new_status}")
                # Trigger next via monitor (or direct if needed)
                
    except Exception as e:
        logger.error(f"Error handling shipment MQTT (topic {topic}): {e}; raw payload={raw_payload[:200]}...")


# ----------------------- Completion MQTT Handler -----------------------
async def handle_completion(db, task_assigner, message):
    try:
        topic = str(message.topic)
        raw_payload = message.payload.decode('utf-8', errors='ignore')
        # ADDED: Log raw for tracing
        logger.debug(f"Raw completion MQTT: topic={topic}, payload={raw_payload}")
        
        if 'harboursense/edge/' in topic and '/completed' in topic:
            try:
                payload = json.loads(raw_payload)
                # ADDED: Log parsed
                logger.debug(f"Parsed completion payload: {json.dumps(payload, indent=2)}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for completion {topic}: {e}; raw={raw_payload}")
                return
            
            device_id = topic.split('/')[2]
            shipment_id = payload.get('shipmentId')
            task = payload.get('task', '')
            location = payload.get('location', 'A1')
            # ADDED: Log extracted values
            logger.debug(f"Completion extracted: device={device_id}, shipment={shipment_id} (Null? {shipment_id=='Null'}), task={task}, loc={location}")
            
            logger.info(f"Received completion for {device_id} on task {task} for shipment {shipment_id}")
            
            # Advance shipment and assign next
            await task_assigner.handle_completion(device_id, task)
            
            # If shipment linked, publish updated status back
            if shipment_id and shipment_id != 'Null':
                shipment = await db.shipments.find_one({'id': shipment_id})
                # ADDED: Log shipment lookup
                logger.debug(f"Shipment lookup for {shipment_id}: {json.dumps(shipment, default=str, indent=2) if shipment else 'Not found'}")
                if shipment:
                    update_payload = {
                        'id': shipment_id,
                        'status': shipment.get('status', 'stored'),
                        'currentNode': location,
                        'updatedAt': datetime.now().isoformat() + 'Z'
                    }
                    # ADDED: Log publish payload
                    logger.debug(f"Publishing completion update for {shipment_id}: {json.dumps(update_payload, indent=2)}")
                    await task_assigner.mqtt_client.publish(f"harboursense/shipments/{shipment_id}", json.dumps(update_payload))
                    logger.info(f"Published phase update for {shipment_id}: {update_payload['status']} at {location}")
            else:
                logger.warning(f"Invalid/Null shipment_id '{shipment_id}' in completion from {device_id}; skipping publish")
    except Exception as e:
        logger.error(f"Error handling completion MQTT (topic {topic}): {e}; raw={raw_payload[:200]}...")

# ----------------------- MQTT Handler (Updated) -----------------------
async def mqtt_handler(db, mqtt_client, analyzer, task_assigner):
    # FIXED: Subscribe to individual shipments, completions, and edges
    await mqtt_client.subscribe("harboursense/shipments/+")  # Specific IDs
    logger.info("Subscribed to shipments/+ (individual IDs)")
    await mqtt_client.subscribe("harboursense/edge/+/completed")  # Completions
    logger.info("Subscribed to edge completions")
    await mqtt_client.subscribe("harboursense/edge/+/update")
    logger.info("Subscribed to edge updates")

    async for message in mqtt_client.messages:
        topic = str(message.topic)
        try:
            raw_payload = message.payload.decode('utf-8', errors='ignore')
            # ADDED: Log EVERY message for full tracing
            logger.debug(f"=== MQTT MESSAGE RECEIVED === Topic: {topic}, Raw Payload: {raw_payload} (len: {len(raw_payload)})")
        except Exception as e:
            logger.error(f"Error decoding MQTT payload for {topic}: {e}, raw bytes: {message.payload}")
            continue

        # FIXED: Route to handlers
        if 'harboursense/shipments/' in topic:
            await handle_shipment_update(db, task_assigner, analyzer, message)
            continue
        elif 'harboursense/edge/' in topic and '/completed' in topic:
            await handle_completion(db, task_assigner, message)
            continue
        elif 'harboursense/edge/' in topic and '/update' in topic:
            try:
                data = json.loads(raw_payload)
                # ADDED: Log parsed edge update
                logger.debug(f"Processed edge update payload: {json.dumps(data, indent=2)}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON error in edge update {topic}: {e}; raw={raw_payload}")
                continue
            edge_id = topic.split('/')[2]
            logger.debug(f"Processing edge update for {edge_id}: {json.dumps(data, indent=2)}")
            await handle_traffic_update(db, mqtt_client, edge_id, analyzer)
            # Check for arrival/completion logic (if phase 'assigned' and loc==final)
            edge = await db.edgeDevices.find_one({'id': edge_id})
            # ADDED: Log edge for arrival check
            logger.debug(f"Edge doc for arrival check {edge_id}: {json.dumps(edge, default=str, indent=2) if edge else 'None'}")
            if edge and edge.get('taskPhase') == 'assigned' and data.get('currentLocation') == edge.get('finalNode'):
                shipment = await db.shipments.find_one({'assignedDevices': edge_id, 'status': {'$in': ['offloading', 'transporting']}})
                # ADDED: Log shipment lookup
                logger.debug(f"Shipment lookup for arrival {edge_id}: {json.dumps(shipment, default=str, indent=2) if shipment else 'None'}")
                if shipment:
                    await db.shipments.update_one({'id': shipment['id']}, {'$set': {'status': 'completed'}})
                    logger.info(f"Shipment {shipment['id']} completed by {edge_id} on arrival")
            continue

        try:
            data = json.loads(raw_payload)
            edge_id = topic.split('/')[2] if '/' in topic else 'unknown'
            # ADDED: Log unhandled payload
            logger.debug(f"Unhandled MQTT payload for {edge_id}: {json.dumps(data, indent=2)}")
        except Exception as e:
            logger.error(f"Error processing MQTT message for {edge_id or 'unknown'}: {e} - Payload: {raw_payload}")

# ----------------------- Setup -----------------------
async def setup():
    uri = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
    client = AsyncIOMotorClient(uri)
    db = client["port"]
    logger.info("Connected to MongoDB")

    # ADDED: Log DB collections for init check
    collections = await db.list_collection_names()
    logger.debug(f"DB collections: {collections}")

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
        # ADDED: Log raw graph for invalid tracing
        logger.debug(f"Raw graph docs count: {len(raw_nodes)}; sample: {json.dumps(raw_nodes[:1], default=str) if raw_nodes else 'Empty'}")
        if not raw_nodes:
            logger.error("Graph missing or empty in DB")
            return

        processed_nodes = convert_bson_numbers(raw_nodes)
        graph = {node['id']: node for node in processed_nodes if 'id' in node}
        logger.info(f"Loaded graph with {len(graph)} nodes")
        # ADDED: Log graph sample
        logger.debug(f"Graph sample keys: {list(graph.keys())[:5]}")

        analyzer = TrafficAnalyzer(db, mqtt_client, graph)
        asyncio.create_task(analyzer.start_mqtt_listener())
        logger.info("TrafficAnalyzer listener started")

        task_assigner = TaskAssigner(db, mqtt_client, analyzer)
        shipment_manager = ShipmentManager(db, task_assigner)

        # ADDED: Log initial shipments/edges for Null tracing
        initial_shipments = await db.shipments.find().to_list(None)
        logger.debug(f"Initial shipments count: {len(initial_shipments)}; sample dests: {[s.get('destination') for s in initial_shipments[:3]]}")
        initial_edges = await db.edgeDevices.find().to_list(None)
        logger.debug(f"Initial edges count: {len(initial_edges)}; sample finalNodes: {[e.get('finalNode') for e in initial_edges[:3] if e]}")
        
        await analyzer.analyze_metrics("startup")
        logger.info("Initial traffic analysis complete")

        # FIXED: Start unified monitor (includes watcher)
        asyncio.create_task(task_assigner.monitor_and_assign())
        asyncio.create_task(mqtt_handler(db, mqtt_client, analyzer, task_assigner))

        logger.info("Setup complete. Manager running.")
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(setup())
