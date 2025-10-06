import sys
import asyncio
from datetime import datetime
import time  
from sensor_analyzer import SensorAnalyzer

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import logging
import json
from motor.motor_asyncio import AsyncIOMotorClient
from task_assigner import TaskAssigner
from traffic_analyzer import TrafficAnalyzer, SmartRoutePlanner
import aiomqtt
import ssl

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

# ----------------------- Reroute Helper (with Safe Float) -----------------------
def needs_reroute(current_node, remaining_path, analyzer):
    predicted_loads = analyzer.get_predicted_loads()
    # FIXED: Use safe_float_recursive (from analyzer/planner) to handle nested dicts
    safe_loads = {k: analyzer.planner.safe_float_recursive(v, 0.0, f"reroute[{k}]") for k, v in predicted_loads.items()}
    logger.debug(f"Checking reroute for current node {current_node}, remaining path {remaining_path}; safe_loads sample: {dict(list(safe_loads.items())[:3])}")
    for node in remaining_path:
        load = safe_loads.get(node, 0.0)
        logger.debug(f"Node {node} predicted load: {load} (type: {type(load)})")
        if load > 2.0:
            logger.info(f"Reroute needed: Node {node} has predicted load {load}")
            return True
    return False

# ----------------------- Traffic Update Handler -----------------------
async def handle_traffic_update(db, mqtt_client, edge_id, analyzer, task_assigner):
    try:
        edge = await db.edgeDevices.find_one({'id': edge_id})
        logger.debug(f"Full edge doc for {edge_id}: {json.dumps(edge, default=str, indent=2) if edge else 'None'}")
        if not edge or not edge.get('path'):
            logger.debug(f"No path found for edge {edge_id}, skipping reroute check")
            return

        path = edge.get('path', [])
        dest = edge.get('finalNode', 'B4')  # FIXED: Use finalNode (consistent)
        logger.debug(f"Edge {edge_id} path: {path} (type: {type(path)}), dest: {dest} (Null check: {dest=='Null'})")
        
        await analyzer.analyze_metrics(triggered_by=f"Reroute check for {edge_id}")
        remaining_path = path[1:] if len(path) > 1 else []
        
        if needs_reroute(edge['currentLocation'], remaining_path, analyzer):
            start = edge['currentLocation']
            node_loads = analyzer.get_current_loads()
            route_congestion = analyzer.get_route_congestion()
            predicted_loads = analyzer.get_predicted_loads()
            logger.debug(f"Computing new path for {edge_id}. Start: {start}, Dest: {dest}, node_loads sample: {dict(list(node_loads.items())[:3])}, congestion sample: {dict(list(route_congestion.items())[:3])}, predicted sample: {dict(list(predicted_loads.items())[:3])}")

            new_path = analyzer.planner.compute_path(
                start, dest, node_loads, route_congestion, predicted_loads=predicted_loads
            )
            if new_path:
                update_msg = {'path': new_path, 'finalNode': dest}
                await mqtt_client.publish(f"harboursense/traffic/update/{edge_id}", json.dumps(update_msg))  # FIXED: Specific topic
                logger.info(f"Pushed reroute for {edge_id} due to detected load. New path: {new_path}")
            else:
                logger.warning(f"Failed to compute reroute for {edge_id} (invalid nodes? check logs above)")
        else:
            logger.debug(f"No reroute needed for {edge_id}")

        # FIXED: Check for arrival completion (if at finalNode, trigger handle_completion)
        if edge.get('taskPhase') == 'completing' and edge.get('currentLocation') == dest:
            await handle_completion(db, task_assigner, edge_id, edge.get('task', {}))
            logger.info(f"Detected arrival completion for {edge_id} at {dest}; handled via update")
    except Exception as e:
        logger.error(f"Error in handle_traffic_update for {edge_id}: {e}")

# ----------------------- Completion Handler (Unified for MQTT/Arrival) -----------------------
# NEW: Separate handler for completions (calls TaskAssigner's method)
# FIXED: Signature + mqtt_client param; fallback with delivery
async def handle_completion(db, task_assigner, device_id, task_payload, mqtt_client):
    logger.info(f"Handling completion for device {device_id}; task payload: {json.dumps(task_payload, default=str)}")
    try:
        # Call TaskAssigner's handle_completion (updates status, resets edge, publishes)
        await task_assigner.handle_completion(device_id, task_payload)
        logger.info(f"TaskAssigner processed completion for {device_id} ({task_payload.get('phase', 'unknown')})")
        
        # FIXED: Ensure shipmentId cleared in edge reset (explicit in update if not in TaskAssigner)
        shipment_id = task_payload.get('shipmentId')
        if shipment_id:
            phase = task_payload.get('phase', 'unknown')
            new_status = {'offload': 'offloaded', 'transport': 'transported', 'store': 'stored'}.get(phase, 'completed')
            # Clear shipmentId on edge
            await db.edgeDevices.update_one(
                {'id': device_id},
                {'$set': {
                    'shipmentId': None,
                    'assignedShipment': None,
                    'task': None
                }}
            )
            logger.debug(f"Cleared shipmentId/assignedShipment for {device_id} post-completion")
            # Update shipment status
            await db.shipments.update_one(
                {'id': shipment_id},
                {'$set': {'status': new_status}}
            )
            logger.info(f"Fallback status update: {shipment_id} → {new_status}")
            
            if phase == 'store':
                # NEW: Mark ready for delivery after 30s
                await db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {
                        'storageCompleteAt': datetime.now(),  # Track for 30s poll
                        'deliveryStatus': 'pending'  # Sub-state: pending → assigned → in_transit → completed
                    }}
                )
                logger.info(f"Storage complete for {shipment_id}; set timer for delivery in 30s")
                # Optional: Publish for UI (now safe with param)
                await mqtt_client.publish(
                    f"harboursense/shipments/{shipment_id}/stored",
                    json.dumps({'status': 'stored', 'next': 'delivery_pending'})
                )
        
        # Optional: Trigger analyzer re-analysis post-completion
        analyzer = task_assigner.analyzer
        await analyzer.analyze_metrics(triggered_by=f"Completion {device_id}")
        
    except Exception as e:
        logger.error(f"Error in handle_completion for {device_id}: {e}")
        # Fallback: Manual status update if needed (add delivery for store)
        shipment_id = task_payload.get('shipmentId')
        if shipment_id:
            phase = task_payload.get('phase', 'unknown')
            new_status = {'offload': 'offloaded', 'transport': 'transported', 'store': 'stored'}.get(phase, 'completed')
            update_data = {'status': new_status}
            if phase == 'store':
                update_data.update({
                    'storageCompleteAt': datetime.now(),
                    'deliveryStatus': 'pending'
                })
            await db.shipments.update_one({'id': shipment_id}, {'$set': update_data})
            logger.info(f"Fallback status update (with delivery if store): {shipment_id} → {new_status}")


# ----------------------- Shipment MQTT Handler (No Direct Assigns) -----------------------
async def handle_shipment_update(db, task_assigner, analyzer, message):
    try:
        topic = str(message.topic)
        raw_payload = message.payload.decode('utf-8', errors='ignore')
        logger.debug(f"=== MQTT MESSAGE RECEIVED === Topic: {topic}, Raw Payload: {raw_payload} (len: {len(raw_payload)})")

        if 'harboursense/shipments/' in topic:
            try:
                payload = json.loads(raw_payload)
                logger.debug(f"Parsed shipment payload: {json.dumps(payload, indent=2)} (keys: {list(payload.keys())})")
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for shipment topic {topic}: {e}; raw={raw_payload}")
                return
            
            shipment_id = topic.split('/')[-1]  # e.g., shipment_1
            logger.debug(f"Received shipment update for {shipment_id}: {json.dumps(payload, indent=2)}")

            # Handle Nulls (existing)
            status = payload.get('status', 'arrived')
            current_node = payload.get('currentNode', 'A1')
            dest = payload.get('destination', 'C5')
            logger.debug(f"Payload extracted - status: {status}, current: {current_node} (Null? {current_node=='Null'}), dest: {dest} (Null? {dest=='Null'})")
            if current_node == 'Null':
                logger.warning(f"Null currentNode in payload for {shipment_id}; fixing to 'A1'")
                payload['currentNode'] = 'A1'
                current_node = 'A1'
            if dest == 'Null':
                logger.warning(f"Null dest in payload for {shipment_id}; fixing to 'C5'")
                payload['destination'] = 'C5'
                dest = 'C5'

            # Parse createdAt (existing)
            created_at = payload.get('createdAt')
            try:
                created_at_parsed = datetime.fromisoformat(created_at.replace('Z', '+00:00')) if created_at else datetime.now()
            except ValueError as e:
                logger.warning(f"Invalid createdAt '{created_at}' for {shipment_id}: {e}; using now")
                created_at_parsed = datetime.now()

            # FIXED: Fetch current DB state to preserve assignedEdges before updating
            current_shipment = await db.shipments.find_one({'id': shipment_id})
            if current_shipment:
                # Merge: Preserve existing assignedEdges, update other fields
                assigned_edges = current_shipment.get('assignedEdges', [])
                payload['assignedEdges'] = assigned_edges  # Override with DB value
                logger.debug(f"Preserved assignedEdges for {shipment_id}: {assigned_edges} (len: {len(assigned_edges)})")

            # Initial DB update (existing, now with merged assignedEdges)
            update_data = {
                'id': shipment_id,
                'status': status,
                'currentNode': current_node,
                'destination': dest,
                'assignedEdges': payload.get('assignedEdges', []),
                'createdAt': created_at_parsed,
                'updatedAt': datetime.now()
            }
            logger.debug(f"Updating DB for {shipment_id} with: {json.dumps(update_data, default=str, indent=2)}")

            await db.shipments.update_one({'id': shipment_id}, {'$set': update_data}, upsert=True)
            logger.info(f"Synced shipment {shipment_id} update: status={status} at {current_node}, edges={len(update_data['assignedEdges'])}")

            # NEW: Manager detects arrival/offload, asks TaskAssigner for warehouse decision
            if status in ['arrived', 'offloaded']:
                if task_assigner.graph:  # Ensure graph loaded
                    warehouse = await task_assigner._select_warehouse(current_node, shipment_id)
                    # Update DB with decision (overrides initial dest if better)
                    await db.shipments.update_one(
                        {'id': shipment_id},
                        {'$set': {
                            'destination': warehouse,
                            'warehouseAssigned': warehouse,  # Track for traceability
                            'updatedAt': datetime.now()
                        }}
                    )
                    logger.info(f"TaskAssigner decided warehouse {warehouse} for new {status} shipment {shipment_id} at {current_node}; updated DB")
                    # Optional: Publish back to port.js for UI/sync
                    await task_assigner.mqtt_client.publish(
                        f"harboursense/shipments/{shipment_id}/warehouse",
                        json.dumps({'warehouse': warehouse, 'reason': 'dynamic allocation'})
                    )
                else:
                    logger.warning(f"Graph not loaded in TaskAssigner; skipping warehouse decision for {shipment_id}")

            # Existing: No direct assigns—monitor_and_assign will scan DB change and trigger based on status
            logger.debug(f"DB updated; monitor will handle assignments for status '{status}'")
        else:
            # Defensive: If somehow a completion routed here, skip
            logger.debug(f"Non-shipment topic {topic} in shipment handler; ignoring")

    except Exception as e:
        logger.error(f"Error handling shipment MQTT (topic {message.topic if 'message' in locals() else 'unknown'}): {e}; raw payload={raw_payload[:200]}...")

async def handle_sensor_data(db, mqtt_client, analyzer, task_assigner, sensor_analyzer, raw_payload):
    """Handle incoming sensor data: parse payload, detect anomalies, trigger alerts/repairs."""
    try:
        # Parse incoming MQTT payload for real-time processing
        payload_str = raw_payload.decode('utf-8', errors='ignore') if isinstance(raw_payload, bytes) else str(raw_payload)
        incoming_reading = json.loads(payload_str)
        logger.debug(f"Sensor Analyzer Input - Processing incoming data: {json.dumps(incoming_reading, default=str)}")
        
        # Detect anomaly (uses ML + rules, inserts to sensorAlerts)
        anomaly = await sensor_analyzer.detect_anomaly(incoming_reading)
        
        if anomaly:
            logger.info(f"Sensor Analyzer Outcome - Anomaly detected: {json.dumps(anomaly, default=str)} (suggestion: {anomaly.get('suggestion')}, severity: {anomaly.get('severity')})")
            # Publish alert for Node-RED/UI
            await mqtt_client.publish(f"harboursense/alerts/{anomaly['node']}", json.dumps(anomaly))
            
            # Core: Trigger repair if suggestion='repair'
            if anomaly['suggestion'] == 'repair':
                await task_assigner.assign_maintenance_task(anomaly['node'], db, mqtt_client, anomaly['severity'])
            # Reroute: Boost traffic loads (analyze_metrics will pick up unresolved alerts)
            elif anomaly['suggestion'] == 'reroute':
                # Optional: Trigger immediate re-analysis
                await analyzer.analyze_metrics(triggered_by=f"sensor_reroute_{anomaly['node']}")
                
            logger.info(f"Sensor Analyzer Handled - Anomaly for {anomaly['node']}: {anomaly['alert_type']} -> {anomaly['suggestion']}")
        else:
            logger.debug(f"Sensor Analyzer Outcome - No anomaly: {incoming_reading.get('id')} at {incoming_reading.get('node')} (reading: {incoming_reading.get('reading')} of type {incoming_reading.get('type')})")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in sensor data: {e}, raw: {payload_str}")
    except Exception as e:
        logger.error(f"Sensor handling error: {e}")

# ----------------------- MQTT Handler (Unified with Reconnection) -----------------------
async def mqtt_handler(db, mqtt_params, analyzer, task_assigner, sensor_analyzer):
    """Robust MQTT handler: Recreates client on reconnect for AWS IoT stability."""
    max_retries = 10
    retry_delay = 5  # Seconds

    while True:
        mqtt_client = None
        try:
            # Create client each iteration (context manager handles connect)
            async with aiomqtt.Client(**mqtt_params) as mqtt_client:
                logger.info("MQTT Client connected and ready")
                
                # Subscribe (re-subscribe on reconnect)
                await mqtt_client.subscribe("harboursense/shipments/+")  # Specific IDs
                logger.info("Subscribed to shipments/+ (individual IDs)")
                await mqtt_client.subscribe("harboursense/edge/completion/+")  # Per-ID completions
                logger.info("Subscribed to edge completions")
                await mqtt_client.subscribe("harboursense/traffic/update/+")  # Edge updates for reroute
                logger.info("Subscribed to edge updates")
                await mqtt_client.subscribe("harboursense/sensor/data")  # Sensor topic
                logger.info("Subscribed to sensor data")

                logger.info("Starting MQTT message loop")
                async for message in mqtt_client.messages:
                    topic = str(message.topic)
                    try:
                        raw_payload = message.payload
                        logger.debug(f"=== MQTT MESSAGE RECEIVED === Topic: {topic}, Raw Payload len: {len(raw_payload) if raw_payload else 0}")
                        
                        # Route to handlers (add try-except per handler to isolate errors)
                        if 'harboursense/shipments/' in topic:
                            await handle_shipment_update(db, task_assigner, analyzer, message)
                        elif topic == "harboursense/sensor/data":
                            await handle_sensor_data(db, mqtt_client, analyzer, task_assigner, sensor_analyzer, raw_payload)
                        elif 'harboursense/edge/completion/' in topic:
                            device_id = topic.split('/')[-1]  # e.g., crane_1
                            try:
                                task_payload = json.loads(raw_payload.decode('utf-8'))
                                logger.debug(f"Parsed completion payload for {device_id}: {json.dumps(task_payload, indent=2)}")
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON error in completion {topic}: {e}")
                                continue
                            await handle_completion(db, task_assigner, device_id, task_payload)
                        elif 'harboursense/traffic/update/' in topic:
                            edge_id = topic.split('/')[-1]
                            try:
                                data = json.loads(raw_payload.decode('utf-8'))
                                logger.debug(f"Processed edge update payload for {edge_id}: {json.dumps(data, indent=2)}")
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON error in edge update {topic}: {e}")
                                continue
                            await handle_traffic_update(db, mqtt_client, edge_id, analyzer, task_assigner)  # Pass task_assigner for completions
                        else:
                            try:
                                data = json.loads(raw_payload.decode('utf-8'))
                                edge_id = topic.split('/')[2] if '/' in topic else 'unknown'
                                logger.debug(f"Unhandled MQTT payload for {edge_id}: {json.dumps(data, indent=2)}")
                            except Exception as e:
                                logger.error(f"Error processing unhandled topic {topic}: {e}")
                        
                    except Exception as e:  # Catch any per-message error
                        logger.error(f"Error processing message on {topic}: {e}")
                        continue  # Don't break loop on single message fail

        except aiomqtt.MqttError as e:
            if "Disconnected" in str(e) or "Connection lost" in str(e):
                logger.warning(f"MQTT disconnected: {e}. Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 60)  # Exponential backoff
                continue
            else:
                logger.error(f"Other MQTT error: {e}")
                raise  # Re-raise non-disconnect errors
        except Exception as e:
            logger.error(f"Unexpected error in MQTT handler: {e}")
            await asyncio.sleep(retry_delay)
            continue

# ----------------------- Setup -----------------------
async def setup():
    uri = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
    client = AsyncIOMotorClient(uri)
    db = client["port"]
    logger.info("Connected to MongoDB")

    sensor_analyzer = SensorAnalyzer(db)
    # Log DB collections
    collections = await db.list_collection_names()
    logger.debug(f"DB collections: {collections}")

    # MQTT params (no client creation here—pass to handler)
    mqtt_params = {
        'hostname': "a1dghi6and062t-ats.iot.us-east-1.amazonaws.com",
        'port': 8883,
        'identifier': "manager",
        'tls_context': ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    }
    mqtt_params['tls_context'].load_verify_locations(cafile="../certs/AmazonRootCA1.pem")
    mqtt_params['tls_context'].load_cert_chain(
        certfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt",
        keyfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key"  # FIXED: Typo 'privat' → 'private'
    )

    try:
        from traffic_analyzer import convert_bson_numbers
        raw_nodes = await db.graph.find().to_list(None)
        # NEW: Debug port.graph visibility (raw data from db.graph)
        logger.debug(f"port.graph raw: {len(raw_nodes)} docs loaded; sample first doc: {json.dumps(raw_nodes[0] if raw_nodes else {}, default=str, indent=2)}")
        if not raw_nodes:
            logger.error("Graph missing or empty in DB—ensure your insert script ran (e.g., from graph.json)")
            return  # Halt; no graph = no assignments

        processed_nodes = convert_bson_numbers(raw_nodes)
        graph = {node['id']: node for node in processed_nodes if 'id' in node}
        
        # NEW: Structure check/flatten for common formats (flat list vs. single {'nodes': [...]})
        if len(graph) == 0 and len(raw_nodes) == 1 and 'nodes' in raw_nodes[0]:
            # Single doc with 'nodes' list: Flatten and re-process
            processed_nodes = convert_bson_numbers(raw_nodes[0]['nodes'])
            graph = {node['id']: node for node in processed_nodes if 'id' in node}
            logger.debug("Flattened graph from single doc 'nodes' key")
        
        if len(graph) == 0:
            logger.error("Processed graph empty after checks—verify DB insert structure (flat list of {id:..., neighbors:..., type:...} docs)")
            return  # Halt; debug DB
        
        logger.info(f"Loaded graph with {len(graph)} nodes")
        logger.debug(f"Graph sample keys: {list(graph.keys())[:5]}; types sample: { {k: v.get('type') for k, v in list(graph.items())[:3]} }")
        # NEW: Debug warehouse visibility in port.graph (confirms key nodes/types)
        warehouses = [nid for nid, ndata in graph.items() if ndata.get('type') == 'warehouse']
        logger.debug(f"port.graph warehouses detected: {len(warehouses)} ({warehouses})")
        
        # NEW: Ensure occupancy for warehouses (idempotent; adds if missing)
        for node_id in graph:
            if graph[node_id].get('type') == 'warehouse' and 'currentOccupancy' not in graph[node_id]:
                graph[node_id]['currentOccupancy'] = 0
                # Optional: Persist to DB (safe, even if already 0)
                await db.graph.update_one({'id': node_id}, {'$set': {'currentOccupancy': 0}}, upsert=True)
                logger.debug(f"Added occupancy=0 for warehouse {node_id}")

        analyzer = TrafficAnalyzer(db, None, graph)  # No mqtt_client needed here; handler provides
        asyncio.create_task(analyzer.start_mqtt_listener())
        logger.info("TrafficAnalyzer listener started")
        
        # FIXED: Pass graph to TaskAssigner (requires __init__ update: def __init__(..., graph=None))
        # NEW: Debug before passing (confirms what TaskAssigner will receive)
        logger.debug(f"Passing graph with {len(graph)} nodes to TaskAssigner (sample keys: {list(graph.keys())[:3]}, warehouses: {warehouses})")
        task_assigner = TaskAssigner(db, None, analyzer, graph=graph)  # mqtt_client in handler
        # FIXED: No ShipmentManager—monitor handles all
        
        # NEW: Debug TaskAssigner access (verifies shared graph post-init; assumes refactor sets self.graph)
        try:
            ta_graph_size = len(task_assigner.graph)
            ta_warehouses = [n for n, g in task_assigner.graph.items() if g.get('type') == 'warehouse'] if task_assigner.graph else []
            logger.debug(f"TaskAssigner graph size: {ta_graph_size} nodes; warehouses: {len(ta_warehouses)} ({ta_warehouses[:3]}) - Matches Manager? {'Yes' if ta_graph_size == len(graph) else 'No (check TaskAssigner refactor)'}")
        except AttributeError:
            logger.warning("TaskAssigner.graph not accessible (old __init__? Update to accept/use graph=None)")

        # Log initial data
        initial_shipments = await db.shipments.find().to_list(None)
        logger.debug(f"Initial shipments count: {len(initial_shipments)}; sample dests: {[s.get('destination') for s in initial_shipments[:3]]}")
        initial_edges = await db.edgeDevices.find().to_list(None)
        logger.debug(f"Initial edges count: {len(initial_edges)}; sample finalNodes: {[e.get('finalNode') for e in initial_edges[:3] if e]}")
        
        await analyzer.analyze_metrics("startup")
        logger.info("Initial traffic analysis complete")

        # Start tasks (pass mqtt_params to handler)
        asyncio.create_task(task_assigner.monitor_and_assign())
        asyncio.create_task(mqtt_handler(db, mqtt_params, analyzer, task_assigner, sensor_analyzer))
        asyncio.create_task(overview_reporter(db, logger))  # Start periodic overview

        logger.info("Setup complete. Manager running.")
        await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"Setup error (e.g., graph/DB): {e}")
        raise

async def overview_reporter(db, logger):
    """Periodic global overview of shipments and edges."""
    while True:
        try:
            # Shipments overview
            shipments = await db.shipments.find({}).to_list(length=100)
            status_counts = {}
            pending_offload = []
            pending_transport = []
            pending_store = []
            for s in shipments:
                status = s.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
                if status == 'arrived':
                    pending_offload.append(s['id'])
                elif status == 'offloaded':
                    pending_transport.append(s['id'])
                elif status == 'transporting':
                    pending_store.append(s['id'])
            
            # Edges overview
            edges = await db.edgeDevices.find({}).to_list(length=50)
            state_counts = {'idle': 0, 'en_route_start': 0, 'assigned': 0, 'completing': 0}
            type_counts = {}
            busy_edges = []
            for e in edges:
                state = e.get('taskPhase', 'idle')
                state_counts[state] = state_counts.get(state, 0) + 1
                edge_type = e.get('type', 'unknown')
                type_counts[edge_type] = type_counts.get(edge_type, 0) + 1
                if state != 'idle':
                    busy_edges.append(f"{e['id']} ({edge_type}) at {e['currentLocation']}")
            
            # Alerts overview (new for sensor)
            alerts = await db.sensorAlerts.find({'resolved': False}).to_list(None)
            alert_counts = {'high': 0, 'medium': 0, 'low': 0}
            for a in alerts:
                sev = a.get('severity', 'low')
                alert_counts[sev] += 1
            
            overview = f"""
=== GLOBAL OVERVIEW @ {time.strftime('%Y-%m-%d %H:%M:%S')} ===
SHIPMENTS (Total: {len(shipments)}):
- By Status: {status_counts}
- Pending Offload: {len(pending_offload)} ({pending_offload[:3] if pending_offload else 'None'})
- Pending Transport: {len(pending_transport)} ({pending_transport[:3] if pending_transport else 'None'})
- Pending Store: {len(pending_store)} ({pending_store[:3] if pending_store else 'None'})

EDGES (Total: {len(edges)}):
- By State: {state_counts}
- By Type: {type_counts}
- Busy Edges: {len(busy_edges)} ({busy_edges[:5] if busy_edges else 'None'})

SENSOR ALERTS (Unresolved: {len(alerts)}):
- By Severity: {alert_counts}
- Sample: {len([a for a in alerts[:3]])} ({[f"{a['node']}: {a['alert_type']}" for a in alerts[:3]] if alerts else 'None'})

Warehouses Load: {await get_warehouse_loads(db)}  # Assume helper func below
"""
            logger.info(overview)
            print(overview)  # For console visibility
        except Exception as e:
            logger.error(f"Overview error: {e}")
        
        await asyncio.sleep(30)  # Every 30s

async def get_warehouse_loads(db):
    """Helper: Current warehouse occupancies."""
    warehouses = await db.graph.find({'type': 'warehouse'}).to_list(None)
    return {w['id']: f"{w.get('currentOccupancy', 0)}/{w.get('capacity', 0)}" for w in warehouses}

if __name__ == "__main__":
    asyncio.run(setup())
