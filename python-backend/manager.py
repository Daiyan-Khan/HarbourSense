import sys
import asyncio
from datetime import datetime
import time  
from sensor_analyzer import SensorAnalyzer

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import logging
import json
from backend_config import MongoConfigError, create_mongo_client, get_mongo_settings
from mqtt_config import MqttConfigError, build_aiomqtt_params, get_mqtt_settings
from task_assigner import TaskAssigner
from traffic_analyzer import TrafficAnalyzer, SmartRoutePlanner
import aiomqtt

# ----------------------- Logging Setup -----------------------
logger = logging.getLogger("HarbourSenseManager")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

GRAPH_SEED_MISSING_MESSAGE = (
    "Graph missing or empty in MongoDB. Run the seed harness or graph insert script "
    "for the port.graph collection before starting HarbourSense."
)
NULL_NODE_SENTINELS = {None, "Null", "null", "None", "undefined", ""}


class GraphSeedMissingError(RuntimeError):
    """Raised when the backend manager cannot start because graph seed data is absent."""


def normalize_node(value, fallback):
    """Normalize legacy null-string node values from MQTT payloads."""
    return fallback if value in NULL_NODE_SENTINELS else value


def build_graph_from_raw_nodes(raw_nodes, convert_bson_numbers):
    """Convert seeded MongoDB graph documents to the manager graph map."""
    if not raw_nodes:
        raise GraphSeedMissingError(GRAPH_SEED_MISSING_MESSAGE)

    processed_nodes = convert_bson_numbers(raw_nodes)
    graph = {node['id']: node for node in processed_nodes if 'id' in node}

    # Support the known alternate seed shape: one document containing a nodes list.
    if len(graph) == 0 and len(raw_nodes) == 1 and 'nodes' in raw_nodes[0]:
        processed_nodes = convert_bson_numbers(raw_nodes[0]['nodes'])
        graph = {node['id']: node for node in processed_nodes if 'id' in node}
        logger.debug("Flattened graph from single doc 'nodes' key")

    if len(graph) == 0:
        raise GraphSeedMissingError(
            "Graph collection has documents but no usable node id fields. "
            "Verify the seed structure is a flat list of {id, neighbors, type} documents."
        )

    return graph

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
        dest = normalize_node(edge.get('finalNode'), 'B4')  # FIXED: Use finalNode (consistent)
        logger.debug(f"Edge {edge_id} path: {path} (type: {type(path)}), dest: {dest}")
        
        await analyzer.analyze_metrics(triggered_by=f"Reroute check for {edge_id}")
        remaining_path = path[1:] if len(path) > 1 else []
        
        if needs_reroute(edge['currentLocation'], remaining_path, analyzer):
            start = edge['currentLocation']
            node_loads = analyzer.get_current_loads()
            route_congestion = analyzer.get_route_congestion()
            predicted_loads = analyzer.get_predicted_loads()
            logger.debug(f"Computing new path for {edge_id}. Start: {start}, Dest: {dest}, node_loads sample: {dict(list(node_loads.items())[:3])}, congestion sample: {dict(list(route_congestion.items())[:3])}, predicted sample: {dict(list(predicted_loads.items())[:3])}")

            new_path = SmartRoutePlanner.compute_path(
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
            await handle_completion(db, task_assigner, edge_id, edge.get('task', {}), mqtt_client)
            logger.info(f"Detected arrival completion for {edge_id} at {dest}; handled via update")
    except Exception as e:
        logger.error(f"Error in handle_traffic_update for {edge_id}: {e}")

# ----------------------- Completion Handler (Unified for MQTT/Arrival) -----------------------
# NEW: Separate handler for completions (calls TaskAssigner's method)
# FIXED: Signature + mqtt_client param; fallback with delivery
async def handle_completion(db, task_assigner, device_id, task_payload, mqtt_client=None):
    logger.info(f"Handling completion for device {device_id}; task payload: {json.dumps(task_payload, default=str)}")
    try:
        if mqtt_client:
            task_assigner.mqtt_client = mqtt_client

        # TaskAssigner owns idempotent completion, edge reset, shipment status, and occupancy.
        await task_assigner.handle_completion(device_id, task_payload)
        logger.info(f"TaskAssigner processed completion for {device_id} ({task_payload.get('phase', 'unknown')})")
        
    except Exception as e:
        logger.error(f"Error in handle_completion for {device_id}: {e}")


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


            status = payload.get('status', 'arrived')
            current_node_raw = payload.get('currentNode', 'A1')
            dest_raw = payload.get('destination', 'C5')
            current_node = normalize_node(current_node_raw, 'A1')
            dest = normalize_node(dest_raw, 'C5')
            logger.debug(f"Payload extracted - status: {status}, current: {current_node}, dest: {dest}")
            if current_node != current_node_raw:
                logger.warning(f"Missing currentNode in payload for {shipment_id}; fixing to 'A1'")
                payload['currentNode'] = 'A1'
            if dest != dest_raw:
                logger.warning(f"Missing destination in payload for {shipment_id}; fixing to 'C5'")
                payload['destination'] = 'C5'


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


            if status in ['arrived', 'offloaded'] and task_assigner.graph:
                warehouse = await task_assigner._select_warehouse(current_node, shipment_id) or task_assigner._nearest_warehouse(current_node)
                await db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {'destination': warehouse, 'warehouseAssigned': warehouse, 'updatedAt': datetime.now()}},
                )
                logger.info(f"Selected warehouse {warehouse} for {shipment_id} at {current_node}")
                if task_assigner.mqtt_client:
                    await task_assigner.mqtt_client.publish(
                        f"harboursense/shipments/{shipment_id}/warehouse",
                        json.dumps({'warehouse': warehouse}),
                    )

            logger.debug(f"DB updated; task_assigner monitor owns assignment for status '{status}'")


    except Exception as e:
        logger.error(f"Error handling shipment MQTT (topic {message.topic if 'message' in locals() else 'unknown'}): {e}; raw payload={raw_payload[:200]}...")
async def _handle_relocation_if_needed(db, task_assigner, analyzer, assigned_device, shipment_id, phase, start_node, device_type):
    """Helper: Check if assigned device needs relocation to start_node; compute path, update DB, publish MQTT if yes."""
    try:
        if not assigned_device:
            return
        
        # Skip stationary (e.g., crane for offload)
        if device_type == 'crane':
            logger.debug(f"Skipping reloc for stationary {device_type} {assigned_device}")
            return
        
        # Fetch device
        device_doc = await db.edgeDevices.find_one({'id': assigned_device})
        if not device_doc:
            logger.warning(f"Device {assigned_device} not found; skip reloc")
            return
        
        current_loc = device_doc.get('currentLocation', start_node)
        if current_loc == start_node:
            logger.debug(f"Device {assigned_device} already at {start_node}; no reloc needed")
            return
        
        logger.info(f"Device {assigned_device} at {current_loc} != start {start_node}; relocating for {phase}")
        
        # Compute path (use analyzer or empty fallbacks)
        node_loads = analyzer.get_current_loads() if analyzer else {}
        route_congestion = analyzer.get_route_congestion() if analyzer else {}
        reloc_path = SmartRoutePlanner.compute_path(
            current_loc, start_node, node_loads, route_congestion
        )
        if not reloc_path or len(reloc_path) < 2:
            logger.warning(f"Invalid reloc path {current_loc}→{start_node}; proceed with teleport")
            # Optional: Still set currentLocation to start_node
            await db.edgeDevices.update_one({'id': assigned_device}, {'$set': {'currentLocation': start_node}})
            return
        
        # Update edge to relocating (temp field for sim)
        await db.edgeDevices.update_one(
            {'id': assigned_device},
            {'$set': {
                'taskPhase': 'relocating',
                'relocPath': reloc_path,
                'updatedAt': datetime.now()
            }}
        )
        
        # Publish for sim (port.js executes, then completes)
        reloc_payload = {
            'deviceId': assigned_device,
            'relocPath': reloc_path,
            'target': start_node,
            'shipmentId': shipment_id,
            'phase': phase
        }
        if task_assigner.mqtt_client:
            await task_assigner.mqtt_client.publish(
                f"harboursense/edge/relocate/{assigned_device}",
                json.dumps(reloc_payload)
            )
        logger.info(f"Relocation published for {assigned_device} ({phase}): {reloc_path[:3]}... to {start_node}")
        
    except Exception as e:
        logger.error(f"Reloc error for {assigned_device} ({phase}): {e}")

def resolve_maintenance_target_node(alert_payload, asset_location=None):
    """Resolve the graph node that needs maintenance from an edge-analyzer alert."""
    if asset_location not in NULL_NODE_SENTINELS:
        return normalize_node(asset_location, asset_location)

    telemetry = alert_payload.get("telemetry") or {}
    telemetry_node = telemetry.get("node")
    if telemetry_node not in NULL_NODE_SENTINELS:
        return normalize_node(telemetry_node, telemetry_node)

    asset_id = alert_payload.get("assetId")
    if asset_id not in NULL_NODE_SENTINELS:
        return normalize_node(asset_id, asset_id)

    return None


async def resolve_maintenance_node(db, alert_payload):
    """Look up the asset's current location, then fall back to payload hints."""
    asset_id = alert_payload.get("assetId")
    if asset_id in NULL_NODE_SENTINELS:
        return None

    asset_doc = await db.edgeDevices.find_one({"id": asset_id})
    if asset_doc:
        location = asset_doc.get("currentLocation")
        if location not in NULL_NODE_SENTINELS:
            return resolve_maintenance_target_node(alert_payload, location)

    return resolve_maintenance_target_node(alert_payload)


async def maintenance_assignment_active(db, target_node):
    """Return True when a non-idle robot is already assigned to this maintenance node."""
    if target_node in NULL_NODE_SENTINELS:
        return False

    edges = await db.edgeDevices.find({"taskPhase": {"$ne": "idle"}}).to_list(None)
    for edge in edges:
        task = edge.get("task")
        if not isinstance(task, dict) or task.get("phase") != "maintenance":
            continue
        if normalize_node(task.get("finalNode"), "") == target_node:
            return True
    return False


async def maintenance_alert_already_handled(db, asset_id):
    """Skip duplicate MQTT alerts when an unresolved alert already triggered assignment."""
    if asset_id in NULL_NODE_SENTINELS:
        return False

    existing = await db.maintenanceAlerts.find_one({
        "assetId": asset_id,
        "resolved": False,
        "assignmentTriggered": True,
    })
    return existing is not None


async def mark_maintenance_alert_assigned(db, asset_id):
    """Record that backend maintenance assignment was triggered for this asset."""
    if asset_id in NULL_NODE_SENTINELS:
        return

    await db.maintenanceAlerts.update_one(
        {"assetId": asset_id, "resolved": False},
        {"$set": {"assignmentTriggered": True, "assignmentTriggeredAt": datetime.now()}},
    )


async def handle_maintenance_alert(db, mqtt_client, task_assigner, raw_payload):
    """Handle predictive maintenance alerts from edge-analyzer MQTT."""
    try:
        payload_str = raw_payload.decode("utf-8", errors="ignore") if isinstance(raw_payload, bytes) else str(raw_payload)
        alert_payload = json.loads(payload_str)
        asset_id = alert_payload.get("assetId")
        logger.debug(
            "Maintenance alert received for asset %s: %s",
            asset_id,
            json.dumps(alert_payload, default=str),
        )

        if asset_id in NULL_NODE_SENTINELS:
            logger.warning("Maintenance alert missing assetId; skipping assignment")
            return

        target_node = await resolve_maintenance_node(db, alert_payload)
        if target_node in NULL_NODE_SENTINELS:
            logger.warning("Maintenance alert for %s has no resolvable target node; skipping", asset_id)
            return

        if await maintenance_alert_already_handled(db, asset_id):
            logger.info("Maintenance alert for %s already triggered assignment; skipping duplicate", asset_id)
            return

        if await maintenance_assignment_active(db, target_node):
            logger.info(
                "Maintenance assignment already active for node %s (asset %s); skipping duplicate",
                target_node,
                asset_id,
            )
            return

        await task_assigner.assign_maintenance_task(target_node, db, mqtt_client)
        await mark_maintenance_alert_assigned(db, asset_id)
        logger.info("Maintenance task assigned for asset %s at node %s", asset_id, target_node)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in maintenance alert: %s", e)
    except Exception as e:
        logger.error("Maintenance alert handling error: %s", e)


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
                task_assigner.mqtt_client = mqtt_client
                analyzer.mqtt_client = mqtt_client
                
                # Subscribe (re-subscribe on reconnect)
                await mqtt_client.subscribe("harboursense/shipments/+")  # Specific IDs
                logger.info("Subscribed to shipments/+ (individual IDs)")
                await mqtt_client.subscribe("harboursense/edge/+/completion")  # Canonical per-edge completions
                logger.info("Subscribed to edge/+/completion")
                await mqtt_client.subscribe("harboursense/traffic/update/+")  # Edge updates for reroute
                logger.info("Subscribed to edge updates")
                await mqtt_client.subscribe("harboursense/sensor/data")  # Sensor topic
                logger.info("Subscribed to sensor data")
                await mqtt_client.subscribe("harboursense/alerts/maintenance")  # Edge-analyzer maintenance alerts
                logger.info("Subscribed to maintenance alerts")

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
                        elif topic == "harboursense/alerts/maintenance":
                            await handle_maintenance_alert(db, mqtt_client, task_assigner, raw_payload)
                        elif topic.startswith('harboursense/edge/') and topic.endswith('/completion'):
                            device_id = topic.split('/')[2]  # e.g., harboursense/edge/crane_1/completion
                            try:
                                task_payload = json.loads(raw_payload.decode('utf-8'))
                                logger.debug(f"Parsed completion payload for {device_id}: {json.dumps(task_payload, indent=2)}")
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON error in completion {topic}: {e}")
                                continue
                            await handle_completion(db, task_assigner, device_id, task_payload, mqtt_client)
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
    try:
        mongo_settings = get_mongo_settings()
        client = create_mongo_client(mongo_settings)
    except MongoConfigError as exc:
        logger.error(f"Invalid MongoDB configuration for manager startup: {exc}")
        raise

    db = client[mongo_settings.database_name]
    logger.info(f"Configured MongoDB database '{mongo_settings.database_name}' from MONGO_URI")

    sensor_analyzer = SensorAnalyzer(db)
    # Log DB collections
    try:
        collections = await db.list_collection_names()
    except Exception as exc:
        logger.error(
            "MongoDB is configured but not reachable. Check MONGO_URI/MONGO_DB_NAME in "
            f"HarbourSense/.env and ensure the database is running. Original error type: {exc.__class__.__name__}."
        )
        raise RuntimeError("MongoDB is configured but not reachable; start MongoDB or update HarbourSense/.env.") from None
    logger.debug(f"DB collections: {collections}")

    try:
        mqtt_settings = get_mqtt_settings()
        mqtt_params = build_aiomqtt_params("manager")
    except MqttConfigError as exc:
        logger.error(f"Invalid MQTT configuration for manager startup: {exc}")
        raise
    logger.info(f"Configured MQTT mode '{mqtt_settings.mode}' using broker host '{mqtt_settings.host}'")

    try:
        from traffic_analyzer import convert_bson_numbers
        raw_nodes = await db.graph.find().to_list(None)
        # NEW: Debug port.graph visibility (raw data from db.graph)
        logger.debug(f"port.graph raw: {len(raw_nodes)} docs loaded; sample first doc: {json.dumps(raw_nodes[0] if raw_nodes else {}, default=str, indent=2)}")
        graph = build_graph_from_raw_nodes(raw_nodes, convert_bson_numbers)
        
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
    except GraphSeedMissingError as e:
        logger.error(f"Manager startup blocked: {e}")
        raise
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
        except Exception as e:
            logger.error(f"Overview error: {e}")
        
        await asyncio.sleep(30)  # Every 30s

async def get_warehouse_loads(db):
    """Helper: Current warehouse occupancies."""
    warehouses = await db.graph.find({'type': 'warehouse'}).to_list(None)
    return {w['id']: f"{w.get('currentOccupancy', 0)}/{w.get('capacity', 0)}" for w in warehouses}

if __name__ == "__main__":
    asyncio.run(setup())
