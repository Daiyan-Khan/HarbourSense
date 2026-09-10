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
from port_state import build_port_state_snapshot
from edge_view import load_merged_edge, load_merged_edges
from task_assigner import TaskAssigner, max_status, normalize_shipment_current_node
from traffic_analyzer import TrafficAnalyzer, SmartRoutePlanner, validate_path_adjacency
from route_helpers import edge_is_in_transit, queue_or_publish_route
from edge_command_queue import enqueue_edge_work
import aiomqtt
from demo_runtime import (CURRENT, DemoMqttClient, DemoRunEnded, demo_enabled,
                          validate_demo_settings, run_worker, accept_demo_event, mark_demo_event)

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
async def handle_traffic_update(db, mqtt_client, edge_id, analyzer, task_assigner, payload=None):
    """Process simulator arrival events — metrics only; completion via edge/completion topic."""
    try:
        if payload and payload.get('status') == 'arrived':
            await analyzer.analyze_metrics(triggered_by=f"Arrival event from {edge_id}")
            logger.debug(
                "Traffic arrival edge=%s loc=%s remaining=%s rev=%s",
                edge_id,
                payload.get('currentLocation'),
                payload.get('remainingPath', []),
                payload.get('routeRevision'),
            )
    except Exception as e:
        logger.error(f"Error in handle_traffic_update for {edge_id}: {e}")


async def maybe_reroute_edge(db, mqtt_client, edge_id, analyzer):
    """Gated reroute: only at hop boundary; publishes to edge route command topic."""
    try:
        edge = await load_merged_edge(db, edge_id)
        if not edge or not edge.get('path'):
            return

        task_phase = edge.get('taskPhase', '')
        if task_phase in ('idle', 'completing'):
            return

        if edge_is_in_transit(edge):
            logger.debug(f"Skipping reroute for {edge_id}: in transit progress={edge.get('progressToNext')}")
            return

        dest = normalize_node(edge.get('finalNode'), 'B4')
        current = normalize_node(edge.get('currentLocation'), '')
        if current == dest and len(edge.get('path', [])) <= 1:
            return

        if task_phase not in ('en_route_start', 'assigned', 'relocating'):
            return

        path = edge.get('path', [])
        remaining_path = [node for node in path if node != current]

        if not needs_reroute(edge['currentLocation'], remaining_path, analyzer):
            return

        await analyzer.analyze_metrics(triggered_by=f"Reroute check for {edge_id}")
        start = edge['currentLocation']
        node_loads = analyzer.get_current_loads()
        route_congestion = analyzer.get_route_congestion()
        predicted_loads = analyzer.get_predicted_loads()

        task = edge.get('task') if isinstance(edge.get('task'), dict) else {}
        phase = task.get('phase')
        pickup = normalize_node(
            task.get('pickupNode') or task.get('requiredPlace') or task.get('startNode'),
            '',
        )
        current_norm = normalize_node(start, '')

        if phase in ('transport', 'delivery') and pickup and pickup != dest and not edge.get('pickupCompleted'):
            if current_norm != pickup:
                to_pickup = analyzer.planner.compute_path(
                    start, pickup, node_loads, route_congestion, predicted_loads=predicted_loads
                )
                to_pickup = validate_path_adjacency(to_pickup, analyzer.planner)
                to_dest = analyzer.planner.compute_path(
                    pickup, dest, node_loads, route_congestion, predicted_loads=predicted_loads
                )
                to_dest = validate_path_adjacency(to_dest, analyzer.planner)
                if not to_pickup or not to_dest:
                    logger.warning(f"Failed pickup-aware reroute for {edge_id}")
                    return
                new_path = list(to_pickup)
                if to_dest:
                    if new_path and new_path[-1] == to_dest[0]:
                        new_path.extend(to_dest[1:])
                    else:
                        new_path.extend(to_dest)
            else:
                new_path = analyzer.planner.compute_path(
                    pickup, dest, node_loads, route_congestion, predicted_loads=predicted_loads
                )
        else:
            new_path = analyzer.planner.compute_path(
                start, dest, node_loads, route_congestion, predicted_loads=predicted_loads
            )
        new_path = validate_path_adjacency(new_path, analyzer.planner)
        if not new_path:
            logger.warning(f"Failed to compute valid adjacent reroute for {edge_id}")
            return

        edge_speed = edge.get('speed', 10)
        eta = (len(new_path) - 1) / edge_speed if len(new_path) > 1 else 0
        await queue_or_publish_route(db, mqtt_client, edge_id, new_path, analyzer, eta=eta)
        logger.info(f"Reroute command for {edge_id}: {new_path} (ETA: {eta}s)")
    except Exception as e:
        logger.error(f"Error in maybe_reroute_edge for {edge_id}: {e}")

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

            current_status = current_shipment.get('status', 'arrived') if current_shipment else 'arrived'
            status = max_status(current_status, status)
            current_node = normalize_shipment_current_node(
                current_node,
                current_shipment,
                task_assigner.graph if task_assigner else None,
            )
            if current_node != current_node_raw:
                logger.warning(f"Normalized currentNode for {shipment_id}: {current_node_raw} -> {current_node}")
            if dest != dest_raw:
                logger.warning(f"Missing destination in payload for {shipment_id}; fixing to 'C5'")
                payload['destination'] = 'C5'


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
            if payload.get('scheduledNextAt'):
                update_data['scheduledNextAt'] = payload.get('scheduledNextAt')
            if payload.get('queuePosition') is not None:
                update_data['queuePosition'] = payload.get('queuePosition')
            if payload.get('scheduledNextAt'):
                try:
                    update_data['scheduledNextAt'] = datetime.fromisoformat(
                        str(payload['scheduledNextAt']).replace('Z', '+00:00')
                    )
                except ValueError:
                    update_data['scheduledNextAt'] = payload['scheduledNextAt']
            if payload.get('queuePosition') is not None:
                update_data['queuePosition'] = payload['queuePosition']
            logger.debug(f"Updating DB for {shipment_id} with: {json.dumps(update_data, default=str, indent=2)}")


            await db.shipments.update_one({'id': shipment_id}, {'$set': update_data}, upsert=True)
            logger.info(f"Synced shipment {shipment_id} update: status={status} at {current_node}, edges={len(update_data['assignedEdges'])}")


            if status in ['arrived', 'offloaded'] and task_assigner.graph:
                warehouse = dest if CURRENT.get() and task_assigner.graph.get(dest, {}).get('type') == 'warehouse' else await task_assigner._select_warehouse(current_node, shipment_id) or task_assigner._nearest_warehouse(current_node)
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
        device_doc = await load_merged_edge(db, assigned_device)
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
        reloc_path = analyzer.planner.compute_path(
            current_loc, start_node, node_loads, route_congestion
        )
        reloc_path = validate_path_adjacency(reloc_path, analyzer.planner)
        if not reloc_path or len(reloc_path) < 2:
            logger.warning(f"Invalid reloc path {current_loc}→{start_node}; skipping teleport")
            return

        async def _reloc():
            if task_assigner.mqtt_client:
                await queue_or_publish_route(
                    db,
                    task_assigner.mqtt_client,
                    assigned_device,
                    reloc_path,
                    analyzer,
                )
            return True

        await enqueue_edge_work(assigned_device, _reloc)
        logger.info(
            f"Relocation route queued for {assigned_device} ({phase}): "
            f"{reloc_path[:3]}... to {start_node}"
        )
        
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

    asset_doc = await load_merged_edge(db, asset_id)
    if asset_doc:
        location = asset_doc.get("currentLocation")
        if location not in NULL_NODE_SENTINELS:
            return resolve_maintenance_target_node(alert_payload, location)

    return resolve_maintenance_target_node(alert_payload)


async def maintenance_assignment_active(db, target_node):
    """Return True when a non-idle robot is already assigned to this maintenance node."""
    if target_node in NULL_NODE_SENTINELS:
        return False

    edges = await load_merged_edges(db)
    edges = [e for e in edges if e.get('taskPhase') != 'idle']
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


async def mark_maintenance_alert_assigned(db, asset_id, node=None):
    """Record that backend maintenance assignment was triggered for this asset."""
    if asset_id in NULL_NODE_SENTINELS:
        return

    await db.maintenanceAlerts.update_many(
        {"assetId": asset_id, "resolved": False},
        {"$set": {"assignmentTriggered": True, "assignedNode": node, "assignmentTriggeredAt": datetime.utcnow()}},
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
            await mark_maintenance_alert_assigned(db, asset_id, target_node)
            logger.info("Maintenance alert for %s already triggered assignment; skipping duplicate", asset_id)
            return

        if await maintenance_assignment_active(db, target_node):
            await mark_maintenance_alert_assigned(db, asset_id, target_node)
            logger.info(
                "Maintenance assignment already active for node %s (asset %s); skipping duplicate",
                target_node,
                asset_id,
            )
            return

        assigned = await task_assigner.assign_maintenance_task(target_node, db, mqtt_client)
        if assigned is False:
            return
        await mark_maintenance_alert_assigned(db, asset_id, target_node)
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
            if incoming_reading.get('type') == 'occupancy' and float(incoming_reading.get('reading', 0)) <= 70:
                await db.sensorAlerts.update_many(
                    {'id': incoming_reading.get('id'), 'alert_type': 'occupancy_high', 'resolved': False},
                    {'$set': {'resolved': True, 'resolvedAt': datetime.utcnow()}})
            logger.debug(f"Sensor Analyzer Outcome - No anomaly: {incoming_reading.get('id')} at {incoming_reading.get('node')} (reading: {incoming_reading.get('reading')} of type {incoming_reading.get('type')})")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in sensor data: {e}, raw: {payload_str}")
    except Exception as e:
        logger.error(f"Sensor handling error: {e}")

# ----------------------- MQTT Handler (Unified with Reconnection) -----------------------
async def mqtt_handler(db, mqtt_params, analyzer, task_assigner, sensor_analyzer):
    """Robust MQTT handler: Recreates client on reconnect for AWS IoT stability."""
    context = CURRENT.get()
    if context:
        mqtt_params = {**mqtt_params, "identifier": f"harboursense-demo-manager-{context.run_id}", "clean_session": False, "keepalive": 10}
    max_retries = 10
    retry_delay = 5  # Seconds

    while True:
        mqtt_client = None
        try:
            # Create client each iteration (context manager handles connect)
            async with aiomqtt.Client(**mqtt_params) as raw_client:
                demo_context = CURRENT.get()
                mqtt_client = DemoMqttClient(raw_client, demo_context) if demo_context else raw_client
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

                if demo_context:
                    await demo_context.heartbeat('ready', mqttConnected=True)
                retry_delay = 1
                logger.info("Starting MQTT message loop")
                async for message in mqtt_client.messages:
                    topic = str(message.topic)
                    try:
                        raw_payload = message.payload
                        demo_payload = json.loads(raw_payload) if demo_context else {}
                        if demo_context and not await accept_demo_event(demo_payload):
                            continue
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
                            await handle_traffic_update(db, mqtt_client, edge_id, analyzer, task_assigner, data)
                            if data.get('status') == 'arrived':
                                await maybe_reroute_edge(db, mqtt_client, edge_id, analyzer)
                        else:
                            try:
                                data = json.loads(raw_payload.decode('utf-8'))
                                edge_id = topic.split('/')[2] if '/' in topic else 'unknown'
                                logger.debug(f"Unhandled MQTT payload for {edge_id}: {json.dumps(data, indent=2)}")
                            except Exception as e:
                                logger.error(f"Error processing unhandled topic {topic}: {e}")
                        
                        if demo_context:
                            await mark_demo_event(demo_payload)
                    except DemoRunEnded:
                        raise
                    except Exception as e:  # Catch any per-message error
                        logger.error(f"Error processing message on {topic}: {e}")
                        continue  # Don't break loop on single message fail

        except DemoRunEnded:
            raise
        except aiomqtt.MqttError as e:
            if CURRENT.get():
                await CURRENT.get().heartbeat('starting', mqttConnected=False)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 5)
                continue
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
async def setup(db_override=None):
    try:
        mongo_settings = get_mongo_settings()
        client = create_mongo_client(mongo_settings)
    except MongoConfigError as exc:
        logger.error(f"Invalid MongoDB configuration for manager startup: {exc}")
        raise

    db = db_override if db_override is not None else client[mongo_settings.database_name]
    owned_tasks = []
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
        if CURRENT.get() is None:
            owned_tasks.append(asyncio.create_task(analyzer.start_mqtt_listener()))
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
        initial_edges = await load_merged_edges(db)
        logger.debug(f"Initial edges count: {len(initial_edges)}; sample finalNodes: {[e.get('finalNode') for e in initial_edges[:3] if e]}")
        
        if CURRENT.get() is None:
            await analyzer.analyze_metrics("startup")
        logger.info("Initial traffic analysis complete")

        # Start tasks (pass mqtt_params to handler)
        owned_tasks.append(asyncio.create_task(task_assigner.monitor_and_assign()))
        owned_tasks.append(asyncio.create_task(mqtt_handler(db, mqtt_params, analyzer, task_assigner, sensor_analyzer)))
        owned_tasks.append(asyncio.create_task(overview_reporter(db, logger)))  # Start periodic overview

        logger.info("Setup complete. Manager running.")
        await asyncio.Event().wait()
    except GraphSeedMissingError as e:
        logger.error(f"Manager startup blocked: {e}")
        raise
    except Exception as e:
        logger.error(f"Setup error (e.g., graph/DB): {e}")
        raise

    finally:
        for task in owned_tasks:
            task.cancel()
        await asyncio.gather(*owned_tasks, return_exceptions=True)

async def overview_reporter(db, logger):
    """Periodic global overview of shipments and edges."""
    while True:
        try:
            snapshot = await build_port_state_snapshot(db)
            pending = snapshot['pending_ids']
            overview = f"""
=== GLOBAL OVERVIEW @ {time.strftime('%Y-%m-%d %H:%M:%S')} ===
SHIPMENTS (Total: {snapshot['total_shipments']}, Active: {snapshot['active_shipments']}):
- By Status: {snapshot['status_counts']}
- Pending Arrived (offload): {snapshot['pending_counts']['pending_arrived']} ({pending['pending_arrived'][:3] or 'None'})
- Pending Offloaded (transport): {snapshot['pending_counts']['pending_offloaded']} ({pending['pending_offloaded'][:3] or 'None'})
- Pending Transported (store_move): {snapshot['pending_counts']['pending_transported']} ({pending['pending_transported'][:3] or 'None'})
- Pending Storing (store_load): {snapshot['pending_counts']['pending_storing']} ({pending['pending_storing'][:3] or 'None'})
- Pending Stored (delivery): {snapshot['pending_counts']['pending_stored']} ({pending['pending_stored'][:3] or 'None'})
- Queue Flags: {snapshot['queue_counts']}

EDGES:
- By State: {snapshot['edge_state_counts']}
- By Type: {snapshot['edge_type_counts']}
- Idle By Type: {snapshot['idle_by_type']}
- Busy Edges: {len(snapshot['busy_edges'])} ({snapshot['busy_edges'][:5] or 'None'})

SENSOR ALERTS (Unresolved: {snapshot['unresolved_alerts']}):
- By Severity: {snapshot['alert_counts']}

Warehouses Load: {await get_warehouse_loads(db)}
"""
            logger.info(overview)
        except Exception as e:
            logger.error(f"Overview error: {e}")
        
        await asyncio.sleep(30)  # Every 30s

async def get_warehouse_loads(db):
    """Helper: Current warehouse occupancies."""
    warehouses = await db.graph.find({'type': 'warehouse'}).to_list(None)
    return {w['id']: f"{w.get('currentOccupancy', 0)}/{w.get('capacity', 0)}" for w in warehouses}

async def main():
    if demo_enabled():
        settings = get_mongo_settings()
        validate_demo_settings(settings)
        client = create_mongo_client(settings)
        base = client[settings.database_name]
        try:
            await run_worker("manager", base, lambda context: setup(context.db))
        finally:
            client.close()
    else:
        await setup()

if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
