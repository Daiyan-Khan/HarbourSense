from demo_runtime import CURRENT, logical_ms, simulation_sleep, simulation_time, simulation_datetime, simulation_checkpoint
import asyncio
import logging
import json
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from math import inf  # For distance inf
import time

try:
    from pymongo import ReturnDocument
except ImportError:  # pragma: no cover - test stubs may omit pymongo
    class ReturnDocument:
        AFTER = 1
        BEFORE = 0

from edge_view import (
    ASSIGNMENT_COLLECTION,
    RUNTIME_COLLECTION,
    clear_assignment,
    load_merged_edge,
    load_merged_edges,
    merge_edge_snapshot,
)
from traffic_analyzer import normalize_node_id, validate_path_adjacency, trim_path_from_current
from route_helpers import next_route_revision, publish_route_command
from edge_command_queue import enqueue_edge_work

COMPLETING_POLL_STALE_SECONDS = 30
INFLIGHT_STALE_SECONDS = 45


logger = logging.getLogger("TaskAssigner")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)



# Fallback GRAPH_LIST (minimal; DB populated, so rarely used)
GRAPH_LIST = [
    {"id": "A1", "neighbors": {"E":"A2","S":"B1"}, "type":"dock", "capacity":8, "currentOccupancy":0},
    {"id": "B4", "neighbors": {"N":"A4","S":"C4","E":"B5","W":"B3"}, "type":"warehouse", "capacity":35, "currentOccupancy":0},
    {"id": "D2", "neighbors": {"N":"C2","S":"E2","E":"D3","W":"D1"}, "type":"warehouse", "capacity":25, "currentOccupancy":0},
    {"id": "E5", "neighbors": {"N":"D5","W":"E4"}, "type":"warehouse", "capacity":35, "currentOccupancy":0},
    # Expand with full 25 from graph.json if needed; but DB handles
]



def parse_graph(graph_data):
    """Parse graph from Manager/DB/fallback (list, dict, or single doc). Handles flat 25-doc DB."""
    graph = {}
    if isinstance(graph_data, list):
        # Flat list (DB to_list() or GRAPH_LIST)
        for node in graph_data:
            node_id = node.get('id')
            if node_id:
                graph[node_id] = {
                    'neighbors': node.get('neighbors', {}),
                    'type': node.get('type', 'route_point'),
                    'capacity': node.get('capacity', 5),
                    'currentOccupancy': node.get('currentOccupancy', 0)  # Default 0 for warehouses
                }
    elif isinstance(graph_data, dict):
        # Single doc: {'nodes': [...]} or flat {id: {...}}
        nodes_list = graph_data.get('nodes', list(graph_data.values()))  # Flatten 'nodes' or values
        if isinstance(nodes_list, list):
            return parse_graph(nodes_list)  # Recurse for list
        else:
            # Dict {id: node}
            graph = graph_data
            # Ensure occupancy for warehouses
            for node_id, node_data in graph.items():
                if node_data.get('type') == 'warehouse' and 'currentOccupancy' not in node_data:
                    graph[node_id]['currentOccupancy'] = 0
    return graph


PHASE_STATUS_MAP = {
    'offload': 'offloaded',
    'transport': 'transported',
    'store_move': 'storing',
    'store_load': 'stored',
    'delivery': 'delivered',
}

PHASE_QUEUE_FLAG = {
    'offload': 'offloadQueued',
    'transport': 'transportQueued',
    'store_move': 'storeQueued',
    'store_load': 'storeQueued',
}

# Queue flags cleared when the corresponding phase completes (store_move shares storeQueued).
PHASE_QUEUE_CLEAR_ON_COMPLETE = {
    'offload': 'offloadQueued',
    'transport': 'transportQueued',
    'store_load': 'storeQueued',
}

SHIPMENT_STATUS_RANK = {
    'arrived': 0,
    'offloaded': 1,
    'transported': 2,
    'transporting': 2,
    'storing': 3,
    'stored': 4,
    'delivered': 5,
}

INVALID_GRAPH_LOCATION_STRINGS = frozenset({'processing'})
NULL_NODE_SENTINELS = frozenset({None, 'Null', 'null', 'None', 'undefined', ''})


def shipment_status_rank(status):
    if not status:
        return 0
    return SHIPMENT_STATUS_RANK.get(status, 0)


def max_status(current, incoming):
    if shipment_status_rank(incoming) >= shipment_status_rank(current):
        return incoming or current
    return current or incoming


def normalize_shipment_current_node(current_node, shipment_doc=None, graph=None):
    """Reject pseudo-nodes like 'processing'; fall back to warehouse/destination."""
    invalid = (
        current_node in NULL_NODE_SENTINELS
        or current_node in INVALID_GRAPH_LOCATION_STRINGS
        or (graph and current_node not in graph)
    )
    if not invalid:
        return current_node
    if shipment_doc:
        for key in ('warehouseAssigned', 'destination', 'currentNode', 'arrivalNode'):
            candidate = shipment_doc.get(key)
            if (
                candidate
                and candidate not in NULL_NODE_SENTINELS
                and candidate not in INVALID_GRAPH_LOCATION_STRINGS
                and (not graph or candidate in graph)
            ):
                return candidate
    return 'A1'


def make_assigned_edge(edge_id, phase, assigned_at=None, completed_at=None):
    """Return the canonical assignedEdges entry used by Phase 2."""
    return {
        'edgeId': edge_id,
        'phase': phase,
        'assignedAt': assigned_at or simulation_datetime(),
        'completedAt': completed_at,
    }


def assigned_edge_matches(entry, phase=None, edge_id=None):
    """Accept both canonical objects and legacy strings while data migrates."""
    if isinstance(entry, dict):
        phase_ok = phase is None or entry.get('phase') == phase
        edge_ok = edge_id is None or entry.get('edgeId') == edge_id
        return phase_ok and edge_ok
    if isinstance(entry, str):
        edge_part, _, phase_part = entry.partition(':')
        phase_ok = phase is None or phase == phase_part or phase in entry
        edge_ok = edge_id is None or edge_id == edge_part
        return phase_ok and edge_ok
    return False


def has_assigned_phase(assigned_edges, phase):
    return any(assigned_edge_matches(entry, phase=phase) for entry in assigned_edges or [])


def has_inflight_phase(assigned_edges, phase):
    """True when a phase has an incomplete assignedEdges entry (device may still be working)."""
    return any(
        isinstance(entry, dict)
        and assigned_edge_matches(entry, phase=phase)
        and not entry.get('completedAt')
        for entry in assigned_edges or []
    )


def phase_complete(assigned_edges, phase):
    """True when a workflow phase has a completed assignedEdges entry."""
    return any(is_completed_assignment(entry, phase=phase) for entry in assigned_edges or [])


def is_completed_assignment(entry, phase=None, edge_id=None):
    return (
        isinstance(entry, dict)
        and assigned_edge_matches(entry, phase=phase, edge_id=edge_id)
        and entry.get('completedAt') is not None
    )


def status_after_phase(phase):
    return PHASE_STATUS_MAP.get(phase, 'completed')


def shipment_fifo_key(shipment):
    """Sort key for FIFO assignment: oldest createdAt first, then id."""
    for key in ('createdAt', 'arrivalTime', 'updatedAt'):
        raw = (shipment or {}).get(key)
        if not raw:
            continue
        if isinstance(raw, datetime):
            ts = raw.replace(tzinfo=None) if raw.tzinfo else raw
            return (ts, shipment.get('id', ''))
        if isinstance(raw, str):
            try:
                ts = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                return (ts.replace(tzinfo=None), shipment.get('id', ''))
            except ValueError:
                continue
    return (datetime.min, (shipment or {}).get('id', ''))


def sort_shipments_fifo(shipments):
    """Return shipments ordered oldest-first for monitor assignment."""
    return sorted(shipments or [], key=shipment_fifo_key)


class TaskAssigner:
    def __init__(self, db, mqtt_client, analyzer, graph=None):  # FIXED: Accepts graph=None
        self.db = db
        self._stage_locks = {}
        self.mqtt_client = mqtt_client
        self.analyzer = analyzer
        self.graph = parse_graph(graph) if graph is not None else {}  # Use provided (Manager's full 25)
        if self.graph:
            logger.info(f"Using provided graph with {len(self.graph)} nodes")
            # Verify key nodes/warehouses (B4/D2/E5 from DB)
            warehouses = [n for n, g in self.graph.items() if g.get('type') == 'warehouse']
            logger.debug(f"Available warehouses: {warehouses}")
            # Idempotent occupancy ensure (Manager adds; safe here too)
            for node_id in self.graph:
                if self.graph[node_id].get('type') == 'warehouse' and 'currentOccupancy' not in self.graph[node_id]:
                    self.graph[node_id]['currentOccupancy'] = 0
                    logger.debug(f"Ensured occupancy=0 for warehouse {node_id}")
        else:
            # Fallback async load (e.g., if direct init without Manager)
            asyncio.create_task(self._load_graph())



    async def _load_graph(self):
        """Fallback: Load full graph from DB (enhanced for 25 docs). Skips if already loaded."""
        if self.graph:
            logger.debug("Graph already loaded; skipping fallback")
            return
        try:
            raw_nodes = await self.db.graph.find().to_list(None)  # FIXED: to_list for full 25 (not find_one)
            logger.debug(f"Fallback raw graph docs count: {len(raw_nodes)}; sample: {json.dumps(raw_nodes[:1], default=str) if raw_nodes else 'Empty'}")
            if raw_nodes:
                # Handle flat list (your DB) or single {'nodes': [...]}
                if len(raw_nodes) == 1 and 'nodes' in raw_nodes[0]:
                    self.graph = parse_graph(raw_nodes[0]['nodes'])
                else:
                    self.graph = parse_graph(raw_nodes)  # Flat 25 docs
                logger.info(f"Fallback loaded graph with {len(self.graph)} nodes from DB")
                node_types = {n: g['type'] for n, g in self.graph.items() if 'type' in g}
                logger.debug(f"Node types (fallback): {dict(list(node_types.items())[:5])}")
                warehouses = [n for n, g in self.graph.items() if g.get('type') == 'warehouse']
                logger.debug(f"Fallback warehouses: {warehouses}")
            else:
                self.graph = parse_graph(GRAPH_LIST)
                logger.warning(f"DB graph empty; using GRAPH_LIST with {len(self.graph)} nodes")
        except Exception as e:
            logger.error(f"Fallback graph load failed: {e}; using GRAPH_LIST")
            self.graph = parse_graph(GRAPH_LIST)



    async def _load_edges_merged(self):
        return await load_merged_edges(self.db)

    async def _load_edge_merged(self, edge_id):
        return await load_merged_edge(self.db, edge_id)

    async def _load_runtime(self, edge_id):
        return await self.db[RUNTIME_COLLECTION].find_one({'id': edge_id})

    async def _load_idle_edges_merged(self, device_type=None):
        query = {'taskPhase': 'idle'}
        if device_type:
            query['type'] = device_type
        runtimes = await self.db[RUNTIME_COLLECTION].find(query).to_list(None)
        merged = []
        for runtime in runtimes:
            assignment = await self.db[ASSIGNMENT_COLLECTION].find_one({'id': runtime['id']}) or {}
            if assignment.get('shipmentId') or assignment.get('assignedShipment'):
                continue
            merged.append(merge_edge_snapshot(assignment, runtime))
        return merged

    def _compute_validated_path(self, start, end, node_loads, route_congestion, predicted_loads=None):
        """Compute a graph-adjacent path or return None."""
        start = normalize_node_id(start)
        end = normalize_node_id(end)
        if not start or not end:
            return None
        if start == end:
            return [start]
        if not self.analyzer or not getattr(self.analyzer, "planner", None):
            return None
        raw_path = self.analyzer.planner.compute_path(
            start, end, node_loads, route_congestion, predicted_loads=predicted_loads
        )
        return validate_path_adjacency(raw_path, self.analyzer.planner)

    def _concat_paths(self, first, second):
        if not first:
            return list(second or [])
        if not second:
            return list(first)
        if first[-1] == second[0]:
            return first + second[1:]
        return first + second

    def _compose_pickup_delivery_path(
        self,
        current_loc,
        pickup_node,
        delivery_node,
        node_loads,
        route_congestion,
        predicted_loads=None,
    ):
        """Build current → pickup → delivery path for transport/delivery stages."""
        current = normalize_node_id(current_loc)
        pickup = normalize_node_id(pickup_node)
        delivery = normalize_node_id(delivery_node)
        if not pickup or not delivery:
            return None
        path = []
        if current != pickup:
            leg = self._compute_validated_path(
                current, pickup, node_loads, route_congestion, predicted_loads=predicted_loads
            )
            if not leg:
                return None
            path = list(leg)
        if pickup != delivery:
            leg = self._compute_validated_path(
                pickup, delivery, node_loads, route_congestion, predicted_loads=predicted_loads
            )
            if not leg:
                return None
            path = self._concat_paths(path, leg)
        # Same-node pickup/delivery (e.g. warehouse E5 → exit E5): empty path for sim assigned→completing.
        if pickup == delivery:
            return []
        return path if path else [pickup]

    def _compute_stage_device_path(
        self,
        phase,
        device_type,
        current_loc,
        start_node,
        required_place,
        final_node,
        node_loads,
        route_congestion,
        predicted_loads=None,
    ):
        """Return validated hop path for a workflow stage candidate, or None to skip."""
        current_loc = normalize_node_id(current_loc) or normalize_node_id(start_node)
        required_place = normalize_node_id(required_place) or current_loc
        final_node = normalize_node_id(final_node) or required_place
        start_node = normalize_node_id(start_node) or required_place

        if phase == 'offload' and device_type == 'crane':
            if current_loc == required_place:
                return []
            return self._compute_validated_path(
                current_loc,
                required_place,
                node_loads,
                route_congestion,
                predicted_loads=predicted_loads,
            )

        if phase in ('transport', 'delivery') and start_node and final_node:
            return self._compose_pickup_delivery_path(
                current_loc,
                start_node,
                final_node,
                node_loads,
                route_congestion,
                predicted_loads=predicted_loads,
            )

        if final_node:
            path_start = (
                current_loc
                if phase in ('store_move', 'store_load', 'transport', 'delivery')
                else start_node
            )
            if path_start == final_node:
                # Same-node stationary work: empty path so sim goes assigned→completing.
                if phase in ('store_move', 'store_load', 'offload', 'delivery'):
                    return []
                return [final_node]
            path = self._compute_validated_path(
                path_start,
                final_node,
                node_loads,
                route_congestion,
                predicted_loads=predicted_loads,
            )
            if not path:
                return None
            if len(path) < 2 and path_start != final_node:
                return None
            return path

        return [required_place]

    def _is_dock_or_berth(self, node):
        """Check if node is dock/berth for offload."""
        if node in self.graph:
            node_type = self.graph[node].get('type', '')
            return node_type in ['dock', 'berth']
        return node.startswith('A')  # Fallback: A-row docks

    def _manhattan_distance(self, node1, node2):
        """Compute Manhattan distance between two nodes using their x/y coords from self.graph. Returns float (0 if invalid/missing)."""
        if not node1 or not node2:
            logger.warning(f"Invalid nodes for distance: {node1} to {node2}; return 0")
            return 0.0
        
        n1_data = self.graph.get(node1, {})
        n2_data = self.graph.get(node2, {})
        x1 = n1_data.get('x', 0)
        y1 = n1_data.get('y', 0)
        x2 = n2_data.get('x', 0)
        y2 = n2_data.get('y', 0)
        
        # Safe float (handles int/str/None from DB/graph.json)
        try:
            x1 = float(x1) if x1 is not None else 0.0
            y1 = float(y1) if y1 is not None else 0.0
            x2 = float(x2) if x2 is not None else 0.0
            y2 = float(y2) if y2 is not None else 0.0
        except (ValueError, TypeError) as e:
            logger.warning(f"Coord parse error for {node1}-{node2}: {e}; default to 0")
            return 0.0
        
        dist = abs(x1 - x2) + abs(y1 - y2)
        logger.debug(f"Manhattan dist {node1} ({x1},{y1}) to {node2} ({x2},{y2}) = {dist}")
        return dist

    def _nearest_warehouse(self, from_node):
        """Find nearest warehouse to from_node (dynamic for multi-warehouses)."""
        warehouses = [n for n, g in self.graph.items() if g.get('type') == 'warehouse']  # B4, D2, E5
        if not warehouses:
            return 'B4'  # Default
        min_dist = inf
        nearest = 'B4'
        for wh in warehouses:
            dist = self._distance(from_node, wh)
            if dist < min_dist:
                min_dist = dist
                nearest = wh
        return nearest



    def _distance(self, loc1, loc2):
        """Manhattan distance (grid-aware)."""
        if len(loc1) < 2 or len(loc2) < 2:
            return inf
        y1, x1 = ord(loc1[0].upper()) - ord('A'), int(loc1[1:])
        y2, x2 = ord(loc2[0].upper()) - ord('A'), int(loc2[1:])
        return abs(y1 - y2) + abs(x1 - x2)



    async def assign_task(self, device_type, task_details, edge_id=None):
        """Assign specific idle edge to task; dynamic from DB, closest to start."""
        shipment_id = task_details.get('shipmentId')
        shipment = await self.db.shipments.find_one({'id': shipment_id}) if shipment_id else None
        current_node = shipment.get('currentNode', task_details.get('startNode', 'A1')) if shipment else task_details.get('startNode', 'A1')



        if not edge_id:
            # Dynamic: Find closest idle of type to current_node/start
            edges = await self._load_idle_edges_merged(device_type)
            if not edges:
                logger.warning(f"No available {device_type} for task at {current_node}")
                return None
            # Sort by distance to start_node
            start_node = task_details.get('startNode', current_node)
            edges.sort(key=lambda e: self._distance(e.get('currentLocation', 'A1'), start_node))
            edge = edges[0]
            edge_id = edge['id']
            logger.debug(f"Selected closest {device_type} {edge_id} (dist: {self._distance(edge.get('currentLocation', 'A1'), start_node)})")
        else:
            # Verify idle + type
            edge = await self._load_edge_merged(edge_id)
            if not edge or edge['taskPhase'] != 'idle' or edge['type'] != device_type:
                logger.warning(f"Edge {edge_id} not idle or wrong type ({device_type}); skipping")
                return None



        # Dynamic start/final based on phase/node (fallback; overridden by required_place in path)
        phase = task_details.get('phase', 'unknown')
        start_node = task_details.get('startNode', current_node)
        if phase == 'offload':
            final_node = start_node  # Crane stays at dock/berth
        elif phase == 'transport':
            final_node = shipment.get('destination', self._nearest_warehouse(start_node)) if shipment else self._nearest_warehouse(start_node)  # To assigned warehouse
        elif phase == 'store':
            final_node = shipment.get('destination', 'C5') if shipment else 'C5'  # To storage
            start_node = task_details.get('pickupNode', final_node)  # From warehouse
        elif phase == 'delivery':
            # NEW: For delivery, start from warehouse, final='E5' (exit)
            final_node = 'E5'  # Exit gate
            start_node = shipment.get('destination', self._nearest_warehouse(current_node)) if shipment else self._nearest_warehouse(current_node)  # Pickup from warehouse
        else:
            final_node = task_details.get('finalNode', 'B4')



        # FIXED: Extended path for "go to place" (logical start from device_loc; en_route_start if needed)
        if 'path' not in task_details or not task_details['path']:
            node_loads = self.analyzer.get_current_loads() if self.analyzer else {}
            route_congestion = self.analyzer.get_route_congestion() if self.analyzer else {}
            predicted_loads = self.analyzer.get_predicted_loads() if self.analyzer else {}
            # Safe dicts
            for d in [node_loads, route_congestion, predicted_loads]:
                if not isinstance(d, dict):
                    logger.warning(f"{d.__class__.__name__} not dict; defaulting")
                    d = {}
            
            # Logical start: From device's actual location
            device_loc = normalize_node_id(edge.get('currentLocation', start_node)) or normalize_node_id(start_node)
            required_place = normalize_node_id(task_details.get('requiredPlace', start_node)) or normalize_node_id(start_node)
            final_node = normalize_node_id(task_details.get('finalNode', final_node)) or normalize_node_id(final_node)
            
            # Path: device_loc → required_place (go-to first) → final_node (if different)
            if device_loc == required_place and required_place == final_node:
                # Already at place + stationary task
                path = [device_loc]
                logger.debug(f"{edge_id} already at {required_place} (stationary {phase}); path=[{device_loc}]")
            elif device_loc == required_place:
                path = self._compute_validated_path(
                    required_place, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                )
                logger.debug(f"{edge_id} at {required_place}; direct to {final_node}: {path}")
            else:
                path_to_place = self._compute_validated_path(
                    device_loc, required_place, node_loads, route_congestion, predicted_loads=predicted_loads
                )
                if required_place == final_node:
                    path = path_to_place
                    logger.debug(f"{edge_id} go to {required_place} only (stationary {phase}): {path}")
                elif path_to_place:
                    path_from_place = self._compute_validated_path(
                        required_place, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                    )
                    path = path_to_place[:-1] + path_from_place if path_from_place else None
                    logger.debug(f"{edge_id} extended: {device_loc} → {required_place} → {final_node}: {path}")
                else:
                    path = self._compute_validated_path(
                        device_loc, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                    )
            
            # Validate: ensure path starts at device_loc and is graph-adjacent
            try:
                if path:
                    trimmed = trim_path_from_current(path, device_loc)
                    if trimmed:
                        path = trimmed
                    validated = validate_path_adjacency(path, self.analyzer.planner if self.analyzer else None)
                    if validated and validated[0] == device_loc:
                        path = validated
                    else:
                        fallback = self._compute_validated_path(
                            device_loc, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                        )
                        if fallback:
                            path = fallback
                            logger.warning(f"Path validation failed for {edge_id}; recomputed: {path}")
                        else:
                            logger.warning(f"Path validation failed for {edge_id}; no valid path for {device_loc}->{final_node}")
                            path = [device_loc] if device_loc == final_node else None
                elif device_loc == final_node:
                    path = [device_loc]
            except Exception as e:
                logger.error(f"Path error for {edge_id} {device_loc}->{final_node}: {e}")
                path = self._compute_validated_path(
                    device_loc, final_node, node_loads, route_congestion, predicted_loads=predicted_loads
                ) or ([device_loc] if device_loc == final_node else None)
        else:
            path = task_details['path']



        device_loc = normalize_node_id(edge.get('currentLocation', start_node)) or normalize_node_id(start_node)
        required_place = normalize_node_id(task_details.get('requiredPlace', start_node)) or normalize_node_id(start_node)

        if phase in ('transport', 'delivery') and device_loc != required_place:
            if not path:
                logger.warning(
                    f"{phase} skipped for {edge_id}: no path from {device_loc} to pickup {required_place}"
                )
                return None
            validated_transport = validate_path_adjacency(
                path, self.analyzer.planner if self.analyzer else None
            )
            if not validated_transport:
                logger.warning(
                    f"{phase} skipped for {edge_id}: invalid path from {device_loc} "
                    f"to pickup {required_place}: {path}"
                )
                return None
            path = validated_transport

        async def claim_and_publish():
            fresh_runtime = await self._load_runtime(edge_id)
            if not fresh_runtime or fresh_runtime.get('taskPhase') != 'idle' or fresh_runtime.get('type') != device_type:
                return None
            fresh_assignment = await self.db[ASSIGNMENT_COLLECTION].find_one({'id': edge_id}) or {}
            if fresh_assignment.get('shipmentId') or fresh_assignment.get('assignedShipment'):
                return None
            fresh_edge = merge_edge_snapshot(fresh_assignment, fresh_runtime)
            from route_helpers import edge_is_in_transit
            if edge_is_in_transit(fresh_edge):
                return None

            route_revision = next_route_revision(fresh_edge)
            assignment_epoch = self._next_assignment_epoch(fresh_edge)
            hop_path = trim_path_from_current(path or [], device_loc) if path else []
            full_details = {
                **task_details,
                'shipmentId': shipment_id,
                'task': phase,
                'phase': phase,
                'startNode': start_node,
                'finalNode': final_node,
                'path': hop_path,
                'routeRevision': route_revision,
                'assignmentEpoch': assignment_epoch,
            }
            updates = {
                '$set': {
                    'finalNode': final_node,
                    'pendingPath': path or [],
                    'routeRevision': route_revision,
                    'assignmentEpoch': assignment_epoch,
                    'task': full_details,
                    'shipmentId': shipment_id,
                    'assignedShipment': shipment_id,
                    'startNode': start_node,
                    'claimedAt': simulation_datetime(),
                    'updatedAt': simulation_datetime(),
                }
            }
            claimed = await self.db[ASSIGNMENT_COLLECTION].find_one_and_update(
                {'id': edge_id, 'shipmentId': None, 'assignedShipment': None},
                updates,
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            if not claimed:
                logger.warning(f"Lost race claiming {edge_id} for assign_task")
                return None

            assigned_entry = make_assigned_edge(edge_id, phase)
            await self.db.shipments.update_one(
                {
                    'id': shipment_id,
                    'assignedEdges': {'$not': {'$elemMatch': {'edgeId': edge_id, 'phase': phase}}},
                },
                {'$push': {'assignedEdges': assigned_entry}, '$set': {'updatedAt': simulation_datetime()}},
            )

            if self.mqtt_client:
                await self.mqtt_client.publish(
                    f"harboursense/edge/{edge_id}/task",
                    json.dumps(full_details, default=str),
                )
            logger.info(
                f"Dynamic assign {device_type} {edge_id} ({phase}) for {shipment_id}: "
                f"{device_loc}->{final_node} (via {required_place}), path={path}"
            )
            return edge_id

        return await enqueue_edge_work(edge_id, claim_and_publish)

    async def _persist_workflow_diagnostic(self, shipment_id, phase=None, assigned_edge_id=None, blocker_code=None, blocker_message=None):
        """Persist last-known workflow blocker for dashboard and structured logs."""
        import port_state as ps

        shipment = await self.db.shipments.find_one({'id': shipment_id})
        if not shipment:
            return

        edges = await self._load_edges_merged()
        graph_meta = {'nodes': self.graph}

        if assigned_edge_id:
            diagnostic = {
                'nextPhase': phase,
                'blockerCode': 'ASSIGNED',
                'blockerMessage': f'Assigned {assigned_edge_id} for {phase}',
                'checkedAt': simulation_datetime(),
            }
            full_diag = None
        elif blocker_code:
            diagnostic = {
                'nextPhase': phase,
                'blockerCode': blocker_code,
                'blockerMessage': blocker_message or blocker_code,
                'checkedAt': simulation_datetime(),
            }
            full_diag = None
        else:
            full_diag = ps.diagnose_shipment_blocker(shipment, edges, graph_meta)
            diagnostic = {
                'nextPhase': full_diag.get('nextPhase') or phase,
                'blockerCode': full_diag.get('blockerCode'),
                'blockerMessage': full_diag.get('blockerMessage'),
                'checkedAt': simulation_datetime(),
            }

        await self.db.shipments.update_one(
            {'id': shipment_id},
            {'$set': {'workflowDiagnostic': diagnostic, 'updatedAt': simulation_datetime()}},
        )

        blocker = diagnostic.get('blockerCode')
        if blocker and blocker != 'NONE':
            logger.info(
                "[WF] shipment=%s next=%s blocker=%s idle_%s=%s msg=%s",
                shipment_id,
                diagnostic.get('nextPhase'),
                blocker,
                (full_diag or {}).get('requiredDeviceType'),
                (full_diag or {}).get('idleOfType'),
                diagnostic.get('blockerMessage'),
            )

    def _phase_in_flight(self, shipment, phase, edges_by_id, shipments_by_id=None):
        """True when a phase has an incomplete assignment on a still-active edge."""
        shipment_id = (shipment or {}).get('id')
        for entry in shipment.get('assignedEdges', []) or []:
            if not isinstance(entry, dict) or entry.get('completedAt'):
                continue
            if entry.get('phase') != phase:
                continue
            edge = edges_by_id.get(entry.get('edgeId'))
            if not edge:
                continue
            open_on_edge = self._open_assignments_for_edge(entry.get('edgeId'), shipments_by_id)
            if self._is_stale_assignment(edge, entry, shipment_id, open_on_edge):
                continue
            if self._is_orphan_inflight(edge, entry):
                continue
            if self._is_lost_assignment(edge, entry, shipment_id, open_on_edge):
                continue
            return True
        return False

    @staticmethod
    def _assignment_sort_key(entry):
        assigned_at = (entry or {}).get('assignedAt')
        if isinstance(assigned_at, datetime):
            return assigned_at.timestamp()
        if isinstance(assigned_at, str):
            try:
                return datetime.fromisoformat(assigned_at.replace('Z', '+00:00')).timestamp()
            except ValueError:
                return 0.0
        return 0.0

    def _open_assignments_for_edge(self, edge_id, shipments_by_id=None):
        """Return (shipment_id, entry) tuples for open assignments bound to edge_id."""
        if not edge_id or not shipments_by_id:
            return []
        open_entries = []
        for shipment in shipments_by_id.values():
            sid = shipment.get('id')
            if not sid:
                continue
            for entry in shipment.get('assignedEdges', []) or []:
                if (
                    isinstance(entry, dict)
                    and entry.get('edgeId') == edge_id
                    and not entry.get('completedAt')
                ):
                    open_entries.append((sid, entry))
        open_entries.sort(key=lambda item: self._assignment_sort_key(item[1]))
        return open_entries

    @staticmethod
    def _edge_has_pending_task(edge, shipment_id=None):
        """True when Mongo still holds a task payload not yet applied by the simulator."""
        if not edge:
            return False
        task = edge.get('task')
        if task in (None, 'idle', ''):
            return False
        if not isinstance(task, dict):
            return True
        if not task.get('phase') and not task.get('shipmentId'):
            return False
        if shipment_id and task.get('shipmentId') not in (None, shipment_id):
            return False
        return True

    def _is_lost_assignment(self, edge, entry, shipment_id, open_on_edge=None):
        """Open assignment whose edge reset idle without finishing — monitor should retry, not wait."""
        if not edge or not isinstance(entry, dict) or entry.get('completedAt'):
            return False
        if edge.get('id') != entry.get('edgeId'):
            return False
        if edge.get('taskPhase') != 'idle':
            return False
        if edge.get('shipmentId') or edge.get('assignedShipment'):
            return False
        if self._edge_has_pending_task(edge, shipment_id):
            return False
        open_on_edge = open_on_edge or []
        if len(open_on_edge) > 1:
            newest_sid, _ = open_on_edge[-1]
            return shipment_id == newest_sid
        return True

    def _is_stale_assignment(self, edge, entry, shipment_id=None, open_on_edge=None):
        """Edge finished and reset idle but shipment assignedEdges lacks completedAt."""
        if not edge or not isinstance(entry, dict) or entry.get('completedAt'):
            return False
        if edge.get('id') != entry.get('edgeId'):
            return False
        if edge.get('taskPhase') != 'idle':
            return False
        if edge.get('shipmentId') or edge.get('assignedShipment'):
            return False
        if self._edge_has_pending_task(edge):
            return False
        open_on_edge = open_on_edge or []
        if len(open_on_edge) > 1:
            newest_sid, _ = open_on_edge[-1]
            if shipment_id == newest_sid:
                return False
        return True

    async def _assign_stage_device(self, shipment_id, phase, details):
        # MQTT completion and the monitor can both request the next stage. Keep
        # its duplicate check and claim in one process-local critical section.
        lock = self._stage_locks.setdefault((shipment_id, phase), asyncio.Lock())
        async with lock:
            return await self._assign_stage_device_inner(shipment_id, phase, details)

    async def _assign_stage_device_inner(self, shipment_id, phase, details):
        """Assign nearest idle device for phase; compute path with loads/congestion if mobile. FIXED: Pass node_loads/route_congestion to compute_path; offload=crane (no path); try/except per candidate."""
        logger.info(f"Assigning {phase} for {shipment_id}: {details}")
        shipment = await self.db.shipments.find_one({'id': shipment_id})
        if shipment and phase_complete(shipment.get('assignedEdges', []), phase):
            logger.info(f"{shipment_id} already completed {phase}; skipping duplicate assignment")
            return None
        if shipment and has_inflight_phase(shipment.get('assignedEdges', []), phase):
            logger.info(f"{shipment_id} already has in-flight {phase}; skipping duplicate assignment")
            return None
        
        # Device type mapping (distinctions: tempo vs delivery trucks; robot/forklift stages)
        device_type_map = {
            'offload': 'crane',      # Crane travels to dock/berth when not already on site
            'transport': 'truck_tempo',  # Dock → wh
            'delivery': 'truck_delivery',  # Wh → E5 exit
            'store_move': 'robot',   # Wh internal move
            'store_load': 'forklift' # Wh place + occ inc
        }
        device_type = device_type_map.get(phase, 'agv')  # Fallback AGV for unknown
        
        required_place = (
            details.get('requiredPlace')
            or details.get('destNode')
            or details.get('pickupNode')
            or details.get('startNode')
            or 'A1'
        )
        start_node = details.get('startNode') or details.get('pickupNode') or required_place
        final_node = details.get('finalNode')
        if not final_node and phase in ('offload', 'store_load'):
            final_node = required_place
        if phase == 'transport' and not details.get('requiredPlace'):
            required_place = final_node or required_place
        if phase == 'delivery' and not details.get('requiredPlace'):
            required_place = final_node or 'E5'
        
        dist_anchor = details.get('pickupNode') or start_node if phase in ('transport', 'delivery') else required_place
        
        # Get current/traffic data (safe dicts; empty if analyzer None)
        node_loads = self.analyzer.get_current_loads() if self.analyzer else {}
        route_congestion = self.analyzer.get_route_congestion() if self.analyzer else {}
        predicted_loads = self.analyzer.get_predicted_loads() if self.analyzer else {}
        planner = getattr(self.analyzer, 'planner', None) if self.analyzer else None
        
        candidates = []
        try:
            # Scan idle devices of type (add location filter if needed, e.g., near wh for store)
            query = {'taskPhase': 'idle', 'type': device_type}
            
            idle_devices = await self._load_idle_edges_merged(device_type)
            logger.debug(f"Found {len(idle_devices)} idle {device_type} for {phase}; selecting nearest to {required_place}")
            
            for device in idle_devices:
                try:
                    device_id = device['id']
                    fresh = await self._load_edge_merged(device_id)
                    if not fresh or fresh.get('taskPhase') != 'idle':
                        continue
                    if fresh.get('shipmentId') or fresh.get('assignedShipment'):
                        continue
                    from route_helpers import edge_is_in_transit
                    if edge_is_in_transit(fresh):
                        continue
                    device = fresh
                    current_loc = device.get('currentLocation', start_node)
                    dist = self._manhattan_distance(current_loc, dist_anchor)
                    
                    path = self._compute_stage_device_path(
                        phase,
                        device_type,
                        current_loc,
                        start_node,
                        required_place,
                        final_node,
                        node_loads,
                        route_congestion,
                        predicted_loads=predicted_loads,
                    )
                    if path is None:
                        logger.warning(
                            f"Invalid path for {device_id} {current_loc}→{required_place or final_node}; skip candidate"
                        )
                        continue
                    if len(path) > 1:
                        dist += sum(self._edge_weight(p) for p in path[1:])
                    
                    candidates.append((device_id, dist, path, device))
                    logger.debug(f"Candidate {device_id} at {current_loc}: dist={dist}, path={path[:3]}...")
                    
                except Exception as e:
                    logger.warning(f"Path/assign fail for candidate {device.get('id', 'unknown')}: {e}; next")
                    continue
            
            if candidates:
                candidates.sort(key=lambda item: item[1])
                for nearest_id, nearest_dist, best_path, nearest_device in candidates:

                    async def try_claim_edge(
                        edge_id=nearest_id,
                        edge_dist=nearest_dist,
                        edge_path=best_path,
                        edge_doc=nearest_device,
                    ):
                        fresh_runtime = await self._load_runtime(edge_id)
                        if not fresh_runtime or fresh_runtime.get('taskPhase') != 'idle':
                            return None
                        fresh_assignment = await self.db[ASSIGNMENT_COLLECTION].find_one({'id': edge_id}) or {}
                        if fresh_assignment.get('shipmentId') or fresh_assignment.get('assignedShipment'):
                            return None
                        fresh_edge = merge_edge_snapshot(fresh_assignment, fresh_runtime)
                        from route_helpers import edge_is_in_transit
                        if edge_is_in_transit(fresh_edge):
                            return None

                        device_loc = (
                            normalize_node_id(fresh_edge.get('currentLocation', start_node))
                            or normalize_node_id(start_node)
                        )
                        required_norm = normalize_node_id(required_place) or device_loc
                        at_work = device_loc == required_norm
                        pending_path = edge_path or []
                        final_norm = normalize_node_id(final_node)
                        start_norm = normalize_node_id(start_node)
                        same_node_at_dest = (
                            at_work
                            and device_loc == final_norm
                            and (
                                phase in ('store_move', 'store_load', 'offload')
                                or (phase == 'delivery' and start_norm == final_norm)
                            )
                        )
                        if same_node_at_dest:
                            pending_path = []
                            hop_path = []
                        else:
                            hop_path = trim_path_from_current(pending_path, device_loc) or pending_path
                        route_revision = next_route_revision(fresh_edge)
                        assignment_epoch = self._next_assignment_epoch(fresh_edge)
                        task = {
                            'shipmentId': shipment_id,
                            'phase': phase,
                            'path': hop_path,
                            'routeRevision': route_revision,
                            'assignmentEpoch': assignment_epoch,
                            'startNode': start_node,
                            'finalNode': final_node,
                            'subPhase': details.get('subPhase'),
                            'requiredPlace': required_place,
                            'destNode': details.get('destNode') or final_node or required_place,
                            'assignedAt': simulation_datetime(),
                        }
                        update_data = {
                            '$set': {
                                'shipmentId': shipment_id,
                                'assignedShipment': shipment_id,
                                'task': task,
                                'pendingPath': pending_path,
                                'routeRevision': route_revision,
                                'assignmentEpoch': assignment_epoch,
                                'startNode': start_node,
                                'finalNode': final_node,
                                'claimedAt': simulation_datetime(),
                                'updatedAt': simulation_datetime(),
                            }
                        }
                        claimed = await self.db[ASSIGNMENT_COLLECTION].find_one_and_update(
                            {
                                'id': edge_id,
                                'shipmentId': None,
                                'assignedShipment': None,
                            },
                            update_data,
                            upsert=True,
                            return_document=ReturnDocument.AFTER,
                        )
                        if not claimed:
                            logger.debug(
                                f"Lost race claiming idle {device_type} {edge_id} for {shipment_id}; next candidate"
                            )
                            return None

                        logger.info(
                            f"Assigned nearest {device_type} {edge_id} for {shipment_id} "
                            f"{phase} (dist={edge_dist:.1f})"
                        )

                        if self.mqtt_client:
                            await self.mqtt_client.publish(
                                f"harboursense/edge/{edge_id}/task",
                                json.dumps(task, default=str),
                            )
                            if pending_path:
                                await publish_route_command(
                                    self.mqtt_client,
                                    edge_id,
                                    hop_path,
                                    route_revision,
                                )
                            await self.mqtt_client.publish(
                                f"harboursense/edge/assign/{edge_id}",
                                json.dumps(
                                    {'device': edge_id, 'task': task, 'shipment': shipment_id},
                                    default=str,
                                ),
                            )

                        assigned_entry = make_assigned_edge(edge_id, phase)
                        await self.db.shipments.update_one(
                            {
                                'id': shipment_id,
                                'assignedEdges': {
                                    '$not': {'$elemMatch': {'edgeId': edge_id, 'phase': phase}},
                                },
                            },
                            {'$push': {'assignedEdges': assigned_entry}, '$set': {'updatedAt': simulation_datetime()}},
                        )
                        await self._persist_workflow_diagnostic(
                            shipment_id, phase, assigned_edge_id=edge_id
                        )
                        return edge_id

                    claimed_id = await enqueue_edge_work(nearest_id, try_claim_edge)
                    if claimed_id:
                        return claimed_id

                logger.warning(
                    f"All {len(candidates)} {device_type} candidates lost idle claim race for "
                    f"{shipment_id} {phase}; queue"
                )
                await self._persist_workflow_diagnostic(shipment_id, phase)
                queued_key = PHASE_QUEUE_FLAG.get(phase)
                if queued_key:
                    await self.db.shipments.update_one(
                        {'id': shipment_id},
                        {'$set': {queued_key: True, 'updatedAt': simulation_datetime()}},
                    )
                return None

            logger.warning(
                f"No valid {device_type} candidates for {shipment_id} {phase} "
                f"({len(idle_devices)} idle checked); queue"
            )
            if not idle_devices:
                blocker_code = 'NO_IDLE_DEVICE'
                blocker_message = (
                    f"No idle {device_type} available for {phase}; "
                    f"all units are busy on other shipments"
                )
            else:
                blocker_code = 'NO_VALID_PATH'
                blocker_message = (
                    f"No graph-adjacent path for {device_type} "
                    f"({len(idle_devices)} idle checked)"
                )
            await self._persist_workflow_diagnostic(
                shipment_id,
                phase,
                blocker_code=blocker_code,
                blocker_message=blocker_message,
            )
            queued_key = PHASE_QUEUE_FLAG.get(phase)
            if queued_key:
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {queued_key: True, 'updatedAt': simulation_datetime()}},
                )
            return None
                
        except Exception as e:
            logger.error(f"Unexpected error in _assign_stage_device for {shipment_id} {phase}: {e}")
            return None

    def _edge_weight(self, edge_node):
        """Optional: Weight for path cost (e.g., type-based slowdown); default 1.0."""
        node = self.graph.get(edge_node, {})
        return node.get('weight', 1.0) or 1.0

    def _next_phase(self, current_phase):
        """Helper: Chain phases (offload → transport, transport → store_move, etc.)."""
        phase_chain = {
            'offload': 'transport',
            'transport': 'store_move',
            'store_move': 'store_load',
            'store_load': 'delivery',
            'delivery': 'completed'
        }
        return phase_chain.get(current_phase, 'idle')

    def _next_assignment_epoch(self, edge):
        """Monotonic assignment id for MQTT task idempotency (sim ignores stale epochs)."""
        current = 0
        if edge:
            raw = edge.get('assignmentEpoch')
            if raw is not None:
                try:
                    current = int(raw)
                except (TypeError, ValueError):
                    current = 0
        ts_ms = int(simulation_time() * 1000)
        return max(current + 1, ts_ms)

    async def _try_assign_next(self, shipment_id, next_phase, shipment=None):
        """Immediately assign the next workflow phase after completion; monitor is fallback."""
        if not next_phase or next_phase in ('completed', 'idle'):
            return None

        shipment = shipment or await self.db.shipments.find_one({'id': shipment_id})
        if not shipment:
            logger.warning(f"_try_assign_next: shipment {shipment_id} not found")
            return None

        if next_phase == 'transport':
            return await self._try_assign_transport(shipment_id, shipment)

        if next_phase == 'store_move':
            if phase_complete(shipment.get('assignedEdges', []), 'store_move'):
                return None
            if has_inflight_phase(shipment.get('assignedEdges', []), 'store_move'):
                return None
            current_node = shipment.get('currentNode', 'B4') or 'B4'
            warehouse = shipment.get('destination') or self._nearest_warehouse(current_node)
            if not self._can_assign_store(shipment, warehouse):
                logger.debug(f"Store_move deferred for {shipment_id}: cannot assign at {warehouse}")
                return None
            details = {
                'shipmentId': shipment_id,
                'pickupNode': warehouse,
                'finalNode': warehouse,
                'subPhase': 'move',
            }
            assigned = await self._assign_stage_device(shipment_id, 'store_move', details)
            if assigned:
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {'status': 'storing', 'storeQueued': False, 'updatedAt': simulation_datetime()}},
                )
            return assigned

        if next_phase == 'store_load':
            if phase_complete(shipment.get('assignedEdges', []), 'store_load'):
                return None
            if has_inflight_phase(shipment.get('assignedEdges', []), 'store_load'):
                return None
            if not phase_complete(shipment.get('assignedEdges', []), 'store_move'):
                logger.debug(f"Store_load deferred for {shipment_id}: store_move not complete")
                return None
            warehouse = shipment.get('destination') or shipment.get('currentNode', 'B4')
            reserved = await self._count_warehouse_reservations(warehouse)
            _, wh_cap, _ = self._warehouse_load(warehouse)
            if reserved >= wh_cap:
                logger.warning(f"Full wh {warehouse}; defer store_load for {shipment_id}")
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {'storeQueued': True, 'updatedAt': simulation_datetime()}},
                )
                return None
            details = {
                'shipmentId': shipment_id,
                'pickupNode': warehouse,
                'finalNode': warehouse,
                'requiredPlace': warehouse,
                'subPhase': 'load',
            }
            assigned = await self._assign_stage_device(shipment_id, 'store_load', details)
            if assigned:
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {'storeQueued': False, 'updatedAt': simulation_datetime()}},
                )
            return assigned

        if next_phase == 'delivery':
            if phase_complete(shipment.get('assignedEdges', []), 'delivery'):
                return None
            if has_inflight_phase(shipment.get('assignedEdges', []), 'delivery'):
                return None
            if (
                shipment.get('status') != 'stored'
                and not phase_complete(shipment.get('assignedEdges', []), 'store_load')
            ):
                return None
            warehouse = shipment.get('destination') or 'B4'
            if not self._is_warehouse(warehouse):
                return None
            details = {
                'shipmentId': shipment_id,
                'startNode': warehouse,
                'finalNode': 'E5',
                'pickupNode': warehouse,
                'requiredPlace': 'E5',
                'destNode': 'E5',
            }
            assigned = await self._assign_stage_device(shipment_id, 'delivery', details)
            if assigned:
                await self.db.shipments.update_one(
                    {'id': shipment_id},
                    {'$set': {'deliveryStatus': 'assigned', 'updatedAt': simulation_datetime()}},
                )
            return assigned

        return None

    def _pickup_node_for_transport(self, shipment):
        """Resolve dock/berth pickup after offload; tolerate currentNode drift to warehouse."""
        if not shipment:
            return None
        for candidate in (
            shipment.get('currentNode'),
            shipment.get('arrivalNode'),
        ):
            if candidate and self._is_dock_or_berth(candidate):
                return candidate
        return None

    async def _count_warehouse_reservations(self, wh_id):
        """Shipments physically occupying warehouse slots (stored only).

        Shipments in ``storing`` have not completed store_load yet and do not
        consume capacity; counting them blocked store_load for the whole backlog.
        """
        stored = await self.db.shipments.count_documents({'status': 'stored', 'destination': wh_id})
        return int(stored or 0)

    async def _sync_warehouse_occupancy(self):
        """Refresh in-memory graph occupancy from Mongo and clamp runaway counters."""
        warehouses = await self.db.graph.find({'type': 'warehouse'}).to_list(None)
        for wh in warehouses:
            wh_id = wh.get('id')
            if not wh_id:
                continue
            cap = int(wh.get('capacity', 35) or 35)
            db_occ = max(0, int(wh.get('currentOccupancy', 0) or 0))
            expected = await self._count_warehouse_reservations(wh_id)
            reconciled = db_occ
            if db_occ > expected:
                reconciled = expected
                await self.db.graph.update_one(
                    {'id': wh_id},
                    {'$set': {'currentOccupancy': reconciled}},
                )
                logger.info(
                    f"Reconciled warehouse {wh_id} occupancy {db_occ} -> {reconciled} "
                    f"(stored={expected}, cap={cap})"
                )
            if wh_id in self.graph:
                self.graph[wh_id]['currentOccupancy'] = reconciled
                self.graph[wh_id]['capacity'] = cap
            else:
                self.graph[wh_id] = {
                    'type': 'warehouse',
                    'capacity': cap,
                    'currentOccupancy': reconciled,
                }

    async def _adjust_warehouse_occupancy(self, wh_id, delta):
        """Increment/decrement warehouse occupancy in DB and keep self.graph in sync."""
        if not wh_id or not self._is_warehouse(wh_id):
            return
        await self.db.graph.update_one({'id': wh_id}, {'$inc': {'currentOccupancy': delta}})
        doc = await self.db.graph.find_one({'id': wh_id}) or {}
        occ = max(0, int(doc.get('currentOccupancy', 0) or 0))
        if occ != doc.get('currentOccupancy'):
            await self.db.graph.update_one({'id': wh_id}, {'$set': {'currentOccupancy': occ}})
        if wh_id in self.graph:
            self.graph[wh_id]['currentOccupancy'] = occ

    def _warehouse_load(self, wh_id):
        """Return occupancy, capacity, and load ratio for a warehouse node."""
        wh_data = self.graph.get(wh_id, {})
        occ = max(0, int(wh_data.get('currentOccupancy', 0) or 0))
        cap = int(wh_data.get('capacity', 35) or 35)
        if cap > 0:
            occ = min(occ, cap)
        if self.analyzer:
            try:
                pred_loads = self.analyzer.get_predicted_loads()
                pred = int(pred_loads.get(wh_id, 0)) if isinstance(pred_loads, dict) else 0
            except (AttributeError, TypeError):
                pred = 0
        else:
            pred = 0
        total = occ + pred
        load_pct = total / cap if cap > 0 else 0.0
        logger.debug(f"Wh {wh_id} load: occ={occ}, pred={pred}, total={total}/{cap} (pct={load_pct:.2f})")
        return total, cap, load_pct

    async def _try_assign_transport(self, shipment_id, shipment=None):
        """Assign truck_tempo for an offloaded shipment waiting at a dock/berth."""
        shipment = shipment or await self.db.shipments.find_one({'id': shipment_id})
        if not shipment or shipment.get('status') != 'offloaded':
            return None
        if has_inflight_phase(shipment.get('assignedEdges', []), 'transport'):
            return None

        pickup_node = self._pickup_node_for_transport(shipment)
        if not pickup_node:
            logger.warning(
                f"Transport skipped for {shipment_id}: no dock pickup "
                f"(current={shipment.get('currentNode')}, arrival={shipment.get('arrivalNode')})"
            )
            return None

        warehouse = shipment.get('destination') or self._nearest_warehouse(pickup_node)
        _, _, wh_pct = self._warehouse_load(warehouse)
        if wh_pct >= 0.8:
            logger.debug(f"High wh load {wh_pct:.1%}; queue transport {shipment_id}")
            await self.db.shipments.update_one(
                {'id': shipment_id},
                {'$set': {'transportQueued': True, 'updatedAt': simulation_datetime()}},
            )
            return None

        logger.info(
            f"Transport (truck_tempo) for {shipment_id}: {pickup_node} → {warehouse} "
            f"(load {wh_pct:.1%})"
        )
        details = {
            'shipmentId': shipment_id,
            'startNode': pickup_node,
            'finalNode': warehouse,
            'pickupNode': pickup_node,
            'requiredPlace': pickup_node,
            'destNode': warehouse,
        }
        assigned = await self._assign_stage_device(shipment_id, 'transport', details)
        await self.db.shipments.update_one(
            {'id': shipment_id},
            {
                '$set': {
                    'transportQueued': not bool(assigned),
                    'updatedAt': simulation_datetime(),
                }
            },
        )
        return assigned

    # Ensure _a_star_fallback and _calculate_eta exist (from prior; add if missing)
    def _a_star_fallback(self, start, goal):
        """BFS fallback."""
        if start == goal:
            return [start]
        from collections import deque
        queue = deque([([start], start)])
        visited = set([start])
        while queue:
            path, current = queue.popleft()
            if current == goal:
                return path
            for neigh in self.graph.get(current, {}).get('neighbors', {}).values():
                if neigh not in visited:
                    visited.add(neigh)
                    queue.append((path + [neigh], neigh))
        return None

    def _calculate_eta(self, edge, path, phase):
        """ETA: dist * (1/speed) + phase_time."""
        if len(path) == 1:
            return 5 if phase in ['offload', 'store_load'] else 2  # Stationary action time
        dist = len(path) - 1
        speed = edge.get('speed', 10)
        return (dist / speed) * 60 + (5 if phase in ['offload', 'delivery'] else 0)  # s


    

    async def handle_completion(self, device_id, task_payload):
        """Apply one edge completion idempotently and advance the shipment state."""
        async def _run():
            return await self._handle_completion_inner(device_id, task_payload)

        return await enqueue_edge_work(device_id, _run)

    async def _handle_completion_inner(self, device_id, task_payload):
        """Inner completion handler (serialized per edge via enqueue_edge_work)."""
        logger.info(f"Handling completion for {device_id}; raw payload: {task_payload}")
        try:
            if isinstance(task_payload, (str, bytes)):
                if isinstance(task_payload, bytes):
                    task_payload = task_payload.decode('utf-8', errors='ignore')
                try:
                    task_payload = json.loads(task_payload)
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode fail for {device_id}: {e}; using DB task state")
                    task_payload = {}
            elif not isinstance(task_payload, dict):
                logger.error(f"Invalid payload type for {device_id}: {type(task_payload)}; skip")
                return

            edge = await self._load_edge_merged(device_id) or {}
            edge_task = edge.get('task') if isinstance(edge.get('task'), dict) else {}
            shipment_id = task_payload.get('shipmentId') or edge_task.get('shipmentId') or edge.get('shipmentId')
            phase = task_payload.get('phase') or edge_task.get('phase') or 'unknown'
            completed_at = task_payload.get('completedAt') or simulation_datetime()
            completion_location = task_payload.get('location') or task_payload.get('currentLocation') or edge.get('finalNode') or edge.get('currentLocation')

            if not shipment_id:
                logger.warning(f"Completion for {device_id} has no shipmentId; skip")
                return

            shipment = await self.db.shipments.find_one({'id': shipment_id}) or {}
            assigned_edges = shipment.get('assignedEdges', [])
            already_completed = any(
                is_completed_assignment(entry, phase=phase, edge_id=device_id)
                for entry in assigned_edges
            )
            if already_completed:
                logger.info(f"Duplicate completion ignored for {shipment_id} {phase} by {device_id}")
                return

            edge_shipment = edge.get('shipmentId') or edge.get('assignedShipment')
            completion_epoch = task_payload.get('assignmentEpoch') or edge_task.get('assignmentEpoch')
            edge_epoch = edge.get('assignmentEpoch')
            if (
                edge_shipment
                and edge_shipment != shipment_id
                and edge.get('taskPhase') not in ('idle', None)
            ):
                logger.info(
                    f"Completion ignored for {shipment_id} {phase} on {device_id}: "
                    f"edge active for {edge_shipment}"
                )
                return
            if completion_epoch is not None and edge_epoch is not None:
                try:
                    if int(edge_epoch) != int(completion_epoch):
                        logger.info(
                            f"Stale epoch completion ignored for {shipment_id} {phase} "
                            f"on {device_id} (edge={edge_epoch}, ack={completion_epoch})"
                        )
                        return
                except (TypeError, ValueError):
                    pass

            if phase == 'maintenance':
                maintenance = await self.db.maintenanceTasks.find_one({'id': shipment_id})
                if not maintenance or maintenance.get('status') == 'completed':
                    return
                await self.db.maintenanceTasks.update_one({'id': shipment_id}, {'$set': {
                    'status': 'completed', 'completedAt': completed_at, 'completedBy': device_id}})
                await self.db.maintenanceAlerts.update_many(
                    {'assignedNode': maintenance['node'], 'resolved': False},
                    {'$set': {'resolved': True, 'resolvedAt': datetime.utcnow(), 'repairTaskId': shipment_id}})
                await clear_assignment(self.db, device_id)
                if self.analyzer:
                    await self.analyzer.analyze_metrics(f'maintenance_complete_{device_id}')
                return

            status = max_status(shipment.get('status', 'arrived'), status_after_phase(phase))
            update_fields = {
                'status': status,
                'updatedAt': simulation_datetime(),
            }
            if completion_location:
                update_fields['currentNode'] = completion_location
            if phase == 'store_load':
                update_fields['storageCompleteAt'] = completed_at
                update_fields['deliveryStatus'] = 'pending'
            elif phase == 'delivery':
                update_fields['deliveryCompleteAt'] = completed_at
                update_fields['deliveryStatus'] = 'completed'

            queue_clear = PHASE_QUEUE_CLEAR_ON_COMPLETE.get(phase)
            if queue_clear:
                update_fields[queue_clear] = False

            await self._mark_assignment_complete(
                shipment_id, device_id, phase, completed_at, update_fields
            )

            await clear_assignment(self.db, device_id)

            if phase == 'store_load':
                warehouse = completion_location or shipment.get('destination')
                if warehouse:
                    await self._adjust_warehouse_occupancy(warehouse, 1)
            elif phase == 'delivery':
                warehouse = shipment.get('destination') or shipment.get('warehouseAssigned')
                if warehouse:
                    await self._adjust_warehouse_occupancy(warehouse, -1)

            if self.analyzer:
                await self.analyzer.analyze_metrics(f"completion_{device_id}")
            logger.info(f"Completed {phase} for {shipment_id} -> {status}; edge {device_id} idle")

            next_phase = self._next_phase(phase)
            if next_phase and next_phase not in ('completed', 'idle'):
                fresh_shipment = await self.db.shipments.find_one({'id': shipment_id})
                await self._try_assign_next(shipment_id, next_phase, fresh_shipment)

            await self._persist_workflow_diagnostic(shipment_id)

        except Exception as e:
            logger.error(f"Error in handle_completion for {device_id}: {e}")
            await clear_assignment(self.db, device_id)

    # NEW: Helper (add after handle_completion)
    async def _await_and_assign_forklift(self, shipment_id, warehouse):
        """Wait for robot store_move complete, then assign store_load (forklift)."""
        await simulation_sleep(15)  # Estimate robot time; adjustable
        shipment = await self.db.shipments.find_one({'id': shipment_id})
        if not shipment or shipment.get('nextPhase') != 'store_load':
            return  # Already progressed or canceled
        
        robot_complete = None
        for runtime in await self.db[RUNTIME_COLLECTION].find({
            'type': 'robot',
            'taskPhase': 'idle',
        }).to_list(None):
            assignment = await self.db[ASSIGNMENT_COLLECTION].find_one({'id': runtime['id']}) or {}
            if assignment.get('assignedShipment') == shipment_id or assignment.get('shipmentId') == shipment_id:
                robot_complete = merge_edge_snapshot(assignment, runtime)
                break
        if robot_complete:
            logger.info(f"Robot complete for {shipment_id}; assigning store_load (forklift)")
            load_details = {'shipmentId': shipment_id, 'pickupNode': warehouse, 'subPhase': 'load'}
            await self._assign_stage_device(shipment_id, 'store_load', load_details)
            # Forklift completion will trigger delivery in handle_completion above
        else:
            logger.debug(f"Robot still active for {shipment_id}; retry in 5s")
            await simulation_sleep(5)
            await self._await_and_assign_forklift(shipment_id, warehouse)  # Retry with backoff

    async def _delay_truck_return(self, truck_id, delay_secs=12):
        """Log truck return delay; simulator owns runtime location reset."""
        await simulation_sleep(delay_secs)
        logger.info(f"Truck {truck_id} return delay elapsed ({delay_secs}s); sim owns idle state")



    async def _mark_assignment_complete(self, shipment_id, device_id, phase, completed_at, update_fields):
        """Mark matching assignedEdges entries complete (canonical or legacy)."""
        shipment = await self.db.shipments.find_one({'id': shipment_id}) or {}
        assigned_edges = list(shipment.get('assignedEdges', []) or [])
        updated_edges = []
        matched = False
        for entry in assigned_edges:
            if assigned_edge_matches(entry, phase=phase, edge_id=device_id):
                matched = True
                if isinstance(entry, dict):
                    updated_edges.append({**entry, 'completedAt': completed_at})
                else:
                    updated_edges.append(
                        make_assigned_edge(device_id, phase, completed_at=completed_at)
                    )
            else:
                updated_edges.append(entry)
        if not matched:
            updated_edges.append(make_assigned_edge(device_id, phase, completed_at=completed_at))
        await self.db.shipments.update_one(
            {'id': shipment_id},
            {'$set': {**update_fields, 'assignedEdges': updated_edges, 'updatedAt': simulation_datetime()}},
        )

    def _edge_updated_age_seconds(self, edge):
        context = CURRENT.get()
        if context and (edge or {}).get('demoUpdatedSimMs') is not None:
            return (logical_ms(context.state) - edge['demoUpdatedSimMs']) / 1000
        updated = (edge or {}).get('updatedAt')
        if not updated:
            return None
        if isinstance(updated, str):
            try:
                updated = datetime.fromisoformat(updated.replace('Z', '+00:00'))
            except ValueError:
                return None
        if not isinstance(updated, datetime):
            return None
        return (simulation_datetime() - updated.replace(tzinfo=None)).total_seconds()

    def _is_orphan_inflight(self, edge, entry):
        """Idle edge still bound to an open assignment long enough to treat as abandoned."""
        if not edge or not isinstance(entry, dict) or entry.get('completedAt'):
            return False
        if edge.get('id') != entry.get('edgeId'):
            return False
        if edge.get('taskPhase') != 'idle':
            return False
        shipment_id = edge.get('shipmentId') or edge.get('assignedShipment')
        if not shipment_id:
            return False
        age = self._edge_updated_age_seconds(edge)
        if age is None or age < INFLIGHT_STALE_SECONDS:
            return False
        return True

    async def _reconcile_queue_flags(self, shipment):
        """Clear stale queue flags left set after a phase already completed."""
        if not shipment or not shipment.get('id'):
            return False
        assigned = shipment.get('assignedEdges', [])
        clears = {}
        if phase_complete(assigned, 'offload') and shipment.get('offloadQueued'):
            clears['offloadQueued'] = False
        if phase_complete(assigned, 'transport') and shipment.get('transportQueued'):
            clears['transportQueued'] = False
        if phase_complete(assigned, 'store_load') and shipment.get('storeQueued'):
            clears['storeQueued'] = False
        if not clears:
            return False
        clears['updatedAt'] = simulation_datetime()
        await self.db.shipments.update_one({'id': shipment['id']}, {'$set': clears})
        shipment.update(clears)
        return True

    async def _reconcile_stale_idle_edges(self, edges, shipments_by_id):
        """Clear shipment/task bindings on idle edges with no open assignment."""
        idle_reset = {
            'shipmentId': None,
            'assignedShipment': None,
            'task': 'idle',
            'path': [],
            'remainingPath': [],
            'nextNode': None,
            'startNode': None,
            'finalNode': None,
            'eta': None,
            'journeyTime': None,
            'updatedAt': simulation_datetime(),
        }
        cleared = 0
        for edge in edges:
            if edge.get('taskPhase') != 'idle':
                continue
            edge_id = edge.get('id')
            has_binding = (
                edge.get('shipmentId')
                or edge.get('assignedShipment')
                or (isinstance(edge.get('task'), dict) and edge.get('task'))
                or (isinstance(edge.get('task'), str) and edge.get('task') != 'idle')
            )
            if not has_binding:
                continue
            shipment_id = edge.get('shipmentId') or edge.get('assignedShipment')
            has_open = False
            if shipment_id and shipment_id in shipments_by_id:
                for entry in shipments_by_id[shipment_id].get('assignedEdges', []) or []:
                    if (
                        isinstance(entry, dict)
                        and entry.get('edgeId') == edge_id
                        and not entry.get('completedAt')
                    ):
                        has_open = True
                        break
            if has_open:
                continue
            from edge_view import clear_assignment

            await clear_assignment(self.db, edge_id)
            for key in ('shipmentId', 'assignedShipment', 'task', 'pendingPath', 'startNode', 'finalNode'):
                edge[key] = idle_reset.get(key)
            cleared += 1
            logger.info(f"Cleared stale idle bindings on {edge_id} (was shipment={shipment_id})")
        return cleared

    async def _reconcile_stuck_same_node_delivery(self, edges_by_id):
        """Normalize stuck delivery edges at E5 with empty routes so sim can self-heal."""
        changed = False
        for edge_id, edge in edges_by_id.items():
            if edge.get('type') != 'truck_delivery':
                continue
            if edge.get('taskPhase') != 'en_route_start':
                continue
            task = edge.get('task') if isinstance(edge.get('task'), dict) else {}
            if task.get('phase') != 'delivery':
                continue
            loc = normalize_node_id(edge.get('currentLocation'))
            final = normalize_node_id(
                edge.get('finalNode') or task.get('finalNode') or task.get('destNode')
            )
            start = normalize_node_id(
                edge.get('startNode') or task.get('startNode') or task.get('pickupNode')
            )
            if not loc or not final or loc != final:
                continue
            if start and start != final:
                continue
            if edge.get('path') or edge.get('pendingPath'):
                continue
            stale_task_path = task.get('path') or []
            needs_sync = (
                stale_task_path
                or edge.get('startNode') != start
                or edge.get('finalNode') != final
            )
            if not needs_sync:
                continue
            task_update = {**task, 'path': [], 'startNode': start or final, 'finalNode': final}
            await self.db[ASSIGNMENT_COLLECTION].update_one(
                {'id': edge_id},
                {
                    '$set': {
                        'startNode': start or final,
                        'finalNode': final,
                        'task': task_update,
                        'pendingPath': [],
                        'updatedAt': simulation_datetime(),
                    }
                },
                upsert=True,
            )
            edge.update({
                'startNode': start or final,
                'finalNode': final,
                'task': task_update,
                'pendingPath': [],
            })
            logger.info(
                f"Reconciled stuck same-node delivery on {edge_id} "
                f"at {loc} (cleared stale task.path, synced nodes)"
            )
            changed = True
        return changed

    async def _reconcile_superseded_assignments(self, shipment, edges_by_id):
        """Close duplicate in-flight entries for phases already completed elsewhere."""
        if not shipment or not shipment.get('id'):
            return False
        shipment_id = shipment['id']
        assigned = list(shipment.get('assignedEdges', []) or [])
        modified = False
        for phase in PHASE_STATUS_MAP:
            if not phase_complete(assigned, phase):
                continue
            for idx, entry in enumerate(assigned):
                if not isinstance(entry, dict):
                    continue
                if entry.get('phase') != phase or entry.get('completedAt'):
                    continue
                edge_id = entry.get('edgeId')
                assigned[idx] = {
                    **entry,
                    'completedAt': simulation_datetime(),
                    'superseded': True,
                }
                modified = True
                edge = edges_by_id.get(edge_id)
                if edge and (edge.get('shipmentId') == shipment_id or edge.get('assignedShipment') == shipment_id):
                    await clear_assignment(self.db, edge_id)
                    edge.update({
                        'shipmentId': None,
                        'assignedShipment': None,
                        'task': 'idle',
                    })
                    logger.info(
                        f"Released superseded {phase} on {edge_id} for {shipment_id} "
                        f"(phase already complete)"
                    )
        if modified:
            await self.db.shipments.update_one(
                {'id': shipment_id},
                {'$set': {'assignedEdges': assigned, 'updatedAt': simulation_datetime()}},
            )
            shipment['assignedEdges'] = assigned
        return modified

    async def _reconcile_orphan_assignments(self, shipment, edges_by_id):
        """Release stale idle edges stuck with shipment bindings so monitor can retry."""
        if not shipment or not shipment.get('id'):
            return False
        shipment_id = shipment['id']
        changed = False
        assigned = list(shipment.get('assignedEdges', []) or [])
        for entry in assigned:
            if not isinstance(entry, dict) or entry.get('completedAt'):
                continue
            edge = edges_by_id.get(entry.get('edgeId'))
            if not self._is_orphan_inflight(edge, entry):
                continue
            edge_id = entry.get('edgeId')
            task = edge.get('task')
            if (
                isinstance(task, dict)
                and self.mqtt_client
                and not entry.get('orphanRecoveryAttempted')
            ):
                await self.mqtt_client.publish(
                    f"harboursense/edge/{edge_id}/task",
                    json.dumps(task, default=str),
                )
                pending_path = edge.get('pendingPath') or task.get('path') or []
                if pending_path:
                    await publish_route_command(
                        self.mqtt_client,
                        edge_id,
                        pending_path,
                        edge.get('routeRevision') or task.get('routeRevision') or 1,
                    )
                for e in assigned:
                    if (
                        isinstance(e, dict)
                        and e.get('edgeId') == edge_id
                        and e.get('phase') == entry.get('phase')
                        and not e.get('completedAt')
                    ):
                        e['orphanRecoveryAttempted'] = True
                changed = True
                logger.info(
                    f"Republished orphan recovery task for {edge_id} "
                    f"{shipment_id} {entry.get('phase')}"
                )
                continue
            await clear_assignment(self.db, edge_id)
            assigned = [
                e for e in assigned
                if not (
                    isinstance(e, dict)
                    and e.get('edgeId') == edge_id
                    and e.get('phase') == entry.get('phase')
                    and not e.get('completedAt')
                )
            ]
            changed = True
            logger.info(
                f"Released orphan {entry.get('phase')} on {edge_id} for {shipment_id} "
                f"(idle binding stale >{INFLIGHT_STALE_SECONDS}s)"
            )
        if changed:
            await self.db.shipments.update_one(
                {'id': shipment_id},
                {'$set': {'assignedEdges': assigned, 'updatedAt': simulation_datetime()}},
            )
            shipment['assignedEdges'] = assigned
        return changed

    async def _reconcile_lost_assignments(self, shipment, edges_by_id, shipments_by_id):
        """Drop open assignedEdges entries whose edge reset idle so monitor can retry MQTT."""
        if not shipment or not shipment.get('id'):
            return False
        shipment_id = shipment['id']
        changed = False
        assigned = list(shipment.get('assignedEdges', []) or [])
        kept = []
        for entry in assigned:
            if not isinstance(entry, dict) or entry.get('completedAt'):
                kept.append(entry)
                continue
            edge = edges_by_id.get(entry.get('edgeId'))
            open_on_edge = self._open_assignments_for_edge(entry.get('edgeId'), shipments_by_id)
            if not self._is_lost_assignment(edge, entry, shipment_id, open_on_edge):
                kept.append(entry)
                continue
            changed = True
            logger.info(
                f"Cleared lost {entry.get('phase')} on {entry.get('edgeId')} "
                f"for {shipment_id} (edge idle; will retry assignment)"
            )
        if changed:
            await self.db.shipments.update_one(
                {'id': shipment_id},
                {'$set': {'assignedEdges': kept, 'updatedAt': simulation_datetime()}},
            )
            shipment['assignedEdges'] = kept
        return changed

    async def _reconcile_stale_assignments(self, shipment, edges_by_id, shipments_by_id=None):
        """Replay completion side effects when simulator reset edge before MQTT landed."""
        if CURRENT.get():
            return False  # Demo completion evidence must come from the real MQTT acknowledgement.
        if not shipment or not shipment.get('id'):
            return False
        shipment_id = shipment['id']
        changed = False
        for entry in list(shipment.get('assignedEdges', []) or []):
            if not isinstance(entry, dict) or entry.get('completedAt'):
                continue
            edge = edges_by_id.get(entry.get('edgeId'))
            open_on_edge = self._open_assignments_for_edge(entry.get('edgeId'), shipments_by_id)
            if not self._is_stale_assignment(edge, entry, shipment_id, open_on_edge):
                continue
            phase = entry.get('phase')
            edge_id = entry.get('edgeId')
            location = (
                (edge or {}).get('currentLocation')
                or (edge or {}).get('finalNode')
                or shipment.get('destination')
                or shipment.get('currentNode')
            )
            logger.info(
                f"Reconciled stale {phase} for {shipment_id} "
                f"on {edge_id} (edge idle, no completion ack)"
            )
            await self.handle_completion(
                edge_id,
                {
                    'shipmentId': shipment_id,
                    'phase': phase,
                    'location': location,
                    'completedAt': simulation_datetime(),
                },
            )
            fresh = await self.db.shipments.find_one({'id': shipment_id})
            if fresh:
                shipment['assignedEdges'] = fresh.get('assignedEdges', [])
                shipment['status'] = fresh.get('status')
            changed = True
        return changed

    async def monitor_and_assign(self):
        """Dynamic loop: Scan shipments by status/node, assign phases. CLEANED: TaskAssigner context (self.graph/self.analyzer); truck distinction + robot/forklift pairing; no dups with handler."""
        while True:
            try:
                await simulation_checkpoint()
                logger.info("Monitor: Scanning shipments...")
                await self._sync_warehouse_occupancy()
                shipments = await self.db.shipments.find({'status': {'$ne': 'delivered'}}).to_list(None)
                edges = await self._load_edges_merged()
                edges_by_id = {edge['id']: edge for edge in edges if edge.get('id')}
                shipments_by_id = {s['id']: s for s in shipments if s.get('id')}

                for shipment in shipments:
                    await self._reconcile_orphan_assignments(shipment, edges_by_id)
                    await self._reconcile_lost_assignments(shipment, edges_by_id, shipments_by_id)
                    await self._reconcile_stale_assignments(shipment, edges_by_id, shipments_by_id)
                    await self._reconcile_queue_flags(shipment)
                    reconciled = await self._reconcile_shipment_status(shipment)
                    if reconciled != shipment.get('status'):
                        shipment['status'] = reconciled

                pending_offloaded = sort_shipments_fifo([
                    s for s in shipments
                    if s.get('status') == 'offloaded'
                    and not phase_complete(s.get('assignedEdges', []), 'transport')
                ])
                pending_arrived = sort_shipments_fifo([
                    s for s in shipments
                    if s.get('status') in ('arrived', 'waiting')
                    and not phase_complete(s.get('assignedEdges', []), 'offload')
                ])
                pending_store_phases = sort_shipments_fifo([
                    s for s in shipments
                    if s.get('status') in ('transported', 'transporting', 'storing')
                    and not phase_complete(s.get('assignedEdges', []), 'store_load')
                ])
                pending_stored = sort_shipments_fifo([
                    s for s in shipments
                    if s.get('status') == 'stored'
                    and not phase_complete(s.get('assignedEdges', []), 'delivery')
                ])
                
                logger.debug(
                    f"Monitor counts: offloaded={len(pending_offloaded)}, arrived={len(pending_arrived)}, "
                    f"store_phases={len(pending_store_phases)}, stored={len(pending_stored)}"
                )

                # Transports (truck_tempo for offloaded) – queue high wh
                for shipment in pending_offloaded:
                    try:
                        if self._phase_in_flight(shipment, 'transport', edges_by_id, shipments_by_id):
                            continue
                        await self._try_assign_transport(shipment['id'], shipment)
                    except Exception as e:
                        logger.warning(f"Transport fail {shipment.get('id')}: {e}")
                    await simulation_sleep(0.1)

                # Offloads (crane for arrived)
                for shipment in pending_arrived:
                    try:
                        shipment_id = shipment['id']
                        if self._phase_in_flight(shipment, 'offload', edges_by_id, shipments_by_id):
                            continue
                        current_node = shipment.get('currentNode', 'A1') or 'A1'
                        if self._is_dock_or_berth(current_node):
                            logger.info(f"Offload (crane) for {shipment_id} at {current_node}")
                            details = {'shipmentId': shipment_id, 'destNode': current_node}
                            assigned = await self._assign_stage_device(shipment_id, 'offload', details)
                            if assigned:
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'offloadQueued': False, 'updatedAt': simulation_datetime()}})
                            else:
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'offloadQueued': True, 'updatedAt': simulation_datetime()}})
                    except Exception as e:
                        logger.warning(f"Offload fail {shipment_id}: {e}")
                    await simulation_sleep(0.1)

                # Stores (robot_move + forklift_load) – includes storing with incomplete store_move
                for shipment in pending_store_phases:
                    try:
                        shipment_id = shipment['id']
                        current_node = shipment.get('currentNode', 'B4') or 'B4'  # Post-transport at wh
                        warehouse = shipment.get('destination') or self._nearest_warehouse(current_node)
                        if self._can_assign_store(shipment, warehouse):
                            queued = shipment.get('storeQueued', False)
                            if queued:
                                logger.info(f"Priority store for queued {shipment_id}")

                            move_complete = phase_complete(shipment.get('assignedEdges', []), 'store_move')
                            load_complete = phase_complete(shipment.get('assignedEdges', []), 'store_load')

                            if not move_complete:
                                if self._phase_in_flight(shipment, 'store_move', edges_by_id, shipments_by_id):
                                    continue
                                logger.info(f"Store_move (robot) for {shipment_id} at {warehouse}")
                                details = {
                                    'shipmentId': shipment_id,
                                    'pickupNode': warehouse,
                                    'finalNode': warehouse,
                                    'subPhase': 'move',
                                }
                                assigned = await self._assign_stage_device(shipment_id, 'store_move', details)
                                if assigned:
                                    await self.db.shipments.update_one(
                                        {'id': shipment_id},
                                        {'$set': {'status': 'storing', 'storeQueued': False, 'updatedAt': simulation_datetime()}},
                                    )
                                else:
                                    await self.db.shipments.update_one(
                                        {'id': shipment_id},
                                        {'$set': {'storeQueued': True, 'updatedAt': simulation_datetime()}},
                                    )
                            elif not load_complete:
                                if self._phase_in_flight(shipment, 'store_load', edges_by_id, shipments_by_id):
                                    continue
                                wh_total, wh_cap, wh_pct = self._warehouse_load(warehouse)
                                reserved = await self._count_warehouse_reservations(warehouse)
                                if reserved >= wh_cap:
                                    logger.warning(
                                        f"Full wh {warehouse} ({reserved}/{wh_cap}); queue store_load {shipment_id}"
                                    )
                                    await self.db.shipments.update_one(
                                        {'id': shipment_id},
                                        {'$set': {'storeQueued': True, 'updatedAt': simulation_datetime()}},
                                    )
                                    continue

                                logger.info(f"Store_load (forklift) pair for {shipment_id} at {warehouse}")
                                details = {
                                    'shipmentId': shipment_id,
                                    'pickupNode': warehouse,
                                    'finalNode': warehouse,
                                    'requiredPlace': warehouse,
                                    'subPhase': 'load',
                                }
                                assigned = await self._assign_stage_device(shipment_id, 'store_load', details)
                                if assigned:
                                    await self.db.shipments.update_one(
                                        {'id': shipment_id},
                                        {'$set': {'storeQueued': False, 'updatedAt': simulation_datetime()}},
                                    )
                                    logger.debug(
                                        f"Store_load assigned for {shipment_id}; "
                                        f"completion will mark stored and increment occupancy"
                                    )
                                else:
                                    await self.db.shipments.update_one(
                                        {'id': shipment_id},
                                        {'$set': {'storeQueued': True, 'updatedAt': simulation_datetime()}},
                                    )
                            else:
                                await self.db.shipments.update_one(
                                    {'id': shipment_id},
                                    {'$set': {'status': 'stored', 'storeQueued': False, 'updatedAt': simulation_datetime()}},
                                )
                    except Exception as e:
                        logger.warning(f"Store fail {shipment_id}: {e}")
                    await simulation_sleep(0.1)

                # Deliveries (truck_delivery for stored)
                for shipment in pending_stored:
                    try:
                        shipment_id = shipment['id']
                        if self._phase_in_flight(shipment, 'delivery', edges_by_id, shipments_by_id):
                            continue
                        warehouse = shipment.get('destination') or 'B4'
                        if self._is_warehouse(warehouse):
                            logger.info(f"Delivery (truck_delivery) for {shipment_id}: {warehouse} → E5")
                            details = {
                                'shipmentId': shipment_id,
                                'startNode': warehouse,
                                'finalNode': 'E5',
                                'pickupNode': warehouse,
                                'requiredPlace': 'E5',
                                'destNode': 'E5',
                            }
                            assigned = await self._assign_stage_device(shipment_id, 'delivery', details)  # → truck_delivery
                            if assigned:
                                await self.db.shipments.update_one({'id': shipment_id}, {'$set': {'deliveryStatus': 'assigned', 'updatedAt': simulation_datetime()}})
                    except Exception as e:
                        logger.warning(f"Delivery fail {shipment_id}: {e}")
                    await simulation_sleep(0.1)

                logger.info(
                    f"Monitor cycle: "
                    f"{len(pending_offloaded) + len(pending_arrived) + len(pending_store_phases) + len(pending_stored)} assigns"
                )

                stale_cutoff = simulation_datetime() - timedelta(seconds=COMPLETING_POLL_STALE_SECONDS)
                completing = [] if CURRENT.get() else await self.db[RUNTIME_COLLECTION].find({
                    'taskPhase': 'completing',
                    'updatedAt': {'$lt': stale_cutoff},
                }).to_list(None)
                for edge in completing:
                    try:
                        merged = await self._load_edge_merged(edge.get('id'))
                        logger.info(
                            f"Stale completing poll for {edge.get('id')} "
                            f"(>{COMPLETING_POLL_STALE_SECONDS}s); invoking handle_completion"
                        )
                        await self.handle_completion(edge['id'], (merged or {}).get('task', {}))
                    except Exception as e:
                        logger.warning(f"Completion fail {edge.get('id')}: {e}")

                await simulation_sleep(1)  # Fast poll
            
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await simulation_sleep(5)

    # Add if missing: Warehouse check helper (in task_assigner.py or here)
    def _is_warehouse(self, node):
        """Check if node is a warehouse using the loaded graph, with seed fallback."""
        if node in self.graph:
            return self.graph[node].get('type') == 'warehouse'
        return node in ['B4', 'D2', 'E5']

    def _can_assign_store(self, shipment, warehouse):
        """Relaxed store guard: allow post-transport when currentNode drifted from warehouse."""
        if not self._is_warehouse(warehouse):
            return False
        status = shipment.get('status')
        if status in ('transported', 'transporting', 'storing', 'stored'):
            return True
        current_node = shipment.get('currentNode') or warehouse
        destination = shipment.get('destination')
        warehouse_assigned = shipment.get('warehouseAssigned')
        allowed = {n for n in (warehouse, destination, warehouse_assigned) if n}
        if current_node in allowed:
            return True
        if status == 'transported' and warehouse_assigned:
            return True
        return False

    async def _reconcile_shipment_status(self, shipment):
        """Bump status when assignedEdges show a later completed phase than stored status."""
        if not shipment:
            return 'arrived'
        best_status = shipment.get('status', 'arrived')
        for entry in shipment.get('assignedEdges', []):
            if isinstance(entry, dict) and entry.get('completedAt'):
                implied = status_after_phase(entry.get('phase'))
                best_status = max_status(best_status, implied)
        if shipment_status_rank(best_status) > shipment_status_rank(shipment.get('status', 'arrived')):
            await self.db.shipments.update_one(
                {'id': shipment['id']},
                {'$set': {'status': best_status, 'updatedAt': simulation_datetime()}},
            )
            logger.info(f"Reconciled {shipment['id']} status -> {best_status} from assignedEdges")
        return best_status


    async def _select_warehouse(self, current_node, shipment_id):
        """... (existing)"""
        if not self.graph:
            return 'B4'
        
        warehouses = {nid: ndata for nid, ndata in self.graph.items() if ndata.get('type') == 'warehouse'}  # B4=3, D2=2, E5=35
        if not warehouses:
            return 'B4'  # Primary
        
        # FIXED: Dynamic max_cap from graph (E5=35; avoids hardcode if changes)
        max_cap = max(wh_data.get('capacity', 35) for wh_data in warehouses.values()) or 35
        
        predicted_loads = self.analyzer.get_predicted_loads() if self.analyzer else {}  # Dict or None → {}
        if not isinstance(predicted_loads, dict):
            predicted_loads = {}
            logger.warning("Predicted loads not dict; default empty")
        
        best_warehouse = None
        best_score = float('inf')
        
        for wh_id, wh_data in warehouses.items():
            # Coords/dist (existing)
            if current_node not in self.graph:
                dist = float('inf')
            else:
                c_row, c_col = self._node_to_coords(current_node)
                w_row, w_col = self._node_to_coords(wh_id)
                dist = abs(c_row - w_row) + abs(c_col - w_col)
            
            # FIXED: Safe cap/load (all have cap, but guard occ/pred)
            cap = wh_data.get('capacity')  # e.g., 3 for B4
            if cap is None:
                cap = 35
                logger.debug(f"None cap fallback for {wh_id}")
            
            occ = wh_data.get('currentOccupancy', 0)
            safe_occ = int(occ) if occ is not None else 0  # None → 0
            
            pred_load = predicted_loads.get(wh_id, 0)
            if pred_load is None:
                pred_load = 0
                logger.debug(f"None pred_load fallback for {wh_id}")
            pred_load = int(pred_load) if isinstance(pred_load, (int, float)) else 0
            
            total_load = safe_occ + pred_load  # Now int
            load_score = total_load / cap if cap > 0 else float('inf')  # Safe / int
            
            cap_score = cap / max_cap  # Normalize to graph max (e.g., 3/35=0.09 for B4)
            
            score = 0.6 * dist + 0.2 * (1 - cap_score) + 0.2 * load_score
            logger.debug(f"{wh_id}: dist={dist}, cap_score={cap_score:.2f} (cap={cap}/{max_cap}), load_score={load_score:.2f} (total={total_load}), score={score:.2f}")
            
            if score < best_score:
                best_score = score
                best_warehouse = wh_id
        
        if best_warehouse:
            logger.info(f"Selected {best_warehouse} for {shipment_id} at {current_node} (score {best_score:.2f}; favors low dist/load, high cap)")
            return best_warehouse
        return 'B4'  # Fallback primary warehouse


    def _node_to_coords(self, node_id):
        """Helper: Parse node to (row, col) for Manhattan dist (A1=1,1; B4=2,4)."""
        row = ord(node_id[0].upper()) - ord('A') + 1  # A=1, B=2, etc.
        col = int(node_id[1:]) if node_id[1:].isdigit() else 1  # A1 col=1
        return row, col


    # NEW: Maintenance task assignment (integrated with analyzer.planner; no self.planner)
    async def assign_maintenance_task(self, node, db, mqtt_client, severity=None):
        """Assign idle robot/truck for repair/inspection on anomalous node."""
        logger.info(f"Assigning maintenance for anomaly at {node}")
        idle_edges = []
        for runtime in await db[RUNTIME_COLLECTION].find({"type": {"$in": ["robot"]}, "taskPhase": "idle"}).to_list(None):
            assignment = await db[ASSIGNMENT_COLLECTION].find_one({"id": runtime["id"]}) or {}
            if not assignment.get("shipmentId") and not assignment.get("assignedShipment"):
                idle_edges.append(merge_edge_snapshot(assignment, runtime))
        if not idle_edges:
            logger.warning(f"No idle edges for maintenance at {node}")
            return False

        edge = idle_edges[0]
        path = self.analyzer.planner.compute_path(
            edge['currentLocation'], node, self.analyzer.get_current_loads(),
            self.analyzer.get_route_congestion(), predicted_loads=self.analyzer.get_predicted_loads()) if self.analyzer.planner else None
        if not path:
            logger.warning('No reachable maintenance route to %s', node)
            return False
        epoch = int(edge.get('assignmentEpoch') or 0) + 1
        maint_task = {
            "shipmentId": f"repair_{node}_{int(simulation_time())}",
            "phase": "maintenance",
            "startNode": edge["currentLocation"],
            "finalNode": node,
            "requiredPlace": node,
            "task": "inspect_repair",
            "path": path,
            "assignmentEpoch": epoch,
            "routeRevision": 1,
        }
        await db.maintenanceTasks.update_one({'id': maint_task['shipmentId']}, {'$setOnInsert': {
            'id': maint_task['shipmentId'], 'node': node, 'edgeId': edge['id'], 'status': 'assigned',
            'assignmentEpoch': epoch, 'createdAt': datetime.utcnow()}}, upsert=True)
        await db[ASSIGNMENT_COLLECTION].update_one(
            {"id": edge["id"]},
            {
                "$set": {
                    "shipmentId": maint_task["shipmentId"],
                    "assignedShipment": maint_task["shipmentId"],
                    "task": maint_task,
                    "startNode": edge["currentLocation"],
                    "finalNode": node,
                    "assignmentEpoch": epoch,
                    "routeRevision": 1,
                    "claimedAt": simulation_datetime(),
                    "updatedAt": simulation_datetime(),
                }
            },
            upsert=True,
        )
        await mqtt_client.publish(f"harboursense/edge/{edge['id']}/task", json.dumps(maint_task))
        logger.info(f"Assigned {edge['id']} for repair at {node}")

        return True
