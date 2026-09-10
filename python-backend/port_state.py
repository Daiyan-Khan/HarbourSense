"""Read-only port-state aggregation shared by overview logging and future APIs."""

from datetime import datetime

from edge_view import load_merged_edges
from task_assigner import has_assigned_phase, is_completed_assignment

PENDING_BUCKET_KEYS = (
    'pending_arrived',
    'pending_offloaded',
    'pending_transported',
    'pending_storing',
    'pending_stored',
)

PHASE_DEVICE_TYPE = {
    'offload': 'crane',
    'transport': 'truck_tempo',
    'store_move': 'robot',
    'store_load': 'forklift',
    'delivery': 'truck_delivery',
}

PHASE_QUEUE_FLAG = {
    'offload': 'offloadQueued',
    'transport': 'transportQueued',
    'store_move': 'storeQueued',
    'store_load': 'storeQueued',
}

DOCK_TYPES = frozenset({'dock', 'berth'})

KNOWN_STALL_CODES = frozenset({
    'EMPTY_PATH_STALL',
    'PICKUP_LEG_MISSING',
    'MQTT_TASK_NEVER_APPLIED',
})


def classify_pending_backlog(shipments):
    """Mirror monitor_and_assign pending buckets (read-only; no reconcile)."""
    return {
        'pending_offloaded': [
            s for s in shipments
            if s.get('status') == 'offloaded'
            and not _phase_complete(s.get('assignedEdges', []), 'transport')
        ],
        'pending_arrived': [
            s for s in shipments
            if s.get('status') in ('arrived', 'waiting')
            and not _phase_complete(s.get('assignedEdges', []), 'offload')
        ],
        'pending_transported': [
            s for s in shipments
            if s.get('status') == 'transported'
            and not _phase_complete(s.get('assignedEdges', []), 'store_move')
        ],
        'pending_storing': [
            s for s in shipments
            if s.get('status') == 'storing'
            and _phase_complete(s.get('assignedEdges', []), 'store_move')
            and not _phase_complete(s.get('assignedEdges', []), 'store_load')
        ],
        'pending_stored': [
            s for s in shipments
            if s.get('status') == 'stored'
            and not _phase_complete(s.get('assignedEdges', []), 'delivery')
        ],
    }


def _phase_complete(assigned_edges, phase):
    return any(is_completed_assignment(entry, phase=phase) for entry in assigned_edges or [])


def next_required_phase(shipment):
    """Return the next workflow phase required for this shipment, or None if complete."""
    if not shipment:
        return None
    status = shipment.get('status', 'arrived')
    assigned = shipment.get('assignedEdges', [])
    if status == 'delivered':
        return None
    if status in ('arrived', 'waiting') and not _phase_complete(assigned, 'offload'):
        return 'offload'
    if status == 'offloaded' and not _phase_complete(assigned, 'transport'):
        return 'transport'
    if status == 'transported' and not _phase_complete(assigned, 'store_move'):
        return 'store_move'
    if status == 'storing' and _phase_complete(assigned, 'store_move') and not _phase_complete(assigned, 'store_load'):
        return 'store_load'
    if status == 'stored' and not _phase_complete(assigned, 'delivery'):
        return 'delivery'
    return None


def _is_dock_or_berth(node, graph_meta):
    if not node:
        return False
    node_data = graph_meta.get('nodes', {}).get(node, {})
    if node_data.get('type') in DOCK_TYPES:
        return True
    return str(node).startswith('A')


def _warehouse_load_pct(warehouse_id, graph_meta):
    node = graph_meta.get('nodes', {}).get(warehouse_id, {})
    cap = int(node.get('capacity', 35) or 35)
    occ = max(0, int(node.get('currentOccupancy', 0) or 0))
    if cap > 0:
        occ = min(occ, cap)
    return occ / cap if cap > 0 else 0.0, occ, cap


def _incomplete_assignments(shipment, phase=None):
    result = []
    for entry in shipment.get('assignedEdges', []) or []:
        if not isinstance(entry, dict):
            continue
        if entry.get('completedAt'):
            continue
        entry_phase = entry.get('phase')
        if phase and entry_phase != phase:
            continue
        result.append({'edgeId': entry.get('edgeId'), 'phase': entry_phase})
    return result


def _workflow_diagnostic_is_stale(shipment, next_phase):
    """True when persisted workflowDiagnostic no longer matches shipment state."""
    stored = shipment.get('workflowDiagnostic') or {}
    if not stored.get('blockerMessage'):
        return True
    stored_phase = stored.get('nextPhase')
    stored_code = stored.get('blockerCode')
    assigned = shipment.get('assignedEdges', [])
    if stored_phase and stored_phase != next_phase:
        return True
    if stored_code == 'ASSIGNED' and stored_phase and _phase_complete(assigned, stored_phase):
        return True
    return False


def diagnose_shipment_blocker(shipment, edges, graph_meta=None):
    """Infer why a shipment has not advanced to its next phase."""
    graph_meta = graph_meta or {}
    shipment_id = shipment.get('id', 'unknown')
    status = shipment.get('status', 'arrived')
    next_phase = next_required_phase(shipment)
    device_type = PHASE_DEVICE_TYPE.get(next_phase) if next_phase else None
    idle_of_type = sum(
        1 for e in edges
        if e.get('taskPhase') == 'idle'
        and e.get('type') == device_type
        and not e.get('shipmentId')
        and not e.get('assignedShipment')
    ) if device_type else 0

    def _route_fields(edge_doc):
        if not edge_doc:
            return {}
        return {
            'pathLen': len(edge_doc.get('path') or []),
            'pendingPathLen': len(edge_doc.get('pendingPath') or []),
            'nextNode': edge_doc.get('nextNode'),
            'routeRevision': edge_doc.get('routeRevision'),
        }

    base = {
        'shipmentId': shipment_id,
        'status': status,
        'nextPhase': next_phase,
        'requiredDeviceType': device_type,
        'idleOfType': idle_of_type,
        'assignedIncomplete': _incomplete_assignments(shipment),
    }

    incomplete_relevant = (
        _incomplete_assignments(shipment, phase=next_phase)
        if next_phase
        else _incomplete_assignments(shipment)
    )
    if incomplete_relevant:
        edge_id = incomplete_relevant[0].get('edgeId')
        edge = next((e for e in edges if e.get('id') == edge_id), {})
        route_info = _route_fields(edge)
        if edge.get('taskPhase') == 'idle' and (edge.get('shipmentId') or edge.get('assignedShipment')):
            return {
                **base,
                **route_info,
                'nextPhase': incomplete_relevant[0].get('phase') or next_phase,
                'blockerCode': 'STALE_INFLIGHT',
                'blockerMessage': (
                    f"{edge_id} idle with open {incomplete_relevant[0].get('phase')} assignment; "
                    f"awaiting simulator task apply or orphan release"
                ),
                'assignedIncomplete': incomplete_relevant,
            }
        if _edge_actively_working(edge):
            edge_phase = edge.get('taskPhase', 'unknown')
            active_phase = incomplete_relevant[0].get('phase') or next_phase
            return {
                **base,
                **route_info,
                'nextPhase': active_phase or next_phase,
                'blockerCode': 'AWAITING_COMPLETION',
                'blockerMessage': f"Awaiting {edge_id} to finish {active_phase} (edge phase: {edge_phase})",
                'assignedIncomplete': incomplete_relevant,
            }

    if next_phase is None:
        return {
            **base,
            'blockerCode': 'NONE',
            'blockerMessage': 'Workflow complete or delivered',
        }

    stored = shipment.get('workflowDiagnostic') or {}
    if _workflow_diagnostic_is_stale(shipment, next_phase):
        stored = {}

    queue_key = PHASE_QUEUE_FLAG.get(next_phase)
    if queue_key and shipment.get(queue_key):
        if next_phase == 'transport':
            warehouse = shipment.get('destination') or shipment.get('warehouseAssigned') or 'warehouse'
            load_pct, occ, cap = _warehouse_load_pct(warehouse, graph_meta)
            if load_pct >= 0.8:
                return {
                    **base,
                    'blockerCode': 'WAREHOUSE_FULL',
                    'blockerMessage': f"Warehouse {warehouse} at {load_pct:.0%} capacity ({occ}/{cap}); transport queued",
                }
        return {
            **base,
            'blockerCode': 'QUEUED',
            'blockerMessage': stored.get('blockerMessage') or f"Queued for {next_phase} ({device_type})",
        }

    if next_phase == 'offload':
        current_node = shipment.get('currentNode') or shipment.get('arrivalNode') or 'A1'
        if not _is_dock_or_berth(current_node, graph_meta):
            return {
                **base,
                'blockerCode': 'NOT_AT_DOCK',
                'blockerMessage': f"Shipment not at dock/berth (currentNode={current_node})",
            }

    if next_phase == 'transport':
        pickup = shipment.get('currentNode') or shipment.get('arrivalNode')
        if not _is_dock_or_berth(pickup, graph_meta):
            return {
                **base,
                'blockerCode': 'NO_DOCK_PICKUP',
                'blockerMessage': f"No dock pickup node (current={pickup})",
            }
        warehouse = shipment.get('destination') or shipment.get('warehouseAssigned')
        if warehouse:
            load_pct, occ, cap = _warehouse_load_pct(warehouse, graph_meta)
            if load_pct >= 0.8:
                return {
                    **base,
                    'blockerCode': 'WAREHOUSE_FULL',
                    'blockerMessage': f"Warehouse {warehouse} at {load_pct:.0%} ({occ}/{cap}); cannot assign transport",
                }

    if next_phase == 'store_load':
        warehouse = shipment.get('destination') or shipment.get('warehouseAssigned') or shipment.get('currentNode')
        if warehouse:
            load_pct, occ, cap = _warehouse_load_pct(warehouse, graph_meta)
            if load_pct >= 1.0:
                return {
                    **base,
                    'blockerCode': 'WAREHOUSE_FULL',
                    'blockerMessage': f"Warehouse {warehouse} full ({occ}/{cap}); cannot assign store_load",
                }

    if idle_of_type == 0:
        loc = shipment.get('currentNode') or shipment.get('arrivalNode') or 'site'
        return {
            **base,
            'blockerCode': 'NO_IDLE_DEVICE',
            'blockerMessage': f"No idle {device_type} available for {next_phase} (pickup near {loc})",
        }

    stored_code = stored.get('blockerCode')
    if stored.get('blockerMessage') and not (
        stored_code == 'NO_IDLE_DEVICE' and idle_of_type > 0
    ) and not (
        stored_code == 'NO_VALID_PATH' and '0 idle' in (stored.get('blockerMessage') or '')
    ):
        return {
            **base,
            'blockerCode': stored_code or 'WAITING_ASSIGNMENT',
            'blockerMessage': stored['blockerMessage'],
        }

    if next_phase == 'transport' and idle_of_type > 0:
        pickup = shipment.get('currentNode') or shipment.get('arrivalNode') or 'dock'
        return {
            **base,
            'blockerCode': 'WAITING_ASSIGNMENT',
            'blockerMessage': (
                f"{idle_of_type} idle {device_type}(s) available; "
                f"awaiting assignment to pick up at {pickup}"
            ),
        }

    return {
        **base,
        'blockerCode': 'AT_DOCK_OK' if next_phase == 'offload' else 'WAITING_ASSIGNMENT',
        'blockerMessage': f"Waiting for {device_type} assignment for {next_phase}",
    }


def _edge_actively_working(edge):
    """True when the edge still holds an in-flight assignment (not a stale idle reset)."""
    if not edge or edge.get('taskPhase') == 'idle':
        return bool(edge and (edge.get('shipmentId') or edge.get('assignedShipment')))
    return edge.get('taskPhase') not in ('idle', 'unknown')


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


def _open_assignments_for_edge(edge_id, shipments_by_id):
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
    open_entries.sort(key=lambda item: _assignment_sort_key(item[1]))
    return open_entries


def _edge_has_pending_task(edge, shipment_id=None):
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


def _is_stale_open_assignment(edge, shipment_id, entry, open_on_edge):
    """True when edge finished work but this shipment's assignedEdges entry is still open."""
    if not edge or not isinstance(entry, dict) or entry.get('completedAt'):
        return False
    if edge.get('id') != entry.get('edgeId'):
        return False
    if edge.get('taskPhase') != 'idle':
        return False
    if edge.get('shipmentId') or edge.get('assignedShipment'):
        return False
    if _edge_has_pending_task(edge):
        return False
    if len(open_on_edge) > 1:
        newest_sid, _ = open_on_edge[-1]
        if shipment_id == newest_sid:
            return False
    return True


def _extract_edge_stall(edge):
    """Return stallCode and workflowLeg from simulator-written edge fields."""
    debug = edge.get('debugEvent') if isinstance(edge.get('debugEvent'), dict) else {}
    stall = edge.get('stallCode') or debug.get('stallCode') or debug.get('code')
    workflow_leg = edge.get('workflowLeg')

    if not stall and edge.get('taskPhase') == 'en_route_start':
        loc = edge.get('currentLocation')
        final = edge.get('finalNode')
        path_empty = not (edge.get('path') or edge.get('pendingPath'))
        if loc and final and loc == final and path_empty and edge.get('shipmentId'):
            stall = 'EMPTY_PATH_STALL'

    if not stall and edge.get('taskPhase') == 'idle' and edge.get('shipmentId'):
        task = edge.get('task') if isinstance(edge.get('task'), dict) else {}
        if task.get('phase') and not (edge.get('path') or edge.get('pendingPath')):
            stall = 'MQTT_TASK_NEVER_APPLIED'

    return stall, workflow_leg


def _stall_diagnostic(edge, stall_code, workflow_leg=None):
    """Build idle/stuck diagnostic payload for a recognized simulator stall."""
    extra = {}
    if workflow_leg:
        extra['workflowLeg'] = workflow_leg
    leg_hint = f" (leg={workflow_leg})" if workflow_leg else ""
    messages = {
        'EMPTY_PATH_STALL': f"Empty path at final node{leg_hint}; awaiting sim path recovery",
        'PICKUP_LEG_MISSING': f"Transport pickup leg not resolved{leg_hint}",
        'MQTT_TASK_NEVER_APPLIED': f"Task assigned in Mongo but simulator never applied MQTT task{leg_hint}",
    }
    return stall_code, messages.get(stall_code, f"Simulator stall: {stall_code}{leg_hint}"), extra


def diagnose_idle_edge(edge, shipments_by_id, graph_meta=None, pending_counts=None, backlog=None):
    """Infer why an idle edge device has no active task."""
    if backlog and pending_counts is None:
        pending_counts = {
            key: len(value)
            for key, value in backlog.items()
            if str(key).startswith('pending_') and isinstance(value, list)
        }
    elif isinstance(graph_meta, dict) and graph_meta and any(str(k).startswith('pending_') for k in graph_meta):
        pending_counts = {
            key: len(value)
            for key, value in graph_meta.items()
            if str(key).startswith('pending_') and isinstance(value, list)
        }
        graph_meta = {}

    pending_counts = pending_counts or {}
    graph_meta = graph_meta or {}
    edge_id = edge.get('id', 'unknown')
    edge_type = edge.get('type', 'unknown')

    def _result(idle_code, idle_message, **extra):
        return {
            'edgeId': edge_id,
            'edgeType': edge_type,
            'taskPhase': edge.get('taskPhase', 'idle'),
            'currentLocation': edge.get('currentLocation'),
            'idleCode': idle_code,
            'idleMessage': idle_message,
            'whyIdle': idle_code,
            'detail': idle_message,
            **extra,
        }

    stall_code, workflow_leg = _extract_edge_stall(edge)
    if stall_code:
        code, message, stall_extra = _stall_diagnostic(edge, stall_code, workflow_leg)
        if code in KNOWN_STALL_CODES or edge.get('taskPhase') != 'idle':
            return _result(code, message, **stall_extra)

    for shipment_id, shipment in (shipments_by_id or {}).items():
        for entry in shipment.get('assignedEdges', []) or []:
            if not isinstance(entry, dict) or entry.get('completedAt'):
                continue
            if entry.get('edgeId') != edge_id:
                continue
            edge_shipment = edge.get('shipmentId') or edge.get('assignedShipment')
            if edge_shipment and edge_shipment != shipment_id:
                continue
            if _edge_actively_working(edge):
                return _result(
                    'COMPLETION_PENDING',
                    f"Awaiting completion ack for {shipment_id} ({entry.get('phase')})",
                    shipmentId=shipment_id,
                )

    open_on_edge = _open_assignments_for_edge(edge_id, shipments_by_id)
    edge_shipment = edge.get('shipmentId') or edge.get('assignedShipment')
    if edge.get('taskPhase') == 'idle' and not edge_shipment and open_on_edge:
        stale_entries = [
            (sid, ent)
            for sid, ent in open_on_edge
            if _is_stale_open_assignment(edge, sid, ent, open_on_edge)
        ]
        if stale_entries:
            sid, entry = stale_entries[0]
            return _result(
                'STALE_COMPLETION_ACK',
                f"Stale assignment for {sid} ({entry.get('phase')}); monitor will reconcile",
                shipmentId=sid,
            )
        newest_sid, newest_entry = open_on_edge[-1]
        if _edge_has_pending_task(edge, newest_sid):
            return _result(
                'COMPLETION_PENDING',
                f"Awaiting simulator to apply task for {newest_sid} ({newest_entry.get('phase')})",
                shipmentId=newest_sid,
            )

    shipment_id = edge.get('shipmentId') or edge.get('assignedShipment')
    if shipment_id and _edge_actively_working(edge):
        shipment = shipments_by_id.get(shipment_id, {})
        incomplete = _incomplete_assignments(shipment)
        if incomplete:
            return _result(
                'COMPLETION_PENDING',
                f"Awaiting completion ack for {shipment_id} ({incomplete[0].get('phase')})",
                shipmentId=shipment_id,
            )

    if edge.get('taskPhase') != 'idle':
        return _result(
            'NOT_IDLE',
            f"Edge is {edge.get('taskPhase')}, not idle",
        )

    phase_for_type = next(
        (phase for phase, dtype in PHASE_DEVICE_TYPE.items() if dtype == edge_type),
        None,
    )
    pending_key = {
        'offload': 'pending_arrived',
        'transport': 'pending_offloaded',
        'store_move': 'pending_transported',
        'store_load': 'pending_storing',
        'delivery': 'pending_stored',
    }.get(phase_for_type)

    if pending_key and pending_counts.get(pending_key, 0) > 0:
        return _result(
            'WAITING_MONITOR',
            f"No assignment yet; {pending_counts[pending_key]} shipment(s) pending {phase_for_type}",
        )

    if edge.get('path') and len(edge.get('path', [])) > 0:
        return _result(
            'STALE_PATH_CLEARED',
            'Idle with stale path (monitor or loop will clear)',
        )

    return _result(
        'NO_TASK',
        'No pending assignment for this device type',
    )


async def _load_graph_meta(db):
    nodes = {}
    if not hasattr(db, 'graph'):
        return {'nodes': nodes}
    cursor = db.graph.find({})
    if hasattr(cursor, 'to_list'):
        graph_docs = await cursor.to_list(None)
    else:
        graph_docs = []
        async for doc in cursor:
            graph_docs.append(doc)
    for doc in graph_docs:
        node_id = doc.get('id')
        if node_id:
            nodes[node_id] = {
                'type': doc.get('type'),
                'capacity': doc.get('capacity', 35),
                'currentOccupancy': doc.get('currentOccupancy', 0),
            }
    return {'nodes': nodes}


async def build_port_state_snapshot(db):
    """Aggregate fleet, backlog, queue, alert counts, and causal diagnostics from MongoDB."""
    active_shipments = await db.shipments.find({'status': {'$ne': 'delivered'}}).to_list(None)
    all_shipments = await db.shipments.find({}).to_list(None)

    status_counts = {}
    for shipment in all_shipments:
        status = shipment.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1

    backlog = classify_pending_backlog(active_shipments)
    pending_counts = {key: len(backlog[key]) for key in PENDING_BUCKET_KEYS}
    queue_counts = {
        'offloadQueued': sum(1 for s in active_shipments if s.get('offloadQueued')),
        'transportQueued': sum(1 for s in active_shipments if s.get('transportQueued')),
        'storeQueued': sum(1 for s in active_shipments if s.get('storeQueued')),
    }

    edges = await load_merged_edges(db)
    edge_state_counts = {'idle': 0, 'en_route_start': 0, 'assigned': 0, 'completing': 0}
    edge_type_counts = {}
    idle_by_type = {}
    busy_edges = []
    for edge in edges:
        state = edge.get('taskPhase', 'idle')
        edge_state_counts[state] = edge_state_counts.get(state, 0) + 1
        edge_type = edge.get('type', 'unknown')
        edge_type_counts[edge_type] = edge_type_counts.get(edge_type, 0) + 1
        if state == 'idle':
            idle_by_type[edge_type] = idle_by_type.get(edge_type, 0) + 1
        else:
            busy_edges.append(
                f"{edge['id']} ({edge_type}) at {edge.get('currentLocation')}"
            )

    alerts = await db.sensorAlerts.find({'resolved': False}).to_list(None)
    alert_counts = {'high': 0, 'medium': 0, 'low': 0}
    for alert in alerts:
        severity = alert.get('severity', 'low')
        alert_counts[severity] = alert_counts.get(severity, 0) + 1

    graph_meta = await _load_graph_meta(db)
    shipments_by_id = {s['id']: s for s in active_shipments if s.get('id')}

    shipment_diagnostics = [
        diagnose_shipment_blocker(s, edges, graph_meta)
        for s in active_shipments
    ]
    idle_edge_diagnostics = [
        diagnose_idle_edge(e, shipments_by_id, graph_meta, pending_counts)
        for e in edges
        if e.get('taskPhase') == 'idle'
    ]

    return {
        'total_shipments': len(all_shipments),
        'active_shipments': len(active_shipments),
        'status_counts': status_counts,
        'pending_counts': pending_counts,
        'pending_ids': {key: [s['id'] for s in backlog[key]] for key in PENDING_BUCKET_KEYS},
        'queue_counts': queue_counts,
        'edge_state_counts': edge_state_counts,
        'edge_type_counts': edge_type_counts,
        'idle_by_type': idle_by_type,
        'busy_edges': busy_edges,
        'unresolved_alerts': len(alerts),
        'alert_counts': alert_counts,
        'shipment_diagnostics': shipment_diagnostics,
        'idle_edge_diagnostics': idle_edge_diagnostics,
        'snapshotAt': datetime.now().isoformat(),
        'generatedAt': datetime.now().isoformat(),
    }
