"""Merge edgeAssignments (backend) + edgeRuntime (sim) into legacy /api/edges shape."""

from __future__ import annotations

RUNTIME_COLLECTION = "edgeRuntime"
ASSIGNMENT_COLLECTION = "edgeAssignments"
LEGACY_COLLECTION = "edgeDevices"

ASSIGNMENT_FIELDS = frozenset({
    "shipmentId",
    "assignedShipment",
    "task",
    "pendingPath",
    "routeRevision",
    "assignmentEpoch",
    "startNode",
    "finalNode",
    "claimedAt",
})

RUNTIME_FIELDS = frozenset({
    "type",
    "roles",
    "speed",
    "capacity",
    "currentLocation",
    "taskPhase",
    "path",
    "remainingPath",
    "nextNode",
    "progressToNext",
    "eta",
    "activeHop",
    "stateRevision",
    "workflowLeg",
    "pickupCompleted",
    "acceptedAssignmentEpoch",
    "completedAssignmentEpoch",
    "stallCode",
    "lastTransitionAt",
    "debugEvent",
    "journeyTime",
    "taskCompletionTime",
})

RUNTIME_ONLY_WRITE_FIELDS = RUNTIME_FIELDS | frozenset({"updatedAt"})
ASSIGNMENT_ONLY_WRITE_FIELDS = ASSIGNMENT_FIELDS | frozenset({"updatedAt", "id"})


def _has_open_assignment(assignment: dict | None) -> bool:
    if not assignment:
        return False
    return bool(assignment.get("shipmentId") or assignment.get("assignedShipment"))


def split_edge_document(doc: dict) -> tuple[dict, dict]:
    """Split a legacy edgeDevices document into (assignment, runtime) docs."""
    edge_id = doc.get("id")
    if not edge_id:
        raise ValueError("edge document requires id")

    runtime = {"id": edge_id}
    assignment = {"id": edge_id}

    for key, value in doc.items():
        if key == "id":
            continue
        if key in ASSIGNMENT_FIELDS:
            assignment[key] = value
        elif key in RUNTIME_FIELDS:
            runtime[key] = value
        elif key in ("updatedAt",):
            runtime[key] = value
            assignment[key] = value
        else:
            runtime[key] = value

    if not _has_open_assignment(assignment):
        # Keep routing metadata even when no active shipment binding.
        kept = {"id": edge_id}
        for key in ("routeRevision", "pendingPath", "assignmentEpoch"):
            if key in assignment:
                kept[key] = assignment[key]
        assignment = kept

    return assignment, runtime


def merge_edge_snapshot(assignment: dict | None, runtime: dict) -> dict:
    """Merge assignment + runtime into the shape expected by /api/edges."""
    if not runtime:
        raise ValueError("runtime document is required")

    merged = dict(runtime)
    merged["id"] = runtime.get("id") or (assignment or {}).get("id")

    if assignment:
        for key in ASSIGNMENT_FIELDS:
            if key in assignment:
                merged[key] = assignment[key]

    if not _has_open_assignment(assignment):
        merged.setdefault("shipmentId", None)
        merged.setdefault("assignedShipment", None)
        if merged.get("taskPhase") == "idle":
            merged.setdefault("task", "idle")

    merged.setdefault("stateRevision", 0)
    merged.setdefault("routeRevision", assignment.get("routeRevision", 0) if assignment else 0)
    merged.setdefault("progressToNext", 0)
    return merged


async def load_assignments_by_id(db) -> dict[str, dict]:
    assignments = {}
    async for doc in db[ASSIGNMENT_COLLECTION].find({}):
        edge_id = doc.get("id")
        if edge_id:
            assignments[edge_id] = doc
    return assignments


async def load_merged_edges(db) -> list[dict]:
    """Load merged edges; falls back to legacy edgeDevices when split collections are empty."""
    runtime_count = await db[RUNTIME_COLLECTION].count_documents({})
    if runtime_count == 0:
        legacy = [doc async for doc in db[LEGACY_COLLECTION].find({})]
        if legacy:
            return legacy

    assignments = await load_assignments_by_id(db)
    merged = []
    async for runtime in db[RUNTIME_COLLECTION].find({}):
        edge_id = runtime.get("id")
        assignment = assignments.get(edge_id)
        merged.append(merge_edge_snapshot(assignment, runtime))
    return merged


async def load_merged_edge(db, edge_id: str) -> dict | None:
    runtime = await db[RUNTIME_COLLECTION].find_one({"id": edge_id})
    if not runtime:
        return await db[LEGACY_COLLECTION].find_one({"id": edge_id})
    assignment = await db[ASSIGNMENT_COLLECTION].find_one({"id": edge_id})
    return merge_edge_snapshot(assignment, runtime)


async def clear_assignment(db, edge_id: str) -> None:
    await db[ASSIGNMENT_COLLECTION].update_one(
        {"id": edge_id},
        {
            "$set": {
                "shipmentId": None,
                "assignedShipment": None,
                "task": None,
                "pendingPath": [],
                "startNode": None,
                "finalNode": None,
            },
            "$unset": {
                "assignmentEpoch": "",
                "routeRevision": "",
                "claimedAt": "",
            },
        },
        upsert=True,
    )
