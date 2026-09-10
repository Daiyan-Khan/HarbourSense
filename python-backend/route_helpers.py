"""Route command helpers for hop-boundary movement authority."""

import json
import logging
from datetime import datetime

from edge_command_queue import enqueue_edge_work
from edge_view import ASSIGNMENT_COLLECTION, RUNTIME_COLLECTION, load_merged_edge, merge_edge_snapshot

logger = logging.getLogger("RouteHelpers")


def edge_is_in_transit(edge_doc):
    """True when simulator owns an active movement leg (matches port-sim isMidTransit)."""
    if not edge_doc:
        return False
    progress = edge_doc.get("progressToNext") or 0
    return 0 < progress < 100


def next_route_revision(edge_doc, increment=1):
    current = edge_doc.get("routeRevision") if edge_doc else 0
    if current is None:
        current = 0
    return int(current) + increment


async def publish_route_command(mqtt_client, edge_id, path, route_revision, eta=None):
    """Publish reroute/assign path to simulator-owned route topic."""
    if not mqtt_client or not path:
        return False
    payload = {
        "path": path,
        "routeRevision": route_revision,
    }
    if eta is not None:
        payload["eta"] = eta
    topic = f"harboursense/edge/{edge_id}/route"
    await mqtt_client.publish(topic, json.dumps(payload))
    logger.info(
        "Published route command edge=%s rev=%s path=%s",
        edge_id,
        route_revision,
        path,
    )
    return True


async def queue_or_publish_route(db, mqtt_client, edge_id, path, analyzer, eta=None):
    """
    Apply route at hop boundary via MQTT route command, or queue pendingPath in Mongo mid-transit.
    """
    from traffic_analyzer import normalize_node_id, validate_path_adjacency, trim_path_from_current

    async def _apply():
        edge = await load_merged_edge(db, edge_id)
        if not edge or not path:
            return False

        device_loc = normalize_node_id(edge.get("currentLocation"))
        trimmed = trim_path_from_current(path, device_loc) if device_loc else path
        validated = validate_path_adjacency(trimmed, analyzer.planner if analyzer else None)
        if not validated:
            logger.warning("Invalid route for %s: %s", edge_id, path)
            return False

        revision = next_route_revision(edge)

        if edge_is_in_transit(edge):
            await db[ASSIGNMENT_COLLECTION].update_one(
                {"id": edge_id},
                {
                    "$set": {
                        "pendingPath": validated,
                        "routeRevision": revision,
                        "updatedAt": datetime.now(),
                    }
                },
                upsert=True,
            )
            logger.info(
                "Queued pendingPath for %s rev=%s (mid-transit progress=%s)",
                edge_id,
                revision,
                edge.get("progressToNext"),
            )
            return True

        return await publish_route_command(mqtt_client, edge_id, validated, revision, eta=eta)

    return await enqueue_edge_work(edge_id, _apply)
