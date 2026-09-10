"""SSE hub for live merged edge snapshots driven by MQTT progress/traffic/completion events."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime

import aiomqtt

from edge_view import load_merged_edge, load_merged_edges
from mqtt_config import MqttConfigError, build_aiomqtt_params, get_mqtt_settings

logger = logging.getLogger("EdgeStream")

_subscribers: set[asyncio.Queue] = set()
_coalesce_last: dict[str, float] = {}
COALESCE_SECONDS = 0.1
_hub_task: asyncio.Task | None = None


def _format_sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, default=str)}\n\n"


def subscribe() -> asyncio.Queue:
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    _subscribers.add(queue)
    return queue


def unsubscribe(queue: asyncio.Queue) -> None:
    _subscribers.discard(queue)


async def _broadcast(payload: dict) -> None:
    dead: list[asyncio.Queue] = []
    for queue in list(_subscribers):
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(queue)
    for queue in dead:
        unsubscribe(queue)


async def push_merged_edges(db, event_type: str = "patch", edge_id: str | None = None) -> None:
    if edge_id:
        now = time.monotonic()
        last = _coalesce_last.get(edge_id, 0.0)
        if now - last < COALESCE_SECONDS:
            return
        _coalesce_last[edge_id] = now
        edge = await load_merged_edge(db, edge_id)
        payload = {
            "type": event_type,
            "edges": [edge] if edge else [],
            "ts": datetime.now().isoformat(),
        }
    else:
        edges = await load_merged_edges(db)
        payload = {
            "type": "snapshot",
            "edges": edges,
            "ts": datetime.now().isoformat(),
        }
    await _broadcast(payload)


def _edge_id_from_topic(topic: str) -> str | None:
    parts = topic.split("/")
    if len(parts) < 4:
        return None
    if parts[0] != "harboursense" or parts[1] != "edge":
        return None
    return parts[2]


async def _mqtt_listener(db, mqtt_params) -> None:
    while True:
        try:
            async with aiomqtt.Client(**mqtt_params) as client:
                await client.subscribe("harboursense/edge/+/progress")
                await client.subscribe("harboursense/traffic/update/+")
                await client.subscribe("harboursense/edge/+/completion")
                logger.info("Edge stream hub subscribed to progress/traffic/completion topics")
                async for message in client.messages:
                    topic = str(message.topic)
                    edge_id = _edge_id_from_topic(topic)
                    if not edge_id and topic.startswith("harboursense/traffic/update/"):
                        edge_id = topic.split("/")[-1]
                    if edge_id:
                        await push_merged_edges(db, "patch", edge_id)
        except aiomqtt.MqttError as exc:
            logger.warning("Edge stream MQTT disconnected: %s; retrying in 5s", exc)
            await asyncio.sleep(5)
        except Exception as exc:
            logger.error("Edge stream hub error: %s", exc)
            await asyncio.sleep(5)


async def start_edge_stream_hub(db) -> None:
    global _hub_task
    if _hub_task and not _hub_task.done():
        return
    try:
        mqtt_settings = get_mqtt_settings()
        mqtt_params = build_aiomqtt_params("edge_stream")
    except MqttConfigError as exc:
        logger.warning("Edge stream hub disabled: %s", exc)
        return
    _hub_task = asyncio.create_task(_mqtt_listener(db, mqtt_params))


async def stop_edge_stream_hub() -> None:
    global _hub_task
    if _hub_task:
        _hub_task.cancel()
        try:
            await _hub_task
        except asyncio.CancelledError:
            pass
        _hub_task = None


async def sse_event_generator(db, queue: asyncio.Queue):
    try:
        edges = await load_merged_edges(db)
        yield _format_sse({"type": "snapshot", "edges": edges, "ts": datetime.now().isoformat()})
        while True:
            payload = await queue.get()
            yield _format_sse(payload)
    finally:
        unsubscribe(queue)
