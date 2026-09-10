"""Per-edge asyncio serialization for backend Mongo/MQTT commands (mirrors port-sim enqueueEdgeWork)."""

import asyncio
import logging
from collections import defaultdict

logger = logging.getLogger("EdgeCommandQueue")

_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


def reset_edge_queues_for_tests():
    """Clear lock registry between tests."""
    _locks.clear()


async def enqueue_edge_work(edge_id, coro_factory):
    """
    Run coro_factory() exclusively for edge_id.
    coro_factory must be a zero-arg callable returning an awaitable.
    """
    if not edge_id:
        return await coro_factory()

    lock = _locks[edge_id]
    async with lock:
        try:
            return await coro_factory()
        except Exception as exc:
            logger.warning("Edge command failed edge=%s: %s", edge_id, exc)
            raise
