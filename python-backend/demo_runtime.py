"""Opt-in demo isolation, logical time, and MQTT run fencing.

The control document lives in the explicitly local demo database. Each run owns
its own collection prefix, so even a delayed operation cannot write a new run.
Wall-clock timestamps remain available for retention and connection health.
"""
from __future__ import annotations

import asyncio
import contextvars
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from urllib.parse import urlparse

CONTROL_COLLECTION = "_demoControl"
WORKER_COLLECTION = "_demoWorkers"
CURRENT = contextvars.ContextVar("harboursense_demo_context", default=None)
WRITE_METHODS = {"insert_one", "insert_many", "update_one", "update_many",
                 "replace_one", "delete_one", "delete_many", "find_one_and_update",
                 "find_one_and_replace", "find_one_and_delete", "bulk_write"}


class DemoRunEnded(RuntimeError):
    pass


def demo_enabled(env=None):
    return (os.environ if env is None else env).get("DEMO_MODE", "").lower() == "true"


def validate_demo_settings(settings, env=None):
    env = os.environ if env is None else env
    if not demo_enabled(env):
        raise ValueError("DEMO_MODE=true is required")
    uri = urlparse(settings.uri)
    name = settings.database_name
    if (uri.scheme != "mongodb" or uri.hostname not in {"localhost", "127.0.0.1", "::1", "mongo", "mongo-demo"}
            or uri.username or uri.password or "," in uri.netloc
            or not re.fullmatch(r"harboursense_demo(?:_test_[a-z0-9_]+)?", name)
            or uri.path.strip("/") not in {"", name}):
        raise ValueError("Demo requires an unauthenticated local MongoDB URI and an owned harboursense_demo database")
    if env.get("MQTT_MODE", "local") != "local" or env.get("MQTT_BROKER_HOST", "localhost") not in {"localhost", "127.0.0.1", "mqtt", "mqtt-demo"}:
        raise ValueError("Demo requires the local MQTT broker")


def logical_ms(state, wall_ms=None):
    clock = state.get("clock", {})
    value = float(clock.get("baseMs", 0))
    if state.get("status") == "running":
        value += max(0, (time.time() * 1000 if wall_ms is None else wall_ms) - clock.get("anchorWallMs", 0)) * state.get("speed", 1)
    return value


def run_prefix(run_id):
    if not re.fullmatch(r"[a-f0-9]{32}", run_id):
        raise ValueError("Invalid demo run identity")
    return f"run_{run_id}_"


class RunCollection:
    def __init__(self, raw, context):
        self.raw, self.context = raw, context

    def __getattr__(self, name):
        method = getattr(self.raw, name)
        if name not in WRITE_METHODS:
            return method

        async def write(*args, **kwargs):
            async with self.context.operation():
                return await method(*args, **kwargs)
        return write


class RunDatabase:
    def __init__(self, base, context, guarded=True):
        self.base, self.context, self.guarded = base, context, guarded
        self.name = base.name
        self.client = base.client

    def __getitem__(self, name):
        raw = self.base[run_prefix(self.context.run_id) + name]
        return RunCollection(raw, self.context) if self.guarded else raw

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self[name]

    async def command(self, *args, **kwargs):
        return await self.base.command(*args, **kwargs)

    async def list_collection_names(self):
        prefix = run_prefix(self.context.run_id)
        return [name[len(prefix):] for name in await self.base.list_collection_names() if name.startswith(prefix)]


class DemoContext:
    def __init__(self, base, run_id, role="api"):
        self.base, self.run_id, self.role = base, run_id, role
        self.state = {}
        self.ready = False
        self.mqtt_connected = False
        self.db = RunDatabase(base, self)

    async def refresh(self):
        state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
        if not state or state.get("runId") != self.run_id:
            raise DemoRunEnded("Demo run was replaced")
        self.state = state
        return state

    async def checkpoint(self):
        while True:
            state = await self.refresh()
            if state["status"] == "running":
                return state
            if state["status"] == "resetting":
                raise DemoRunEnded("Demo run stopped")
            await asyncio.sleep(.05)

    @asynccontextmanager
    async def operation(self):
        while True:
            await self.checkpoint()
            result = await self.base[CONTROL_COLLECTION].update_one(
                {"_id": "active", "runId": self.run_id, "status": "running"},
                {"$inc": {"activeOperations": 1}})
            if result.modified_count:
                break
        try:
            yield
        finally:
            await self.base[CONTROL_COLLECTION].update_one(
                {"_id": "active", "runId": self.run_id}, {"$inc": {"activeOperations": -1}})

    async def sleep(self, seconds):
        state = await self.checkpoint()
        target = logical_ms(state) + seconds * 1000
        while logical_ms(await self.checkpoint()) < target:
            await asyncio.sleep(.025)

    async def heartbeat(self, status="ready", **extra):
        self.ready = status == "ready"
        if "mqttConnected" in extra:
            self.mqtt_connected = extra["mqttConnected"]
        await self.base[WORKER_COLLECTION].update_one(
            {"_id": f"{self.run_id}:{self.role}"}, {"$set": {"role": self.role, "runId": self.run_id, "status": status,
              "updatedAt": datetime.utcnow(), "mqttConnected": self.mqtt_connected, **extra}}, upsert=True)


async def simulation_sleep(seconds):
    context = CURRENT.get()
    if context:
        await context.sleep(seconds)
    else:
        await asyncio.sleep(seconds)


def simulation_time():
    context = CURRENT.get()
    if context and context.state:
        return context.state.get("startedWallMs", 0) / 1000 + logical_ms(context.state) / 1000
    return time.time()


def simulation_datetime():
    return datetime.utcfromtimestamp(simulation_time()) if CURRENT.get() else datetime.now()


async def simulation_checkpoint():
    context = CURRENT.get()
    if context:
        await context.checkpoint()


class DemoMqttClient:
    def __init__(self, client, context):
        self.client, self.context = client, context

    def __getattr__(self, name):
        return getattr(self.client, name)

    async def subscribe(self, topic, *args, **kwargs):
        kwargs.setdefault("qos", 1)
        return await self.client.subscribe(topic, *args, **kwargs)

    async def publish(self, topic, payload, *args, **kwargs):
        data = json.loads(payload) if isinstance(payload, (str, bytes, bytearray)) else dict(payload)
        if data.get("runId", self.context.run_id) != self.context.run_id:
            raise DemoRunEnded("Refusing a previous-run publication")
        async with self.context.operation():
            state = await self.context.base[CONTROL_COLLECTION].find_one_and_update(
                {"_id": "active", "runId": self.context.run_id}, {"$inc": {"eventSequence": 1}}, return_document=True)
            data.update(sequence=state["eventSequence"], runId=self.context.run_id, scenarioId=state["scenarioId"],
                        eventId=data.get("eventId", uuid.uuid4().hex),
                        simulatedTimeMs=logical_ms(state), wallTime=datetime.utcnow().isoformat() + "Z")
            kwargs.setdefault("qos", 1)
            return await self.client.publish(topic, json.dumps(data, default=str), *args, **kwargs)


async def accept_demo_event(payload):
    context = CURRENT.get()
    if not context:
        return True
    if not isinstance(payload, dict) or payload.get("runId") != context.run_id or not payload.get("eventId"):
        return False
    await context.checkpoint()
    return not await context.db.demoInbox.find_one({"_id": payload["eventId"]})


async def mark_demo_event(payload):
    context = CURRENT.get()
    if context and payload.get("eventId"):
        await context.db.demoInbox.update_one({"_id": payload["eventId"]},
            {"$set": {"processedAt": datetime.utcnow()}}, upsert=True)


async def run_worker(role, base, callback):
    """Rebind a whole worker after reset; cancellation releases old operations."""
    while True:
        task = None
        context = None
        try:
            state = await base[CONTROL_COLLECTION].find_one({"_id": "active"})
            if not state:
                await asyncio.sleep(.25)
                continue
            context = DemoContext(base, state["runId"], role)
            context.state = state
            token = CURRENT.set(context)
            task = asyncio.create_task(callback(context))
            CURRENT.reset(token)
            while True:
                current = await base[CONTROL_COLLECTION].find_one({"_id": "active"})
                if not current or current["runId"] != context.run_id:
                    break
                if task.done():
                    error = task.exception()
                    if error and not isinstance(error, DemoRunEnded):
                        raise error
                    if current["status"] not in {"complete", "resetting", "failed"}:
                        break
                await context.heartbeat(("ready" if context.ready else "starting") if not task.done() else "stopped")
                await asyncio.sleep(.5)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if context:
                try:
                    await context.heartbeat('starting', mqttConnected=False, error=type(exc).__name__)
                except Exception:
                    pass
            await asyncio.sleep(1)
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
