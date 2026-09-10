"""Control and record the real, explicitly isolated local demonstration."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

from demo_runtime import (CONTROL_COLLECTION, WORKER_COLLECTION, DemoContext,
                          RunDatabase, logical_ms, run_prefix, validate_demo_settings)
from edge_view import load_merged_edges, split_edge_document
from port_state import build_port_state_snapshot

SPEC_DIR = Path(os.environ.get("DEMO_SPEC_DIR", Path(__file__).resolve().parents[1] / "demo"))
ROLES = ("manager", "portsim", "sensors", "analyzer")


class DemoCommandError(RuntimeError):
    def __init__(self, code, message, status=409):
        super().__init__(message)
        self.code, self.status = code, status


def clean(value):
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items() if key != "_id"}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat() + ("Z" if value.tzinfo is None else "")
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    return str(value)


class DemoService:
    def __init__(self, base, settings):
        validate_demo_settings(settings)
        self.base = base
        self.specs = json.loads((SPEC_DIR / "scenarios.json").read_text(encoding="utf-8"))["scenarios"]
        self.lock = asyncio.Lock()
        self.sampler_task = None
        self.last_snapshot = {}
        root = Path(__file__).resolve().parents[1]
        paths = list((root / 'python-backend').glob('*.py')) + list((root / 'port-sim' / 'lib').glob('*.js')) + list(SPEC_DIR.glob('*.json'))
        paths += [root / 'edge-analyzer' / name for name in ('demo_analyzer.py', 'model_utils.py', 'telemetry_handler.py')]
        self.source_hashes = {str(path.relative_to(root)).replace('\\', '/'): hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                              for path in paths if path.is_file() and path.is_relative_to(root)}
        self.source_hashes.update({f"demo/{p.name}": hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest() for p in SPEC_DIR.glob("*.json")})

    def scenario(self, scenario_id):
        found = next((item for item in self.specs if item["id"] == scenario_id), None)
        if not found:
            raise DemoCommandError("unknown_scenario", "Choose normal, congestion, or crane-fault", 422)
        return found

    async def initialize(self):
        state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
        if not state:
            await self.new_run("normal")
        await self.base[WORKER_COLLECTION].create_index("updatedAt", expireAfterSeconds=86400)
        await self.base["_demoCommands"].create_index("createdAt", expireAfterSeconds=86400)
        # The API is the sole controller/recorder; restart preserves the active run.
        self.sampler_task = asyncio.create_task(self.sample_loop())

    async def close(self):
        if self.sampler_task:
            self.sampler_task.cancel()
            await asyncio.gather(self.sampler_task, return_exceptions=True)

    async def new_run(self, scenario_id):
        spec = self.scenario(scenario_id)
        run_id = uuid.uuid4().hex
        fixture = json.loads((SPEC_DIR / "fixtures.json").read_text(encoding="utf-8"))
        context = DemoContext(self.base, run_id)
        db = RunDatabase(self.base, context, guarded=False)
        graph = fixture["graph"]
        edges = fixture["edges"]
        assignments, runtime = zip(*(split_edge_document(edge) for edge in edges))
        await db.graph.insert_many(graph)
        await db.edgeRuntime.insert_many(list(runtime))
        await db.edgeAssignments.insert_many(list(assignments))
        nodes = {node["id"] for node in graph}
        sensors = list({s["id"]: s for s in fixture["sensors"] if s["node"] in nodes}.values())
        if sensors:
            await db.sensorList.insert_many(sensors)
        for collection in ("graph", "edgeRuntime", "edgeAssignments", "shipments", "sensorList"):
            await db[collection].create_index("id", unique=True)
        for collection in ("sensorData", "craneTelemetry", "trafficData"):
            await db[collection].create_index("timestamp", expireAfterSeconds=86400)
        now = time.time() * 1000
        state = {"_id": "active", "schemaVersion": 1, "runId": run_id,
                 "scenarioId": scenario_id, "status": "idle", "speed": spec["defaultSpeed"],
                 "sequence": 0, "clock": {"baseMs": 0, "anchorWallMs": now},
                 "startedWallMs": now, "createdAt": datetime.utcnow(), "updatedAt": datetime.utcnow(),
                 "activeOperations": 0, "timeline": [], "error": None}
        await self.base[CONTROL_COLLECTION].replace_one({"_id": "active"}, state, upsert=True)
        await self.base["_demoRuns"].update_one({"_id": run_id}, {"$set": {"createdAt": datetime.utcnow(), "scenarioId": scenario_id, "sourceSha256": self.source_hashes}}, upsert=True)
        self.last_snapshot = {}
        await self.record_snapshot(state)
        return state

    async def database(self):
        state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
        if not state:
            raise DemoCommandError("not_initialized", "Demo is initializing", 503)
        return RunDatabase(self.base, DemoContext(self.base, state["runId"]), guarded=False)

    async def state(self):
        state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
        if not state:
            raise DemoCommandError("not_initialized", "Demo is initializing", 503)
        result = clean({key: value for key, value in state.items() if key not in {"clock", "activeOperations", "startedWallMs"}})
        result.update(enabled=True, mode="demo", simTimeMs=round(logical_ms(state)), scenarios=[
            {key: spec[key] for key in ("id", "title", "description")} for spec in self.specs])
        workers = {item.get("role", item["_id"]): item async for item in self.base[WORKER_COLLECTION].find({"runId": state["runId"]})}
        result["services"] = {}
        for role in ROLES:
            worker = workers.get(role, {})
            age = (datetime.utcnow() - worker["updatedAt"]).total_seconds() if isinstance(worker.get("updatedAt"), datetime) else None
            fresh = age is not None and age < 4 and worker.get("runId") == state["runId"]
            result["services"][role] = {"status": worker.get("status", "starting") if fresh else "stale",
                "ready": fresh and worker.get("status") == "ready", "ageMs": round(age * 1000) if age is not None else None,
                "updatedAt": clean(worker.get("updatedAt")), "runId": worker.get("runId"),
                "mqttConnected": bool(fresh and worker.get("mqttConnected")), "error": worker.get("error")}
        return result

    async def wait_quiescent(self, state, allow_fenced_reset=False):
        deadline = time.monotonic() + 10
        while True:
            current = await self.base[CONTROL_COLLECTION].find_one({"_id": "active", "runId": state["runId"]})
            if not current or current.get("activeOperations", 0) == 0:
                return
            if time.monotonic() > deadline:
                if allow_fenced_reset:
                    return  # Any orphaned operation still targets the old run collection prefix.
                raise DemoCommandError("workers_busy", "An operation could not be confirmed stopped. Reset this isolated demo to recover safely.", 503)
            await asyncio.sleep(.025)

    async def wait_workers_ready(self):
        deadline = time.monotonic() + 15
        while True:
            state = await self.state()
            if all(service['ready'] and service['mqttConnected'] for service in state['services'].values()):
                return
            if time.monotonic() > deadline:
                raise DemoCommandError('workers_unavailable', 'Wait for manager, simulator, sensors, and analyzer to connect before starting', 503)
            await asyncio.sleep(.1)

    async def command(self, action, payload):
        command_id = payload.get("commandId")
        if not isinstance(command_id, str) or not 8 <= len(command_id) <= 128:
            raise DemoCommandError("invalid_command", "A stable commandId of 8–128 characters is required", 422)
        fingerprint = hashlib.sha256(json.dumps([action, payload], sort_keys=True).encode()).hexdigest()
        async with self.lock:
            existing = await self.base["_demoCommands"].find_one({"_id": command_id})
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise DemoCommandError("command_conflict", "That commandId was already used for another command")
                return {**existing["response"], "replayed": True}
            state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
            status = state["status"]
            patch = {"updatedAt": datetime.utcnow(), "clock": {"baseMs": logical_ms(state), "anchorWallMs": time.time() * 1000}}
            if action == "start":
                if status != "idle":
                    raise DemoCommandError("invalid_transition", "Reset this run before starting again")
                scenario_id = payload.get("scenarioId", state["scenarioId"])
                if scenario_id != state["scenarioId"]:
                    state = await self.new_run(scenario_id)
                spec = self.scenario(scenario_id)
                await self.wait_workers_ready()
                patch.update(status="running", startedWallMs=time.time() * 1000,
                             clock={"baseMs": 0, "anchorWallMs": time.time() * 1000})
            elif action == "pause":
                if status not in {"running", "paused"}:
                    raise DemoCommandError("invalid_transition", "Only a running demo can pause")
                patch["status"] = "paused"
            elif action == "resume":
                if status not in {"paused", "running"}:
                    raise DemoCommandError("invalid_transition", "Only a paused demo can resume")
                patch["status"] = "running"
            elif action == "speed":
                speed = payload.get("speed")
                if isinstance(speed, bool) or speed not in {1, 2, 4}:
                    raise DemoCommandError("invalid_speed", "Supported speeds are 1, 2 and 4", 422)
                if status not in {"idle", "running", "paused"}:
                    raise DemoCommandError("invalid_transition", "Reset the completed demo to change speed")
                patch["speed"] = speed
            elif action == "reset":
                self.scenario(payload.get("scenarioId", state["scenarioId"]))
                patch["status"] = "resetting"
            else:
                raise DemoCommandError("unknown_command", "Unsupported demo command", 404)
            await self.base[CONTROL_COLLECTION].update_one({"_id": "active", "runId": state["runId"]}, {"$set": patch, "$inc": {"sequence": 1}})
            if action in {"pause", "reset"}:
                try:
                    await self.wait_quiescent(state, allow_fenced_reset=action == "reset")
                except DemoCommandError as exc:
                    await self.base[CONTROL_COLLECTION].update_one({'_id': 'active', 'runId': state['runId']},
                        {'$set': {'status': 'failed', 'error': {'code': exc.code, 'message': str(exc)}}})
                    raise
            if action == "reset":
                await self.new_run(payload.get("scenarioId", state["scenarioId"]))
                await self.cleanup_old_runs()
            response = {"commandId": command_id, "acknowledged": True, "replayed": False, "state": await self.state()}
            await self.base["_demoCommands"].insert_one({"_id": command_id, "fingerprint": fingerprint, "response": response, "createdAt": datetime.utcnow()})
            return response

    async def cleanup_old_runs(self):
        # Explicit reset keeps three runs, including their actual recordings.
        runs = await self.base["_demoRuns"].find({}).sort("createdAt", -1).to_list(None)
        names = await self.base.list_collection_names()
        for run in runs[3:]:
            prefix = run_prefix(run["_id"])
            for name in names:
                if name.startswith(prefix):
                    await self.base.drop_collection(name)
            await self.base[WORKER_COLLECTION].delete_many({"runId": run["_id"]})
            await self.base["_demoRuns"].delete_one({"_id": run["_id"]})

    async def snapshot(self, state):
        db = RunDatabase(self.base, DemoContext(self.base, state["runId"]), guarded=False)
        graph = {node["id"]: clean(node) async for node in db.graph.find({})}
        return clean({"graph": {"nodes": graph}, "edges": await load_merged_edges(db),
            "sensors": await db.sensorData.find({}).sort("timestamp", -1).limit(100).to_list(None),
            "shipments": await db.shipments.find({}).to_list(None),
            "sensorAlerts": await db.sensorAlerts.find({"resolved": False}).to_list(None),
            "maintenanceAlerts": await db.maintenanceAlerts.find({"resolved": False}).to_list(None),
            "maintenanceHistory": await db.maintenanceAlerts.find({}).to_list(None),
            "maintenanceTasks": await db.maintenanceTasks.find({}).to_list(None),
            "craneTelemetry": await db.craneTelemetry.find({}).sort("timestamp", -1).limit(100).to_list(None),
            "portState": await build_port_state_snapshot(db)})

    async def record_snapshot(self, state):
        snapshot = await self.snapshot(state)
        at_ms = round(logical_ms(state))
        previous = self.last_snapshot.get(state["runId"], {})
        old_shipments = {s["id"]: s for s in previous.get("shipments", [])}
        events = []
        for shipment in snapshot["shipments"]:
            if shipment.get("status") != old_shipments.get(shipment["id"], {}).get("status"):
                events.append({"id": uuid.uuid4().hex, "atMs": at_ms, "type": "shipment",
                    "message": f"{shipment['id']}: {shipment.get('status', 'unknown')}", "shipmentId": shipment["id"]})
        if len(snapshot["maintenanceAlerts"]) > len(previous.get("maintenanceAlerts", [])):
            events.append({"id": uuid.uuid4().hex, "atMs": at_ms, "type": "maintenance", "message": "The analyzer raised a maintenance alert from crane telemetry."})
        old_tasks = {task["id"]: task for task in previous.get("maintenanceTasks", [])}
        for task in snapshot.get("maintenanceTasks", []):
            if task.get("status") != old_tasks.get(task["id"], {}).get("status"):
                events.append({"id": uuid.uuid4().hex, "atMs": at_ms, "type": "maintenance", "deviceId": task.get("edgeId"),
                    "message": f"Maintenance at {task.get('node')}: {task.get('status')}"})
        old_edges = {edge["id"]: edge for edge in previous.get("edges", [])}
        for edge in snapshot["edges"]:
            old = old_edges.get(edge["id"], {})
            if edge.get("routeRevision", 0) > old.get("routeRevision", 0) and edge.get("pendingPath"):
                events.append({"id": uuid.uuid4().hex, "atMs": at_ms, "type": "route", "deviceId": edge["id"],
                    "message": f"{edge['id']} received route revision {edge['routeRevision']}: {' → '.join(edge['pendingPath'])}"})
        self.last_snapshot[state["runId"]] = snapshot
        await self.base[run_prefix(state["runId"]) + "recordingFrames"].insert_one(
            {"atMs": at_ms, "wallTime": datetime.utcnow(), "snapshot": snapshot, "events": events})
        if events:
            await self.base[CONTROL_COLLECTION].update_one({"_id": "active", "runId": state["runId"]},
                {"$push": {"timeline": {"$each": events, "$slice": -100}}})
        return snapshot

    async def sample_loop(self):
        while True:
            try:
                async with self.lock:
                    state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
                    if state and state["status"] == "running":
                        snapshot = await self.record_snapshot(state)
                        spec = self.scenario(state["scenarioId"])
                        complete = len(snapshot["shipments"]) == len(spec["shipments"]) and all(s.get("status") == "delivered" for s in snapshot["shipments"])
                        complete = complete and all(edge.get('taskPhase') == 'idle' and not edge.get('shipmentId') and not edge.get('assignedShipment') for edge in snapshot['edges'])
                        if state["scenarioId"] == "crane-fault":
                            complete = complete and bool(snapshot["maintenanceHistory"]) and any(task.get("status") == "completed" for task in snapshot["maintenanceTasks"])
                        timeout = logical_ms(state) > spec["timeoutMs"]
                        if complete or timeout:
                            patch = {"status": "complete" if complete else "failed", "updatedAt": datetime.utcnow(),
                                "clock": {"baseMs": logical_ms(state), "anchorWallMs": time.time() * 1000}}
                            if timeout and not complete:
                                patch["error"] = {"code": "scenario_timeout", "message": "The recorded pipeline did not reach its terminal state in the scenario time budget."}
                            await self.base[CONTROL_COLLECTION].update_one({"_id": "active", "runId": state["runId"], "status": "running"}, {"$set": patch})
                            await self.base["_demoRuns"].update_one({"_id": state["runId"]}, {"$set": {"terminalState": patch["status"]}})
                            await self.wait_quiescent(state)
                            await self.record_snapshot({**state, **patch})
            except asyncio.CancelledError:
                raise
            except Exception:
                # A temporary database outage must not kill the recorder/controller.
                await asyncio.sleep(1)
            await asyncio.sleep(.5)

    async def recording(self, run_id=None):
        state = await self.base[CONTROL_COLLECTION].find_one({"_id": "active"})
        run_id = run_id or state["runId"]
        run = await self.base["_demoRuns"].find_one({"_id": run_id})
        if not run:
            raise DemoCommandError("recording_missing", "No retained recording exists for this run", 404)
        frames = await self.base[run_prefix(run_id) + "recordingFrames"].find({}).sort("atMs", 1).to_list(None)
        if not frames:
            raise DemoCommandError("recording_empty", "No observed frames have been captured", 409)
        spec = self.scenario(run["scenarioId"])
        return clean({"schemaVersion": 1, "scenario": {key: spec[key] for key in ("id", "title", "description")},
            "recordedAt": run["createdAt"], "durationMs": frames[-1]["atMs"], "initialSnapshot": frames[0]["snapshot"],
            "frames": [{"atMs": frame["atMs"], "snapshot": frame["snapshot"], "events": frame["events"]} for frame in frames],
            "provenance": {"generator": "HarbourSense real local MQTT/MongoDB pipeline", "runId": run_id,
                "scenarioInput": spec, "synthetic": True, "recorded": True, "sourceSha256": run.get("sourceSha256", {}),
                "clock": "frames.atMs is shared simulated time; telemetry timestamps use wall time",
                "terminalState": state["status"] if state["runId"] == run_id else run.get("terminalState", "archived")}})
