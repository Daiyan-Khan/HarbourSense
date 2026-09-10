"""Run-scoped analyzer for the isolated local demonstration.

The normal paho worker remains available when DEMO_MODE is not enabled. Demo
messages carry run identity and retry-stable event IDs supplied by the producer.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import aiomqtt

from backend_config import create_mongo_client, get_mongo_settings
from demo_runtime import DemoMqttClient, accept_demo_event, mark_demo_event, run_worker, validate_demo_settings
from model_utils import analyze_telemetry
from telemetry_handler import ALERT_TOPIC, process_telemetry

logger = logging.getLogger("edge-analyzer")
RAW_DATA_TOPIC = "harboursense/telemetry/crane/+/raw"


async def handle_demo_telemetry(payload, model, context, mqtt_client):
    # Reject old-run or malformed identities before scoring or touching run data.
    if not await accept_demo_event(payload):
        return False
    analysis = analyze_telemetry(payload, model)
    alert, _ = process_telemetry(payload, model, analysis=analysis)
    await context.db.craneTelemetry.update_one(
        {"_id": payload["eventId"]},
        {"$setOnInsert": {**payload, "analysis": analysis, "timestamp": datetime.now(timezone.utc)}},
        upsert=True,
    )
    analyzed = {"craneId": payload.get("craneId", "unknown_crane"), "analysis": analysis,
                "telemetry": payload, "eventId": f"{payload['eventId']}-analyzed"}
    await mqtt_client.publish(f"harboursense/telemetry/crane/{analyzed['craneId']}/analyzed", json.dumps(analyzed))
    if alert:
        await context.db.maintenanceAlerts.update_one(
            {"_id": alert["eventId"]},
            {"$setOnInsert": {**alert, "resolved": False, "source": "edge-analyzer"}},
            upsert=True,
        )
        await mqtt_client.publish(ALERT_TOPIC, json.dumps(alert))
    await mark_demo_event(payload)
    return True


async def run_demo_analyzer(model):
    settings = get_mongo_settings()
    validate_demo_settings(settings)
    mongo = create_mongo_client(settings)
    base = mongo[settings.database_name]

    async def worker(context):
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=os.environ.get("MQTT_BROKER_HOST", "localhost"),
                    port=int(os.environ.get("MQTT_BROKER_PORT", "1883")),
                    identifier=f"harboursense-demo-analyzer-{context.run_id}",
                    clean_session=False,
                    keepalive=10,
                ) as client:
                    await client.subscribe(RAW_DATA_TOPIC, qos=1)
                    mqtt_client = DemoMqttClient(client, context)
                    await context.heartbeat("ready", mqttConnected=True)
                    async for message in client.messages:
                        try:
                            payload = json.loads(message.payload)
                            await handle_demo_telemetry(payload, model, context, mqtt_client)
                        except (json.JSONDecodeError, UnicodeDecodeError, KeyError, ValueError, TypeError) as error:
                            logger.warning("Invalid demo telemetry: %s", type(error).__name__)
            except aiomqtt.MqttError as error:
                await context.heartbeat("reconnecting", mqttConnected=False)
                logger.warning("Demo analyzer MQTT reconnect: %s", type(error).__name__)
                await asyncio.sleep(1)

    try:
        await run_worker("analyzer", base, worker)
    finally:
        mongo.close()
