import json
import logging
import os
import time

import paho.mqtt.client as mqtt

from alert_store import persist_maintenance_alert
from model_utils import MODEL_PATH, ensure_model
from telemetry_handler import ALERT_TOPIC, process_telemetry

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s edge-analyzer %(message)s",
)
logger = logging.getLogger("edge-analyzer")

MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", 1883))
RAW_DATA_TOPIC = "harboursense/telemetry/crane/+/raw"
RECONNECT_DELAY_SECONDS = int(os.environ.get("MQTT_RECONNECT_DELAY_SECONDS", "5"))

malformed_payload_count = 0
anomaly_alert_count = 0
model = None


def load_model():
    global model
    if not os.path.exists(MODEL_PATH):
        logger.warning("Model file missing at %s; generating synthetic training artifact", MODEL_PATH)
    model = ensure_model(MODEL_PATH)
    logger.info("Anomaly model loaded from %s", MODEL_PATH)


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info("Connected to MQTT broker %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)
        client.subscribe(RAW_DATA_TOPIC)
        logger.info("Subscribed to %s", RAW_DATA_TOPIC)
        return
    logger.error("MQTT connect failed with reason code %s", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    logger.warning("Disconnected from MQTT broker (reason=%s); retrying in %ss", reason_code, RECONNECT_DELAY_SECONDS)


def on_message(client, userdata, msg):
    global malformed_payload_count, anomaly_alert_count

    try:
        data = json.loads(msg.payload)
    except json.JSONDecodeError as error:
        malformed_payload_count += 1
        logger.warning(
            "Malformed telemetry JSON topic=%s error=%s malformedCount=%s",
            msg.topic,
            error,
            malformed_payload_count,
        )
        return

    try:
        alert_payload, _ = process_telemetry(data, model)
    except (KeyError, ValueError, TypeError) as error:
        malformed_payload_count += 1
        logger.warning(
            "Invalid telemetry payload topic=%s error=%s malformedCount=%s",
            msg.topic,
            error,
            malformed_payload_count,
        )
        return

    if alert_payload:
        anomaly_alert_count += 1
        logger.warning(
            "Anomaly detected craneId=%s topic=%s alertCount=%s",
            alert_payload.get("assetId", "unknown_crane"),
            msg.topic,
            anomaly_alert_count,
        )
        client.publish(ALERT_TOPIC, json.dumps(alert_payload), qos=1)
        persist_maintenance_alert(alert_payload)


def main():
    load_model()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    while True:
        try:
            logger.info("Connecting to MQTT broker at %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
            client.loop_forever()
        except Exception as error:
            logger.error("MQTT connection error: %s; retrying in %ss", error, RECONNECT_DELAY_SECONDS)
            time.sleep(RECONNECT_DELAY_SECONDS)


if __name__ == "__main__":
    main()
