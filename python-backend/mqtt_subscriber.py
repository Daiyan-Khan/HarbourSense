import asyncio
import json
import paho.mqtt.client as mqtt
from backend_config import MongoConfigError, get_mongo_database, get_mongo_settings
from mqtt_config import MqttConfigError, configure_paho_client

# MongoDB setup
try:
    mongo_settings = get_mongo_settings()
    db = get_mongo_database(mongo_settings)
except MongoConfigError as exc:
    raise RuntimeError(f"Invalid MongoDB configuration for MQTT subscriber startup: {exc}") from None
data_col = db.sensorData

# MQTT setup
client = mqtt.Client(client_id="python_subscriber")
try:
    mqtt_settings = configure_paho_client(client)
except MqttConfigError as exc:
    raise RuntimeError(f"Invalid MQTT configuration for subscriber startup: {exc}") from None

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT")
        client.subscribe("harboursense/sensor/data")
    else:
        print(f"Connection failed with code {rc}")

async def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    print(f"Received: {payload}")
    await data_col.insert_one(payload)  # Forward to MongoDB
    # Optionally, trigger manager processing here if not using change streams
    # await process_new_data(payload)  # Define this function in manager.py

client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(mqtt_settings.host, mqtt_settings.port, 60)
except Exception as e:
    print(f"Connection error: {e}")

# Run MQTT loop in background
client.loop_start()

# Keep script running
asyncio.get_event_loop().run_forever()
