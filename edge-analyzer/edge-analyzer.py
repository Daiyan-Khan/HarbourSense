# edge-analyzer/edge-analyzer.py

import os
import json
import joblib
import numpy as np
import paho.mqtt.client as mqtt
import datetime

# --- Configuration ---
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")

MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", 1883))
RAW_DATA_TOPIC = "harboursense/telemetry/crane/+/raw"
ALERT_TOPIC = "harboursense/alerts/maintenance"
MODEL_PATH = 'anomaly_model.pkl'

# --- Load the pre-trained Isolation Forest model ---
try:
    model = joblib.load(MODEL_PATH)
    print(f"Successfully loaded anomaly detection model from {MODEL_PATH}")
except FileNotFoundError:
    print(f"FATAL: Model file not found at {MODEL_PATH}. Ensure the model is in the same directory.")
    exit()

# --- MQTT Event Handlers ---
def on_connect(client, userdata, flags, rc):
    """Callback for when the client connects to the broker."""
    if rc == 0:
        print("EdgeAnalyzer connected successfully to MQTT Broker!")
        client.subscribe(RAW_DATA_TOPIC)
        print(f"Subscribed to raw data topic: {RAW_DATA_TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}\n")

def on_message(client, userdata, msg):
    """Callback for when a message is received."""
    try:
        data = json.loads(msg.payload)
        
        # Ensure message contains the necessary features
        if all(k in data for k in ['motorTemp', 'vibration', 'energyUse']):
            features = np.array([[data['motorTemp'], data['vibration'], data['energyUse']]])
            prediction = model.predict(features)

            # If the model predicts an anomaly (value of -1)
            if prediction[0] == -1:
                crane_id = data.get('craneId', 'unknown_crane')
                print(f"!!! Anomaly Detected for {crane_id} !!! Publishing alert.")
                
                alert_payload = {
                    "assetId": crane_id,
                    "alertType": "PREDICTIVE_MAINTENANCE_REQUIRED",
                    "reason": "Anomalous motor telemetry detected by EdgeAnalyzer.",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z", # ISO 8601 format
                    "telemetry": data
                }
                
                # Publish the structured alert to the cloud-facing topic
                client.publish(ALERT_TOPIC, json.dumps(alert_payload), qos=1)
                
    except (json.JSONDecodeError, KeyError) as e:
        # Silently ignore malformed messages
        pass

# --- Main Execution Block ---
if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Connecting to MQTT broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}...")
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)

    # Blocking call that processes network traffic, dispatches callbacks, and handles reconnecting.
    client.loop_forever()
