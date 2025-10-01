import paho.mqtt.client as mqtt
import json
from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB setup
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
mongo_client = AsyncIOMotorClient(MONGO_DETAILS)
db = mongo_client.port
data_col = db.sensorData

# MQTT setup
client = mqtt.Client(client_id="python_subscriber")  # Deprecation warning: pip install paho-mqtt --upgrade to fix

client.tls_set(
    ca_certs='../certs/AmazonRootCA1.pem',
    certfile='../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt',
    keyfile='../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key'
)

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

client.on_connect = on_connect
client.on_message = on_message

# Use your REAL AWS IoT endpoint here (from AWS Console)
try:
    client.connect("a1dghi6and062t-ats.iot.us-east-1.amazonaws.com", 8883, 60)  # Replace with your actual endpoint
except Exception as e:
    print(f"Connection error: {e}")

client.loop_forever()
