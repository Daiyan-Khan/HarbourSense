import paho.mqtt.client as mqtt
import json
from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB setup
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
mongo_client = AsyncIOMotorClient(MONGO_DETAILS)
db = mongo_client.port
data_col = db.sensorData

# MQTT setup (use AWS IoT endpoint and certs)
client = mqtt.Client(client_id="python_subscriber")
client.tls_set(ca_certs='./certs/AmazonRootCA1.pem',
               certfile='./certs/certificate.pem.crt',
               keyfile='./certs/private.pem.key')

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT")
    client.subscribe("harboursense/sensor/data")

async def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    print(f"Received: {payload}")
    await data_col.insert_one(payload)  # Forward to MongoDB

client.on_connect = on_connect
client.on_message = on_message

client.connect("your-aws-iot-endpoint.iot.us-east-1.amazonaws.com", 8883, 60)
client.loop_forever()
