# main.py - HarbourSense Python Backend

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

# MongoDB connection (use your URI; store securely in env vars for production)
MONGO_DETAILS = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client.port  # 'port' database

# Health check endpoint
@app.get("/")
async def read_root():
    return {"message": "HarbourSense Python Backend is running"}

# Endpoint to pull and return all sensor data
@app.get("/sensors")
async def get_sensors():
    sensor_cursor = db.sensorData.find()
    sensors = []
    async for sensor in sensor_cursor:
        sensor["_id"] = str(sensor["_id"])  # Convert ObjectId to string for JSON
        sensors.append(sensor)
    return sensors

# Endpoint to pull and return all edges data
@app.get("/edges")
async def get_edges():
    edges_cursor = db.edges.find()
    edges = []
    async for edge in edges_cursor:
        edge["_id"] = str(edge["_id"])  # Convert ObjectId to string for JSON
        edges.append(edge)
    return edges
