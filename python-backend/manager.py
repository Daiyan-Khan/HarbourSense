import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
import paho.mqtt.client as mqtt
from task_assigner import TaskAssigner  # Import your fixed task_assigner.py
from traffic_analyzer import analyze_traffic  # Import fixed analyze_traffic

# Logging setup
logger = logging.getLogger("HarbourSenseManager")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.pool").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
logging.getLogger("pymongo.server_selection").setLevel(logging.WARNING)
logging.getLogger("pymongo.cursor").setLevel(logging.WARNING)
logging.getLogger("pymongo.command").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("manager_log.txt", mode="a")
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Global loop
loop = None

class ShipmentManager:
    def __init__(self, db, task_assigner):
        self.db = db
        self.task_assigner = task_assigner

    async def process_shipment_steps(self, shipment):
        if shipment['status'] == 'waiting':
            task_details = {
                'task': 'transport',
                'startNode': shipment.get('arrivalNode', 'A1'),
                'destinationNode': shipment.get('destinationNode', 'B4')
            }
            assigned = await self.task_assigner.assign_task('truck', task_details)
            if assigned:
                await self.db.shipments.update_one({'_id': shipment['_id']}, {'$set': {'status': 'processing'}})
                logger.info(f"Processed shipment {shipment.get('id')}")

async def shipment_watcher(db, shipment_manager):
    while True:
        waiting_shipments = await db.shipments.find({"status": {"$in": ["waiting", "processing"]}}).to_list(None)
        for shipment in waiting_shipments:
            await shipment_manager.process_shipment_steps(shipment)
        await asyncio.sleep(5)

async def periodic_traffic_analysis(db):
    while True:
        await analyze_traffic(db)  # Now accepts db
        await asyncio.sleep(30)

async def setup():
    global loop
    loop = asyncio.get_running_loop()

    uri = "mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net"
    client = AsyncIOMotorClient(uri)
    db = client["port"]
    logger.info("Connected to MongoDB")

    mqtt_client = mqtt.Client(client_id="manager", protocol=mqtt.MQTTv311)
    mqtt_client.tls_set(
        ca_certs="../certs/AmazonRootCA1.pem",
        certfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt",
        keyfile="../certs/8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key",
    )
    mqtt_client.connect("a1dghi6and062t-ats.iot.us-east-1.amazonaws.com", 8883, 60)
    mqtt_client.loop_start()
    logger.info("MQTT connected")

    # Load graph with conversion
    from traffic_analyzer import convert_bson_numbers  # Reuse from traffic_analyzer
    raw_nodes = await db.graph.find().to_list(None)
    if not raw_nodes:
        logger.error("Graph missing or empty in DB")
        return
    processed_nodes = convert_bson_numbers(raw_nodes)
    graph = {}
    for node in processed_nodes:
        node_id = node.get('id')
        if node_id:
            graph[node_id] = node
        else:
            logger.warning(f"Skipping invalid node without 'id': {node}")
    logger.info(f"Loaded graph with {len(graph)} nodes")

    task_assigner = TaskAssigner(db, mqtt_client, graph)
    shipment_manager = ShipmentManager(db, task_assigner)

    loop.create_task(shipment_watcher(db, shipment_manager))
    loop.create_task(periodic_traffic_analysis(db))
    loop.create_task(task_assigner.monitor_and_assign())  # Assuming this method exists in TaskAssigner

    logger.info("Setup complete, monitoring...")

    # Keep running
    await asyncio.Event().wait()  # Or use asyncio.gather if you have tasks to await

if __name__ == "__main__":
    asyncio.run(setup())
