import logging
import os

logger = logging.getLogger("edge-analyzer")

MONGO_URI = os.environ.get("MONGO_URI", "").strip()
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "port")
COLLECTION_NAME = "maintenanceAlerts"


def persist_maintenance_alert(alert_payload):
    """Optionally persist maintenance alerts when MONGO_URI is configured."""
    if not MONGO_URI:
        return False

    try:
        from pymongo import MongoClient
    except ImportError:
        logger.warning("pymongo unavailable; maintenance alert not persisted")
        return False

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        collection = client[MONGO_DB_NAME][COLLECTION_NAME]
        document = {
            **alert_payload,
            "resolved": False,
            "source": "edge-analyzer",
        }
        collection.insert_one(document)
        client.close()
        return True
    except Exception as error:
        logger.warning("Failed to persist maintenance alert: %s", error.__class__.__name__)
        return False
