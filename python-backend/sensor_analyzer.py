from motor.motor_asyncio import AsyncIOMotorClient
from sklearn.ensemble import IsolationForest
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class SensorAnalyzer:
    def __init__(self, db):
        """
        Initialize with MongoDB database.
        No separate client needed—reuse manager's db.
        """
        self.db = db
        self.sensor_data_col = db.sensorData
        self.alerts_col = db.sensorAlerts  # Renamed from sensorAlerts for consistency

    async def get_recent_data(self, sensor_type: str = 'all', window_mins: int = 30, node: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch recent sensor readings for analysis (batched for ML).
        Filters by type/node if specified.
        """
        query = {}
        if sensor_type != 'all':
            query['type'] = sensor_type
        if node:
            query['node'] = node
        start_time = datetime.now() - timedelta(minutes=window_mins)
        query['timestamp'] = {'$gt': start_time}

        cursor = self.sensor_data_col.find(query).sort('timestamp', -1).limit(100)  # Cap for efficiency
        data = []
        docs = []
        async for doc in cursor:
            reading = float(doc.get('reading', 0))
            data.append([reading])
            docs.append(doc)
        return docs, data if data else []

    async def detect_anomaly(self, incoming_reading: Dict[str, Any], sensor_type: str = 'all', window_mins: int = 30) -> Optional[Dict[str, Any]]:
        """
        Detect anomaly in incoming_reading (from MQTT/DB).
        Uses IsolationForest on recent batch + rule-based thresholds.
        Returns alert dict if anomalous, else None. Inserts to DB.
        """
        node = incoming_reading.get('node', 'unknown')
        sensor_id = incoming_reading.get('id', 'unknown')
        reading = float(incoming_reading.get('reading', 0))
        reading_type = incoming_reading.get('type', sensor_type)
        timestamp = incoming_reading.get('timestamp', datetime.now())

        # Rule-based quick checks (real-time, for env/spikes)
        alert_type = None
        severity = 'low'
        suggestion = 'monitor'

        if reading_type == 'vibration':
            if reading > 5:  # Threshold from your sensor.js
                alert_type = 'vibration_spike'
                severity = 'high' if reading > 10 else 'medium'
                suggestion = 'repair'  # Core: trigger maintenance
            elif reading > 3:
                alert_type = 'vibration_elevated'
                severity = 'low'
                suggestion = 'monitor'
        elif reading_type == 'temperature':
            if reading > 40:  # Spike threshold
                alert_type = 'temp_spike'
                severity = 'high' if reading > 50 else 'medium'
                suggestion = 'repair'  # Equipment heat
        elif reading_type == 'humidity':
            if reading > 80:
                alert_type = 'humidity_high'
                severity = 'high' if reading > 90 else 'medium'
                suggestion = 'reroute'  # Avoid for cranes/trucks
        elif reading_type == 'wind':  # Assuming wind in sensor.js (add if needed)
            if reading > 20:  # km/h threshold
                alert_type = 'wind_high'
                severity = 'high'
                suggestion = 'reroute'  # For ships/cranes
        elif reading_type == 'occupancy':
            if reading > 70:
                alert_type = 'occupancy_high'
                severity = 'medium'
                suggestion = 'reroute'  # Congestion

        # ML-based: IsolationForest on recent data (batch for context)
        docs, readings_array = await self.get_recent_data(reading_type, window_mins, node)
        if len(readings_array) > 1:  # Need >1 for fit_predict
            model = IsolationForest(contamination=0.1, random_state=42)
            labels = model.fit_predict(np.array(readings_array))
            # Check incoming against model (append and predict)
            full_data = readings_array + [[reading]]
            full_labels = model.fit_predict(np.array(full_data))
            incoming_label = full_labels[-1]  # Last is incoming
            if incoming_label < 0:  # Anomaly
                if not alert_type:  # Only if no rule-based
                    alert_type = 'ml_anomaly'
                    severity = 'medium'
                    suggestion = 'monitor' if reading_type in ['motion', 'occupancy'] else 'repair'

        if alert_type:
            alert_doc = {
                'id': sensor_id,
                'node': node,
                'type': reading_type,
                'reading': reading,
                'alert_type': alert_type,
                'severity': severity,
                'suggestion': suggestion,
                'timestamp': timestamp,
                'resolved': False
            }
            await self.alerts_col.insert_one(alert_doc)
            logger.info(f"Alert inserted for {node}: {alert_type} (severity: {severity}, suggestion: {suggestion})")
            return alert_doc
        else:
            logger.debug(f"No anomaly for {node}: {reading} ({reading_type})")
            return None

    async def resolve_alert(self, alert_id: str):
        """
        Mark alert as resolved (call after repair completion).
        """
        await self.alerts_col.update_one({'_id': alert_id}, {'$set': {'resolved': True}})
        logger.info(f"Resolved alert {alert_id}")

    async def get_unresolved_alerts(self, node: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get unresolved alerts for traffic boosts (used in analyze_metrics).
        """
        query = {'resolved': False}
        if node:
            query['node'] = node
        return [doc async for doc in self.alerts_col.find(query)]
