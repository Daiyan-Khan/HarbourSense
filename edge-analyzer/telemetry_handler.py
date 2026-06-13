import datetime
import json

from model_utils import extract_features

ALERT_TOPIC = "harboursense/alerts/maintenance"


def process_telemetry(payload, model):
    """
    Validate crane telemetry, run anomaly detection, and return an alert payload when anomalous.

    Returns:
        (alert_payload, None) on anomaly
        (None, None) on normal inlier
        (None, error_kind) on malformed input where error_kind is 'json' or 'payload'
    """
    features = extract_features(payload)
    prediction = model.predict(features)

    if prediction[0] == -1:
        crane_id = payload.get("craneId", "unknown_crane")
        alert_payload = {
            "assetId": crane_id,
            "alertType": "PREDICTIVE_MAINTENANCE_REQUIRED",
            "reason": "Anomalous motor telemetry detected by EdgeAnalyzer.",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "telemetry": payload,
        }
        return alert_payload, None

    return None, None


def decode_telemetry_message(raw_payload, model):
    """Decode MQTT payload bytes and run process_telemetry when JSON is valid."""
    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None, "json"

    try:
        return process_telemetry(data, model)
    except (KeyError, ValueError, TypeError):
        return None, "payload"
