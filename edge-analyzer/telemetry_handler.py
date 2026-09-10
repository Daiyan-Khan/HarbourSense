import datetime
import json

from model_utils import analyze_telemetry

ALERT_TOPIC = "harboursense/alerts/maintenance"


def process_telemetry(payload, model, *, analysis=None):
    """
    Validate crane telemetry, run anomaly detection, and return an alert payload when anomalous.

    Returns:
        (alert_payload, None) on anomaly
        (None, None) on normal inlier
        (None, error_kind) on malformed input where error_kind is 'json' or 'payload'
    """
    analysis = analysis if analysis is not None else analyze_telemetry(payload, model)

    if analysis["anomalous"]:
        crane_id = payload.get("craneId", "unknown_crane")
        alert_payload = {
            "assetId": crane_id,
            "alertType": "PREDICTIVE_MAINTENANCE_REQUIRED",
            "reason": "Unusual crane telemetry detected by a model trained on synthetic healthy data.",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
            "telemetry": payload,
            "analysis": analysis,
        }
        for key in ("runId", "scenarioId", "sequence", "simTime", "simulatedTime", "simulatedTimeMs", "wallTime"):
            if key in payload:
                alert_payload[key] = payload[key]
        if payload.get("eventId"):
            alert_payload["eventId"] = f"{payload['eventId']}-alert"
        return alert_payload, None

    return None, None


def decode_telemetry_message(raw_payload, model):
    """Decode MQTT payload bytes and run process_telemetry when JSON is valid."""
    try:
        data = json.loads(raw_payload)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return None, "json"

    try:
        return process_telemetry(data, model)
    except (KeyError, ValueError, TypeError):
        return None, "payload"
