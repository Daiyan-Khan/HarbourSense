/**
 * Pure MQTT topic/payload helpers aligned with shared-contract.md (Phase 2/4).
 */

const REQUIRED_COMPLETION_FIELDS = ['id', 'shipmentId', 'phase', 'status', 'completedAt'];
const REQUIRED_TASK_FIELDS = ['shipmentId', 'phase', 'task', 'startNode', 'finalNode'];
const REQUIRED_SHIPMENT_FIELDS = ['id', 'status', 'arrivalNode'];
const REQUIRED_SENSOR_FIELDS = ['id', 'type', 'node', 'reading', 'timestamp'];
const REQUIRED_MAINTENANCE_ALERT_FIELDS = ['assetId', 'alertType', 'reason', 'timestamp'];
const REQUIRED_CRANE_TELEMETRY_FIELDS = ['motorTemp', 'vibration', 'energyUse'];

function edgeTopic(edgeId, suffix) {
  if (!edgeId || typeof edgeId !== 'string') {
    throw new Error('edgeId must be a non-empty string');
  }
  return `harboursense/edge/${edgeId}/${suffix}`;
}

function completionTopic(edgeId) {
  return edgeTopic(edgeId, 'completion');
}

function taskTopic(edgeId) {
  return edgeTopic(edgeId, 'task');
}

function routeTopic(edgeId) {
  return edgeTopic(edgeId, 'route');
}

function progressTopic(edgeId) {
  return edgeTopic(edgeId, 'progress');
}

function trafficUpdateTopic(edgeId) {
  return `harboursense/traffic/update/${edgeId}`;
}

function shipmentTopic(shipmentId) {
  return `harboursense/shipments/${shipmentId}`;
}

function sensorDataTopic() {
  return 'harboursense/sensor/data';
}

function maintenanceAlertTopic() {
  return 'harboursense/alerts/maintenance';
}

function craneTelemetryTopic(craneId) {
  return `harboursense/telemetry/crane/${craneId}/raw`;
}

function validateRequiredFields(payload, fields) {
  const missing = fields.filter((field) => payload[field] === undefined || payload[field] === null);
  return { valid: missing.length === 0, missing };
}

function validateCompletionPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { valid: false, missing: REQUIRED_COMPLETION_FIELDS };
  }
  return validateRequiredFields(payload, REQUIRED_COMPLETION_FIELDS);
}

function validateTaskPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { valid: false, missing: REQUIRED_TASK_FIELDS };
  }
  return validateRequiredFields(payload, REQUIRED_TASK_FIELDS);
}

function validateShipmentPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { valid: false, missing: REQUIRED_SHIPMENT_FIELDS };
  }
  return validateRequiredFields(payload, REQUIRED_SHIPMENT_FIELDS);
}

function validateSensorDataPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { valid: false, missing: REQUIRED_SENSOR_FIELDS };
  }
  return validateRequiredFields(payload, REQUIRED_SENSOR_FIELDS);
}

function validateMaintenanceAlertPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { valid: false, missing: REQUIRED_MAINTENANCE_ALERT_FIELDS };
  }
  return validateRequiredFields(payload, REQUIRED_MAINTENANCE_ALERT_FIELDS);
}

function validateCraneTelemetryPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return { valid: false, missing: REQUIRED_CRANE_TELEMETRY_FIELDS };
  }
  return validateRequiredFields(payload, REQUIRED_CRANE_TELEMETRY_FIELDS);
}

function isCanonicalCompletionTopic(topic) {
  return /^harboursense\/edge\/[^/]+\/completion$/.test(topic);
}

function isDeprecatedCompletionTopic(topic) {
  return /^harboursense\/edge\/completion\/[^/]+$/.test(topic);
}

module.exports = {
  REQUIRED_COMPLETION_FIELDS,
  REQUIRED_TASK_FIELDS,
  REQUIRED_SHIPMENT_FIELDS,
  REQUIRED_SENSOR_FIELDS,
  REQUIRED_MAINTENANCE_ALERT_FIELDS,
  REQUIRED_CRANE_TELEMETRY_FIELDS,
  completionTopic,
  taskTopic,
  routeTopic,
  progressTopic,
  trafficUpdateTopic,
  shipmentTopic,
  sensorDataTopic,
  maintenanceAlertTopic,
  craneTelemetryTopic,
  validateCompletionPayload,
  validateTaskPayload,
  validateShipmentPayload,
  validateSensorDataPayload,
  validateMaintenanceAlertPayload,
  validateCraneTelemetryPayload,
  isCanonicalCompletionTopic,
  isDeprecatedCompletionTopic,
};
