import React from 'react';

const panelStyle = {
  background: 'rgba(255, 255, 255, 0.95)',
  border: '1px solid #ccc',
  borderRadius: 8,
  padding: 12,
  marginBottom: 12,
  boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
};

const listStyle = {
  listStyle: 'none',
  padding: 0,
  margin: 0,
  maxHeight: 180,
  overflowY: 'auto',
};

function formatTimestamp(value) {
  if (!value) return 'unknown';
  try {
    return new Date(value).toLocaleString();
  } catch {
    return String(value);
  }
}

export function ShipmentsPanel({ shipments, error }) {
  return (
    <section style={panelStyle} aria-label="Active shipments">
      <h3 style={{ margin: '0 0 8px' }}>Shipments</h3>
      {error && <p role="alert" style={{ color: '#a00', margin: '0 0 8px' }}>{error}</p>}
      {!error && shipments.length === 0 && (
        <p style={{ margin: 0, color: '#666' }}>No shipments yet.</p>
      )}
      <ul style={listStyle}>
        {shipments.map((shipment) => (
          <li key={shipment.id} style={{ marginBottom: 8, fontSize: 14 }}>
            <strong>{shipment.id}</strong>
            <div>Status: {shipment.status || 'unknown'}</div>
            <div>Node: {shipment.currentNode || shipment.arrivalNode || 'unknown'}</div>
            {shipment.warehouseAssigned && (
              <div>Warehouse: {shipment.warehouseAssigned}</div>
            )}
            <div>Updated: {formatTimestamp(shipment.updatedAt)}</div>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function SensorAlertsPanel({ alerts, error }) {
  return (
    <section style={panelStyle} aria-label="Sensor alerts">
      <h3 style={{ margin: '0 0 8px' }}>Sensor Alerts</h3>
      {error && <p role="alert" style={{ color: '#a00', margin: '0 0 8px' }}>{error}</p>}
      {!error && alerts.length === 0 && (
        <p style={{ margin: 0, color: '#666' }}>No unresolved sensor alerts.</p>
      )}
      <ul style={listStyle}>
        {alerts.map((alert) => (
          <li key={alert.id || `${alert.node}-${alert.timestamp}`} style={{ marginBottom: 8, fontSize: 14 }}>
            <strong>{alert.type || alert.alert_type || 'sensor'}</strong> @ {alert.node || 'unknown'}
            <div>Severity: {alert.severity || 'unknown'}</div>
            {alert.suggestion && <div>Suggestion: {alert.suggestion}</div>}
            <div>When: {formatTimestamp(alert.timestamp)}</div>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function MaintenanceAlertsPanel({ alerts, error }) {
  return (
    <section style={panelStyle} aria-label="Maintenance alerts">
      <h3 style={{ margin: '0 0 8px' }}>Maintenance Alerts</h3>
      {error && <p role="alert" style={{ color: '#a00', margin: '0 0 8px' }}>{error}</p>}
      {!error && alerts.length === 0 && (
        <p style={{ margin: 0, color: '#666' }}>No maintenance alerts recorded.</p>
      )}
      <ul style={listStyle}>
        {alerts.map((alert) => (
          <li key={alert._id || `${alert.assetId}-${alert.timestamp}`} style={{ marginBottom: 8, fontSize: 14 }}>
            <strong>{alert.assetId || 'asset'}</strong>
            <div>{alert.alertType || alert.alert_type || 'maintenance'}</div>
            {alert.reason && <div>{alert.reason}</div>}
            <div>When: {formatTimestamp(alert.timestamp)}</div>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function StatusSidebar({ shipments, sensorAlerts, maintenanceAlerts, panelErrors }) {
  return (
    <aside
      style={{
        position: 'fixed',
        top: 56,
        right: 16,
        width: 280,
        zIndex: 500,
        maxHeight: 'calc(100vh - 72px)',
        overflowY: 'auto',
      }}
    >
      <ShipmentsPanel shipments={shipments} error={panelErrors.shipments} />
      <SensorAlertsPanel alerts={sensorAlerts} error={panelErrors.sensorAlerts} />
      <MaintenanceAlertsPanel alerts={maintenanceAlerts} error={panelErrors.maintenanceAlerts} />
    </aside>
  );
}
