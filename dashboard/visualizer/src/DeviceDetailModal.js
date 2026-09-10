import React from 'react';
import { normalizeEdgeDevice } from './viewModels/edgeDevice';
import { normalizeShipment, LIFECYCLE_STEPS } from './viewModels/shipment';

export function DetailDrawer({ title, label, onClose, children }) {
  const drawerRef = React.useRef(null);
  const onCloseRef = React.useRef(onClose);
  onCloseRef.current = onClose;
  React.useEffect(() => {
    const previous = document.activeElement;
    const drawer = drawerRef.current;
    drawer?.querySelector('button')?.focus();
    const handleKey = (event) => {
      if (event.key === 'Escape') { event.preventDefault(); onCloseRef.current(); }
      if (event.key !== 'Tab') return;
      const focusable = [...drawer.querySelectorAll('button:not(:disabled), a[href], select:not(:disabled), input:not(:disabled), [tabindex="0"]')];
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    };
    document.addEventListener('keydown', handleKey);
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', handleKey);
      document.body.style.overflow = previousOverflow;
      if (previous?.isConnected) previous.focus();
    };
  }, []);
  return <div className="modal-overlay" onClick={onClose}>
    <section ref={drawerRef} className="modal-card" role="dialog" aria-modal="true" aria-label={label} onClick={(event) => event.stopPropagation()}>
      <div className="drawer-heading"><div><span className="eyebrow">Operational details</span><h3>{title}</h3></div><button type="button" className="drawer-dismiss" onClick={onClose} aria-label="Close details">×</button></div>
      {children}
      <button type="button" className="modal-close" onClick={onClose}>Close</button>
    </section>
  </div>;
}

export function TelemetryTrend({ device, history = [], sensors = [] }) {
  const matches = (sensor) => [sensor.assetId, sensor.deviceId, sensor.craneId, sensor.telemetry?.craneId].includes(device.id);
  const craneSamples = history.flatMap((frame) => (frame.craneTelemetry || []).filter(matches).flatMap((sample) => {
    const values = { ...sample, ...sample.telemetry, anomalyScore: sample.analysis?.anomalyScore ?? sample.anomalyScore };
    return Object.entries({ motorTemp: '°C', vibration: '', energyUse: '', anomalyScore: '' }).filter(([key]) => Number.isFinite(values[key])).map(([key, unit]) => ({ type: key, reading: Number(values[key].toFixed(4)), timestamp: sample.timestamp, at: frame.at, unit }));
  }));
  const samples = [...craneSamples, ...history.flatMap((frame) => (frame.sensors || []).filter(matches).map((sensor) => ({ ...sensor, at: sensor.timestamp ? Date.parse(sensor.timestamp) : frame.at })))];
  const readings = [...samples, ...sensors.filter(matches)].filter((item) => Number.isFinite(Number(item.reading)) && item.reading !== null);
  const types = [...new Set(readings.map((item) => item.type || 'reading'))];
  return <div className="modal-section"><h4>Sensor trends</h4>
    {!types.length ? <p className="hs-empty">No device-linked sensor readings are available in this snapshot.</p> : types.map((type) => {
      const rows = readings.filter((item) => (item.type || 'reading') === type);
      const unique = [...new Map(rows.map((item) => [`${item.timestamp || item.at}-${item.reading}`, item])).values()].sort((a, b) => (Date.parse(a.timestamp) || a.at || 0) - (Date.parse(b.timestamp) || b.at || 0)).slice(-60);
      const values = unique.map((item) => Number(item.reading));
      const min = Math.min(...values); const max = Math.max(...values);
      const points = values.map((value, index) => `${12 + index * 296 / Math.max(1, values.length - 1)},${88 - ((value - min) / (max - min || 1)) * 68}`).join(' ');
      return <figure className="sensor-trend" key={type}><figcaption>{type} <strong>{values[values.length - 1]} {unique[unique.length - 1]?.unit || ''}</strong></figcaption>
        <svg viewBox="0 0 320 100" role="img" aria-label={`${type}: ${values.length} samples, minimum ${min}, maximum ${max}, latest ${values[values.length - 1]}`}><path d="M12 88H308" stroke="#d6e1e4" /><polyline points={points} fill="none" stroke="#007f83" strokeWidth="2.5" /></svg>
        <p>{values.length} captured samples · min {min} / max {max}</p></figure>;
    })}
    <p className="muted-note">Observed feature changes describe the recorded readings; they do not establish the cause of a fault.</p>
  </div>;
}

function formatDuration(seconds) {
  if (seconds == null) return '—';
  if (seconds === 0) return '0s';
  return `${seconds}s`;
}

function DetailItem({ label, value }) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </>
  );
}

export function DeviceDetailModal({ device, onClose, history, sensors = [], maintenanceAlerts = [], craneTelemetry = [] }) {
  if (!device) return null;

  const d = normalizeEdgeDevice(device);
  const latestAnalysis = craneTelemetry.find((sample) => (sample.craneId || sample.telemetry?.craneId) === d.id)?.analysis;

  return (
    <DetailDrawer title={`${d.typeIcon} ${d.id}`} label="Device details" onClose={onClose}>

        <div className="modal-section">
          <h4>Status</h4>
          <dl className="detail-grid">
            <DetailItem label="Task phase" value={d.statusLabel} />
            <DetailItem label="Workflow" value={d.workflowPhase || '—'} />
            <DetailItem label="Type" value={d.type} />
            {d.desc && <DetailItem label="Description" value={d.desc} />}
          </dl>
        </div>

        <div className="modal-section">
          <h4>Assignment</h4>
          <dl className="detail-grid">
            <DetailItem label="Shipment" value={d.shipmentId || '—'} />
            <DetailItem label="Priority" value={d.priority ?? '—'} />
          </dl>
        </div>

        <div className="modal-section">
          <h4>Movement</h4>
          <dl className="detail-grid">
            <DetailItem label="Location" value={d.location || '—'} />
            <DetailItem
              label="Next node"
              value={d.nextNode || (d.taskPhase === 'en_route_start' ? '—' : 'At node')}
            />
            <DetailItem label="Final node" value={d.finalNode || '—'} />
            <DetailItem label="Progress" value={`${Math.round(d.progressPct)}%`} />
          </dl>
        </div>

        <div className="modal-section">
          <h4>Timing</h4>
          <dl className="detail-grid">
            <DetailItem label="ETA" value={formatDuration(d.etaSec)} />
            <DetailItem label="Task completion" value={formatDuration(d.taskCompletionTimeSec)} />
            <DetailItem label="Journey" value={formatDuration(d.journeyTimeSec)} />
          </dl>
        </div>

        <TelemetryTrend device={device} history={history} sensors={sensors} />
        {latestAnalysis && <div className="modal-section"><h4>Anomaly analysis</h4><dl className="detail-grid"><DetailItem label="Model" value={latestAnalysis.model || 'Unavailable'} /><DetailItem label="Anomaly score" value={latestAnalysis.anomalyScore == null ? 'Unavailable' : Number(latestAnalysis.anomalyScore).toFixed(4)} /><DetailItem label="Threshold" value={latestAnalysis.threshold ?? 'Unavailable'} /><DetailItem label="Flagged" value={latestAnalysis.anomalous ? 'Yes' : 'No'} /></dl><p className="muted-note">{latestAnalysis.explanation || 'No explanation was captured.'}</p></div>}
        {maintenanceAlerts.filter((alert) => alert.assetId === d.id).slice(-5).map((alert, index) => <div className="drawer-alert" key={alert.id || index}><strong>{alert.resolved ? 'Resolved · ' : ''}{alert.alertType || 'Maintenance alert'}</strong><p>{alert.reason || 'No explanation was recorded.'}</p><p>Anomaly score: {alert.anomalyScore ?? alert.analysis?.anomalyScore ?? alert.score ?? 'Unavailable'}</p></div>)}
    </DetailDrawer>
  );
}

export function SensorDetailModal({ sensors, nodeId, onClose }) {
  const [selected, setSelected] = React.useState(null);

  React.useEffect(() => {
    setSelected(null);
  }, [nodeId]);

  return (
    <DetailDrawer title={`Port location ${nodeId}`} label="Sensors at node" onClose={onClose}>
        {!sensors?.length && <p className="hs-empty">No sensor readings are available at this location.</p>}
        <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
          {(sensors || []).map((sensor) => (
            <li key={sensor.id}>
              <button
                type="button"
                className={`sensor-list-btn${selected?.id === sensor.id ? ' sensor-list-btn--active' : ''}`}
                onClick={() => setSelected(sensor)}
              >
                {sensor.type || 'unknown'} — {sensor.node}
              </button>
            </li>
          ))}
        </ul>

        {selected && (
          <div className="modal-section" style={{ marginTop: 16 }}>
            <h4>Reading</h4>
            <dl className="detail-grid">
              <DetailItem label="Type" value={selected.type || '—'} />
              <DetailItem label="Node" value={selected.node || '—'} />
              <DetailItem label="Reading" value={selected.reading != null ? String(selected.reading) : '—'} />
              <DetailItem
                label="Timestamp"
                value={selected.timestamp ? new Date(selected.timestamp).toLocaleString() : '—'}
              />
            </dl>
          </div>
        )}

    </DetailDrawer>
  );
}

export function ShipmentDetailModal({ shipment, diagnostic, onClose }) {
  const item = normalizeShipment(shipment);
  if (!item) return null;
  return <DetailDrawer title={item.id} label="Shipment details" onClose={onClose}>
    <div className="modal-section"><h4>Journey</h4><dl className="detail-grid"><DetailItem label="Status" value={item.currentStep.label} /><DetailItem label="Route" value={item.routeLabel} /><DetailItem label="Current location" value={item.currentNode || 'Unavailable'} /><DetailItem label="Destination" value={item.destination || item.warehouseAssigned || 'Not assigned'} /></dl></div>
    <ol className="drawer-lifecycle">{LIFECYCLE_STEPS.map((step, index) => <li key={step.key} aria-current={index === item.stepIndex ? 'step' : undefined}><span>{index <= item.stepIndex ? '✓' : '○'}</span>{step.label}</li>)}</ol>
    <div className="modal-section"><h4>Assignments</h4>{!item.assignedEdges.length ? <p className="hs-empty">No device has been assigned yet.</p> : <ul className="assigned-edges">{item.assignedEdges.map((edge, index) => <li key={`${edge.edgeId}-${index}`}>{edge.label}</li>)}</ul>}</div>
    {diagnostic && <div className="modal-section"><h4>Operational context</h4><p>{diagnostic.blockerMessage || 'No blocker reported.'}</p><p>Next step: {diagnostic.nextPhaseLabel || 'Complete'}</p></div>}
  </DetailDrawer>;
}
