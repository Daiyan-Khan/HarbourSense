import React, { useMemo, useState } from 'react';
import { usePortDashboard } from './state/PortDashboardProvider';
import { summarizeFleet } from './viewModels/edgeDevice';
import { deriveEffectiveStatus, LIFECYCLE_STEPS } from './viewModels/shipment';

export function formatPlaybackTime(value) {
  const seconds = Math.max(0, Math.floor((value || 0) / 1000));
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}

export function deriveOverview(state) {
  const fleet = summarizeFleet(state.live.edges);
  const shipmentsReady = state.panels.lastSlowPollAt != null && !state.panels.errors.shipments;
  const alertReady = state.panels.lastSlowPollAt != null && !state.panels.errors.sensorAlerts && !state.panels.errors.maintenanceAlerts;
  const shipments = state.panels.shipments;
  const explicitlyDelayed = shipments.filter((item) => item.delayed === true || item.status === 'delayed').length;
  const hasDelayDefinition = shipments.length > 0 && shipments.every((item) => typeof item.delayed === 'boolean');
  const critical = [...state.panels.sensorAlerts, ...state.panels.maintenanceAlerts]
    .filter((item) => ['high', 'critical'].includes(String(item.severity).toLowerCase()) && !item.resolved && item.status !== 'resolved').length;
  return [
    { label: 'Throughput', value: shipmentsReady ? shipments.filter((item) => deriveEffectiveStatus(item) === 'delivered').length : '—', unit: 'delivered', detail: state.source.kind === 'live' ? 'In the loaded shipment history' : 'Since the start of this scenario' },
    { label: 'Active fleet', value: state.live.lastFastPollAt != null ? fleet.active : '—', unit: `/ ${fleet.total} devices`, detail: 'Assigned, moving or completing' },
    { label: 'Delayed shipments', value: shipmentsReady && (hasDelayDefinition || explicitlyDelayed) ? explicitlyDelayed : '—', unit: '', detail: hasDelayDefinition ? 'Explicitly marked late in source data' : 'No delivery deadline is defined' },
    { label: 'Critical alerts', value: alertReady ? critical : '—', unit: 'open', detail: 'High / critical severity · loaded alerts' },
  ];
}

export function Overview() {
  const { state } = usePortDashboard();
  return <section className="overview-grid" aria-label="Operations overview">
    {deriveOverview(state).map((metric, index) => <article className={`metric-card metric-card--${index}`} key={metric.label} aria-label={metric.label}>
      <p className="metric-label"><span className="metric-dot" />{metric.label}</p>
      <p className="metric-value"><strong>{metric.value}</strong><span>{metric.unit}</span></p>
      <p className="metric-detail">{metric.detail}</p>
    </article>)}
  </section>;
}

export function ScenarioPanel() {
  const { state, command, chooseScenario } = usePortDashboard();
  const source = state.source;
  const replay = source.kind === 'replay';
  const enabled = replay || source.controlsEnabled;
  const busy = source.commandPending || ['loading', 'starting', 'resetting'].includes(source.status);
  const selected = source.selectedScenarioId || source.scenario?.id || source.scenarioId || source.scenarios?.[0]?.id || '';
  const running = source.status === 'running';
  const paused = source.status === 'paused';
  const title = running ? 'Pause scenario' : paused ? 'Resume scenario' : 'Play scenario';
  const action = running ? 'pause' : paused ? 'resume' : 'start';
  const progress = source.durationMs ? (source.positionMs / source.durationMs) * 100 : null;
  return <section className="scenario-panel" aria-label="Guided scenario">
    <div className="scenario-intro">
      <span className="eyebrow">{replay ? 'Explore the recorded simulation' : 'Guided port operations'}</span>
      <h2>{source.scenario?.title || 'Follow a shipment through the port'}</h2>
      <p>{source.scenario?.description || 'Watch devices coordinate offloading, transport, storage and delivery.'}</p>
    </div>
    <div className="scenario-controls">
      <label>Scenario<select aria-label="Scenario" value={selected} onChange={(event) => chooseScenario(event.target.value)} disabled={!enabled || busy || (!replay && (running || paused))}>
        {!source.scenarios?.length && <option value="">{replay ? 'Loading scenarios…' : 'Local scenario service unavailable'}</option>}
        {(source.scenarios || []).map((scenario) => <option key={scenario.id} value={scenario.id}>{scenario.title}</option>)}
      </select></label>
      <div className="playback-buttons">
        <button className="primary-button" type="button" aria-label={title} disabled={!enabled || busy || source.status === 'failed' || (!replay && source.status === 'complete')} onClick={() => command(action)}>
          <span aria-hidden="true">{running ? 'Ⅱ' : '▶'}</span> {running ? 'Pause' : paused ? 'Resume' : source.status === 'complete' ? replay ? 'Play again' : 'Run complete' : 'Play scenario'}
        </button>
        <button type="button" className="subtle-button" aria-label="Reset scenario" disabled={!enabled || busy} onClick={() => command('reset')}>↺ Reset</button>
        <label className="speed-control"><span className="sr-only">Playback speed</span><select aria-label="Playback speed" value={source.speed || 1} disabled={!enabled || busy} onChange={(event) => command('speed', Number(event.target.value))}>
          {(replay ? [0.5, 1, 2, 4] : [1, 2, 4]).map((speed) => <option key={speed} value={speed}>{speed}×</option>)}
        </select></label>
      </div>
    </div>
    {enabled && <div className="playback-position">
      <div><span>{formatPlaybackTime(source.positionMs)}{source.durationMs ? ` / ${formatPlaybackTime(source.durationMs)}` : ' simulated'}</span><span className="scenario-state" aria-live="polite">{({ idle: 'Ready when you are', running: 'Scenario in progress', paused: 'Paused · inspect any device', complete: 'Scenario complete', failed: 'Scenario failed', starting: 'Starting scenario…', resetting: 'Resetting scenario…' })[source.status] || 'Loading…'}</span></div>
      {progress != null && <progress aria-label="Scenario playback progress" value={progress} max="100" />}
    </div>}
    {source.commandError && <p className="hs-panel-error" role="alert">{source.commandError}</p>}
    {source.kind === 'demo' && ['starting', 'running', 'paused'].includes(source.status) && Object.entries(source.services || {}).some(([, service]) => !service.ready) && <p className="service-warning">Local services: {Object.entries(source.services || {}).filter(([, service]) => !service.ready).map(([name, service]) => `${name} ${service.status || 'connecting'}`).join(' · ')}. The last acknowledged scenario state is shown.</p>}
    {source.error && <p className="hs-panel-error" role="alert">{source.error}</p>}
    {replay && <p className="recording-disclosure">Recorded simulation · captured from the local system using synthetic inputs. Playback runs in your browser; controls affect only your session.</p>}
  </section>;
}

export function ShipmentJourney() {
  const { state, portStateView, selectShipment } = usePortDashboard();
  const [focusedId, setFocusedId] = useState('');
  const shipments = state.panels.shipments;
  const shipment = shipments.find((item) => item.id === focusedId) || shipments.find((item) => deriveEffectiveStatus(item) !== 'delivered') || shipments[0];
  const phaseIndex = shipment ? LIFECYCLE_STEPS.findIndex((step) => step.key === deriveEffectiveStatus(shipment)) : -1;
  const diagnostic = shipment ? portStateView.shipmentDiagnosticsById[shipment.id] : null;
  return <section className="journey-panel" aria-label="Shipment journey">
    <div className="section-heading"><div><span className="eyebrow">From dock to destination</span><h2>Shipment journey</h2></div>
      {!!shipments.length && <select aria-label="Follow shipment" value={shipment?.id || ''} onChange={(event) => setFocusedId(event.target.value)}>{shipments.map((item) => <option value={item.id} key={item.id}>{item.id}</option>)}</select>}
    </div>
    {!shipment ? <p className="hs-empty">{state.panels.errors.shipments || 'Play a scenario to follow its shipment journey.'}</p> : <>
      <button type="button" className="shipment-link" onClick={() => selectShipment(shipment.id)}>{shipment.id} <span>Inspect shipment ↗</span></button>
      <ol className="journey-steps">{LIFECYCLE_STEPS.map((step, index) => <li key={step.key} className={index < phaseIndex ? 'is-complete' : index === phaseIndex ? 'is-current' : ''} aria-current={index === phaseIndex ? 'step' : undefined}><span aria-hidden="true">{index < phaseIndex ? '✓' : String(index + 1).padStart(2, '0')}</span>{step.label}</li>)}</ol>
      <p className="journey-explainer">{phaseIndex === LIFECYCLE_STEPS.length - 1 ? 'Delivered. The recorded shipment has completed its lifecycle.' : diagnostic?.blockerMessage || `Next: ${diagnostic?.nextPhaseLabel || LIFECYCLE_STEPS[phaseIndex + 1]?.label || 'awaiting assignment'}. Inspect the fleet for device assignments and progress.`}</p>
      <p className="journey-route">{shipment.arrivalNode || 'Dock'} <span>→</span> {shipment.currentNode || shipment.arrivalNode || 'Pending'} <span>→</span> {shipment.destination || shipment.warehouseAssigned || 'Destination pending'}</p>
    </>}
  </section>;
}

export function EventTimeline() {
  const { state, selectDevice, selectShipment } = usePortDashboard();
  const [shipmentFilter, setShipmentFilter] = useState('all');
  const events = useMemo(() => state.timeline.filter((event) => shipmentFilter === 'all' || !event.shipmentId || event.shipmentId === shipmentFilter).slice(-30).reverse(), [state.timeline, shipmentFilter]);
  return <section className="timeline-panel" aria-label="Event timeline">
    <div className="section-heading"><div><span className="eyebrow">What happened & why</span><h2>Event timeline</h2></div>
      <label><span className="sr-only">Timeline shipment</span><select aria-label="Timeline shipment" value={shipmentFilter} onChange={(event) => setShipmentFilter(event.target.value)}><option value="all">All shipments</option>{state.panels.shipments.map((shipment) => <option key={shipment.id} value={shipment.id}>{shipment.id}</option>)}</select></label>
    </div>
    {!events.length ? <p className="hs-empty">{state.source.kind === 'live' ? 'No scenario event history is available for this live connection.' : 'Events appear as the scenario progresses.'}</p> : <ol className="timeline-list">{events.map((event, index) => <li key={event.id || `${event.atMs}-${index}`} className={/fault|alert|congestion/i.test(event.type) ? 'timeline-event--warn' : ''}>
      <time>{formatPlaybackTime(event.atMs)}</time><div><span className="event-type">{String(event.type || 'event').replace(/[_-]/g, ' ')}</span><p>{event.message}</p>
        {event.deviceId && <button className="text-button" type="button" onClick={() => selectDevice(event.deviceId)}>{event.deviceId} ↗</button>}
        {event.shipmentId && <button className="text-button" type="button" onClick={() => selectShipment(event.shipmentId)}>{event.shipmentId} ↗</button>}
      </div>
    </li>)}</ol>}
  </section>;
}

export function MaintenanceResponse() {
  const { state, selectDevice } = usePortDashboard();
  const history = state.panels.maintenanceHistory || [];
  const tasks = state.panels.maintenanceTasks || [];
  if (!history.length && !tasks.length && !/crane|fault/.test(state.source.scenario?.id || '')) return null;
  return <section className="journey-panel" aria-label="Maintenance response">
    <div className="section-heading"><div><span className="eyebrow">Detect · dispatch · resolve</span><h2>Maintenance response</h2></div><span className="count-label">{tasks.filter((task) => task.status === 'completed').length} repairs completed</span></div>
    {!history.length && !tasks.length ? <p className="hs-empty">No fault has been observed at this point in the scenario.</p> : <div className="maintenance-response-list">
      {history.slice(-3).map((alert, index) => <article key={alert.id || alert.eventId || index}><strong>{alert.resolved ? 'Resolved' : 'Alert raised'} · {alert.assetId || alert.craneId || 'Crane'}</strong><p>{alert.reason || alert.analysis?.explanation || 'Anomalous sensor pattern detected.'}</p>{alert.assetId && <button type="button" className="text-button" onClick={() => selectDevice(alert.assetId)}>Inspect sensor trends ↗</button>}</article>)}
      {tasks.slice(-3).map((task) => <article key={task.id}><strong>{task.status === 'completed' ? 'Repair completed' : 'Repair dispatched'} · {task.node || 'Port location'}</strong><p>{task.status === 'completed' ? 'The maintenance task reached its recorded completed state.' : 'A maintenance device is assigned to inspect and repair the affected location.'}</p>{task.edgeId && <button type="button" className="text-button" onClick={() => selectDevice(task.edgeId)}>{task.edgeId} ↗</button>}</article>)}
      {(history.length > 3 || tasks.length > 3) && <p className="muted-note">Showing the latest 3 records of each type · {history.length} alerts and {tasks.length} repair tasks captured.</p>}
    </div>}
  </section>;
}
