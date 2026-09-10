import React, { useState, useMemo } from 'react';
import {
  summarizeFleet,
  filterEdgesByPhase,
  EDGE_PHASE_FILTER_OPTIONS,
} from './viewModels/edgeDevice';
import {
  LIFECYCLE_STEPS,
  partitionShipments,
  filterShipmentsByStatus,
  STATUS_FILTER_OPTIONS,
  getNextArrivalForecast,
  formatCountdown,
} from './viewModels/shipment';

function formatTimestamp(value) {
  if (!value) return 'unknown';
  try {
    return new Date(value).toLocaleString();
  } catch {
    return String(value);
  }
}

function PhaseBadge({ tone, label }) {
  return <span className={`badge badge--${tone}`}>{label}</span>;
}

export function NextArrivalPanel({ shipments, error, nowMs = Date.now() }) {
  const forecast = useMemo(
    () => getNextArrivalForecast(shipments, nowMs),
    [shipments, nowMs],
  );

  return (
    <section className="hs-panel next-arrival-panel" aria-label="Next shipment arrival">
      <h3>Next Arrival</h3>
      {error && <p className="hs-panel-error" role="alert">{error}</p>}
      {!error && !forecast.hasData && (
        <p className="hs-empty">Waiting for first shipment…</p>
      )}
      {!error && forecast.hasData && (
        <>
          <div className="next-arrival-countdown" aria-live="polite">
            <span className="next-arrival-countdown-value">
              {formatCountdown(forecast.secondsUntil)}
            </span>
            <span className="next-arrival-countdown-label">
              {forecast.isOverdue ? 'until next vessel (overdue)' : 'until next vessel (est.)'}
            </span>
          </div>
          {forecast.latestArrival && (
            <div className="next-arrival-detail">
              <div>
                <strong>Last arrival:</strong>{' '}
                {forecast.latestArrival.id} @ {forecast.latestArrival.arrivalNode || '—'}
              </div>
              <div className="fleet-row-sub">
                {formatTimestamp(forecast.latestArrival.createdAt)}
              </div>
            </div>
          )}
          {forecast.awaitingAtDock.length > 0 && (
            <div className="next-arrival-dock-queue">
              <div className="fleet-row-sub" style={{ marginBottom: 4 }}>
                At dock now ({forecast.awaitingAtDock.length})
              </div>
              <ul className="assigned-edges">
                {forecast.awaitingAtDock.map((s) => (
                  <li key={s.id}>
                    {s.id} @ {s.arrivalNode || s.currentNode || '—'}
                  </li>
                ))}
              </ul>
            </div>
          )}
          <div className="fleet-row-sub" style={{ marginTop: 8 }}>
            Avg interval ~{Math.round(forecast.averageIntervalMs / 1000)}s
          </div>
        </>
      )}
    </section>
  );
}

export function EdgeFleetPanel({
  devices,
  error,
  selectedDeviceId,
  onSelectDevice,
  idleEdgeDiagnosticsById = {},
}) {
  const [phaseFilter, setPhaseFilter] = useState('all');
  const [search, setSearch] = useState('');
  const { total, idle, active, completing, devices: fleet } = summarizeFleet(devices);
  const filteredFleet = useMemo(
    () => filterEdgesByPhase(fleet, phaseFilter).filter((device) => `${device.id} ${device.type} ${device.location}`.toLowerCase().includes(search.trim().toLowerCase())),
    [fleet, phaseFilter, search],
  );

  return (
    <section className="hs-panel" aria-label="Edge fleet">
      <div className="shipment-panel-header">
        <h3>Fleet <span className="count-label">{total}</span></h3>
        <label className="shipment-status-filter">
          <span className="shipment-status-filter-label">Phase</span>
          <select
            value={phaseFilter}
            onChange={(event) => setPhaseFilter(event.target.value)}
            aria-label="Filter edges by phase"
          >
            {EDGE_PHASE_FILTER_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
      </div>
      <label className="fleet-search"><span className="sr-only">Search fleet</span><input type="search" aria-label="Search fleet" placeholder="Search device or location…" value={search} onChange={(event) => setSearch(event.target.value)} /></label>
      {error && <p className="hs-panel-error" role="alert">{error}</p>}
      {!error && total === 0 && (
        <p className="hs-empty">No edge devices loaded.</p>
      )}
      {!error && total > 0 && filteredFleet.length === 0 && (
        <p className="hs-empty">No edges match this phase.</p>
      )}
      {!error && total > 0 && (
        <>
          <div className="fleet-summary">
            <span className="summary-chip"><strong>{idle}</strong> idle</span>
            <span className="summary-chip"><strong>{active - completing}</strong> active</span>
            <span className="summary-chip"><strong>{completing}</strong> completing</span>
          </div>
          {filteredFleet.length > 0 && (
          <ul className="fleet-list">
            {filteredFleet.map((d) => {
              const idleDiag = d.taskPhase === 'idle'
                ? idleEdgeDiagnosticsById[d.id]
                : null;

              return (
                <li key={d.id}>
                  <button
                    type="button"
                    className={`fleet-row${selectedDeviceId === d.id ? ' fleet-row--selected' : ''}`}
                    onClick={() => onSelectDevice?.(d.raw)}
                    aria-label={`Device ${d.id}, ${d.statusLabel}`}
                  >
                    <span aria-hidden="true">{d.typeIcon}</span>
                    <div className="fleet-row-main">
                      <div className="fleet-row-id">{d.id}</div>
                      <div className="fleet-row-sub">
                        {d.location || '—'}
                        {d.shipmentId && ` · ${d.shipmentId}`}
                        {d.workflowPhase && ` · ${d.workflowPhase}`}
                      </div>
                      {idleDiag?.subline && (
                        <div className="fleet-row-idle-reason">
                          Why idle: {idleDiag.subline}
                        </div>
                      )}
                      {d.isMoving && (
                        <div className="progress-bar" aria-hidden="true">
                          <div
                            className="progress-bar-fill"
                            style={{ width: `${d.progressPct}%` }}
                          />
                        </div>
                      )}
                    </div>
                    <PhaseBadge tone={d.statusTone} label={d.statusLabel} />
                  </button>
                </li>
              );
            })}
          </ul>
          )}
        </>
      )}
    </section>
  );
}

export function PortBacklogPanel({ pendingBacklog, queueCounts, error }) {
  const [expanded, setExpanded] = useState(false);
  const totalPending = pendingBacklog.reduce((sum, row) => sum + row.count, 0);
  const queueEntries = Object.entries(queueCounts || {}).filter(([, count]) => count > 0);

  if (!error && totalPending === 0 && queueEntries.length === 0) {
    return null;
  }

  return (
    <section className="hs-panel port-backlog-panel" aria-label="Port backlog">
      <div className="port-backlog-header">
        <h3>Port Backlog</h3>
        <button
          type="button"
          className="collapsible-toggle"
          onClick={() => setExpanded((v) => !v)}
          aria-expanded={expanded}
        >
          {expanded ? 'Hide' : 'Show'} details
        </button>
      </div>
      {error && <p className="hs-panel-error" role="alert">{error}</p>}
      {!error && (
        <>
          <div className="port-backlog-summary">
            <span className="summary-chip">
              <strong>{totalPending}</strong> pending workflow
            </span>
          </div>
          {expanded && (
            <>
              {pendingBacklog.length > 0 && (
                <ul className="port-backlog-list">
                  {pendingBacklog.map((row) => (
                    <li key={row.key}>
                      <span>{row.label}</span>
                      <strong>{row.count}</strong>
                    </li>
                  ))}
                </ul>
              )}
              {queueEntries.length > 0 && (
                <div className="port-backlog-queues">
                  {queueEntries.map(([key, count]) => (
                    <span key={key} className="queue-flag">
                      {key}: {count}
                    </span>
                  ))}
                </div>
              )}
            </>
          )}
        </>
      )}
    </section>
  );
}

function LifecycleStepper({ stepIndex }) {
  return (
    <div className="lifecycle-stepper" aria-label="Shipment lifecycle">
      {LIFECYCLE_STEPS.map((step, idx) => {
        let className = 'lifecycle-step';
        if (idx < stepIndex) className += ' lifecycle-step--done';
        if (idx === stepIndex) className += ' lifecycle-step--current';
        return (
          <div key={step.key} className={className} title={step.label}>
            {step.label}
          </div>
        );
      })}
    </div>
  );
}

function ShipmentCard({ shipment, diagnostic }) {
  const incompleteKeys = new Set(
    (diagnostic?.assignedIncomplete || []).map((edge) => `${edge.edgeId}-${edge.phase}`),
  );

  return (
    <article className="shipment-card">
      <div className="shipment-card-header">
        <span className="shipment-card-id">{shipment.id}</span>
        <PhaseBadge
          tone={shipment.isActive ? 'active' : 'idle'}
          label={shipment.effectiveStatus || shipment.status}
        />
      </div>
      <LifecycleStepper stepIndex={shipment.stepIndex} />
      {diagnostic?.nextPhaseLabel && (
        <div className="shipment-diagnostic-line">
          Next phase: {diagnostic.nextPhaseLabel}
        </div>
      )}
      {diagnostic?.hasBlocker && diagnostic.blockerMessage && (
        <div className="shipment-blocker" role="status">
          Waiting for: {diagnostic.blockerMessage}
        </div>
      )}
      <div className="shipment-route">{shipment.routeLabel}</div>
      {shipment.warehouseAssigned && (
        <div className="fleet-row-sub">Warehouse: {shipment.warehouseAssigned}</div>
      )}
      {shipment.assignedEdges.length > 0 && (
        <ul className="assigned-edges" aria-label="Assigned edges">
          {shipment.assignedEdges.map((edge) => {
            const edgeKey = `${edge.edgeId}-${edge.phase}`;
            const isIncomplete = incompleteKeys.has(edgeKey) || !edge.completed;
            return (
              <li
                key={edgeKey}
                className={isIncomplete ? 'assigned-edge--incomplete' : undefined}
              >
                {edge.label}
              </li>
            );
          })}
        </ul>
      )}
      {shipment.queueFlags.length > 0 && (
        <div className="queue-flags">
          {shipment.queueFlags.map((flag) => (
            <span key={flag} className="queue-flag">{flag}</span>
          ))}
        </div>
      )}
      <div className="fleet-row-sub" style={{ marginTop: 6 }}>
        Updated: {formatTimestamp(shipment.updatedAt)}
      </div>
    </article>
  );
}

export function ShipmentsPanel({ shipments, error, shipmentDiagnosticsById = {} }) {
  const [showCompleted, setShowCompleted] = useState(false);
  const [statusFilter, setStatusFilter] = useState('all');

  const filteredShipments = useMemo(
    () => filterShipmentsByStatus(shipments, statusFilter),
    [shipments, statusFilter],
  );
  const { active, completed } = useMemo(
    () => partitionShipments(filteredShipments),
    [filteredShipments],
  );

  return (
    <section className="hs-panel" aria-label="Shipments">
      <div className="shipment-panel-header">
        <h3>Shipments</h3>
        <label className="shipment-status-filter">
          <span className="shipment-status-filter-label">Status</span>
          <select
            value={statusFilter}
            onChange={(event) => setStatusFilter(event.target.value)}
            aria-label="Filter shipments by status"
          >
            {STATUS_FILTER_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
      </div>
      {error && <p className="hs-panel-error" role="alert">{error}</p>}
      {!error && shipments.length === 0 && (
        <p className="hs-empty">No shipments yet.</p>
      )}
      {!error && shipments.length > 0 && filteredShipments.length === 0 && (
        <p className="hs-empty">No shipments match this status.</p>
      )}
      {!error && active.length > 0 && (
        <>
          <div className="fleet-row-sub" style={{ marginBottom: 8 }}>Active ({active.length})</div>
          {active.map((s) => (
            <ShipmentCard
              key={s.id}
              shipment={s}
              diagnostic={shipmentDiagnosticsById[s.id]}
            />
          ))}
        </>
      )}
      {!error && completed.length > 0 && (
        <>
          <button
            type="button"
            className="collapsible-toggle"
            onClick={() => setShowCompleted((v) => !v)}
            aria-expanded={showCompleted}
          >
            {showCompleted ? 'Hide' : 'Show'} completed ({completed.length})
          </button>
          {showCompleted && completed.map((s) => (
            <ShipmentCard
              key={s.id}
              shipment={s}
              diagnostic={shipmentDiagnosticsById[s.id]}
            />
          ))}
        </>
      )}
    </section>
  );
}

export function SensorAlertsPanel({ alerts, error }) {
  return (
    <section className="hs-panel" aria-label="Sensor alerts">
      <h3>Sensor Alerts</h3>
      {error && <p className="hs-panel-error" role="alert">{error}</p>}
      {!error && alerts.length === 0 && (
        <p className="hs-empty">No unresolved sensor alerts.</p>
      )}
      <ul className="alert-list">
        {alerts.map((alert) => (
          <li key={alert.id || `${alert.node}-${alert.timestamp}`}>
            <strong>{alert.type || alert.alert_type || 'sensor'}</strong> @ {alert.node || 'unknown'}
            <div className="fleet-row-sub">Severity: {alert.severity || 'unknown'}</div>
            {alert.suggestion && <div className="fleet-row-sub">Suggestion: {alert.suggestion}</div>}
            <div className="fleet-row-sub">When: {formatTimestamp(alert.timestamp)}</div>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function MaintenanceAlertsPanel({ alerts, error }) {
  return (
    <section className="hs-panel" aria-label="Maintenance alerts">
      <h3>Maintenance Alerts</h3>
      {error && <p className="hs-panel-error" role="alert">{error}</p>}
      {!error && alerts.length === 0 && (
        <p className="hs-empty">No maintenance alerts recorded.</p>
      )}
      <ul className="alert-list">
        {alerts.map((alert) => (
          <li key={alert._id || `${alert.assetId}-${alert.timestamp}`}>
            <strong>{alert.assetId || 'asset'}</strong>
            <div className="fleet-row-sub">{alert.alertType || alert.alert_type || 'maintenance'}</div>
            {alert.reason && <div className="fleet-row-sub">{alert.reason}</div>}
            <div className="fleet-row-sub">When: {formatTimestamp(alert.timestamp)}</div>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function StatusSidebar({
  devices,
  shipments,
  sensorAlerts,
  maintenanceAlerts,
  panelErrors,
  selectedDeviceId,
  onSelectDevice,
  edgesError,
  portStateView,
  nowMs,
}) {
  const shipmentDiagnosticsById = portStateView?.shipmentDiagnosticsById || {};
  const idleEdgeDiagnosticsById = portStateView?.idleEdgeDiagnosticsById || {};

  return (
    <aside className="dashboard-sidebar">
      <NextArrivalPanel shipments={shipments} error={panelErrors.shipments} nowMs={nowMs} />
      <EdgeFleetPanel
        devices={devices}
        error={edgesError}
        selectedDeviceId={selectedDeviceId}
        onSelectDevice={onSelectDevice}
        idleEdgeDiagnosticsById={idleEdgeDiagnosticsById}
      />
      <PortBacklogPanel
        pendingBacklog={portStateView?.pendingBacklog || []}
        queueCounts={portStateView?.queueCounts || {}}
        error={panelErrors.portState}
      />
      <ShipmentsPanel
        shipments={shipments}
        error={panelErrors.shipments}
        shipmentDiagnosticsById={shipmentDiagnosticsById}
      />
      <SensorAlertsPanel alerts={sensorAlerts} error={panelErrors.sensorAlerts} />
      <MaintenanceAlertsPanel alerts={maintenanceAlerts} error={panelErrors.maintenanceAlerts} />
    </aside>
  );
}
