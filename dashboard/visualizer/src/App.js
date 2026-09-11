import React, { useMemo, useCallback, useRef, useEffect } from 'react';
import ReactFlow, { Background, Controls, BaseEdge, ReactFlowProvider, useReactFlow } from 'reactflow';
import 'reactflow/dist/style.css';
import './dashboard.css';
import './portfolio.css';
import { EdgeFleetPanel, ShipmentsPanel, PortBacklogPanel, SensorAlertsPanel, MaintenanceAlertsPanel, NextArrivalPanel } from './StatusPanels';
import { DeviceDetailModal, SensorDetailModal, ShipmentDetailModal } from './DeviceDetailModal';
import { Overview, ScenarioPanel, ShipmentJourney, EventTimeline, MaintenanceResponse } from './OperationsPanels';
import { usePortDashboard } from './state/PortDashboardProvider';
import { selectDataHealth } from './data/freshness';
import { undirectedEdgeKey } from './map/layout';
import PortNode from './nodes/PortNode';
import DeviceMarkerNode from './nodes/DeviceMarkerNode';
import ProjectResources from './ProjectResources';

const nodeTypes = { port: PortNode, device: DeviceMarkerNode };
function SideEdge({ id, sourceX, sourceY, targetX, targetY, data }) {
  const active = data?.active;
  const selected = data?.selected;
  return <BaseEdge id={id} path={`M${data?.sourceX ?? sourceX},${data?.sourceY ?? sourceY} L${data?.targetX ?? targetX},${data?.targetY ?? targetY}`}
    className={`graph-edge${active ? ' graph-edge--active' : ''}${selected ? ' graph-edge--selected' : ''}`}
    style={{ stroke: selected ? '#e39a24' : active ? '#00888b' : '#b8c9cd', strokeWidth: selected ? 5 : active ? 3 : 2, strokeDasharray: active ? '8 4' : undefined }} />;
}
const edgeTypes = { side: SideEdge };

function FitMap({ ready, nodeCount }) {
  const { fitView } = useReactFlow();
  const fitRef = useRef(fitView);
  fitRef.current = fitView;
  const fit = useCallback(() => fitRef.current({ padding: 0.12, minZoom: 0.1, maxZoom: 1.2, duration: window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ? 0 : 200 }), []);
  useEffect(() => {
    if (!ready || !nodeCount) return undefined;
    const timer = window.setTimeout(fit, 150);
    return () => window.clearTimeout(timer);
  }, [ready, nodeCount, fit]);
  return <button className="map-fit-button" type="button" onClick={fit} aria-label="Fit full port">⊞ Fit port</button>;
}

function PortMap({ nodes, edges, onNodeClick, graphNodeCount }) {
  return <ReactFlowProvider><div className="dashboard-flow" aria-label="Interactive port map">
    <ReactFlow nodes={nodes} edges={edges} edgeTypes={edgeTypes} nodeTypes={nodeTypes} onNodeClick={onNodeClick}
      minZoom={0.1} maxZoom={2} nodesDraggable={false} nodesConnectable={false} elementsSelectable
      fitView fitViewOptions={{ padding: 0.12, minZoom: 0.1 }} proOptions={{ hideAttribution: true }}>
      <Background color="#cbdcde" gap={24} size={1} />
      <Controls showInteractive={false} showFitView={false} />
      <FitMap ready nodeCount={graphNodeCount} />
    </ReactFlow>
  </div></ReactFlowProvider>;
}

function App() {
  const { state, flowNodes, flowEdges, portStateView, selectedDevice, nodeSensors, selectDevice, selectSensorNode, selectShipment, retry, frameNow } = usePortDashboard();
  const { graph, live, panels, ui, source } = state;
  const health = selectDataHealth(state, frameNow);
  const sourceLabel = source.kind === 'replay' ? 'Recorded simulation' : source.kind === 'demo' ? 'Local demo' : 'Live data source';
  const selectedShipment = panels.shipments.find((item) => item.id === ui.selectedShipmentId);
  const selectedEdges = useMemo(() => {
    const path = selectedDevice?.remainingPath || selectedDevice?.path || [];
    const route = [selectedDevice?.currentLocation, ...path].filter(Boolean);
    if (selectedDevice?.nextNode) route.splice(1, 0, selectedDevice.nextNode);
    const keys = new Set(route.slice(1).map((id, index) => undirectedEdgeKey(route[index], id)));
    return flowEdges.map((edge) => ({ ...edge, data: { ...edge.data, selected: keys.has(edge.id) } }));
  }, [selectedDevice, flowEdges]);
  const onNodeClick = useCallback((_event, node) => {
    if (node.id.startsWith('dev-')) selectDevice(node.id.slice(4));
    else selectSensorNode(node.id);
  }, [selectDevice, selectSensorNode]);

  return <div className="dashboard-root">
    <a className="skip-link" href="#operations-map">Skip to port map</a>
    <header className="dashboard-header">
      <div className="brand"><span className="brand-mark" aria-hidden="true"><svg viewBox="0 0 32 32"><path d="M6 21h20M9 21V9l12-4v16M21 5h6v4h-6M6 26c3-3 6 3 10 0s7 3 10 0" fill="none" stroke="currentColor" strokeWidth="2" /></svg></span><div><h1>HarbourSense <span>Smart Port</span></h1><p>Connected operations, in view.</p></div></div>
      <div className="dashboard-header-meta"><span className="source-pill">{sourceLabel}</span><span className={`health-pill health-pill--${health.tone}`}><span aria-hidden="true">●</span>{health.label}</span></div>
    </header>
    <div className="workspace-heading"><div><span className="eyebrow">Harbour control / Overview</span><h2>A port in motion.</h2><p>Follow the cargo. Understand the decisions.</p><a className="project-info-jump" href="#project-resources">Report &amp; project info <span aria-hidden="true">↓</span></a></div><div className="freshness-note"><span>{health.ageLabel}</span><small>{source.kind === 'replay' ? (source.recordedAt ? `Captured ${new Date(source.recordedAt).toLocaleDateString()}` : 'Loading verified recordings') : source.transport || 'Connecting to local services'}</small>{(health.tone !== 'ok' || graph.status === 'error') && <button className="text-button" type="button" onClick={retry}>Retry connection</button>}</div></div>
    <Overview />
    <ScenarioPanel />
    {graph.status === 'loading' && <div className="dashboard-banner dashboard-banner--info" role="status">Loading port graph…</div>}
    {graph.status === 'error' && <div className="dashboard-banner dashboard-banner--error" role="alert">Graph unavailable: {graph.error} <button type="button" onClick={retry}>Try again</button></div>}
    {graph.status === 'empty' && <div className="dashboard-banner dashboard-banner--info" role="status">Port graph is empty. Start the isolated local demo to seed its port map.</div>}
    {live.error && graph.status === 'ready' && <div className="dashboard-banner dashboard-banner--warn" role="alert">Live data warning: {live.error}. The last received snapshot is retained.</div>}
    <main className="portfolio-main">
      <div className="primary-column">
        <section className="map-panel" id="operations-map" tabIndex="-1" aria-label="Port overview">
          <div className="section-heading"><div><span className="eyebrow">Spatial operations</span><h2>Port overview <span className="count-label">{graph.rawNodes.length} locations</span></h2></div><span className="map-hint">Select a device to inspect its route</span></div>
          {graph.status === 'ready' ? <PortMap nodes={flowNodes} edges={selectedEdges} onNodeClick={onNodeClick} graphNodeCount={graph.graphNodes.length} /> : <div className="map-empty"><span aria-hidden="true">⌁</span><h3>{graph.status === 'error' ? 'The port is temporarily unavailable' : 'Preparing your port overview'}</h3><p>{graph.status === 'error' ? 'Use the retry action above to reconnect.' : 'Locations and devices will appear when the data source is ready.'}</p></div>}
          <div className="map-footer" aria-label="Map legend"><span><i className="legend-dot legend-dot--dock" />Dock / berth</span><span><i className="legend-dot legend-dot--warehouse" />Warehouse</span><span><i className="legend-line" />Active route</span><span><i className="legend-line legend-line--selected" />Selected route</span><small>Scroll to zoom · drag to pan</small></div>
        </section>
        <ShipmentJourney />
        <MaintenanceResponse />
        <EventTimeline />
      </div>
      <aside className="operations-rail" aria-label="Fleet and diagnostics">
        <EdgeFleetPanel devices={live.edges} error={live.error} selectedDeviceId={ui.selectedDeviceId} onSelectDevice={selectDevice} idleEdgeDiagnosticsById={portStateView.idleEdgeDiagnosticsById} />
        <details className="diagnostic-section"><summary>Shipment details & backlog</summary><ShipmentsPanel shipments={panels.shipments} error={panels.errors.shipments} shipmentDiagnosticsById={portStateView.shipmentDiagnosticsById} /><PortBacklogPanel pendingBacklog={portStateView.pendingBacklog} queueCounts={portStateView.queueCounts} error={panels.errors.portState} />{source.kind === 'live' && <NextArrivalPanel shipments={panels.shipments} error={panels.errors.shipments} nowMs={frameNow} />}</details>
        <details className="diagnostic-section" open={Boolean(panels.sensorAlerts.length || panels.maintenanceAlerts.length)}><summary>Alerts & maintenance <span>{panels.sensorAlerts.length + panels.maintenanceAlerts.length}</span></summary><SensorAlertsPanel alerts={panels.sensorAlerts} error={panels.errors.sensorAlerts} /><MaintenanceAlertsPanel alerts={panels.maintenanceAlerts} error={panels.errors.maintenanceAlerts} /></details>
        <div className="engineering-note"><span className="eyebrow">Under the surface</span><p>Devices coordinate over MQTT. The local system combines live telemetry, route planning and anomaly detection.</p><p>{source.kind === 'replay' ? 'This view replays captured outputs from that pipeline. It does not run a hosted prediction model.' : 'Inspect a device or shipment to see its assignment and operational context.'}</p></div>
      </aside>
      <ProjectResources />
    </main>
    <footer className="dashboard-footer"><span>HarbourSense · Smart port operations</span><span>{source.kind === 'replay' ? 'Synthetic scenario data · independent browser playback' : 'Telemetry freshness: 15s · panel freshness: 30s'}</span></footer>
    {selectedDevice && <DeviceDetailModal device={selectedDevice} history={state.telemetryHistory} sensors={panels.sensors} craneTelemetry={panels.craneTelemetry} maintenanceAlerts={panels.maintenanceHistory.length ? panels.maintenanceHistory : panels.maintenanceAlerts} onClose={() => selectDevice(null)} />}
    {ui.sensorNodeId && <SensorDetailModal sensors={nodeSensors} nodeId={ui.sensorNodeId} onClose={() => selectSensorNode(null)} />}
    {selectedShipment && <ShipmentDetailModal shipment={selectedShipment} diagnostic={portStateView.shipmentDiagnosticsById[selectedShipment.id]} onClose={() => selectShipment(null)} />}
  </div>;
}
export default App;
