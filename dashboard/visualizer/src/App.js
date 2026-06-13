import React, { useEffect, useState, useMemo } from 'react';
import ReactFlow, { Background, Controls, BaseEdge } from 'reactflow';
import 'reactflow/dist/style.css';
import axios from 'axios';
import { getApiBaseUrl } from './apiConfig';
import { StatusSidebar } from './StatusPanels';

// Node & Device sizes
const NODE_WIDTH = 150;
const NODE_HEIGHT = 100;
const DEVICE_SIZE = 30;

// Custom straight edge
const SideEdge = ({ sourceX, sourceY, targetX, targetY, markerEnd }) => {
  const edgePath = `M${sourceX},${sourceY} L${targetX},${targetY}`;
  return <BaseEdge path={edgePath} markerEnd={markerEnd} style={{ stroke: '#222', strokeWidth: 2 }} />;
};

// Compute device position with progress-based interpolation
const computeDevicePosition = (device, nodePositions) => {
  if (!device || !device.currentLocation) return { x: 0, y: 0 };

  const startNode = device.currentLocation;
  const endNode = device.nextNode;
  const progress = device.progressToNext || 0; // 0-100, assume numeric

  // If not en-route (progress 0 or no nextNode), position at current location
  if (progress <= 0 || !endNode || typeof endNode !== 'string') {
    const nodePos = nodePositions[startNode];
    if (!nodePos) return { x: 0, y: 0 };
    return {
      x: nodePos.x + NODE_WIDTH / 2 - DEVICE_SIZE / 2,
      y: nodePos.y + NODE_HEIGHT / 2 - DEVICE_SIZE / 2,
    };
  }

  // En-route: Interpolate between start and end based on progress (0% = start, 100% = end)
  const startPos = nodePositions[startNode];
  const endPos = nodePositions[endNode];
  if (!startPos || !endPos) return { x: 0, y: 0 };

  const progressRatio = progress / 100; // 0 to 1
  const interpolatedX = startPos.x + (endPos.x - startPos.x) * progressRatio;
  const interpolatedY = startPos.y + (endPos.y - startPos.y) * progressRatio;

  return {
    x: interpolatedX + NODE_WIDTH / 2 - DEVICE_SIZE / 2,
    y: interpolatedY + NODE_HEIGHT / 2 - DEVICE_SIZE / 2,
  };
};

// Map devices to React Flow nodes
const mapDevicesToNodes = (devices, nodePositions) =>
  devices.map((d) => {
    const pos = computeDevicePosition(d, nodePositions);

    let label = '⚙️';
    let background = '#ccc';
    let borderRadius = '50%';

    const deviceType = (d.type || '').toLowerCase();

    if (deviceType.includes('truck')) {
      label = '🚚';
      background = 'saddlebrown';
      borderRadius = '5px';
    } else if (deviceType.includes('conveyor')) {
      label = '⬭';
      background = 'grey';
      borderRadius = '50% / 25%';
    } else if (deviceType.includes('robot')) {
      label = '🤖';
      background = 'lightgreen';
      borderRadius = '50%';
    } else if (deviceType.includes('crane')) {
      label = '🅰️';  // Crane icon approximation
      background = 'darkblue';
      borderRadius = '50%';
    }

    // Devices without type get a warning icon
    if (!d.type) label = '⚠️';

    return {
      id: `dev-${d.id}`,
      position: pos,
      data: { 
        label,
        // Optional: Add progress label for visual feedback (e.g., tooltip or badge)
        progress: d.progressToNext ? `${Math.round(d.progressToNext)}%` : null 
      },
      style: {
        width: DEVICE_SIZE,
        height: DEVICE_SIZE,
        background,
        borderRadius,
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        fontSize: 18,
        cursor: 'pointer',
        border: '1px solid #333',
        // Pulse animation for moving devices (optional visual cue)
        ...(d.progressToNext > 0 && d.progressToNext < 100 ? { 
         boxShadow: '0 0 10px rgba(0, 123, 255, 0.5)' 
        } : {}),
      },
      draggable: false,
    };
  });

function App() {
  const [graphNodes, setGraphNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [liveEdges, setLiveEdges] = useState([]);
  const [sensors, setSensors] = useState([]);
  const [positions, setPositions] = useState({});
  const [showSensorMenu, setShowSensorMenu] = useState(false);
  const [selectedNodeSensors, setSelectedNodeSensors] = useState([]);
  const [selectedSensor, setSelectedSensor] = useState(null);
  const [selectedDevice, setSelectedDevice] = useState(null);
  const [graphStatus, setGraphStatus] = useState('loading');
  const [graphError, setGraphError] = useState(null);
  const [liveDataError, setLiveDataError] = useState(null);
  const [shipments, setShipments] = useState([]);
  const [sensorAlerts, setSensorAlerts] = useState([]);
  const [maintenanceAlerts, setMaintenanceAlerts] = useState([]);
  const [panelErrors, setPanelErrors] = useState({
    shipments: null,
    sensorAlerts: null,
    maintenanceAlerts: null,
  });

  const BASE_URL = getApiBaseUrl();

  const edgeTypes = useMemo(
    () => ({
      side: ({ id, data, markerEnd }) => {
        const { sourceX, sourceY, targetX, targetY } = data;
        return <SideEdge id={id} sourceX={sourceX} sourceY={sourceY} targetX={targetX} targetY={targetY} markerEnd={markerEnd} />;
      },
    }),
    []
  );

  // Grid layout: "A1" -> x,y
  const gridLayout = (rawNodes) => {
    const positions = {};
    rawNodes.forEach((node) => {
      const prefix = node.id.match(/[A-Z]+/)[0];
      const suffix = parseInt(node.id.match(/[0-9]+/)[0], 10);

      let row = 0;
      for (let i = 0; i < prefix.length; i++) {
        row = row * 26 + (prefix.charCodeAt(i) - 'A'.charCodeAt(0) + 1);
      }
      row -= 1;
      const col = suffix - 1;

      positions[node.id] = {
        x: col * (NODE_WIDTH + 100),
        y: row * (NODE_HEIGHT + 100),
      };
    });
    return positions;
  };

  // Fetch graph nodes
  useEffect(() => {
    const fetchGraph = async () => {
      setGraphStatus('loading');
      setGraphError(null);
      try {
        const res = await axios.get(`${BASE_URL}/api/graph`);
        const rawNodes = Object.values(res.data.nodes || {});
        if (rawNodes.length === 0) {
          setGraphNodes([]);
          setEdges([]);
          setPositions({});
          setGraphStatus('empty');
          return;
        }
        const pos = gridLayout(rawNodes);
        setPositions(pos);

        const gNodes = rawNodes.map((n) => ({
          id: n.id,
          data: { label: `${n.id} (${n.type || 'unknown'})` },
          position: pos[n.id],
          style: {
            width: NODE_WIDTH,
            height: NODE_HEIGHT,
            border: '2px solid #444',
            borderRadius: 6,
            background: '#f8f8f8',
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
          },
        }));

        const graphEdges = [];
        rawNodes.forEach((node) => {
          Object.values(node.neighbors || {}).forEach((nbrId) => {
            const srcPos = pos[node.id];
            const tgtPos = pos[nbrId];
            if (!srcPos || !tgtPos) return;

            let sourceX, sourceY, targetX, targetY;
            if (srcPos.y === tgtPos.y) {
              sourceX = srcPos.x + NODE_WIDTH;
              sourceY = srcPos.y + NODE_HEIGHT / 2;
              targetX = tgtPos.x;
              targetY = tgtPos.y + NODE_HEIGHT / 2;
            } else if (srcPos.x === tgtPos.x) {
              sourceX = srcPos.x + NODE_WIDTH / 2;
              sourceY = srcPos.y + NODE_HEIGHT;
              targetX = tgtPos.x + NODE_WIDTH / 2;
              targetY = tgtPos.y;
            } else {
              sourceX = srcPos.x + NODE_WIDTH / 2;
              sourceY = srcPos.y + NODE_HEIGHT / 2;
              targetX = tgtPos.x + NODE_WIDTH / 2;
              targetY = tgtPos.y + NODE_HEIGHT / 2;
            }

            graphEdges.push({
              id: `${node.id}-${nbrId}`,
              source: node.id,
              target: nbrId,
              type: 'side',
              data: { sourceX, sourceY, targetX, targetY },
            });
          });
        });

        setGraphNodes(gNodes);
        setEdges(graphEdges);
        setGraphStatus('ready');
      } catch (err) {
        console.error('Error fetching graph:', err);
        setGraphStatus('error');
        setGraphError(err?.response?.data?.detail || err?.message || 'Unable to load port graph');
      }
    };
    fetchGraph();
  }, [BASE_URL]);

  // Fetch live devices, sensors, shipments, and alerts
  useEffect(() => {
    const fetchLiveData = async () => {
      const nextPanelErrors = {
        shipments: null,
        sensorAlerts: null,
        maintenanceAlerts: null,
      };

      try {
        const edgesRes = await axios.get(`${BASE_URL}/api/edges`);
        setLiveEdges(edgesRes.data);

        const sensorsRes = await axios.get(`${BASE_URL}/api/sensors`);
        const uniqueSensors = Array.from(new Map(sensorsRes.data.map((s) => [s.id, s])).values());
        setSensors(uniqueSensors);
        setLiveDataError(null);
      } catch (error) {
        console.error('Error fetching live data:', error);
        setLiveDataError(error?.response?.data?.detail || error?.message || 'Unable to refresh live device data');
      }

      try {
        const shipmentsRes = await axios.get(`${BASE_URL}/api/shipments`);
        setShipments(Array.isArray(shipmentsRes.data) ? shipmentsRes.data : []);
      } catch (error) {
        nextPanelErrors.shipments = error?.response?.data?.detail?.message
          || error?.message
          || 'Unable to load shipments';
      }

      try {
        const sensorAlertsRes = await axios.get(`${BASE_URL}/api/alerts/sensor`);
        setSensorAlerts(Array.isArray(sensorAlertsRes.data) ? sensorAlertsRes.data : []);
      } catch (error) {
        nextPanelErrors.sensorAlerts = error?.response?.data?.detail?.message
          || error?.message
          || 'Unable to load sensor alerts';
      }

      try {
        const maintenanceAlertsRes = await axios.get(`${BASE_URL}/api/alerts/maintenance`);
        setMaintenanceAlerts(Array.isArray(maintenanceAlertsRes.data) ? maintenanceAlertsRes.data : []);
      } catch (error) {
        nextPanelErrors.maintenanceAlerts = error?.response?.data?.detail?.message
          || error?.message
          || 'Unable to load maintenance alerts';
      }

      setPanelErrors(nextPanelErrors);
    };
    if (graphStatus === 'ready') {
      fetchLiveData();
      const interval = setInterval(fetchLiveData, 3000);
      return () => clearInterval(interval);
    }
    return undefined;
  }, [BASE_URL, graphStatus]);

  // Log updates whenever liveEdges change (optional: show every 20% progress in console)
  useEffect(() => {
    if (liveEdges.length > 0) {
      console.log('Edges updated:', {
        count: liveEdges.length,
        sample: liveEdges.slice(0, 3), // Log first 3 for brevity
      });
      // Log progress milestones (every 20%) for debugging/monitoring
      liveEdges.forEach((edge) => {
        if (edge.progressToNext && typeof edge.progressToNext === 'number') {
          const progress = Math.round(edge.progressToNext);
          if (progress > 0 && progress % 20 === 0) {
            console.log(`[PROGRESS ${progress}%] ${edge.id} traveling to ${edge.nextNode}`);
          }
        }
      });
    }
  }, [liveEdges]);

  const onNodeClick = (event, node) => {
    if (node.id.startsWith('dev-')) {
      const devId = node.id.substring(4);
      const dev = liveEdges.find((d) => d.id === devId);
      if (dev) setSelectedDevice(dev);
    } else {
      const nodeSensors = sensors.filter((s) => s.node === node.id);
      setSelectedNodeSensors(nodeSensors);
      setShowSensorMenu(true);
      setSelectedSensor(null);
    }
  };

  return (
    <div style={{ height: '100vh' }}>
      <h2>HarbourSense Smart Port</h2>
      {graphStatus === 'loading' && (
        <p role="status">Loading port graph…</p>
      )}
      {graphStatus === 'error' && (
        <p role="alert">Graph unavailable: {graphError}</p>
      )}
      {graphStatus === 'empty' && (
        <p role="status">Port graph is empty. Seed MongoDB graph data before using the dashboard.</p>
      )}
      {liveDataError && graphStatus === 'ready' && (
        <p role="alert">Live data warning: {liveDataError}</p>
      )}
      {graphStatus === 'ready' && (
        <StatusSidebar
          shipments={shipments}
          sensorAlerts={sensorAlerts}
          maintenanceAlerts={maintenanceAlerts}
          panelErrors={panelErrors}
        />
      )}
      <ReactFlow
        nodes={[...graphNodes, ...mapDevicesToNodes(liveEdges, positions)]}
        edges={edges}
        edgeTypes={edgeTypes}
        onNodeClick={onNodeClick}
        fitView
      >
        <Background />
        <Controls />
      </ReactFlow>

      {/* Sensor popup */}
      {showSensorMenu && (
        <div style={{
          position: 'fixed', top: '20%', left: '30%', background: 'white',
          padding: 20, border: '1px solid #aaa', borderRadius: 8,
          boxShadow: '0 8px 32px rgba(0,0,0,0.08)', minWidth: 400,
          maxHeight: '60vh', overflowY: 'auto', zIndex: 1000
        }}>
          <h3>Sensors at Node</h3>
          <ul style={{ listStyle: 'none', padding: 0, marginBottom: 10 }}>
            {selectedNodeSensors.map((sensor) => (
              <li key={sensor.id} style={{ marginBottom: 8 }}>
                <button
                  style={{
                    padding: '6px 12px',
                    borderRadius: 5,
                    border: '1px solid #ddd',
                    background: selectedSensor && selectedSensor.id === sensor.id ? '#007bff' : '#f0f0f3',
                    color: selectedSensor && selectedSensor.id === sensor.id ? 'white' : 'black',
                    cursor: 'pointer',
                    width: '100%',
                    display: 'block',
                  }}
                  onClick={() => setSelectedSensor(sensor)}
                >
                  {sensor.type || 'unknown'} ({sensor.id})
                </button>
              </li>
            ))}
          </ul>
          <button onClick={() => setShowSensorMenu(false)} style={{ marginTop: 5 }}>Close</button>

          {selectedSensor && (
            <div style={{ marginTop: 20, borderTop: '1px solid #ddd', paddingTop: 12 }}>
              <h4>Sensor Data</h4>
              {Object.entries(selectedSensor).map(([k, v]) => (
                <p key={k}><b>{k}:</b> {String(v)}</p>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Device popup */}
      {selectedDevice && (
        <div style={{
          position: 'fixed', top: '20%', left: '35%', background: 'white',
          padding: 20, border: '1px solid #aaa', borderRadius: 8,
          boxShadow: '0 8px 32px rgba(0,0,0,0.12)', minWidth: 400, zIndex: 2000
        }}>
          <h3>Device Details</h3>
          <p><b>Name:</b> {selectedDevice.id}</p>
          <p><b>Description:</b> {selectedDevice.desc || 'N/A'}</p>
          {/* FIXED: Handle task as object - show phase/task summary or stringify */}
          <p><b>Task:</b> {selectedDevice.task 
            ? (typeof selectedDevice.task === 'object' 
                ? `${selectedDevice.task.phase || 'unknown'} - ${selectedDevice.task.task || 'N/A'} (Shipment: ${selectedDevice.task.shipmentId || 'N/A'})` 
                : selectedDevice.task) 
            : 'N/A'}</p>
          <p><b>Priority:</b> {selectedDevice.prio || selectedDevice.priority || 'N/A'}</p>
          <p><b>Current Location:</b> {selectedDevice.currentLocation || 'N/A'}</p>
          <p><b>Next Node:</b> {selectedDevice.nextNode || 'N/A'}</p>
          <p><b>Final Node:</b> {selectedDevice.finalNode || 'N/A'}</p>
          <p><b>Progress to Next:</b> {selectedDevice.progressToNext ? `${Math.round(selectedDevice.progressToNext)}%` : 'N/A'}</p>
          <p><b>ETA:</b> {selectedDevice.eta ? `${selectedDevice.eta}s` : 'N/A'}</p>
          <p><b>Task Completion Time:</b> {selectedDevice.taskCompletionTime ? `${selectedDevice.taskCompletionTime}s` : 'Not Started'}</p>
          <p><b>Total Journey Time:</b> {selectedDevice.journeyTime ? `${selectedDevice.journeyTime}s` : 'N/A'}</p>
          <button onClick={() => setSelectedDevice(null)} style={{ marginTop: 10 }}>Close</button>
        </div>
      )}
    </div>
  );
}

export default App;
