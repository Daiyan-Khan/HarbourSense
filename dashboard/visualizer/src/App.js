import React, { useEffect, useState, useMemo } from 'react';
import ReactFlow, { Background, Controls, BaseEdge } from 'reactflow';
import 'reactflow/dist/style.css';
import axios from 'axios';

// === Node dimensions ===
const NODE_WIDTH = 150;
const NODE_HEIGHT = 100;

// Device size
const DEVICE_SIZE = 30;

// Custom edge (straight line)
const SideEdge = ({ sourceX, sourceY, targetX, targetY, markerEnd }) => {
  const edgePath = `M${sourceX},${sourceY} L${targetX},${targetY}`;
  return <BaseEdge path={edgePath} markerEnd={markerEnd} style={{ stroke: '#222', strokeWidth: 2 }} />;
};

// Compute device position based on node location
const computeDevicePosition = (device, nodePositions) => {
  const nodeId = device.location; // your API gives "D3", "F10", etc.
  const currPos = nodePositions[nodeId];
  if (!currPos) return { x: 0, y: 0 };

  return {
    x: currPos.x + NODE_WIDTH / 2 - DEVICE_SIZE / 2,
    y: currPos.y + NODE_HEIGHT / 2 - DEVICE_SIZE / 2,
  };
};

// Map devices to React Flow nodes
const mapDevicesToNodes = (devices, nodePositions) =>
  devices.map((d) => {
    const pos = computeDevicePosition(d, nodePositions);

    let label = '⚙️';
    let background = '#ccc';
    let borderRadius = '50%';

    if (d.type.toLowerCase().includes('truck')) {
      label = '🚚';
      background = 'saddlebrown';
      borderRadius = '5px';
    } else if (d.type.toLowerCase().includes('conveyor')) {
      label = '⬭';
      background = 'grey';
      borderRadius = '50% / 25%';
    } else if (d.type.toLowerCase().includes('robot')) {
      label = '🤖';
      background = 'lightgreen';
      borderRadius = '50%';
    }

    return {
      id: `dev-${d.id}`,
      position: pos,
      data: { label },
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

  const BASE_URL = 'http://localhost:8000';

  // Edge types
  const edgeTypes = useMemo(
    () => ({
      side: ({ id, data, markerEnd }) => {
        const { sourceX, sourceY, targetX, targetY } = data;
        return <SideEdge id={id} sourceX={sourceX} sourceY={sourceY} targetX={targetX} targetY={targetY} markerEnd={markerEnd} />;
      },
    }),
    []
  );

  // Grid layout for nodes: e.g., A1=0,0 ; B1=0,100
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

  // Fetch graph nodes and edges
  useEffect(() => {
    const fetchGraph = async () => {
      try {
        const res = await axios.get(`${BASE_URL}/api/graph`);
        const rawNodes = Object.values(res.data.nodes || {});
        const pos = gridLayout(rawNodes);
        setPositions(pos);

        const gNodes = rawNodes.map((n) => ({
          id: n.id,
          data: { label: `${n.id} (${n.type})` },
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
      } catch (err) {
        console.error(err);
      }
    };
    fetchGraph();
  }, []);

  // Fetch live devices & sensors
  useEffect(() => {
    const fetchLiveData = async () => {
      try {
        const edgesRes = await axios.get(`${BASE_URL}/api/edges`);
        setLiveEdges(edgesRes.data);

        const sensorsRes = await axios.get(`${BASE_URL}/api/sensors`);
        const uniqueSensors = Array.from(new Map(sensorsRes.data.map((s) => [s.id, s])).values());
        setSensors(uniqueSensors);
      } catch (error) {
        console.error('Error fetching live data:', error);
      }
    };
    fetchLiveData();
    const interval = setInterval(fetchLiveData, 3000);
    return () => clearInterval(interval);
  }, []);

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
        <div
          style={{
            position: 'fixed',
            top: '20%',
            left: '30%',
            background: 'white',
            padding: 20,
            border: '1px solid #aaa',
            borderRadius: 8,
            boxShadow: '0 8px 32px rgba(0,0,0,0.08)',
            minWidth: 400,
            maxHeight: '60vh',
            overflowY: 'auto',
            zIndex: 1000,
          }}
        >
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
                  {sensor.type} ({sensor.id})
                </button>
              </li>
            ))}
          </ul>

          <button
            onClick={() => setShowSensorMenu(false)}
            style={{ background: '#eee', borderRadius: 5, border: '1px solid #ccc', padding: '5px 14px', cursor: 'pointer' }}
          >
            Close
          </button>

          {selectedSensor && (
            <div style={{ marginTop: 20, borderTop: '1px solid #ddd', paddingTop: 12 }}>
              <h4>Sensor Data</h4>
              {Object.entries(selectedSensor).map(([k, v]) => (
                <p key={k}>
                  <b>{k}:</b> {String(v)}
                </p>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Device popup */}
      {selectedDevice && (
        <div
          style={{
            position: 'fixed',
            top: '20%',
            left: '35%',
            background: 'white',
            padding: 20,
            border: '1px solid #aaa',
            borderRadius: 8,
            boxShadow: '0 8px 32px rgba(0,0,0,0.12)',
            minWidth: 400,
            zIndex: 2000,
          }}
        >
          <h3>Device Details</h3>
          <p>
            <b>Name:</b> {selectedDevice.id}
          </p>
          <p>
            <b>Description:</b> {selectedDevice.description || 'N/A'}
          </p>
          <p>
            <b>Task:</b> {selectedDevice.task || 'N/A'}
          </p>
          <p>
            <b>Priority:</b> {selectedDevice.prio || selectedDevice.priority || 'N/A'}
          </p>
          <p>
            <b>Current Location:</b> {selectedDevice.location}
          </p>
          <button
            onClick={() => setSelectedDevice(null)}
            style={{ marginTop: 10, background: '#eee', borderRadius: 5, border: '1px solid #ccc', padding: '5px 14px', cursor: 'pointer' }}
          >
            Close
          </button>
        </div>
      )}
    </div>
  );
}

export default App;
