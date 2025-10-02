import React, { useEffect, useState } from 'react';
import ReactFlow, { Background, Controls, BaseEdge } from 'reactflow';
import 'reactflow/dist/style.css';
import axios from 'axios';

// === Node dimensions ===
const NODE_WIDTH = 150;
const NODE_HEIGHT = 100;
const H_SPACING = 100; // horizontal gap
const V_SPACING = 100; // vertical gap

// Custom edge that only draws straight horiz/vert lines
const SideEdge = ({ sourceX, sourceY, targetX, targetY, markerEnd }) => {
  const [edgePath] = [`M${sourceX},${sourceY} L${targetX},${targetY}`];
  return (
    <BaseEdge
      path={edgePath}
      markerEnd={markerEnd}
      style={{ stroke: '#222', strokeWidth: 2 }}
    />
  );
};

function App() {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [liveEdges, setLiveEdges] = useState([]);
  const [sensors, setSensors] = useState([]);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [showSensorMenu, setShowSensorMenu] = useState(false);
  const [selectedNodeSensors, setSelectedNodeSensors] = useState([]);
  const [selectedSensor, setSelectedSensor] = useState(null);

  const BASE_URL = 'http://localhost:8000';

  // Grid layout function
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
        x: col * (NODE_WIDTH + H_SPACING),
        y: row * (NODE_HEIGHT + V_SPACING),
      };
    });
    return positions;
  };

  useEffect(() => {
    const fetchGraph = async () => {
      try {
        const res = await axios.get(`${BASE_URL}/api/graph`);
        const rawNodes = Object.values(res.data.nodes || {});
        const positions = gridLayout(rawNodes);

        // === Build nodes ===
        const graphNodes = rawNodes.map((n) => ({
          id: n.id,
          data: { label: `${n.id} (${n.type})` },
          position: positions[n.id],
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

        // === Build edges anchored on correct sides ===
        const graphEdges = [];
        rawNodes.forEach((node) => {
          Object.values(node.neighbors || {}).forEach((nbrId) => {
            const srcPos = positions[node.id];
            const tgtPos = positions[nbrId];

            let sourceX, sourceY, targetX, targetY;

            if (srcPos.y === tgtPos.y) {
              // Horizontal
              sourceX = srcPos.x + NODE_WIDTH;
              sourceY = srcPos.y + NODE_HEIGHT / 2;
              targetX = tgtPos.x;
              targetY = tgtPos.y + NODE_HEIGHT / 2;
            } else if (srcPos.x === tgtPos.x) {
              // Vertical
              sourceX = srcPos.x + NODE_WIDTH / 2;
              sourceY = srcPos.y + NODE_HEIGHT;
              targetX = tgtPos.x + NODE_WIDTH / 2;
              targetY = tgtPos.y;
            } else {
              // Fallback: center-to-center
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

        setNodes(graphNodes);
        setEdges(graphEdges);
      } catch (err) {
        console.error(err);
      }
    };
    fetchGraph();
  }, []);

  // Fetch live edges & sensors
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

  // Override edge renderer to use our custom coords
  const edgeTypes = {
    side: ({ id, data, markerEnd }) => {
      const { sourceX, sourceY, targetX, targetY } = data;
      return (
        <SideEdge
          id={id}
          sourceX={sourceX}
          sourceY={sourceY}
          targetX={targetX}
          targetY={targetY}
          markerEnd={markerEnd}
        />
      );
    },
  };

  const onNodeClick = (event, node) => {
    const nodeSensors = sensors.filter((s) => s.node === node.id);
    setSelectedNodeSensors(nodeSensors);
    setShowSensorMenu(true);
    setSelectedSensor(null);
  };

  const onEdgeClick = (event, edge) => {
    const clickedEdge = liveEdges.find(
      (e) => e.currentLocation === edge.source && e.nextNode === edge.target
    );
    if (clickedEdge) setSelectedEdge(clickedEdge);
  };

  return (
    <div style={{ height: '100vh' }}>
      <h2>HarbourSense Clean Grid Graph</h2>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        edgeTypes={edgeTypes}
        onNodeClick={onNodeClick}
        onEdgeClick={onEdgeClick}
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
                    background:
                      selectedSensor && selectedSensor.id === sensor.id
                        ? '#007bff'
                        : '#f0f0f3',
                    color:
                      selectedSensor && selectedSensor.id === sensor.id
                        ? 'white'
                        : 'black',
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
            style={{
              background: '#eee',
              borderRadius: 5,
              border: '1px solid #ccc',
              padding: '5px 14px',
              cursor: 'pointer',
            }}
          >
            Close
          </button>

          {selectedSensor && (
            <div style={{ marginTop: 20, borderTop: '1px solid #ddd', paddingTop: 12 }}>
              <h4>Sensor Data</h4>
              <p><b>ID:</b> {selectedSensor.id}</p>
              <p><b>Type:</b> {selectedSensor.type}</p>
              <p><b>Reading:</b> {selectedSensor.reading}</p>
              <p><b>Timestamp:</b> {new Date(selectedSensor.timestamp).toLocaleString()}</p>
            </div>
          )}
        </div>
      )}

      {/* Edge sidebar */}
      {selectedEdge && (
        <div
          style={{
            position: 'fixed',
            top: 20,
            right: 20,
            padding: 20,
            background: 'white',
            border: '1px solid gray',
          }}
        >
          <h3>Edge Details</h3>
          <p><b>ID:</b> {selectedEdge.id}</p>
          <p><b>Task:</b> {selectedEdge.task}</p>
          <p><b>Speed:</b> {selectedEdge.speed}</p>
          <p><b>Next Node:</b> {selectedEdge.nextNode}</p>
          <button onClick={() => setSelectedEdge(null)}>Close</button>
        </div>
      )}
    </div>
  );
}

export default App;
