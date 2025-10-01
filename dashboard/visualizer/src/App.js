import React, { useEffect, useState, useMemo } from 'react';
import ReactFlow, { Background, Controls } from 'reactflow';
import 'reactflow/dist/style.css';
import axios from 'axios';

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

  const nodeTypes = useMemo(() => ({}), []);
  const edgeTypes = useMemo(() => ({}), []);

  // Grid layout with bigger horizontal spacing
  function gridLayout(nodeList) {
    const positions = {};
    nodeList.forEach(node => {
      const nodeId = node.id;
      let prefix = nodeId.match(/[a-zA-Z]+/)[0];
      let suffix = nodeId.match(/[0-9]+/)[0];

      let row = 0;
      for (let i = 0; i < prefix.length; i++) {
        row = row * 26 + (prefix.charCodeAt(i) - 'A'.charCodeAt(0) + 1);
      }
      row -= 1;

      let col = parseInt(suffix, 10) - 1;
      // Increase spacing: 200px horizontal, 150px vertical
      positions[nodeId] = { x: col * 200, y: row * 150 };
    });
    return positions;
  }

  useEffect(() => {
    const fetchGraph = async () => {
      try {
        const res = await axios.get(`${BASE_URL}/api/graph`);
        const graphNodesRaw = Object.values(res.data.nodes || {});
        const positions = gridLayout(graphNodesRaw);

        const graphNodes = graphNodesRaw.map(n => ({
          id: n.id,
          data: { label: `${n.id} (${n.type})` },
          position: positions[n.id],
        }));

        const graphEdges = [];
        graphNodesRaw.forEach(node => {
          Object.values(node.neighbors || {}).forEach(neighborId => {
            // compute simple Euclidean distance
            const pos1 = positions[node.id];
            const pos2 = positions[neighborId];
            let dx = pos1.x - pos2.x;
            let dy = pos1.y - pos2.y;
            const dist = Math.sqrt(dx * dx + dy * dy);

            graphEdges.push({
              id: `${node.id}-${neighborId}`,
              source: node.id,
              target: neighborId,
              animated: true,
              label: `${dist.toFixed(0)}m`
            });
          });
        });

        setNodes(graphNodes);
        setEdges(graphEdges);
      } catch (err) {
        console.error('Error fetching graph:', err);
      }
    };
    fetchGraph();
  }, []);

  useEffect(() => {
    const fetchLiveData = async () => {
      try {
        const edgesRes = await axios.get(`${BASE_URL}/api/edges`);
        setLiveEdges(edgesRes.data);

        const sensorsRes = await axios.get(`${BASE_URL}/api/sensors`);
        // Deduplicate by id
        const uniqueSensors = Array.from(
          new Map(sensorsRes.data.map(s => [s.id, s])).values()
        );
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
    const nodeSensors = sensors.filter(s => s.node === node.id);
    setSelectedNodeSensors(nodeSensors);
    setShowSensorMenu(true);
    setSelectedSensor(null);
  };

  const onEdgeClick = (event, edge) => {
    const clickedEdge = liveEdges.find(
      e => e.currentLocation === edge.source && e.nextNode === edge.target
    );
    if (clickedEdge) setSelectedEdge(clickedEdge);
  };

  return (
    <div style={{ height: '600px' }}>
      <h2>HarbourSense Live Dashboard</h2>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodeClick={onNodeClick}
        onEdgeClick={onEdgeClick}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
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
            {selectedNodeSensors.map(sensor => (
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
            <div
              style={{
                marginTop: 20,
                borderTop: '1px solid #ddd',
                paddingTop: 12,
              }}
            >
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
