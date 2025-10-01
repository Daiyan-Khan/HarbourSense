const { MongoClient } = require('mongodb');
const { initialEdges } = require('./initial-edge.js');
const sensorList = require('./sensor.json');
const { graphData } = require('./graph.js');

const uri = 'mongodb+srv://kdaiyan1029_db_user:Lj1dBUioaDGT2K6S@sit314.kzzkjxh.mongodb.net/port';

const sensorsByNode = sensorList.reduce((acc, sensor) => {
  acc[sensor.node] = acc[sensor.node] || [];
  acc[sensor.node].push(sensor.id);
  return acc;
}, {});

const stationaryEdgesByNode = initialEdges
  .filter(edge => !edge.location.includes('-'))
  .reduce((acc, edge) => {
    acc[edge.location] = acc[edge.location] || [];
    acc[edge.location].push(edge.id);
    return acc;
  }, {});

const mapNodes = graphData.map(node => ({
  id: node.id,
  type: node.type,
  neighbors: node.neighbors,
  sensors: sensorsByNode[node.id] || [],
  edges: stationaryEdgesByNode[node.id] || []
}));

const movingEdges = initialEdges.filter(edge => edge.location.includes('-'));

const initialMap = {
  nodes: mapNodes,
  movingEdges: movingEdges
};

async function postMapToTraffic() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db('port');
    const trafficCol = db.collection('traffic');

    await trafficCol.deleteMany({});
    await trafficCol.insertOne({ updatedAt: new Date(), mapData: initialMap });

    console.log('Posted initial map data to traffic collection');
  } catch (error) {
    console.error('Failed to post map data:', error);
  } finally {
    await client.close();
  }
}

postMapToTraffic().catch(console.error);

module.exports = { initialMap, postMapToTraffic };
