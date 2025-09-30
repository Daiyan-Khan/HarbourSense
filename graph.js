const Graph = require('node-dijkstra');

// Function to generate port graph with 100 alphanumeric nodes (A1 to J10)
function generatePortGraph() {
  const graph = new Graph();
  const letters = 'ABCDEFGHIJ'.split('');  // 10 letters
  const nodes = [];

  // Create 100 nodes: A1-A10, B1-B10, ..., J1-J10
  letters.forEach(letter => {
    for (let num = 1; num <= 10; num++) {
      const nodeId = `${letter}${num}`;
      nodes.push(nodeId);
      graph.addNode(nodeId);
    }
  });

  // Add random edges with distances (1-10) for realistic connections
  nodes.forEach((node, index) => {
    // Connect to 3-5 nearby nodes (simulate port layout)
    for (let j = 1; j <= Math.floor(Math.random() * 3) + 3; j++) {
      const targetIndex = (index + j) % nodes.length;  // Wrap around for connectivity
      const target = nodes[targetIndex];
      const distance = Math.floor(Math.random() * 10) + 1;  // Random distance
      graph.addEdge(node, target, distance);
    }
  });

  return graph;
}

// Example usage: Generate graph and find path
const portGraph = generatePortGraph();
const path = portGraph.path('A1', 'J10', { cost: true });
console.log('Optimal Path from A1 to J10:', path.path, 'Cost:', path.cost);

// Export for integration with sensors/ML
module.exports = { generatePortGraph };
