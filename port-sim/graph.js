// graph.js - Loads and exports the port graph from JSON
const graphData = require('./graph.json');  // Auto-parses to JS array/object

// Optional: Add helpers for easier use
const getNode = (id) => graphData.find(node => node.id === id);
const getAllNodeIds = () => graphData.map(node => node.id);

// For testing: Log a sample
console.log('Sample Node:', getNode('A1'));

module.exports = { graphData, getNode, getAllNodeIds };
